{-# LANGUAGE OverloadedStrings, OverloadedRecordDot, DeriveAnyClass #-}
module Control.Events
  (
  -- * Establishing a connection
    withConn, Conn

  -- * Running tasks delimited by events
  , event
  , EventId, EvtMsg(..), simple
  , EvtDone(..), done, failed

  -- ** Topics
  , script, server, healthcheck
  , mkTopic
  ) where

import Data.Maybe
import Data.IORef
import Data.Time.Clock
import GHC.Generics
import Control.Exception
import Data.Aeson as JSON
import Network.URI (parseURI)
import Network.MQTT.Client
import Network.MQTT.Topic
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy as LBS

-- todo: waitForClient wrapper, for subscribers
--------------------------------------------------------------------------------

script, server, healthcheck :: Topic
script = fromJust (mkTopic "script")
server = fromJust (mkTopic "server")
healthcheck = fromJust (mkTopic "healthcheck")

--------------------------------------------------------------------------------

-- | A connection to send events to the broker for a particular service
data Conn = Conn MQTTClient Topic

-- | Open a connection to the broker for this service to send events.
-- The 'Topic' argument is used as the base topic for events sent within this
-- connection, so it should represent the service or type rather than any
-- particular task.
withConn :: Topic -- ^ Service topic, e.g. @'server' <> "kanjideck-fulfillment"@
                  --                    or @'script' <> "finances" <> "mercurybank-hs"@
                  --                    or perhaps even just @'script'@.
         -> (Conn -> IO r)
         -> IO r
withConn serviceTopic = bracket connectBroker disconnectBroker where
  connectBroker = do
    let
      Just uri = parseURI "mqtt://127.0.0.1"
      config = mqttConfig
        {
          _cleanSession = False -- keep msgs the meant for a client which is offline
        , _lwt = Just LastWill
            { _willRetain = False
            , _willQoS = QoS2
            , _willTopic = LBS.fromStrict $ T.encodeUtf8 $ unTopic $
                           serviceTopic <> "last-will-testament"
            , _willMsg = mempty
            , _willProps = []
            }
        , _protocol = Protocol50
        , _connID   = T.unpack (unTopic serviceTopic)
        }
    mc <- connectURI config uri
    pure (Conn mc serviceTopic)

  -- must send DISCONNECT before exiting, otherwise LWT triggers
  disconnectBroker (Conn mc _) = normalDisconnect mc

--------------------------------------------------------------------------------

-- | An identifier to correlate scoped events and start/stop events
data EventId = EventId { correlationId :: LBS.ByteString, evtTopic :: Topic }

data Timed a = Timed { at :: UTCTime, x :: a }
  deriving stock Generic
  deriving anyclass ToJSON

data EvtMsg m = EvtMsg
  { scope    :: Maybe EventId
    -- ^ If this event should be correlated with an 'event' run whose scope
    -- captures this one.
  , expected :: Maybe NominalDiffTime
    -- ^ When is a next message expected, at the latest, after this one, for
    -- the topic this message was sent under?
  , timeout  :: Int
    -- ^ How much time in seconds to wait for a "finished" message before
    -- considering the service dead.
  , label    :: String
    -- ^ A label describing this event
  , content  :: m
    -- ^ Additional content, that may be used e.g. by listeners reacting to
    -- this event
  }
  deriving stock Generic
  deriving anyclass ToJSON

data EvtDone = EvtDone
  { summary   :: String
  , success   :: Bool
  }
  deriving stock Generic
  deriving anyclass ToJSON

simple :: String -> EvtMsg ()
simple x = EvtMsg
  { scope    = Nothing
  , expected = Nothing
  , timeout  = 300 -- seconds
  , label    = x
  , content  = ()
  }

done, failed :: String -> r -> (EvtDone, r)
done   msg r = (EvtDone msg True, r)
failed msg r = (EvtDone msg False, r)

--------------------------------------------------------------------------------

-- | Send a delimited "transactional" event
event :: ToJSON m => Conn -> EvtMsg m -> Topic -> (EventId -> IO (EvtDone, r)) -> IO r
event (Conn mc conn_base) edt topic k
  = bracket startEvent endEvent $ \(ev, ref) ->
      k ev >>= \(edn, r) -> r <$ writeIORef ref (Just edn)
  where
  base_topic = maybe conn_base evtTopic edt.scope
  full_topic = base_topic <> topic

  -- Always do a "transaction": explicit start evt before we do anything,
  -- followed by an end event when done. The start "acquire" is crucial to
  -- also detect cases in which we failed to do the task and couldn't even
  -- send a fail event.
  startEvent = do
    correlationId <- UUID.toLazyASCIIBytes <$> UUID.nextRandom
    ref           <- newIORef Nothing
    now           <- getCurrentTime
    publishq mc (full_topic <> "start") (JSON.encode (Timed now edt)) False{-retain-}
             QoS2 [PropCorrelationData correlationId]
    pure (EventId{correlationId, evtTopic=full_topic}, ref)

  endEvent (EventId{correlationId}, ref) = do
    now <- getCurrentTime
    edn <- fromMaybe exception_done <$> readIORef ref
    publishq mc (full_topic <> "finished") (JSON.encode (Timed now edn)) False{-retain-}
             QoS2 [PropCorrelationData correlationId]
    where
      exception_done = EvtDone
        { summary  = "Exception occurred" -- todo: more info, how?
        , success  = False }

--------------------------------------------------------------------------------

instance ToJSON EventId where toJSON = String . T.decodeASCII . LBS.toStrict . correlationId

