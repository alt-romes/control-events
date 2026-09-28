{-# LANGUAGE OverloadedStrings, OverloadedRecordDot, DeriveAnyClass #-}
module Control.Events
  (
  -- * Establishing a connection
    withConn, Conn

  -- * Running tasks delimited by events
  , event
  , EventId, EvtMeta(..)

  -- ** Topics
  , script, server
  ) where

import Data.Maybe
import Data.Coerce
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
import qualified Data.ByteString.Lazy.Char8 as LBS8

-- todo: waitForClient wrapper, for subscribers
--------------------------------------------------------------------------------

script, server :: Topic
script = fromJust (mkTopic "script")
server = fromJust (mkTopic "server")

--------------------------------------------------------------------------------

-- | A connection to send events to the broker for a particular service
data Conn = Conn MQTTClient Topic

-- | Open a connection to the broker for this service to send events.
-- The 'Topic' argument is used as the base topic for events sent within this
-- connection, so it should represent the service rather than any particular
-- task.
withConn :: Topic -- ^ Service topic, e.g. @'server' <> "kanjideck-fulfillment"@
                    --                    or @'script' <> "finances" <> "mercurybank-hs"@
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
newtype EventId = EventId { correlationId :: LBS.ByteString }

data EvtMeta = EvtMeta
  { start       :: EvtStart
  , done        :: EvtDone
  }

data EvtStart = EvtStart
  { scope   :: Maybe EventId
  , timeout :: Int
  -- , rules   :: [String]
  }
  deriving stock Generic
  deriving anyclass (FromJSON, ToJSON)

data EvtDone = EvtDone
  { summary :: String
  }
  deriving stock Generic
  deriving anyclass (FromJSON, ToJSON)

--------------------------------------------------------------------------------

-- | Send a delimited "transactional" event
event :: Conn -> EvtMeta -> Topic -> (EventId -> IO r) -> IO r
event (Conn mc base) meta topic = bracket startEvent endEvent where

  -- Always do a "transaction": explicit start evt before we do anything,
  -- followed by an end event when done. The start "acquire" is crucial to
  -- also detect cases in which we failed to do the task and couldn't even
  -- send a fail event.
  startEvent = do
    correlationId <- UUID.toLazyASCIIBytes <$> UUID.nextRandom
    publishq mc (base <> topic) (JSON.encode meta.start) False{-retain-}
             QoS2 [PropCorrelationData correlationId]
    pure EventId{correlationId}

  endEvent EventId{correlationId} = do
    publishq mc (base <> topic) (JSON.encode meta.done) False{-retain-}
             QoS2 [PropCorrelationData correlationId]

--------------------------------------------------------------------------------

instance ToJSON EventId   where toJSON    = String . T.decodeASCII . LBS.toStrict . coerce
instance FromJSON EventId where parseJSON = withText "EventId" $ pure . coerce . LBS8.pack . T.unpack

