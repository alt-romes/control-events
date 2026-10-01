{-# LANGUAGE BlockArguments, CPP, OverloadedStrings, OverloadedRecordDot, DeriveAnyClass #-}
{-# OPTIONS_GHC -Wno-orphans #-} -- JSON Topic
module Control.Events
  (
  -- * Establishing a connection
    withConn, Conn
  , healthcheckThread

  -- * Running tasks delimited by events
  , event, event_
  , EventId(..), Timed(..)
  , EvtMsg(..), simple
  , scoped, reacted, withMsg
  , EvtDone(..), done, failed
  , withResult, withTriggers

  -- ** Rules
  , Rules(..)
  , evtTimeout, evtExpected
  , evtSubtasks, evtCritical

  -- ** Topics
  , script, server, healthcheck, trigger
  , mkTopic

  -- * Re-exports
  , module Lens.Micro
  ) where

import Data.Maybe
import Data.Time.Clock
import GHC.Generics
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Aeson as JSON
import Lens.Micro
import Network.URI (parseURI)
import Network.MQTT.Client
import Network.MQTT.Topic
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy as LBS

-- todo: waitForClient wrapper, for subscribers
--------------------------------------------------------------------------------

script, server, healthcheck, trigger :: Topic
script = fromJust (mkTopic "script")
server = fromJust (mkTopic "server")
healthcheck = fromJust (mkTopic "healthcheck")
trigger = fromJust (mkTopic "trigger")

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
withConn = withPersistentConn Nothing

-- | Like 'withConn', but the session is persistent, so messages meant for it
-- are queued even if we are offline, and delivered on reconnect.
withPersistentConn
  :: Maybe String
  -- ^ A persistent connection ID. If the connection goes down, messages meant
  -- for this listener will be queued and delivered when we reconnect using the
  -- same ID. Nothing means the connection is not persistent.
  -> Topic
  -> (Conn -> IO r)
  -> IO r
withPersistentConn mbyID serviceTopic = bracket connectBroker disconnectBroker where
  connectBroker = do
    let
      Just uri = parseURI $ "mqtt://127.0.0.1" ++ maybe "" ('#':) mbyID
                  -- the _connID is parsed from the URI on `connectURI`.
      config = mqttConfig
        {
          _cleanSession = case mbyID of
              Nothing -> True  -- no persistence, do clean session
              Just _  -> False -- keep msgs the meant for a client which is offline
        , _lwt = Just LastWill
            { _willRetain = False
            , _willQoS = QoS2
            , _willTopic = LBS.fromStrict $ T.encodeUtf8 $ unTopic $
                           serviceTopic <> "last-will-testament"
            , _willMsg = mempty
            , _willProps = []
            }
        , _connProps =
            [ -- 1 day, required property for messages to be kept persisted
              -- if not persistent, expire 0 means nothing is kept.
              PropSessionExpiryInterval (maybe 0 (const 86400) mbyID)
            ]
        , _protocol = Protocol50
        , _connID   = fromMaybe "" mbyID -- is always overwritten by the #<id> in the URI.
        }
    mc <- connectURI config uri
    pure (Conn mc serviceTopic)

  -- must send DISCONNECT before exiting, otherwise LWT triggers
  disconnectBroker (Conn mc _) = normalDisconnect mc

-- | The content for a thread to periodically send a healthcheck event.
-- Usage: @forkIO (healthcheckThread ...)@
healthcheckThread :: ToJSON m => Integer {-^ Ping frequency in seconds -} -> EvtMsg m -> Topic -> IO ()
healthcheckThread delay_secs msg0 topic = forever do
  _ <- try @SomeException $ withConn healthcheck \c -> do
    forever do
      let msg = msg0 & evtExpected ?~ fromInteger delay_secs
                     & evtTimeout  .~ 30

      event c msg topic \_ -> pure (done "" ())

      threadDelay (fromInteger delay_secs*1_000_000) -- microseconds

  -- If the `withConn` conn fails (e.g. computer sleeps and the connection
  -- times out), then we don't want this thread to die. Just wait a little and
  -- try again.
  threadDelay (fromInteger delay_secs*1_000_000)

--------------------------------------------------------------------------------

-- | An identifier to correlate scoped events and start/stop events
data EventId = EventId { correlationId :: UUID.UUID, evtTopic :: Topic }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

data Timed a = Timed { at :: UTCTime, x :: a }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

data EvtMsg m = EvtMsg
  { scope    :: Maybe EventId
    -- ^ If this event should be correlated with an 'event' run whose scope
    -- captures this one.
  , reactTo  :: Maybe EventId
    -- ^ If specified, the 'EventId' identifies the event that this one is
    -- reacting to.
    --
    -- A process may subscribe to any event (@trigger/...@ or otherwise) to do
    -- work in reaction to that event having happened. This field lets us
    -- connect the events that were emitted doing this reaction work to the
    -- event that triggered doing it in the first place.
    --
    -- (@trigger/...@ events just happen to be more explicit about warranting a
    -- reaction, but any other event can also be reacted to.)
  , rules    :: Rules
    -- ^ Validation rules
  , label    :: String
    -- ^ A label describing this event
  , content  :: Maybe m
    -- ^ Additional content, that may be used e.g. by listeners reacting to
    -- this event
  }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

simple :: String -> EvtMsg ()
simple x = EvtMsg
  { scope    = Nothing
  , reactTo  = Nothing
  , rules    = Rules
    { timeout = 300
    , expected = Nothing
    , subtasks = Nothing
    , reactions = Nothing
    , critical = False
    }
  , label    = x
  , content  = Nothing
  }

scoped :: Lens' (EvtMsg m) (Maybe EventId)
scoped = lens (\s -> s.scope) (\s b -> s{scope = b})

reacted :: Lens' (EvtMsg m) (Maybe EventId)
reacted = lens (\s -> s.reactTo) (\s b -> s{reactTo = b})

withMsg :: Lens (EvtMsg m) (EvtMsg n) (Maybe m) (Maybe n)
withMsg = lens (\s -> s.content) (\s b -> s{content = b})

-- ** Evt Done -----------------------------------------------------------------

data EvtDone = EvtDone
  { summary   :: String
  , success   :: Bool
  , result    :: Maybe Value
  , triggers  :: Maybe [Trigger]
    -- ^ A finished event may indicate a list of folow-up actions that can be
    -- triggered. The 'Trigger' values specify how to constructed the announced
    -- *trigger* event. (This is useful for an interface which may provide a
    -- way to act on triggers announced by an event)
  }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

-- | A specification for a *trigger* event. A *trigger* event is a normal
-- event, still sent using the 'event' combinator (typically published on
-- @'trigger'/...@).
--
-- An announced 'Trigger' should intent that the announcing process, or one it
-- knows of, will be listening to the given trigger topic and will react to
-- that event, performing some desired follow up action, and hopefully sending
-- a reaction event with @reacted ?~ <id>@.
--
-- Events published in reply to this trigger are identified by the @replyTo@
-- field of the @EvtMsg@, and an expected number of replies may be specified in
-- the rules.
data Trigger = Trigger
  { triggerTopic :: Topic
    -- ^ What topic to send this *trigger* event to
  , triggerLabel :: String
    -- ^ A label describing the trigger action
  , triggerData  :: Maybe Value
    -- ^ The data to send as the 'content' of the 'EvtMsg' constructed for the
    -- trigger event. This data will be used by the listening party to react to
    -- the trigger.
    --
    -- TODO: We say data template because some fields may come in pre-filled vs
    -- expecting user input? Consider JSONSchema
  }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

done, failed :: String -> r -> (EvtDone, r)
done   msg r = (EvtDone msg True Nothing Nothing, r)
failed msg r = (EvtDone msg False Nothing Nothing, r)

withResult :: (FromJSON m, ToJSON m) => Lens' (EvtDone, r) (Maybe m)
withResult = lens (\(s,_) -> join (res . fromJSON <$> s.result))
                  (\(s,r) b -> (s{result = toJSON <$> b}, r))
  where
    res (Error _)   = Nothing
    res (Success v) = Just v

withTriggers :: Lens' (EvtDone, r) (Maybe [Trigger])
withTriggers = lens (\(s,_) -> s.triggers) (\(s,r) b -> (s{triggers = b}, r))

-- ** Rules --------------------------------------------------------------------

-- | Internal consistency/sanity checks/validation rules for this event
data Rules = Rules
  { timeout  :: Int
    -- ^ How much time in seconds to wait for a "finished" message for this
    -- "start" message before considering the service failed?
  , expected :: Maybe NominalDiffTime
    -- ^ When is a next "start" message expected, at the latest, after this
    -- one, for the same topic this message was sent on?
  , subtasks :: Maybe [String]
    -- ^ There must be at least one matching sub-event per entry on the list,
    -- where the @String@ must match the suffix of the topic (whose prefix is
    -- this event's topic) and the sub-event @scope@ must match this event's
    -- correlation-id.
    --
    -- That is, for @[t1, t2, t3]@, we expect at least three events scoped
    -- under this one under these sub-topics exactly, sent like
    -- @event c (msg & scoped ?~ <this_event>) t{1,2,3} ...@
    --
    -- There may be additional sub-events, as long as each of the listed ones has a match.
  , reactions :: Maybe [Filter]
    -- ^ There must be at least one matching event that comes as a reaction to
    -- this event per entry on the list. These "reply" events must have a
    -- @react@ field matching this event's correlation-id. Each @Filter@ must
    -- @'match'@ the topic the reply is sent under.
  , critical :: Bool
    -- ^ A rule validation engine must warn critically (CRITICAL FAILURE) if
    -- any of the rules are violated when @critical = True@
  }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

evtTimeout :: Lens' (EvtMsg m) Int
evtTimeout = lens (\s -> s.rules.timeout) (\s b -> s{rules = s.rules{timeout = b}})

evtExpected :: Lens' (EvtMsg m) (Maybe NominalDiffTime)
evtExpected = lens (\s -> s.rules.expected) (\s b -> s{rules = s.rules{expected = b}})

evtSubtasks :: Lens' (EvtMsg m) (Maybe [String])
evtSubtasks = lens (\s -> s.rules.subtasks) (\s b -> s{rules = s.rules{subtasks = b}})

evtCritical :: Lens' (EvtMsg m) Bool
evtCritical = lens (\s -> s.rules.critical) (\s b -> s{rules = s.rules{critical = b}})

--------------------------------------------------------------------------------

-- | Send a delimited "transactional" event
event :: (ToJSON m) => Conn -> EvtMsg m -> Topic -> (EventId -> IO (EvtDone, r)) -> IO r
event (Conn mc conn_base) edt topic k = do
  mask $ \restore -> do
    eid <- startEvent
    (dn, r)
        <- restore (k eid)
            `catch` \(e::SomeException) -> do
                      endEvent (eid, exception_done e)
                      throwIO e
    _ <- endEvent (eid, dn)
    return r
  where
  base_topic = maybe conn_base evtTopic edt.scope
  full_topic = base_topic <> topic

  -- Always do a "transaction": explicit start evt before we do anything,
  -- followed by an end event when done. The start "acquire" is crucial to
  -- also detect cases in which we failed to do the task and couldn't even
  -- send a fail event.
  startEvent = do
    correlationId <- UUID.nextRandom
    now           <- getCurrentTime
    publishq mc (full_topic <> "start") (JSON.encode (Timed now edt)) False{-retain-}
             QoS2 [PropCorrelationData (UUID.toLazyASCIIBytes correlationId)]
    pure EventId{correlationId, evtTopic=full_topic}

  endEvent (EventId{correlationId}, edn) = do
    now <- getCurrentTime
    publishq mc (full_topic <> "finished") (JSON.encode (Timed now edn)) False{-retain-}
             QoS2 [PropCorrelationData (UUID.toLazyASCIIBytes correlationId)]

  exception_done e = EvtDone
    { summary  = "An exception occurred"
    , success  = False
#if MIN_VERSION_base(4,22,0)
    , result   = Just (toJSON $ displayExceptionWithInfo e)
#else
    , result   = Just (toJSON $ displayException e)
#endif
    , triggers = Nothing
    }

event_ :: ToJSON m => Conn -> EvtMsg m -> Topic -> (EventId -> IO r) -> IO r
event_ c m t k = event c m t (\e -> done "OK" <$> k e)

-- react :: Conn -> Topic -> (EventId -> IO (EvtDone, r)) -> IO r
-- react (Conn mc conn_base) topic k = do

--------------------------------------------------------------------------------

instance ToJSON   Topic  where toJSON    = toJSON . unTopic
instance FromJSON Topic  where parseJSON = withText "Topic" $ pure . fromJust . mkTopic
instance ToJSON   Filter where toJSON    = toJSON . unFilter
instance FromJSON Filter where parseJSON = withText "Topic" $ pure . fromJust . mkFilter
