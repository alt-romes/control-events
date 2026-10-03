{-# LANGUAGE TypeAbstractions, MultiWayIf, BlockArguments, CPP,
             OverloadedStrings, OverloadedRecordDot, DeriveAnyClass #-}
{-# OPTIONS_GHC -Wno-orphans #-} -- JSON Topic
module Control.Events
  (
  -- * Establishing a connection
    withConn, Conn
  , healthcheckThread

  -- * Running tasks delimited by events
  , event, event_
  , react, reactOnce
  , EventId(..), Timed(..)
  , EvtMsg(..), simple
  , scoped, reacted, withMsg
  , EvtDone(..), Trigger(..)
  , done, failed
  , withResult, withTriggers

  -- ** Rules
  , Rules(..)
  , evtTimeout, evtExpected
  , evtSubtasks, evtReactions
  , evtCritical

  -- ** Topics
  , script, server, healthcheck, trigger
  , mkTopic

  -- * Re-exports
  , module Lens.Micro
  ) where

import qualified Data.List.NonEmpty as NE
import qualified Data.List as L
import Data.Semigroup
import Data.Either
import Data.IORef
import Data.Maybe
import Data.Time.Clock
import GHC.Generics
import Control.Concurrent.Async
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Aeson as JSON
import Lens.Micro
import Network.URI (parseURI)
import Network.MQTT.Client
import Network.MQTT.Topic
import Network.MQTT.Types (RetainHandling(..))
import qualified Data.Map as Map
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy as LBS


-- * Topics --------------------------------------------------------------------

script, server, healthcheck, trigger :: Topic
script = fromJust (mkTopic "script")
server = fromJust (mkTopic "server")
healthcheck = fromJust (mkTopic "healthcheck")
trigger = fromJust (mkTopic "trigger")

-- * Connection ----------------------------------------------------------------

-- | A connection to send events to the broker for a particular service
data Conn = Conn
  { connClient    :: MQTTClient
  , connBaseTopic :: Topic
  , connHandlers  :: IORef (Map.Map Filter MsgHandler)
  , connPendingTx :: IORef (Map.Map (UUID.UUID, Filter) PendingTxn)
    -- ^ We can only react to completed transactions.
    -- When we receive a /start we insert the event in the map with correlation
    -- id and the IO action that runs the handler. On /finished, we pop it from
    -- the map and actually run the handler.
    --
    -- We additionally evacuate pending transactions from this map when they
    -- timeout.
    --
    -- If there are two overlapping handlers (e.g. for trigger/# and
    -- trigger/something), they will handle the same message. Therefore, the
    -- map key must also include the handler filter, since otherwise the two
    -- messages would collapse into just one on the pending map.
  }

data MsgHandler = forall m. FromJSON m => SomeMsgHandler (EventId -> EvtMsg m -> IO (EvtDone -> IO ()))

-- | A pending transaction is waiting for the other half of the transaction.
-- Typically, /start comes first, but a /finished may arrive first because of
-- out of order delivery from the broker across topics or the unordered
-- SimpleCallback (the non-blocking call-back type we use, rather than
-- OrderedCallback which raises complicated questions about deadlocks due to
-- how it orders requests)
data PendingTxn
  -- | A transaction .../start, waiting for the .../finished event.
  -- Captures the handler action that is only missing an 'EvtDone' to run.
  --
  -- This transaction will be cleared from the map if its pair doesn't arrive
  -- within the rules timeout for this event.
  = PendingStart (EvtDone -> IO ())
  -- | A .../finished event arrived before the /start. Hold on to the 'EvtDone'
  -- and run the handler when the /start arrives.
  --
  -- This transaction is cleared if the pair doesn't arrive within a short
  -- amount of time. We expect it to be available very soon after start if it's
  -- simply caused by an out-of-order issue.
  | PendingFinished EvtDone

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
    handlersRef <- newIORef Map.empty
    pendingRef  <- newIORef Map.empty
    let
      Just uri = parseURI $ "mqtt://127.0.0.1" ++ maybe "" ('#':) mbyID
                  -- the _connID is parsed from the URI on `connectURI`.
      config = mqttConfig
        { _msgCB = globalMsgCallback handlersRef pendingRef
        , _cleanSession = case mbyID of
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
    pure Conn
      { connClient    = mc
      , connBaseTopic = serviceTopic
      , connHandlers  = handlersRef
      , connPendingTx = pendingRef
      }

  -- must send DISCONNECT before exiting, otherwise LWT triggers
  disconnectBroker Conn{connClient} = normalDisconnect connClient

  globalMsgCallback handlers pending = SimpleCallback $ \_c topic msg props -> do
    hs <- readIORef handlers
    -- Try all the handlers
    forM_ (Map.toList hs) $ \(filt, SomeMsgHandler @m handler) ->
      if | match filt (txnTopic topic)
         , [i]       <- mapMaybe corrData props
         , Just uuid <- UUID.fromLazyASCIIBytes i
         , let termin = last (split topic)
         -> if | "start" <- termin
               , Just (Timed _ emsg) <- decode @(Timed (EvtMsg m)) msg
               -> do
                  h_p2 <- handler (EventId uuid (txnTopic topic)) emsg
                  pair (uuid, filt) (emsg.rules.timeout + 30) -- expected /finished according to rules.timeout
                       (PendingStart h_p2)

               | "finished" <- termin
               , Just (Timed _ edn) <- decode @(Timed EvtDone) msg
               -> pair (uuid, filt) 30 -- expect /start very soon after /finished
                       (PendingFinished edn)

               | otherwise
               -> pure ()

         | otherwise
         -> pure ()
    where
      pair (uuid, filt) timeout ptxn = do
        atomicModifyIORef' pending (\pm -> case (Map.lookup (uuid, filt) pm, ptxn) of
          (Nothing, _)
            -> (Map.insert (uuid, filt) ptxn pm, Nothing)
          (Just (PendingStart act),   PendingFinished dn)
            -> (Map.delete (uuid, filt) pm, Just (act dn))
          (Just (PendingFinished dn), PendingStart act)
            -> (Map.delete (uuid, filt) pm, Just (act dn))
          _ -> (pm, Just (pure ())) -- duplicate start or finish (impossible with QoS2)
          ) >>= \case
            Nothing -> do
                -- Remove this pending entry from the map after the timeout.
                -- If there was already a match, Map.delete uuid will be a no-op.
                _ <- forkIO $ do
                  threadDelay (timeout*1_000_000)
                  modifyPending (Map.delete (uuid, filt))
                pure ()
            Just runIt -> runIt

      corrData (PropCorrelationData i) = Just i
      corrData _                       = Nothing

      -- drop "/{start,finished}" from the topic
      txnTopic = sconcat . NE.fromList . init . split

      modifyPending f = atomicModifyIORef' pending (\pm -> (f pm, ()))

-- | The content for a thread to periodically send a healthcheck event.
-- Usage: @forkIO (healthcheckThread ...)@
healthcheckThread :: ToJSON m => Integer {-^ Ping frequency in seconds -} -> Topic -> EvtMsg m -> IO ()
healthcheckThread delay_secs topic msg0 = forever do
  _ <- try @SomeException $ withConn healthcheck \c -> do
    forever do
      let msg = msg0 & evtExpected ?~ fromInteger delay_secs
                     & evtTimeout  .~ 30

      event c topic msg \_ -> pure (done "" ())

      threadDelay (fromInteger delay_secs*1_000_000) -- microseconds

  -- If the `withConn` conn fails (e.g. computer sleeps and the connection
  -- times out), then we don't want this thread to die. Just wait a little and
  -- try again.
  threadDelay (fromInteger delay_secs*1_000_000)


-- * Messages ------------------------------------------------------------------

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
    --
    -- Each announced trigger is only meant to be used once. It would be
    -- surprising if there's some action we can do repeatedly but only is
    -- announced after a certain event: we assume all triggers are one-shot.
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

withTriggers :: Lens' (EvtDone, r) [Trigger]
withTriggers = lens (\(s,_) -> fromMaybe [] s.triggers) (\(s,r) b -> (s{triggers = Just b}, r))

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

evtReactions :: Lens' (EvtMsg m) (Maybe [Filter])
evtReactions = lens (\s -> s.rules.reactions) (\s b -> s{rules = s.rules{reactions = b}})

evtCritical :: Lens' (EvtMsg m) Bool
evtCritical = lens (\s -> s.rules.critical) (\s b -> s{rules = s.rules{critical = b}})


-- * Publishing ----------------------------------------------------------------

-- | Send a delimited "transactional" event
event :: (ToJSON m) => Conn -> Topic -> EvtMsg m -> (EventId -> IO (EvtDone, r)) -> IO r
event (Conn mc conn_base _ _) topic edt k = do
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

-- | 'event', but the result is @'done' "OK"@ unless an exception is thrown.
event_ :: ToJSON m => Conn -> Topic -> EvtMsg m -> (EventId -> IO r) -> IO r
event_ c t m k = event c t m (\e -> done "OK" <$> k e)


-- * Subscribing ---------------------------------------------------------------

-- | Block waiting for "completed" events under this topic, forever, until the
-- broker disconnects. The handler will be run for matching topics whenever a
-- pair @.../start@ + @.../finished@ is received. The handler
--    @(EventId -> EvtMsg m -> IO (EvtDone -> IO ()))@
--
-- is run in two phases, where the first IO action is run on /start, but it
-- should typically only "do the main action" on the inner action on @EvtDone@,
-- which is run when the matching /finished arrives.
--
-- For every message that arrives matching this 'Filter', try to decode it as
-- an @EvtMsg m@ and pass it to the given handler. If decoding fails, the msg
-- is ignored.
--
-- To react to multiple topics you can run 'react' under 'withAsync': you spawn
-- multiple 'react's asynchronously and wait for all (or some) of them at the
-- end.
--
-- Example of subscribing to two topics:
-- @
-- concurrently (react c t1 (\(x::EvtMsg MyData) -> ...))
--              (react c t2 ((\y::EvtMsg OtherData) -> ...))
-- @
--
-- It is safe to call 'react' many times, and from various threads. All
-- handlers will be registered and messages delegated to the corresponding
-- handler.
--
-- Registering two handlers for the same topic is not supported and is
-- considered UB.
--
-- If 'react' is canceled or the broker disconnects, the handler will be
-- unregistered and we'll unsubscribe further messages on this topic to the
-- broker (if it is still connected).
--
-- (The 'withConn' "base topic" is unused in 'react', since we may want to
-- react to topics outside of the base topic we're publishing at. For instance,
-- we may want to react to @trigger/finances/gen-invoice@ from a process
-- publishing under a @script/finances@ topic)
react :: FromJSON m => Conn -> Filter -> (EventId -> EvtMsg m -> IO (EvtDone -> IO ())) -> IO ()
react (Conn mc _conn_base handlersRef _) f h = bracket sub unsub (\() -> waitForClient mc)
  where
    sub = do
      atomicModifyIORef' handlersRef (\m -> (Map.insert f (SomeMsgHandler h) m, ()))
      (merrs, _) <- subscribe mc (map (,sub_opts) tfs) []
      case lefts merrs of
        []   -> pure ()
        errs -> fail (show errs)

    unsub () = do
      atomicModifyIORef' handlersRef (\m -> (Map.delete f m, ()))
      hasConn <- isConnected mc
      when hasConn $ do
        _ <- unsubscribe mc tfs []
        pure ()

    tfs
      | Just (_, "#") <- L.unsnoc (split f)
      = [f] -- already matches .../start and .../finished
      | otherwise
      = [f <> "start", f <> "finished"]

    sub_opts = SubOptions
      { _retainHandling = SendOnSubscribe -- on subscribe, receive all retained messages always
      , _retainAsPublished = False -- default
      , _noLocal = True -- don't receive your own messages
      , _subQoS = QoS2  -- msgs published as QoS2 can be sent from the broker to us with QoS2 too
      }

-- | Block waiting to 'react' exactly once to one message matching this filter
-- and then unsubscribe, unregister the handler, and resume.
reactOnce :: FromJSON m => Conn -> Filter -> (EventId -> EvtMsg m -> EvtDone -> IO ()) -> IO ()
reactOnce mc f h = do
  w <- newEmptyMVar
  race (react mc f (\i m -> pure $ \d -> void (tryPutMVar w (i, m, d))))
                      -- tryPutMVar: the first handler run succeeds writing the
                      -- msg, the following handler runs ignore the msg
       (takeMVar w) >>= \case
    Left ()      -- react finished before the handler ran:
      -> fail "reactOnce: disconnected or canceled before receiving a message"
    Right (i, m, d) -- handler ran and stored the first message:
      -> h i m d

-- * Instances -----------------------------------------------------------------

instance ToJSON   Topic  where toJSON    = toJSON . unTopic
instance FromJSON Topic  where parseJSON = withText "Topic" $ pure . fromJust . mkTopic
instance ToJSON   Filter where toJSON    = toJSON . unFilter
instance FromJSON Filter where parseJSON = withText "Topic" $ pure . fromJust . mkFilter
