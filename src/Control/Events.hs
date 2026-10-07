{-# LANGUAGE RequiredTypeArguments, TypeData, TypeAbstractions, MultiWayIf, BlockArguments, CPP,
             OverloadedStrings, OverloadedRecordDot, DeriveAnyClass,
             RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-} -- JSON Topic
module Control.Events
  (
  -- * Establishing a connection
    withConn, withPersistentConn, Conn
  , SessionData(..), SessionType(..)
  , newPersistentSession
  , isConnUp, waitConnDisconnect

  -- * Running tasks delimited by events

  , event, event_

  , EvtHandler
  , react, reactOnce, react'

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
  , mkTopic, mkFilter
  , knownFilter

  -- ** For persistent sessions
  , StaticTopic, KnownFilters

  -- * Re-exports
  , module Lens.Micro
  ) where

import qualified Data.List.NonEmpty as NE
import qualified Data.List as L
import GHC.TypeLits
import Data.Semigroup
import Data.Either
import Data.IORef
import Data.Maybe
import Data.Time.Clock
import GHC.Generics
import Control.Concurrent
import Control.Concurrent.STM
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
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString.Lazy as LBS
import Data.Proxy
import GHC.TypeError
import Control.Applicative
import System.IO.Error


-- * Topics --------------------------------------------------------------------

script, server, healthcheck, trigger :: Topic
script = fromJust (mkTopic "script")
server = fromJust (mkTopic "server")
healthcheck = fromJust (mkTopic "healthcheck")
trigger = fromJust (mkTopic "trigger")

--------------------------------------------------------------------------------
-- * Connection
--------------------------------------------------------------------------------

-- | A connection to send events to the broker for a particular service
--
-- The @session@ type argument statically tracks for persistent sessions the
-- superset of topics the client will subscribe to. See the haddocks on
-- @SessionType@ for more details.
data Conn (session :: SessionType) = Conn
  { connClient    :: MQTTClient
  , connBaseTopic :: Topic
  , connHandlers  :: IORef (Map.Map Filter SomeEvtHandler)
  , connPendingTx :: IORef PendingMap
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

-- | A persistent session must declare upfront all topics it may ever subscribe.
--
-- If the client was disconnected, the broker will keep the messages this
-- client was subscribed to, and replay them all *on reconnect* (rather than
-- when re-subscribing).
--
-- Therefore, to support the dynamically registered 'react's, we track at the
-- type level the superset of subscribed topics by a persistent session and
-- queue on our end all
type data SessionType = CleanSession | PersistentSession [Symbol]

data SomeEvtHandler = forall m. FromJSON m => SomeEvtHandler (EvtHandler m ())

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
  = PendingStart (Timed EvtDone -> IO ())
  -- | A .../finished event arrived before the /start. Hold on to the 'EvtDone'
  -- and run the handler when the /start arrives.
  --
  -- This transaction is cleared if the pair doesn't arrive within a short
  -- amount of time. We expect it to be available very soon after start if it's
  -- simply caused by an out-of-order issue.
  | PendingFinished (Timed EvtDone)

type PendingMap = (Map.Map (UUID.UUID, Filter) PendingTxn)

data SessionData (s :: SessionType) where
  SCleanSession      :: SessionData CleanSession
  SPersistentSession :: KnownFilters topics => String -> IORef PendingMap -> SessionData (PersistentSession topics)
  -- ^ The string is a persistent connection ID. If the connection goes down, messages meant
  -- for this listener will be queued and delivered when we reconnect using the
  -- same ID.

-- | Create a persistent session 'SessionData' from the persistent identifier
-- for this session. See 'withPersistentConn' for more details about sharing
-- this 'SessionData' across reconnects.
newPersistentSession :: String -> forall topics -> KnownFilters topics => IO (SessionData (PersistentSession topics))
newPersistentSession persistId topics = SPersistentSession @topics persistId <$> newIORef Map.empty

-- | Open a connection to the broker for this service to send events.
-- The 'Topic' argument is used as the base topic for events sent within this
-- connection, so it should represent the service or type rather than any
-- particular task.
withConn :: forall r
          . Topic -- ^ Service topic, e.g. @'server' <> "kanjideck-fulfillment"@
                  --                    or @'script' <> "finances" <> "mercurybank-hs"@
                  --                    or perhaps even just @'script'@.
         -> (Conn CleanSession -> IO r)
         -> IO r
withConn = withPersistentConn SCleanSession

-- | Like 'withConn', but the session is persistent, so messages meant for it
-- are queued even if we are offline, and delivered on reconnect.
--
-- == Persistent sessions in detail
--
-- If a "persistent connection" fails and is disconnected, we are guaranteed a
-- few properties if we re-connect (calling 'withPersistentConn' again) using
-- the same persistent session identifier.
--
-- __The main property__: any "delimited events" sent to the broker while we were
-- disconnected, under topics that we were subscribed to then, are delivered
-- when we re-register the 'react' handlers for those topics after we reconnect
-- (new 'withPersistentConn'). This is guaranteed without re-using any state at
-- all across the two 'withPersistentConn' sessions, i.e. if we have a process
-- crash during a persistent session, and launch a *brand new* process using
-- the same persistent identifier, for each 'react' handler we register, the
-- handler will run for all messages that were queued while we were offline.
--
-- __The delimited property__: if a "delimited event" was halfway through when
-- the connection goes down, i.e. we had received a /start MQTT event, but not
-- yet a /finished one, then we might still be able to match the /finished to
-- the /start after reconnecting. There are two scenarios:
--
--   * In-process reconnect: given @session <- 'newPersistentSession' "my-persistent-id"@,
--   if we re-connect @'withPersistentConn' session@ where @session@ is the
--   same session used for the previous connection which crashed, then the,
--   when the /finished event (that was sent while we were offline) is
--   delivered, we can still match it against the /start that had arrived
--   before us going down, and the full delimited-event is reacted to.
--
--   * Across-process reconnect: if we reconnect to the persistent session with
--   a brand new 'newPersistentSesion' (e.g. on a fresh process), then we won't
--   be able to match the /finished MQTT event (which will be delivered on
--   reconnect) with anything, and we'll throw it away. So, an unrecoverable
--   crash in between receiving the /start and /finished will result in the
--   event being invalidated out because /finished was discarded.
--
-- Put simply, a crash between /start and /finished will not affect the
-- delivery of the full event if the @SessionData@ is shared across the
-- persistent sessions; if the data can't be shared across re-connects, then a
-- crash between /start and /finished means the event transaction won't
-- complete, and the event will be considered invalid. This is fine -- this is
-- exactly why we have delimiting events. In the unlikely case there's an
-- unrecoverable crash between a /start and /finished, that event will be invalid
-- and will be flagged as such (even if it might be completed).
--
-- In other words, it's sound to not match /start and /finished across fresh
-- re-connects, even if it's not complete.
withPersistentConn
  :: SessionData s
  -> Topic
  -> (Conn s -> IO r)
  -> IO r
withPersistentConn sd serviceTopic = bracket connectBroker disconnectBroker where

  mbyID = case sd of SCleanSession            -> Nothing
                     SPersistentSession sid _ -> Just sid

  connectBroker = do
    pendingRef <- case sd of
      SCleanSession           -> newIORef Map.empty
      SPersistentSession _ pr -> pure pr

    handlersRef <- newIORef Map.empty
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
  disconnectBroker Conn{connClient} =
    normalDisconnect connClient `catches`
      [ Handler \case
          e :: MQTTException
            -- Tried to disconnect, but connection is already down.
            | Discod _ <- e
            -> pure ()
          e -> throwIO e
      , Handler \case
          e :: IOException
            | isResourceVanishedError e
            -- The connection got ECONNRESET rather terminating cleanly with EOF,
            -- likely because of a race between DISCONNECT and acknowledging
            -- events on a (after DISCONNECT) closed connection.
            -- See test "closing the connection while events arrive doesn't throw"
            -> pure ()
          e -> throwIO e
      ]

  globalMsgCallback handlers pending = SimpleCallback $ \_c topic msg props -> do
    -- TODO: the default handlers should queue messages we get on connect and
    -- relay them to the subscribers as they come when using a persistent
    -- session.
    hs <- readIORef handlers
    -- Try all the handlers
    forM_ (Map.toList hs) $ \(filt, SomeEvtHandler @m handler) ->
      if | Just ttopic <- txnTopic topic
         , match filt ttopic
         , [i]       <- mapMaybe corrData props
         , Just uuid <- UUID.fromLazyASCIIBytes i
         , let termin = last (split topic)
         -> if | "start" <- termin
               , Just emsg <- decode @(Timed (EvtMsg m)) msg
               -> do
                  h_p2 <- handler (EventId uuid ttopic) emsg
                  pair (uuid, filt) (emsg.e.rules.timeout*2)
                          -- expected /finished according to rules.timeout
                          -- x2 to have bigger window to match a delayed pair
                          -- (the dashboard may want to display a timed-out but
                          -- received later /finished)
                       (PendingStart h_p2)

               | "finished" <- termin
               , Just edn <- decode @(Timed EvtDone) msg
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
                --
                -- TODO: We should provide a way of handling /finished events
                -- that arrive after the timeout or on their own. Using react,
                -- we will lose all events that are finished beyond their
                -- timeout or across restarts, whereas when the dashboard was
                -- doing this manually it registered a /finished that arrived
                -- much later.
                _ <- forkIO $ do
                  threadDelay (timeout*1_000_000)
                  modifyPending (Map.delete (uuid, filt))
                pure ()
            Just runIt -> runIt

      corrData (PropCorrelationData i) = Just i
      corrData _                       = Nothing

      -- drop "/{start,finished}" from the topic
      txnTopic = fmap sconcat . NE.nonEmpty . init . split

      modifyPending f = atomicModifyIORef' pending (\pm -> (f pm, ()))

      topicsTerm :: [Filter]
      topicsTerm = case sd of
        SPersistentSession @topics _ _ -> reifyFilters (Proxy @topics)
        SCleanSession                  -> []

isConnUp :: Conn s -> STM Bool
isConnUp Conn{connClient} = isConnectedSTM connClient

--------------------------------------------------------------------------------
-- * Messages
--------------------------------------------------------------------------------

-- | An identifier to correlate scoped events and start/stop events
data EventId = EventId { correlationId :: UUID.UUID, evtTopic :: Topic }
  deriving stock Generic
  deriving anyclass (ToJSON, FromJSON)

data Timed a = Timed { at :: UTCTime, e :: a }
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

--------------------------------------------------------------------------------
-- ** Evt Done
--------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------
-- ** Rules
--------------------------------------------------------------------------------

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


--------------------------------------------------------------------------------
-- * Publishing
--------------------------------------------------------------------------------

-- | Send a delimited "transactional" event
event :: (ToJSON m) => Conn s -> Topic -> EvtMsg m -> (EventId -> IO (EvtDone, r)) -> IO r
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
event_ :: ToJSON m => Conn s -> Topic -> EvtMsg m -> (EventId -> IO r) -> IO r
event_ c t m k = event c t m (\e -> done "OK" <$> k e)


--------------------------------------------------------------------------------
-- * Subscribing
--------------------------------------------------------------------------------

type EvtHandler m a = EventId -> Timed (EvtMsg m) -> IO (Timed EvtDone -> IO a)

-- | Subscribe and register a handler for delimited events under this topic.
-- The handler will be run for matching topics whenever a pair @.../start@ +
-- @.../finished@ is received. The handler:
--
--    @(EventId -> EvtMsg m -> IO (EvtDone -> IO ()))@
--
-- is run in two phases, where the first IO action is run on /start, and the
-- second IO action which is run when the matching /finished arrives.
-- The handler "main action" should be most often only be done when the event
-- is "completed", on the second @EvtDone@ IO action.
--
-- For every message that arrives matching this topic, we try to decode it as
-- an @EvtMsg m@ and pass it to the given handler. If decoding fails, the msg
-- is ignored.
--
-- The return value is an action to unsubscribe and unregister this handler. It
-- needn't ever be run.
--
-- Example of subscribing to two topics:
-- @
-- unsub_t1 <- react c t1 (\(x::EvtMsg MyData) -> ...)
-- unsub_t2 <- react c t2 ((\y::EvtMsg OtherData) -> ...)
-- ...
-- waitConnDisconnect c
-- @
--
-- It is safe to call 'react' many times, and from various threads. All
-- handlers will be registered and messages delegated to the corresponding
-- handler.
--
-- Registering two handlers for the same topic is not supported and is
-- considered UB. Registering two handlers with overlapping topics is also UB
-- at the moment. Any given event should match at most one handler topic.
--
-- (The 'withConn' "base topic" is unused in 'react', since we may want to
-- react to topics outside of the base topic we're publishing at. For instance,
-- we may want to react to @trigger/finances/gen-invoice@ from a process
-- publishing under a @script/finances@ topic)
react :: forall m s. Conn s
      -> forall topic
      -> StaticTopic s topic => KnownSymbol topic => FromJSON m
      => EvtHandler m ()
      -> IO (IO ())
      -- ^ Returns the action to unsubscribe and unregister this handler for
      -- this topic
react (Conn mc _conn_base handlersRef _) topic h = sub >> return unsub
  where
    sub = do
      atomicModifyIORef' handlersRef (\m -> (Map.insert f (SomeEvtHandler h) m, ()))
      (merrs, _) <- subscribe mc (map (,sub_opts) tfs) []
      case lefts merrs of
        []   -> pure ()
        errs -> fail (show errs)

    unsub = do
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
    f = fromJust (mkFilter (T.pack (symbolVal (Proxy @topic))))

    sub_opts = SubOptions
      { _retainHandling = SendOnSubscribe -- on subscribe, receive all retained messages always
      , _retainAsPublished = False -- default
      , _noLocal = False -- /do/ receive your own messages, e.g. the dashboard wants to see the events it sends.
      , _subQoS = QoS2  -- msgs published as QoS2 can be sent from the broker to us with QoS2 too
      }

-- | Block waiting to 'react' exactly once to one message matching this filter
-- and then unsubscribe, unregister the handler, and resume.
reactOnce :: forall m s a
           . Conn s
          -> forall topic
          -> StaticTopic s topic => KnownSymbol topic => FromJSON m
          => EvtHandler m a
          -> IO a
reactOnce mc topic h = do
  w <- newEmptyTMVarIO
  unsub <- react mc topic $ \i m -> pure $ \d ->
             -- tryPutMVar: the first handler run succeeds writing the msg, the
             -- following handler runs ignore the msg
             void (atomically (tryPutTMVar w (i, m, d)))

  waited <- atomically $
    (Right <$> takeTMVar w) -- handler ran and stored the first message
      <|>                   -- and, don't block forever if conn disconnects in
    (Left () <$ (check . not =<< isConnUp mc))                 -- the meantime

  case waited of
    Left () -> fail "reactOnce: disconnected before receiving a message"
    Right (i, m, d) -> do
      unsub
      f <- h i m
      x <- f d
      pure x

-- | 'react' specialized to 'CleanSession', so the 'Filter' is given directly
react' :: FromJSON m => Conn CleanSession -> Filter -> EvtHandler m () -> IO (IO ())
react' c f h = knownFilter f $ \ @topic -> react c topic h

-- | Block waiting for the broker to disconnect.
--
-- Typically used after 'react's if you want to keep reacting forever (until
-- the broker disconnects for some reason).
waitConnDisconnect :: Conn s -> IO ()
waitConnDisconnect Conn{..} = waitForClient connClient

--------------------------------------------------------------------------------
-- * Subscribing in persistent connection (see SessionType)
--------------------------------------------------------------------------------

-- ** Dynamic topics in clean sessions

-- | In a clean session, the topic can be constructed dynamically (doesn't have
-- to be known statically). This is a helper to reify a Filter to the type system.
knownFilter :: Filter -> (forall topic. KnownSymbol topic => r) -> r
knownFilter f k = withSomeSSymbol (T.unpack (unFilter f)) (\(ss :: SSymbol s) -> withKnownSymbol ss (k @s))

-- ** Checking the topic is statically declared

-- | Make sure the topic is declared in the list of persistent topics if this is a persistent session
class StaticTopic (s :: SessionType) (topic :: Symbol)

-- | In a clean session we don't track statically the subscribed topics, so
-- anything can be subscribed. On disconnect, no messages will be queued for us.
instance StaticTopic CleanSession topic

instance Unsatisfiable ('Text "Topic " ':<>: 'ShowType topic ':<>:
          'Text " must be declared in the SPersistentSession topics list.")
         => StaticTopic (PersistentSession '[]) topic

-- | If the topic is declared in the static superset of topics of this
-- persistent session, all is good. We'll keep all messages queued for us on
-- re-connect, and replay them when 'react' for a matching topic is called.
--
-- We assume that if we have any queued message, it is meant to be delivered to
-- the matching react as soon as it starts subscribing. Because if at some
-- point you became dynamically uninterested in a topic, then you canceled your
-- 'react', which unsubscribed so the broker wouldn't queue any messages.
instance {-# OVERLAPPING #-}
         StaticTopic (PersistentSession (topic ': rest)) topic

-- | Inductive case
instance {-# OVERLAPPABLE #-}
         StaticTopic (PersistentSession rest) topic
      => StaticTopic (PersistentSession (other ': rest)) topic

-- ** Reifying a list of filters

-- | Reify a list of filters
class KnownFilters (topics :: [Symbol]) where
  reifyFilters :: Proxy topics -> [Filter]

instance KnownFilters '[] where reifyFilters _ = []
instance (KnownSymbol x, KnownFilters xs) => KnownFilters (x ': xs) where
  reifyFilters _ = fromJust (mkFilter (T.pack (symbolVal (Proxy @x)))) : reifyFilters (Proxy @xs)

--------------------------------------------------------------------------------
-- * Instances
--------------------------------------------------------------------------------

instance ToJSON   Topic  where toJSON    = toJSON . unTopic
instance FromJSON Topic  where parseJSON = withText "Topic"  $ maybe (fail "invalid topic")  pure . mkTopic
instance ToJSON   Filter where toJSON    = toJSON . unFilter
instance FromJSON Filter where parseJSON = withText "Filter" $ maybe (fail "invalid filter") pure . mkFilter
