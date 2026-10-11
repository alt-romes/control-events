{-# LANGUAGE RequiredTypeArguments, TypeData, TypeAbstractions, MultiWayIf, BlockArguments, CPP,
             OverloadedStrings, OverloadedRecordDot, DeriveAnyClass,
             RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-} -- JSON Topic

-- | Send and react to delimited events
--
-- A delimited event delimits the execution of a certain action. A /start event
-- is published before starting the action, and a /finished event is published
-- after the action is complete. A /finished event can report success or
-- failure (e.g. wrong value, or an exception was raised). The lack of a
-- /finished event to a matching /start indicates an unrecoverable failure
-- happened (e.g. a system crash, power outage).
--
-- == /Event Delivery Guarantees/
--
-- On the publisher side, the delimited approach means the receivers will
-- always know if something went wrong, as long as we can get a /start sent.
-- If the process crashes before sending the /start, we won't be able to know
-- the process even attempted to run, unless there is an external check
-- expecting a certain /start to happen at a particular time.
--
-- On the broker side, when it receives published events, it queues them for
-- persistent sessions that are currently disconnected. The broker saves these
-- queued messages every 5 minutes (configurable, even autosave_on_change). If
-- there's a hard crash on the broker process or entire system, the queued
-- messages will be lost forever.
--
-- On the receiver side, if there's a system-level/whole process crash between
-- the message being delivered to the handler and the handler beginning to do
-- anything with it, the message (e.g a /start) will be lost forever (the
-- broker will consider it delivered too).
-- On re-connecting a persistent session, as a specific instance of the above,
-- if there's a hard crash between the queued messages arriving and the
-- handlers being registered with 'react', the messages will never be delivered
-- to the handler and will be lost forever. If they had been delivered but the
-- handler crashed right away as above, same thing.
--
-- An approach to mitigate and detect these crashes is to have long running
-- processes send a healthcheck event periodically (see 'Control.Events.Healthcheck')
-- to a main dashboard process (which runs the event 'Rules', like 'expected'),
-- potentially with a list of expected healthcheck services, and have a an
-- external service monitoring that main process.
--
-- All in all, the library guarantees reliable delivery of delimited events,
-- but it is not 100% in the presence of system crashes or power outages. If
-- exactly once handling of messages across processes and network can never
-- ever absolutely fail, no matter how tiny the chance (MISSION CRITICAL), this
-- library isn't right for you (and prolly neither is MQTT and message brokers,
-- you need atomic guarantees end-to-end).
--
-- If you need a reliable and ergonomic library for sending and reacting to
-- events across a distributed system, with simplicity yet delivery guarantees
-- over 99% of the time, dynamic registering of handlers, with delimited events
-- with rules providing a good way to monitor the sending actions and making it
-- easy to react only to events that completed, then this is a fairly good
-- library for that.
--
-- It was originally designed, and is currently used, for __controlling__ and
-- __monitoring__ various long-running system processes or one-shot
-- {manual,scheduled} scripts across machines via a dashboard reacting to '#'.
-- Integrating these events into most of my system processes gives me an
-- overview of my systems, notifies me of issues, I can run some announced
-- available actions manually (e.g. to unblock automations), and have chain of
-- reactions be triggered across processes or automations that react to certain
-- known events in the system.
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
  , react, reactOnce

  , EventId(..), Timed(..)
  , EvtMsg(..), simple
  , scoped, reacted, withMsg

  , EvtDone(..), Trigger(..)
  , done, failed
  , withResult, withTriggers

  , OverlappingFilterException(..)

  -- ** Rules
  , Rules(..)
  , evtTimeout, evtExpected
  , evtSubtasks, evtReactions
  , evtCritical

  -- ** Topics
  , script, server, healthcheck, trigger
  , mkTopic, mkFilter

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
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
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
data Conn (session :: SessionType) = Conn
  { connClient    :: MQTTClient
  , connBaseTopic :: Topic
  , connHandlers  :: TVar (Map.Map Filter SomeEvtHandler)
  , connQueuedMs  :: TVar MsgQueue
  , connPendingTx :: IORef PendingMap
  , connBacklogs  :: TVar [Async ()]
    -- ^ If connection is closed, wait for backlog threads.
    -- They may be using Conn resources and because we link the backlogs to
    -- main thread we want to delimit their execution to the scope of
    -- `withConn`.
  }

-- | In a 'PersistentSession', if the client was disconnected, the broker will
-- keep the messages this client was subscribed to, and replay them all *on
-- re-connect*. A 'CleanSession' starts from a clean slate and is not preserved
-- across re-connects.
type data SessionType = CleanSession | PersistentSession

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

-- | We react to events in two stages: when the first /start arrives, and to
-- completed transactions (on a /finished with a matching /start).
--
-- When we receive a /start, after the first stage, we insert the event in the
-- map with correlation id and the IO action that runs the handler on
-- completion. On /finished, we pop it from the map and actually run the
-- "completed txn" handler.
--
-- We additionally evacuate pending transactions from this map when they
-- timeout.
type PendingMap = Map.Map UUID.UUID PendingTxn

-- | A queue of messages represented by the topic and the action to runs on a
-- registered handler whose filter 'match'es the topic. Enqueue new messages at
-- the head, take older messages from tail (last is oldest)
type MsgQueue = [(Topic, SomeEvtHandler -> IO ())]

data SessionData (s :: SessionType) where
  SCleanSession
    :: SessionData CleanSession
  SPersistentSession
    :: String
    -- ^ The string is a persistent connection ID. If the connection goes down, messages meant
    -- for this listener will be queued and delivered when we reconnect using the
    -- same ID.
    -> IORef PendingMap
    -- ^ A reference to the pending /start,/finished events to re-use across
    -- same-process persistent session (so we can still match a /finished on
    -- re-connected to a /start from the previous connection)
    -> TVar MsgQueue
    -- ^ A reference to the msg queue re-used across same-process persistent
    -- sessions (so we can still deliver queued messages if we happen to
    -- disconnect before handling them all)
    -> SessionData PersistentSession

-- | Create a persistent session 'SessionData' from the persistent identifier
-- for this session. See 'withPersistentConn' for more details about sharing
-- this 'SessionData' across reconnects.
newPersistentSession :: String -> IO (SessionData PersistentSession)
newPersistentSession persistId = SPersistentSession persistId <$> newIORef Map.empty <*> newTVarIO []

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
-- __The main property__: any events sent to the broker while we were
-- disconnected, under topics that we were subscribed to then, are delivered
-- when we re-register the 'react' handlers for those topics after we reconnect
-- (new 'withPersistentConn')[1]. This is guaranteed[2] without re-using any state at
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
--   same session used for the previous connection which crashed, then,
--   when the /finished event (that was sent while we were offline) is
--   delivered, we can still match it against the /start that had arrived
--   before us going down, and the full delimited-event is reacted to.
--
--   * Across-process reconnect: if we reconnect to the persistent session with
--   a brand new 'newPersistentSesion' (e.g. on a fresh process), then we won't
--   be able to match the /finished MQTT event (which will be delivered on
--   reconnect) with anything, and we'll throw it away. So, an unrecoverable
--   crash in between receiving the /start and /finished will result in the
--   event being lost because /finished was discarded[3].
--
-- Put simply, a crash between /start and /finished will not affect the
-- delivery of the full event if the @SessionData@ is shared across the
-- persistent sessions; if the data can't be shared across re-connects, then a
-- crash between /start and /finished means the event transaction won't
-- complete and the /finished lost.
--
-- [1] To be clear, the handlers for the persistent session messages delivered
-- on re-connect are /not/ run on re-connect. They are only run after being
-- registered with 'react' again, and only then are the messages we queued on
-- our side delivered to those handlers. 'react' registers handlers
-- dynamically, and it would be at best surprising if on re-connect handlers
-- started running before being registered again. It also wouldn't be possible
-- across connections that can't re-use the same persistent 'SessionData'.
--
-- [2] The exactly once delivery guarantees don't hold in all events of
-- system crashes or power outage. For instance, if there's a system crash
-- immediately after a /start event starts getting handled by a 'react', but
-- before the handler does anything. See the module header for a comprehensive
-- overview of how a message might not be delivered in certain system-level
-- crash dependent scenarios. Guaranteed delivery isn't entirely unachievable,
-- but would lead to a much much more complicated library (e.g. where the acks
-- are point to point and databases abound etc). This library can be used
-- for most cases where events are useful, as long as a very very tiny chance
-- of losing a /start is not *mission critical*.
--
-- [3] That said, an application or watchdog dashboard could have stored the
-- /start as it arrived, so that the lack of /finished on re-connect simply
-- shows as a timed out/invalidated event, like others where there's a /start
-- without a /finish (even if it might have completed).
withPersistentConn
  :: SessionData s
  -> Topic
  -> (Conn s -> IO r)
  -> IO r
withPersistentConn sd serviceTopic = bracket connectBroker disconnectBroker where

  mbyID = case sd of SCleanSession              -> Nothing
                     SPersistentSession sid _ _ -> Just sid

  connectBroker = do
    connBacklogs <- newTVarIO []
    pendingRef <- case sd of
      SCleanSession             -> newIORef Map.empty
      SPersistentSession _ pr _ -> pure pr

    msgQueue <- case sd of
      SCleanSession             -> newTVarIO []
      SPersistentSession _ _ qm -> pure qm

    handlersRef <- newTVarIO Map.empty
    let
      Just uri = parseURI $ "mqtt://127.0.0.1" ++ maybe "" ('#':) mbyID
                  -- the _connID is parsed from the URI on `connectURI`.
      config = mqttConfig
        { _msgCB = globalMsgCallback handlersRef pendingRef msgQueue
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
      , connQueuedMs  = msgQueue
      , connBacklogs
      }

  -- must send DISCONNECT before exiting, otherwise LWT triggers
  disconnectBroker Conn{connClient, connBacklogs} = do
    mapM_ wait =<< readTVarIO connBacklogs -- don't leak backlog threads on scope end
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

  globalMsgCallback handlers pending msgQueue = SimpleCallback $ \_c topic msg props ->
    if
      | Just ttopic <- txnTopic topic
      , [i]         <- mapMaybe corrData props
      , Just uuid   <- UUID.fromLazyASCIIBytes i
      , let termin = last (split topic)
      -> do

        let
          runHandler (SomeEvtHandler @m handler)
            | "start" <- termin
            , Just emsg <- decode @(Timed (EvtMsg m)) msg
            = do
              h_p2 <- handler (EventId uuid ttopic) emsg
              pair uuid (emsg.e.rules.timeout*2)
                      -- expected /finished according to rules.timeout
                      -- x2 to have bigger window to match a delayed pair
                      -- (the dashboard may want to display a timed-out but
                      -- received later /finished)
                   (PendingStart h_p2)
              return ()

            | "finished" <- termin
            , Just edn <- decode @(Timed EvtDone) msg
            = do
              pair uuid 30 -- expect /start very soon after /finished
                   (PendingFinished edn)
              return ()

            | otherwise
            = pure () -- invalid, skip

        -- Atomically ensures a message is only queued if there's
        -- no matching handler
        matching_h <- atomically $ do
          hs <- Map.toList <$> readTVar handlers
          case filter (\(filt, _) -> match filt ttopic) hs of
            matching_h:_ -> -- there should be a single matching handler
              pure (Just matching_h)

            -- When we reconnect on a persistent session, the queued messages
            -- will be delivered to the global handler straight away. If none
            -- of the registered handlers match a message we receive[1], then
            -- we queue them up. We deliver them when The Matching Handler[2] is
            -- registered with 'react'.
            --
            -- ('react' ensures atomically the handler will only run on new
            -- messages after the pending queue is processed in order, to
            -- prefer delivering older messages in order despite SimpleCallback
            -- not guaranteeing order)
            --
            -- [1] We only receive messages we are subscribed to, or were
            -- subscribed to when the persistent session disconnected
            --
            -- [2] Recall we don't support overlapping filters, so a topic can
            -- only match one handler
            []
              | SCleanSession <- sd
              -- This is a clean session, so any messages no current handlers
              -- match are just ignored. I think it may happen if unsubscribe
              -- and a broker msg delivery race.
              -> pure Nothing
              | SPersistentSession{} <- sd
              -> do
                 modifyTVar msgQueue ((ttopic, runHandler):)
                 pure Nothing

        case matching_h of
          Just (_, hd) -> runHandler hd
          Nothing      -> pure ()

      | otherwise
      -> pure () -- skip, invalid event

    where

      pair uuid timeout ptxn = do
        atomicModifyIORef' pending (\pm -> case (Map.lookup uuid pm, ptxn) of
          (Nothing, _)
            -> (Map.insert uuid ptxn pm, Nothing)
          (Just (PendingStart act),   PendingFinished dn)
            -> (Map.delete uuid pm, Just (act dn))
          (Just (PendingFinished dn), PendingStart act)
            -> (Map.delete uuid pm, Just (act dn))
          _ -> (pm, Just (pure ())) -- duplicate start or finish (impossible with QoS2)
          ) >>= \case
            Nothing -> do
                -- Remove this pending entry from the map after the timeout.
                -- If there was already a match, Map.delete uuid will be a no-op.
                --
                -- TODO: Should we provide a way of handling /finished events
                -- that arrive after the timeout or on their own? Using react,
                -- we will lose all events that are /finished beyond their
                -- timeout or across restarts
                _ <- forkIO $ do
                  threadDelay (timeout*1_000_000)
                  modifyPending (Map.delete uuid)
                pure ()
            Just runIt -> runIt

      corrData (PropCorrelationData i) = Just i
      corrData _                       = Nothing

      -- drop "/{start,finished}" from the topic
      txnTopic = fmap sconcat . NE.nonEmpty . init . split

      modifyPending f = atomicModifyIORef' pending (\pm -> (f pm, ()))

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
    --
    -- TODO: This list of triggers assumes they are conjunctive. We might
    -- imagine disjunction too, where triggering one trigger manually
    -- invalidates the others. We probably don't want to go as far as having a
    -- tree of conjunctions and disjunctions. Just allowing one of disjunction
    -- vs conjunction for the announced triggers is probably enough, and a tree
    -- can be implemented by reacting to a disjunctive trigger with a follow up
    -- announcement of many conjunctive triggers and so on.
    -- Design: something like NoTriggers | ConjunctiveTriggers [Trigger] | DisjunctiveTriggers [Trigger]?
    -- but at that point why not just the full blown tree :)
    -- it would still be recursive in nature, and thus easy to handle in the
    -- rules engine/dashboard.
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
event Conn{connClient=mc, connBaseTopic=conn_base} topic edt k = do
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
-- The handler "main action" should most often only be done when the event
-- is "completed", on the second @EvtDone@ IO action.
--
-- For every message that arrives matching this topic, we try to decode it as
-- an @EvtMsg m@ and pass it to the given handler. If decoding fails, the msg
-- is ignored. The order in which messages arrive is likely the order in which
-- they were sent, but ordering is not guaranteed.
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
-- However, registering two overlapping handlers for the same topic is not
-- supported. We check and throw at runtime if you register two overlapping
-- filters. Any given event should match at most one handler topic.
--
-- (The 'withConn' "base topic" is unused in 'react', since we may want to
-- react to topics outside of the base topic we're publishing at. For instance,
-- we may want to react to @trigger/finances/gen-invoice@ from a process
-- publishing under a @script/finances@ topic)
react :: forall m s
       . FromJSON m
      => Conn s
      -> Filter
      -> EvtHandler m ()
      -> IO (IO ())
      -- ^ Returns the action to unsubscribe and unregister this handler for
      -- this topic
react Conn{connClient=mc, connHandlers, connQueuedMs, connBacklogs} f h = sub >>= return . unsub

  where
    sub = do
      -- The registered handler blocks on any new message until its queued
      -- messages are all processed (the handler may receive messages as soon
      -- as it's registered, even before `subscribe`, if it's registered while
      -- queued messages are still arriving)
      readyForNew <- newTVarIO Nothing
      let waitReady = atomically do
            readTVar readyForNew >>= \case
              Nothing -> retry
              Just ra -> waitSTM ra
                  -- ensures exception thrown by backlog queue is raised here too
                  -- all queued and blocked msgs will be lost and the conn
                  -- killed if a handler crashes. better bracket!

      -- Detect overlapping filters and register handler otherwise, atomically.
      -- See 'overlaps'.
      bad <- atomically $ do
        m <- readTVar connHandlers
        let bad = any (overlaps f) (Map.keys m)
        when (not bad) $ do
          writeTVar connHandlers $
            Map.insert f (SomeEvtHandler (\e t -> waitReady >> h e t)) m
        pure bad

      when bad $
        Map.keys <$> readTVarIO connHandlers
          >>= throwIO . OverlappingFilterException f

      -- The global handler atomically gives the message to a handler OR to the queue.
      -- After registering the handler, no more messages for this handler can be queued.
      -- Therefore, we can simply take all matching messages and know we won't miss any.
      todoMsgs <- atomically $ do
        msgs <- readTVar connQueuedMs
        let (matches, rest) = L.partition (match f . fst) msgs
        writeTVar connQueuedMs rest
        pure (reverse matches) -- process in order they arrived

      backlogThread <- async do
        forM_ todoMsgs (\(_t, runHandler) -> runHandler (SomeEvtHandler h))

      link backlogThread -- if it throws at any time, async excp is thrown to us
      atomically (writeTVar readyForNew (Just backlogThread))
        -- handler will wait for it to be done before doing more work
      atomically (modifyTVar connBacklogs (backlogThread:))
        -- to scope it properly to withConn

      (merrs, _) <- subscribe mc (map (,sub_opts) mqtt_fs) []
      case lefts merrs of
        []   -> pure backlogThread
        errs -> cancel backlogThread >> fail (show errs)

    unsub backlogThread = do
      wait backlogThread
      atomically $ modifyTVar connHandlers (Map.delete f)
      hasConn <- isConnected mc
      when hasConn $ do
        _ <- unsubscribe mc mqtt_fs []
        pure ()

    mqtt_fs
      | Just (_, "#") <- L.unsnoc (split f)
      = [f] -- already matches .../start and .../finished
      | otherwise
      = [f <> "start", f <> "finished"]

    sub_opts = SubOptions
      { _retainHandling = SendOnSubscribe -- on subscribe, receive all retained messages always
      , _retainAsPublished = False -- default
      , _noLocal = False -- /do/ receive your own messages, e.g. the dashboard wants to see the events it sends.
      , _subQoS = QoS2  -- msgs published as QoS2 can be sent from the broker to us with QoS2 too
      }

-- | Block waiting to 'react' exactly once to one message matching this filter
-- and then unsubscribe, unregister the handler, and resume.
reactOnce :: forall m s a
           . FromJSON m
          => Conn s
          -> Filter
          -> EvtHandler m a
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

-- | Block waiting for the broker to disconnect.
--
-- Typically used after 'react's if you want to keep reacting forever (until
-- the broker disconnects for some reason).
waitConnDisconnect :: Conn s -> IO ()
waitConnDisconnect Conn{..} = waitForClient connClient

-- | Two 'react' filters cannot overlap. We will get 1 copy of the event per
-- subscription, and all overlapping handlers will match against *every* copy.
-- The 'PendingMap' invariants would also be violated.
--
-- Now, we could imagine using a subscription identifier property, but those
-- are in-memory and wouldn't survive a crash. Queued messages delivered on
-- reconnect would then be duplicate for overlapping filters without a way to
-- avoid it.
--
-- Perhaps more importantly, this is a much simpler specification: no
-- overlapping handlers. A specification for overlapping filters would be
-- complex at best.
--
-- A user should just keep the more general filter and defer by topic in the
-- handler
overlaps :: Filter -> Filter -> Bool
overlaps f g = go (split f) (split g) where
  go ["#"] _ = True
  go _ ["#"] = True
  go [] [] = True
  go (x:xs) (y:ys) = (x == "+" || y == "+" || x == y) && go xs ys
  go _ _ = False

data OverlappingFilterException = OverlappingFilterException Filter [Filter] deriving Show
instance Exception OverlappingFilterException where
  displayException (OverlappingFilterException f fs) =
    "OverlappingFilterException: filter " ++ show f ++
    " overlaps with existing filters " ++ show fs ++
    ".\nRegistering overlapping filters is not supported."

--------------------------------------------------------------------------------
-- * Instances
--------------------------------------------------------------------------------

instance ToJSON   Topic  where toJSON    = toJSON . unTopic
instance FromJSON Topic  where parseJSON = withText "Topic"  $ maybe (fail "invalid topic")  pure . mkTopic
instance ToJSON   Filter where toJSON    = toJSON . unFilter
instance FromJSON Filter where parseJSON = withText "Filter" $ maybe (fail "invalid filter") pure . mkFilter
