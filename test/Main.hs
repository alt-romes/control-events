-- | Requires an MQTT broker listening on 127.0.0.1:1883.
module Main (main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Data.List (sort)
import Data.Maybe
import GHC.TypeLits (KnownSymbol)
import qualified Data.UUID as UUID
import qualified Data.UUID.V4 as UUID
import Network.MQTT.Topic
import System.IO.Error (isUserError)
import Test.Tasty
import Test.Tasty.ExpectedFailure
import Test.Tasty.HUnit

import Control.Events

main :: IO ()
main = defaultMain $ localOption (mkTimeout 30_000_000) $ testGroup "control-events"
  [ testGroup "react with a dynamic filter in a clean session"
    [ testCase "reacts to every event sent" $ cleanTest \c base -> do
        seen <- newLog
        _ <- react' c (base <> "job") (logTo seen)
        forM_ ["one", "two", "three"] \l -> event_ c "job" (simple l) \_ -> pure ()
        seen `logged` [Done "one", Done "two", Done "three"]

    , testCase "wildcard filter matches subtopics" $ cleanTest \c base -> do
        seen <- newLog
        _  <- react' c (base <> "#") (logTo seen)
        () <- event_ c "a"          (simple "a")   mempty
        () <- event_ c ("b" <> "c") (simple "b/c") mempty
        seen `logged` [Done "a", Done "b/c"]

    , testCase "ignores events outside the filter" $ cleanTest \c base -> do
        (got, ()) <- concurrently (knownFilter (base <> "job") \ @job -> reactOnce c job outcome) do
          threadDelay 200_000 -- wait sub is active
          () <- event_ c "other" (simple "other") mempty
          () <- event_ c "job"   (simple "job")   mempty
          return ()
        got @?= Done "job"

    , testCase "reports failed and throwing events as unsuccessful" $ cleanTest \c base -> do
        seen <- newLog
        _  <- react' c (base <> "job") (logTo seen)
        () <- event c "job" (simple "failed") \_ -> pure (failed "nope" ())
        _  <- try @ErrorCall (event_ c "job" (simple "threw") \_ -> throwIO (ErrorCall "boom"))
        seen `logged` [Failed "failed", Failed "threw"]

    , expectFail $ testCase "concurrent reacts with overlapping filters each get their events (regression)" do
        -- The duplicate delivery only shows up under some message orderings
        replicateM_ 50 $ cleanTest \c base -> do
          seenA   <- newLog
          seenAll <- newLog
          _  <- react' c (base <> "a") (logTo seenA)
          _  <- react' c (base <> "#") (logTo seenAll)
          () <- event_ c "a" (simple "a") mempty
          () <- event_ c "b" (simple "b") mempty
          seenAll `logged` [Done "a", Done "b"]
          seenA   `logged` [Done "a"]

    , testCase "an unsubscribed react no longer runs its handler" $ cleanTest \c base -> do
        seenA   <- newLog
        seenAll <- newLog

        unsubA <- react' c (base <> "a") (logTo seenA)
        unsubA -- unsubscribe

        _  <- react' c (base <> "#") (logTo seenAll)
        () <- event_ c "a" (simple "a") mempty

        seenAll `logged` [Done "a"]
        seenA   `logged` []

    , testCase "unsubscribing a react keeps overlapping reacts subscribed" $ cleanTest \c base -> do
        seenA   <- newLog
        seenAll <- newLog

        _      <- react' c (base <> "#") (logTo seenAll)
        unsubA <- react' c (base <> "a") (logTo seenA)
        unsubA

        event_ c "a" (simple "a") \_ -> pure ()

        seenAll `logged` [Done "a"]
        seenA   `logged` []

    , testCase "closing the connection while events arrive doesn't throw (regression)" do
        -- The connection is closed by withConn as soon as sending the event
        -- returns, but we may be in the middle of sending acknowledges to the
        -- 'react'. There's a race where we DISCONNECT then send more messages
        -- on the connection the broker has closed, which makes the recv fail with
        -- ECONNRESET instead of EOF, and this exception is thrown by
        -- `normalDisconnect` which was waiting for the recv thread:
        --
        --  @Network.Socket.recvBuf: resource vanished (Connection reset by peer)@
        --
        -- We want to test that when the conn is disconnected, we don't crash
        -- with these kinds of errors, because nothing is actually wrong.
        -- Disconnect just happens to throw because of a race which writes
        -- things to a closed connection.
        --
        -- Regarding the unfinished receive: If the receive handshake is
        -- unfinished, the messages will be replayed on a persistent
        -- connection and they shouldn't matter on a clean one.
        replicateM_ 10 $ cleanTest \c base -> do
          _ <- react' @() c (base <> "job") \_ _ -> pure \_ -> pure ()
          forM_ ["one", "two", "three"] \l -> event_ c "job" (simple l) \_ -> pure ()
    ]

  , testGroup "persistent session"
    [ testCase "react to a declared topic" $ persistentTest \ @job base session ->
        withPersistentConn session base \c -> do
          seen <- newLog
          _    <- react c job (logTo seen)
          ()   <- event_ c "job" (simple "job") mempty
          seen `logged` [Done "job"]

    , testCase "reconnects explicitly after going down with an exception" $ persistentTest \ @job base session -> do

        r <- try @ErrorCall @() $ withPersistentConn session base \c -> do
          seen <- newLog
          _  <- react c job (logTo seen)
          () <- event_ c "job" (simple "before") mempty
          seen `logged` [Done "before"]
          throwIO (ErrorCall "going down")
        r @?= Left (ErrorCall "going down")

        withPersistentConn session base \c -> do
          seen <- newLog
          _  <- react c job (logTo seen)
          () <- event_ c "job" (simple "after") mempty
          seen `logged` [Done "after"]

    , expectFail $ testCase "receives events sent while it was down (regression)" $ persistentTest \ @job base session -> do

        _ <- try @ErrorCall $ withPersistentConn session base \c -> do
          _ <- react @() c job \_ _ -> pure \_ -> pure ()
          throwIO (ErrorCall "going down")

        -- Sent while persisent subscriber is down
        withConn base \c -> event_ c "job" (simple "offline") \_ -> pure ()

        withPersistentConn session base \c -> do
          -- delay: wait for queued events to arrive on connect, before 'react'ing.
          -- (We need to accumulate them, then deliver on react. We want to
          -- make sure we don't rely on 'react' happening before the events are
          -- delivered to receive queued messages.)
          threadDelay 100_000
          seen <- newLog
          _ <- react c job (logTo seen)
          seen `logged` [Done "offline"] -- expect to receive msg sent while offline

    , expectFail $ testCase "pairs a start and finish received across a reconnect (regression)" $ persistentTest \ @job base session -> do

        -- We do support matching start / finish across reconnects, as long as
        -- the session is re-used.
        --
        -- If we reconnect e.g. from a new process using the same persistent
        -- session ID but without the same in-memory session object, the /start
        -- is lost and the /finished will be not be paired. This should appear
        -- as an incomplete "event transaction" to a monitoring job that keeps
        -- the unmatched starts (dashboard) and needs to be looked into
        -- manually.
        --
        -- We don't intend to support pairing start/finish across new processes
        -- reconnecting to same persistent session.

        seen <- newLog
        reconnected <- newEmptyMVar
        withConn base \producer -> do

          sending <- withPersistentConn session base \c -> do
            _ <- react c job (logTo seen)
            sending <- async (event_ producer "job" (simple "job") \_ -> takeMVar reconnected)
            seen `loggedStart` "job" -- the /start reached us before going down
            pure sending

          withPersistentConn session base \c -> do
            _ <- react c job (logTo seen)
            putMVar reconnected ()
            wait sending
            seen `logged` [Done "job"]

    , testCase "reactOnce doesn't hang when the broker drops the connection" $ persistentTest \ @job base session -> do

        r <- withPersistentConn session base \c ->
          -- the broker drops a client when another connects with the same id
          try @IOException $ concurrently (reactOnce c job outcome) do
            threadDelay 200_000 -- wait for subscription to be active
            withPersistentConn session base \_ -> pure ()
        case r of
          Left e | isUserError e -> pure ()
          _ -> assertFailure ("expected reactOnce to fail, got: " ++ show r)

    , testCase "closing a connection the broker dropped doesn't throw" $ persistentTest \ @_ base session ->
        withPersistentConn session base \c -> do
          -- the broker drops a client when another connects with the same id
          withPersistentConn session base \_ -> pure ()
          atomically (isConnUp c >>= \up -> when up retry)
    ]
  ]

--------------------------------------------------------------------------------
-- * Driver
--------------------------------------------------------------------------------

-- | A clean connection on a fresh base topic
cleanTest :: (Conn CleanSession -> Filter -> IO r) -> IO r
cleanTest k = do
  base <- freshTopic
  withConn base \c -> k c (toFilter base)

-- | A fresh base topic and a fresh persistent session declaring its "job" subtopic.
persistentTest :: (forall job. KnownSymbol job => Topic -> SessionData (PersistentSession '[job]) -> IO r) -> IO r
persistentTest k = do
  base <- freshTopic
  sid <- UUID.toString <$> UUID.nextRandom
  knownFilter (toFilter base <> "job") \ @job -> k @job base (SPersistentSession sid)

-- | Use a unique base topic per test
freshTopic :: IO Topic
freshTopic = do
  u <- UUID.nextRandom
  pure ("test" <> fromJust (mkTopic (UUID.toText u)))

data Outcome = Done String | Failed String
  deriving (Eq, Ord, Show)

-- | Handle an event by returning how it finished.
outcome :: EvtHandler () Outcome
outcome _ m = pure \d -> pure ((if d.e.success then Done else Failed) m.e.label)

--------------------------------------------------------------------------------
-- ** Test Trace
--------------------------------------------------------------------------------

-- | The events a handler saw start, and how they finished.
data Log = Log (TVar [String]) (TVar [Outcome])

newLog :: IO Log
newLog = Log <$> newTVarIO [] <*> newTVarIO []

-- | Handle an event by logging its start and how it finished.
logTo :: Log -> EvtHandler () ()
logTo (Log starts finishes) i m = do
  push starts m.e.label
  finish <- outcome i m
  pure (finish >=> push finishes)
  where push v x = atomically (readTVar v >>= writeTVar v . (x :))

-- | Wait for exactly these events to finish, in any order.
logged :: HasCallStack => Log -> [Outcome] -> IO ()
logged l@(Log _ finishes) xs = do
  _ <- waitUntil ((>= length xs) . length <$> readTVar finishes)
  threadDelay 200_000 -- in case more arrive
  os <- readTVarIO finishes
  unless (sort os == sort xs) $ failLog l ("expected finished: " ++ show (sort xs))

-- | Wait for this event to start.
loggedStart :: HasCallStack => Log -> String -> IO ()
loggedStart l@(Log starts _) x = do
  ok <- waitUntil ((x `elem`) <$> readTVar starts)
  unless ok $ failLog l (" expected started: " ++ show x)

-- | Retries until the condition holds or until timeout (2s)
waitUntil :: STM Bool -> IO Bool
waitUntil cond = do
  late <- registerDelay 2_000_000
  atomically $ (cond >>= check >> pure True) `orElse` (readTVar late >>= check >> pure False)

failLog :: HasCallStack => Log -> String -> IO ()
failLog (Log starts finishes) expected = do
  ss <- readTVarIO starts
  os <- readTVarIO finishes
  assertFailure $ unlines
    [ expected
    , "     got finished: " ++ show (sort os)
    , "      got started: " ++ show (sort ss)
    ]
