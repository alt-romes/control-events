{-# LANGUAGE TypeAbstractions, MultiWayIf, BlockArguments, CPP,
             OverloadedStrings, OverloadedRecordDot, DeriveAnyClass #-}
-- | Expose slightly higher level abstraction for common use cases
module Control.Events.Health
  ( healthcheckThread )
  where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Aeson as JSON
import Lens.Micro
import Network.MQTT.Client

import Control.Events

-- * Healthcheck ---------------------------------------------------------------

-- | The content for a thread to periodically send a healthcheck event.
-- Usage: @forkIO (healthcheckThread ...)@
--
-- If the connection fails (e.g. the computer slept, or the broker is down),
-- it reconnects after a few seconds. It stops when killed or cancelled.
healthcheckThread :: ToJSON m => Integer {-^ Ping frequency in seconds -} -> Topic -> EvtMsg m -> IO ()
healthcheckThread delay_secs topic msg0 = forever do
  r <- tryJust notAsync $
    withConn healthcheck \c -> do
      let msg = msg0 & evtExpected ?~ fromInteger delay_secs
                     & evtTimeout  .~ 30

      event c topic msg \_ -> pure (done "" ())

      threadDelay (fromInteger delay_secs*1_000_000) -- microseconds

  case r of
    Left _   -> threadDelay 5_000_000
    Right () -> pure ()
  where
    notAsync e = case fromException @SomeAsyncException e of
      Just _  -> Nothing
      Nothing -> Just e
