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
