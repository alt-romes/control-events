{-# LANGUAGE OverloadedStrings, BlockArguments, DeriveAnyClass #-}
module Main (main) where

import GHC.Generics
import Control.Monad
import Control.Concurrent
import Options.Generic
import System.Exit
import System.Process
import Control.Events
import Network.MQTT.Topic (Topic)

data Command
  = Script      Text FilePath [String]
  | Healthcheck Text FilePath [String]
  -- | Trigger     Text
      -- ^ Trigger a message to a topic
  deriving (Generic, Show, ParseRecord)

main :: IO ()
main = getRecord "control-events" >>= \case
  Script topic exe args
    | Just tp <- mkTopic topic -> runScript tp exe args
    | otherwise                -> die "<topic> isn't a valid MQTT topic"
  Healthcheck topic exe args
    | Just tp <- mkTopic topic -> runHealthcheck tp exe args
    | otherwise                -> die "<topic> isn't a valid MQTT topic"

runScript :: Topic -> FilePath -> [String] -> IO ()
runScript topic exe args = do
  exitCode <- withConn script \c -> do

    event c (simple (unwords (exe:args))) topic \_ -> do
      (_,_,_,ph) <- createProcess (proc exe args)
        -- we can't read the output of the program without changing its
        -- behavior wrt the stdout / tty things. The stdout/err/in must remain
        -- as 'Inherit' to ensure it is just as if the program had been invoked
        -- directly.

      ec <- waitForProcess ph
      case ec of
        ExitSuccess   -> pure (done "Ran successfully" ec)
        ExitFailure f -> pure (failed ("Failed with exit code " ++ show f) ec)

  exitWith exitCode

runHealthcheck :: Topic -> FilePath -> [String] -> IO ()
runHealthcheck topic exe args = do
  _ <- forkIO do
    withConn healthcheck \c ->
      forever do
        let msg = simple (unwords (exe:args))
                    & evtExpected ?~ 60{-seconds-}
                    & evtTimeout  .~ 60{-seconds-}

        event c msg topic \_ -> pure (done "" ())

        threadDelay (1*60*1_000_000) -- microseconds

  (_,_,_,ph) <- createProcess (proc exe args)
  waitForProcess ph >>= exitWith

