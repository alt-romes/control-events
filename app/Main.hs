{-# LANGUAGE OverloadedStrings, BlockArguments #-}
module Main (main) where

import Control.Concurrent
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Options.Applicative
import System.Exit
import System.Process
import Control.Events
import Control.Events.Health
import Network.MQTT.Topic (Topic, unTopic)

-- | What to run, and the rules its events carry.
data Opts = Opts
  { expected :: Maybe Integer
  , timeout  :: Maybe Int
  , critical :: Bool
  , label    :: Maybe String
  , topic    :: Topic
  , exe      :: FilePath
  , args     :: [String]
  }

opts :: Parser Opts
opts = Opts
  <$> optional (option auto (long "expected" <> metavar "SECS" <> help "The next run is expected within this many seconds"))
  <*> optional (option auto (long "timeout" <> metavar "SECS" <> help "The run must finish within this many seconds"))
  <*> switch (long "critical" <> help "Any problem with a run is a critical failure")
  <*> optional (strOption (long "label" <> metavar "LABEL" <> help "What the run is (default: the topic)"))
  <*> argument (maybeReader (mkTopic . T.pack)) (metavar "TOPIC")
  <*> strArgument (metavar "EXE")
  <*> many (strArgument (metavar "ARGS..."))

data Command = Script Opts | Healthcheck Opts

cmds :: Parser Command
cmds = hsubparser
  (  cmd "script" Script "Send an event for the program's run"
  <> cmd "healthcheck" Healthcheck "Send a healthcheck event every --expected seconds (default 60) while the program runs" )
  where
    cmd n f d = command n (info (f <$> opts) (progDesc d))

main :: IO ()
main = execParser (info (cmds <**> helper) (progDesc "Run a program, sending control-events about it")) >>= \case
  Script o      -> runScript o
  Healthcheck o -> runHealthcheck o

msgOf :: Opts -> EvtMsg ()
msgOf o = simple (fromMaybe (T.unpack (unTopic o.topic)) o.label)
  & evtExpected .~ (fromInteger <$> o.expected)
  & evtTimeout %~ (\t -> fromMaybe t o.timeout)
  & evtCritical .~ o.critical

runScript :: Opts -> IO ()
runScript o = do
  exitCode <- withConn script \c -> do

    event c o.topic (msgOf o) \_ -> do
      (_,_,_,ph) <- createProcess (proc o.exe o.args)
        -- we can't read the output of the program without changing its
        -- behavior wrt the stdout / tty things. The stdout/err/in must remain
        -- as 'Inherit' to ensure it is just as if the program had been invoked
        -- directly.

      ec <- waitForProcess ph
      case ec of
        ExitSuccess   -> pure (done "Ran successfully" ec)
        ExitFailure f -> pure (failed ("Failed with exit code " ++ show f) ec)

  exitWith exitCode

runHealthcheck :: Opts -> IO ()
runHealthcheck o = do
  _          <- forkIO $ healthcheckThread (fromMaybe 300 o.expected) o.topic (msgOf o)
  (_,_,_,ph) <- createProcess (proc o.exe o.args)
  waitForProcess ph >>= exitWith
