{-# LANGUAGE OverloadedStrings, BlockArguments #-}
module Main (main) where

import Data.String
import System.Environment
import System.Exit
import System.Process
import Control.Events

main :: IO ()
main = getArgs >>= \case
    (topic:"--":exe:args)
      -> case mkTopic (fromString topic) of
           Nothing -> die "<topic> isn't a valid MQTT topic"
           Just tp -> go tp exe args
    _ -> die "Usage: control-events <topic> -- <executable> <arg1> ... <argn>"
  where
    go topic exe args = do
      exitCode <- withConn (script <> topic) \c -> do

        event c meta "run" \_ -> do
          (_,_,_,ph) <- createProcess (proc exe args)
          -- todo: how to read a summary message off of the output? the challenge
          -- is we want to behave exactly as if stdout was inherited by the
          -- subprocess. maybe just don't have summary messages for running scripts like this.

          done "" <$> waitForProcess ph

      exitWith exitCode
