module Control.Events.Servant where

import Data.Bifunctor
import Data.Aeson
import Servant.Server
import Network.MQTT.Topic
import Control.Events

-- | Like 'event', but when in the servant 'Handler' monad
eventH :: ToJSON m => Conn s -> Topic
       -> EvtMsg m -> (EventId -> Handler (EvtDone, a)) -> Handler a
eventH conn topic msg k = MkHandler $ do
  event conn topic msg $ \ev -> do
    r <- runHandler (k ev)
    pure $ case r of
      Left serr -> failed "Server Error" (Left serr)
                      & withResult .~ Just (show serr)
      Right x   -> second Right x

eventH_ :: ToJSON m => Conn s -> Topic -> EvtMsg m -> (EventId -> Handler a) -> Handler a
eventH_ conn topic msg k = eventH conn topic msg (\e -> done "OK" <$> k e)
