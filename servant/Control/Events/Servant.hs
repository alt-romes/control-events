module Control.Events.Servant where

import Data.Bifunctor
import Data.Aeson
import Servant.Server
import Network.MQTT.Topic
import Control.Events

-- | Like 'event', but when in the servant 'Handler' monad
eventH :: forall m a. ToJSON m => Conn -> EvtMsg m -> Topic
       -> (EventId -> Handler (EvtDone, a)) -> Handler a
eventH conn msg topic k = MkHandler $ do
  event conn msg topic $ \ev -> do
    r <- runHandler (k ev)
    pure $ case r of
      Left serr -> failed "Server Error" (Left serr)
                      & withResult .~ Just (show serr)
      Right x   -> second Right x

