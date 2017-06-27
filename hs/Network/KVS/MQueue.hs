-- |A variant of "Control.Concurrent.Chan" that keeps a link to the most recently added item.

module Network.KVS.MQueue
  ( MQueue
  , newMQueue
  , singletonMQueue
  , putMQueue
  , getMQueue
  , viewMQueue
  ) where

import           Control.Concurrent.MVar (MVar, newEmptyMVar, newMVar, modifyMVarMasked, putMVar, takeMVar, readMVar, tryReadMVar, withMVar)
import           Control.Exception (mask_)

type MQueueEntry a = MVar (MQueueItem a)

data MQueueItem a = MQueueItem
  { _mqueueItem :: a
  , _mqueueNext :: MQueueEntry a
  }

data MQueueTail a = MQueueTail
  { _mqueueLast :: MQueueEntry a
  , _mqueueWrite :: MQueueEntry a
  }

data MQueue a = MQueue
  { _mqueueRead :: MVar (MQueueEntry a)
  , _mqueueTail :: MVar (MQueueTail a)
  }

-- |Create a new, empty 'MQueue'.
newMQueue :: IO (MQueue a)
newMQueue = do
  e <- newEmptyMVar
  r <- newMVar e
  t <- newMVar $ MQueueTail e e
  return $ MQueue r t

-- |Equivalent to @'putMQueue' x =<< 'newMQueue'@
singletonMQueue :: a -> IO (MQueue a)
singletonMQueue x = do
  e <- newEmptyMVar
  i <- newMVar $ MQueueItem x e
  r <- newMVar i
  t <- newMVar $ MQueueTail i e
  return $ MQueue r t

-- |Add a new value at the end of an 'MQueue'.  Should not block.
putMQueue :: a -> MQueue a -> IO ()
putMQueue x (MQueue _ t) = do
  e <- newEmptyMVar
  mask_ $ do
    MQueueTail _ w <- takeMVar t
    putMVar w $ MQueueItem x e
    putMVar t $ MQueueTail w e

-- |Get the first value from an 'MQueue', blocking if necessary.
getMQueue :: MQueue a -> IO a
getMQueue (MQueue r _) = modifyMVarMasked r $ \v -> do
  MQueueItem x n <- takeMVar v
  return (n, x)

-- |Get the last, most recently-added value from an 'MQueue', blocking if necessary.
viewMQueue :: MQueue a -> IO a
viewMQueue (MQueue _ t) = do
  ev <- withMVar t $ \(MQueueTail l w) ->
    maybe (Left w) Right <$> tryReadMVar l
  MQueueItem x _ <- either readMVar return ev
  return x
