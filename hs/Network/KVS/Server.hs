{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Network.KVS.Server
  ( serve
  ) where

import           Control.Arrow (first)
import           Control.Concurrent (myThreadId, forkFinally)
import           Control.Concurrent.MVar (MVar, newEmptyMVar, newMVar, modifyMVar, modifyMVar_, tryPutMVar, takeMVar, tryTakeMVar, readMVar)
import           Control.Exception (mask_)
import           Control.Monad (when, unless, forever)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import           Data.Semigroup (Semigroup(..))
import qualified Data.Set as Set
import           Data.Tuple (swap)
import qualified Network.Socket as Net
#ifdef VERSION_unix
import           System.Posix.Resource (getResourceLimit, setResourceLimit, Resource(ResourceOpenFiles), softLimit, hardLimit, ResourceLimit(..))
#endif
import           System.IO (hPutStrLn, stderr)

import           Network.KVS.Types
import           Network.KVS.Internal

#ifdef VERSION_unix
instance Ord ResourceLimit where
  ResourceLimit x <= ResourceLimit y = x <= y
  ResourceLimitUnknown <= _ = True
  _ <= ResourceLimitUnknown = False
  _ <= ResourceLimitInfinity = True
  ResourceLimitInfinity <= _ = False
#endif

logger :: String -> IO ()
logger = hPutStrLn stderr

-- |A non-empty FIFO queue that also provides an efficient view operation for the most-recently added value at the tail.
data Queue a = Queue
  { _queueRead :: [a]
  , _queueWrite :: !(NE.NonEmpty a)
  }

instance Semigroup (Queue a) where
  Queue r1 w1 <> Queue [] w2 = Queue r1 (w2 <> w1)
  Queue r1 (w NE.:| w1) <> Queue r2 w2 = Queue (r1 ++ reverse w1 ++ w : r2) w2

queueSingleton :: a -> Queue a
queueSingleton a = Queue [] (a NE.:| [])

queueGet :: Queue a -> (a, Maybe (Queue a))
queueGet (Queue [] (t NE.:| [])) = (t, Nothing)
queueGet (Queue (h:l) t) = (h, Just (Queue l t))
queueGet (Queue [] (t NE.:| r)) = (h, Just (Queue l (t NE.:| []))) where h:l = reverse r

queueView :: Queue a -> a
queueView (Queue _ (t NE.:| _)) = t

queueLength :: Queue a -> Int
queueLength (Queue h (_ NE.:| t)) = succ $ length h + length t

-- |Lookup the given key in the map, inserting and returning an empty 'MVar' if missing.
mvLookup :: Ord k => k -> Map.Map k (MVar a) -> IO (MVar a, Map.Map k (MVar a))
mvLookup k m = do
#if 1
  v <- newEmptyMVar -- this version wastes an allocation to save an additional lookup
  return $ first (fromMaybe v) $ Map.insertLookupWithKey (\_ _ -> id) k v m
#else
  -- this version is more portable to other map implementations
  maybe
    ((, Map.insert k v m) <$> newEmptyMVar)
    (return . (, m))
    $ Map.lookup k m
#endif

-- |Populate an 'MVar', either with a value if it's empty, or by applying a function to that value and the current value if non-empty.
-- Asynchronous exceptions are masked during this operation, but no other exception handing is done.
replaceMVar :: MVar a -> (a -> a -> a) -> a -> IO ()
replaceMVar v f = mask_ . loop where
  loop s = do
    r <- tryPutMVar v s
    unless r $
      loop . maybe s (f s) =<< tryTakeMVar v

-- |An 'MVar' 'Queue' that represents a queue of values, or an empty MVar when empty
type MVQ a = MVar (Queue a)
type KVS = Map.Map Key (MVQ EncodedValue)

client :: MVar KVS -> Net.Socket -> Net.SockAddr -> IO Bool
client mkvs s a = loop where
  loop = recvAll s 4 >>= cmd
  cmd "clos" = do
    Net.shutdown s Net.ShutdownBoth
    return False
  cmd "down" =
    return True
  cmd "dump" = do
    fail "TODO"
    loop
  cmd "put_" = do
    key <- recvLenBS s
    ev <- recvEncodedValue s
    modifyMVar_ mkvs $ \kvs -> do
      (v, kvs') <- mvLookup key kvs
      replaceMVar v (flip (<>)) $ queueSingleton ev
      return kvs'
    loop
  cmd "get_" = do
    v <- recvmv
    (ev, q) <- queueGet <$> takeMVar v
    mapM_ (replaceMVar v (<>)) q
    sendEncodedValue s ev
    loop
  cmd "view" = do
    v <- recvmv
    ev <- queueView <$> readMVar v
    sendEncodedValue s ev
    loop
  cmd c = fail $ "unknown op: " ++ show c
  recvmv = do
    key <- recvLenBS s
    modifyMVar mkvs $
      fmap swap . mvLookup key

serve :: Net.SockAddr -> IO ()
serve a = do
#ifdef VERSION_unix
  nof <- getResourceLimit ResourceOpenFiles
  let tnof = min (hardLimit nof) (ResourceLimit 4096)
  when (softLimit nof < tnof) $
    setResourceLimit ResourceOpenFiles nof{ softLimit = tnof }
#endif
  clients <- newMVar Set.empty
  kvs <- newMVar Map.empty
  
  s <- Net.socket (case a of
    Net.SockAddrInet{} -> Net.AF_INET
    Net.SockAddrInet6{} -> Net.AF_INET6
    Net.SockAddrUnix{} -> Net.AF_UNIX
    Net.SockAddrCan{} -> Net.AF_CAN) Net.Stream Net.defaultProtocol
  Net.setSocketOption s Net.ReuseAddr 1
  Net.bind s a
  a <- Net.getSocketName s
  logger $ "Server running at " ++ show a
  Net.listen s 4000

  forever $ do
    (c, ca) <- Net.accept s
    logger $ "Acceped connect from " ++ show ca
    tid <- forkFinally
      (client kvs c ca)
      (\r -> do
        tid <- myThreadId
        modifyMVar_ clients $ return . Set.delete tid
        logger $ "Closing connection from " ++ show ca ++ either ((": " ++) . show) (const "") r
        Net.close c)
    modifyMVar_ clients $ return . Set.insert tid
