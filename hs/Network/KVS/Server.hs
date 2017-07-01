{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Network.KVS.Server
  ( serve
  ) where

import           Control.Concurrent (ThreadId, myThreadId, forkIOWithUnmask, throwTo, killThread, threadWaitRead)
import           Control.Concurrent.MVar (MVar, newMVar, modifyMVar, modifyMVar_, readMVar)
import           Control.Exception (Exception(..), SomeException, AsyncException, asyncExceptionToException, asyncExceptionFromException, catch, handle, try, mask, mask_)
import           Control.Monad (when, forever)
#ifdef VERSION_unordered_containers
import           Data.Hashable (Hashable)
import qualified Data.HashMap.Strict as Map
import qualified Data.HashSet as Set
#else
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
#endif
import qualified Network.Socket as Net
#ifdef VERSION_unix
import           System.Posix.Resource (getResourceLimit, setResourceLimit, Resource(ResourceOpenFiles), softLimit, hardLimit, ResourceLimit(..))
#endif
import           System.Posix.Types (Fd(Fd))
import           System.IO (hPutStrLn, stderr)

import           Network.KVS.Types
import           Network.KVS.Internal
import           Network.KVS.MQueue

#ifdef VERSION_unix
instance Ord ResourceLimit where
  ResourceLimit x <= ResourceLimit y = x <= y
  ResourceLimitUnknown <= _ = True
  _ <= ResourceLimitUnknown = False
  _ <= ResourceLimitInfinity = True
  ResourceLimitInfinity <= _ = False
#endif

#ifdef VERSION_unordered_containers
type Map = Map.HashMap
type Mappable a = (Eq a, Hashable a)
#else
type Map = Map.Map
type Mappable a = Ord a
#endif

logger :: String -> IO ()
logger = hPutStrLn stderr

data StopWaiting = StopWaiting
  deriving (Show)
instance Exception StopWaiting where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

type KVS = Map Key (MQueue EncodedValue)

lookupMQueue :: Mappable k => k -> Map k (MQueue a) -> IO (Map k (MQueue a), MQueue a)
lookupMQueue k m = maybe
  (do
    q <- newMQueue
    return (Map.insert k q m, q))
  (return . (,) m)
  $ Map.lookup k m

client :: MVar KVS -> Net.Socket -> Net.SockAddr -> ThreadId -> IO Bool
client mkvs s _a tid = loop where
  loop = recvAll s 4 >>= cmd
  cmd "put_" = do
    q <- getkeyq
    val <- recvEncodedValue s
    putMQueue val q
    loop
  cmd "get_" = do
    q <- getkeyq
    val <- wait $ getMQueue q
    mapM_ (sendEncodedValue s) val
    loop
  cmd "view" = do
    q <- getkeyq
    val <- wait $ viewMQueue q
    mapM_ (sendEncodedValue s) val
    loop
  cmd "dump" = do
    fail "TODO"
    loop
  cmd "clos" = do
    Net.shutdown s Net.ShutdownBoth
    return False
  cmd "down" =
    return True
  cmd c = fail $ "unknown op: " ++ show c
  getkeyq = modifyMVar mkvs . lookupMQueue =<< recvLenBS s
  wait f = mask $ \unmask -> do
    wid <- forkIOWithUnmask ($ do
      threadWaitRead $ Fd $ Net.fdSocket s
      throwTo tid StopWaiting)
    -- we don't explicitly unmask f: instead only allow interruptions while blocked, otherwise defer the StopWaiting
    r <- (Just <$> f) `catch` \StopWaiting -> return Nothing
    -- if we got stopped after f's wait completed, just ignore it
    handle (\StopWaiting -> return ()) $ unmask $ killThread wid
    return r

serve :: Net.SockAddr -> IO ()
serve sa = do
#ifdef VERSION_unix
  nof <- getResourceLimit ResourceOpenFiles
  let tnof = min (hardLimit nof) (ResourceLimit 4096)
  when (softLimit nof < tnof) $
    setResourceLimit ResourceOpenFiles nof{ softLimit = tnof }
#endif

  clients <- newMVar Set.empty
  kvs <- newMVar Map.empty
  
  s <- Net.socket (case sa of
    Net.SockAddrInet{} -> Net.AF_INET
    Net.SockAddrInet6{} -> Net.AF_INET6
    Net.SockAddrUnix{} -> Net.AF_UNIX
    Net.SockAddrCan{} -> Net.AF_CAN) Net.Stream Net.defaultProtocol
  Net.setSocketOption s Net.ReuseAddr 1
  Net.bind s sa
  a <- Net.getSocketName s
  logger $ "Server running at " ++ show a
  Net.listen s 4000

  pid <- myThreadId
  handle (\e -> logger $ "Shutting down: " ++ show (e :: AsyncException)) $ mask_ $ forever $ do
    (c, ca) <- Net.accept s
    logger $ "Acceped connect from " ++ show ca
    Net.setSocketOption c Net.NoDelay 1
    tid <- forkIOWithUnmask $ \unmask -> do
      tid <- myThreadId
      r <- try $ unmask $ client kvs c ca tid
      modifyMVar_ clients $ return . Set.delete tid
      unmask $ do
        logger $ "Closing connection from " ++ show ca ++ either (\e -> ": " ++ show (e :: SomeException)) (const "") r
        Net.close c
        when (either (const False) id r) $ killThread pid
    modifyMVar_ clients $ return . Set.insert tid

  mapM_ killThread =<< readMVar clients
