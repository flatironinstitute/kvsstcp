{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Network.KVS.Server
  ( serve
  ) where

import           Control.Arrow ((***), second)
import           Control.Concurrent (myThreadId, forkFinally)
import           Control.Concurrent.MVar (MVar, newEmptyMVar, newMVar, modifyMVar, modifyMVar_, tryPutMVar, takeMVar, readMVar, tryReadMVar)
import           Control.Exception (mask_)
import           Control.Monad (when, forever)
#ifdef VERSION_unordered_containers
import           Data.Hashable (Hashable)
import qualified Data.HashMap.Strict as Map
import qualified Data.HashSet as Set
#else
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
#endif
import           Data.Tuple (swap)
import qualified Network.Socket as Net
#ifdef VERSION_unix
import           System.Posix.Resource (getResourceLimit, setResourceLimit, Resource(ResourceOpenFiles), softLimit, hardLimit, ResourceLimit(..))
#endif
import           System.IO (hPutStrLn, stderr)

import           Network.KVS.Types
import           Network.KVS.Internal
import qualified Network.KVS.Queue as Q

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

data Store a
  = StoreQueue !(Q.Queue a)
  | StoreMVar !(MVar a)

type KVS = Map Key ((Store EncodedValue))

putStore :: MVar a -> a -> IO (Store a)
putStore v x = do
  r <- tryPutMVar v x
  if r
    then return $ StoreMVar v
    else maybe
      (putStore v x)
      (return . StoreQueue . Q.doubleton x)
      =<< tryReadMVar v

getViewStore :: Mappable k => (Q.Queue a -> (Map k (Store a) -> Map k (Store a), a)) -> k -> Map k (Store a) -> IO (Map k (Store a), Either (MVar a) a)
getViewStore f k m = case Map.lookup k m of
  Nothing -> do
    v <- newEmptyMVar
    return (Map.insert k (StoreMVar v) m, Left v)
  Just (StoreMVar v) ->
    return (m, Left v)
  Just (StoreQueue q) ->
    return $ ($ m) *** Right $ f q

client :: MVar KVS -> Net.Socket -> Net.SockAddr -> IO Bool
client mkvs s a = loop where
  loop = recvAll s 4 >>= cmd
  cmd "put_" = do
    key <- recvLenBS s
    val <- recvEncodedValue s
    modifyMVar_ mkvs $ \kvs -> do
      s' <- case Map.lookup key kvs of
        Nothing             -> -- return $ StoreQueue $ Q.singleton val
          StoreMVar <$> newMVar val
        Just (StoreQueue q) -> return $ StoreQueue $ Q.put val q
        Just (StoreMVar v)  -> putStore v val
      return $ Map.insert key s' kvs
    loop
  cmd "get_" = do
    key <- recvLenBS s
    ev <- modifyMVar mkvs $
      getViewStore (swap . second (maybe (Map.delete key) (Map.insert key . StoreQueue)) . Q.get) key
    val <- either takeMVar return ev
    sendEncodedValue s val
    loop
  cmd "view" = do
    key <- recvLenBS s
    ev <- modifyMVar mkvs $
      getViewStore ((,) id . Q.view) key
    val <- either readMVar return ev
    sendEncodedValue s val
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

  forever $ do
    (c, ca) <- Net.accept s
    logger $ "Acceped connect from " ++ show ca
    Net.setSocketOption c Net.NoDelay 1
    tid <- forkFinally
      (client kvs c ca)
      (\r -> do
        tid <- myThreadId
        modifyMVar_ clients $ return . Set.delete tid
        logger $ "Closing connection from " ++ show ca ++ either ((": " ++) . show) (const "") r
        Net.close c)
    modifyMVar_ clients $ return . Set.insert tid
