module Network.KVS.Queue
  ( Queue
  , singleton
  , get
  , view
  , length
  ) where

import           Prelude hiding (length)

import qualified Data.List as L
import qualified Data.List.NonEmpty as NE
import           Data.Semigroup (Semigroup(..))

-- |A non-empty FIFO queue that also provides an efficient view operation for the most-recently added value at the tail.
data Queue a = Queue
  { _queueRead :: [a]
  , _queueWrite :: !(NE.NonEmpty a)
  }

instance Semigroup (Queue a) where
  Queue r1 w1 <> Queue [] w2 = Queue r1 (w2 <> w1)
  Queue r1 (w NE.:| w1) <> Queue r2 w2 = Queue (r1 ++ L.reverse w1 ++ w : r2) w2

singleton :: a -> Queue a
singleton a = Queue [] (a NE.:| [])

-- |Take the oldest value off the front of a queue and return the remaining queue (if non-empty). Amortized /O(1)/.
get :: Queue a -> (a, Maybe (Queue a))
get (Queue [] (t NE.:| [])) = (t, Nothing)
get (Queue (h:l) t) = (h, Just (Queue l t))
get (Queue [] (t NE.:| r)) = (h, Just (Queue l (t NE.:| []))) where h:l = L.reverse r

-- |Retrieve the most recently added value from the queue. /O(1)/.
view :: Queue a -> a
view (Queue _ (t NE.:| _)) = t

-- |Determine the length of a queue. /O(n)/.
length :: Queue a -> Int
length (Queue h t) = L.length h + NE.length t

