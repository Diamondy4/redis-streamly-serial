{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BlockArguments #-}

module Database.Redis.Streams.Streamly.Serialize where

import           Codec.Winery
import           Data.ByteString                ( ByteString )
import           Data.Function
import           Database.Redis                 ( Redis )
import qualified Database.Redis                as Redis
import qualified Database.Redis.Streams.Streamly
                                               as SRedis
import           Streamly.Prelude               ( IsStream )
import qualified Streamly.Prelude              as Streamly

readStream
    :: (IsStream t, Serialise a) => String -> t Redis (Either WineryException a)
readStream streamIn =
      -- Key should be "data", but not checked for performance
                      SRedis.readStream streamIn
    & Streamly.map \(_key, value) -> deserialise value

sendStream :: Serialise a => String -> Streamly.SerialT Redis a -> Redis ()
sendStream streamOut stream =
    stream
        & Streamly.map (\x -> ("data", serialise x))
        & SRedis.sendStream streamOut
