{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Redis.Streams.Serialize where

import           Codec.Winery
import           Data.ByteString                ( ByteString )
import           Database.Redis                 ( Redis
                                                , RedisCtx
                                                )
import qualified Database.Redis.Streams        as SRedis


sendUpstream
    :: (RedisCtx m (Either er), Show er, Serialise a)
    => String
    -> a
    -> m ByteString
sendUpstream streamOut x = SRedis.sendUpstream streamOut "data" (serialise x)
