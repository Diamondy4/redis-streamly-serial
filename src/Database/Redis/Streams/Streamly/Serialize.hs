{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module Database.Redis.Streams.Streamly.Serialize where

import           Codec.Winery
import           Data.ByteString                ( ByteString )
import           Data.Function
import           Database.Redis                 ( Redis )
import qualified Database.Redis                as Redis
import qualified Database.Redis.Streams.Streamly
                                               as SRedis
import qualified Streamly.Data.Unfold          as Unfold
import           Streamly.Prelude               ( IsStream )
import qualified Streamly.Prelude              as Streamly

type StreamName = String

readStream
    :: forall a t
     . (IsStream t, Serialise a)
    => StreamName
    -> t Redis (ByteString, Either WineryException a)
readStream streamIn = readStreamFrom streamIn "$"

readStreamFrom
    :: forall a t
     . (IsStream t, Serialise a)
    => StreamName
    -> ByteString
    -> t Redis (ByteString, Either WineryException a)
readStreamFrom streamIn startMsgId =
      -- Key should be "data", but not checked for performance
    SRedis.readStreamStartingFrom streamIn startMsgId
        & Streamly.map (\Redis.StreamsRecord {..} -> (recordId, ) <$> keyValues)
        & Streamly.unfoldMany Unfold.fromList
        & Streamly.map \(msgId, (_key, value)) -> (msgId, deserialise value)

sendStream
    :: (IsStream t, Serialise a)
    => StreamName
    -> t Redis a
    -> t Redis (Either Redis.Reply ByteString)
sendStream streamOut stream =
    stream
        & Streamly.map (\x -> ("data", serialise x))
        & SRedis.sendStream streamOut
