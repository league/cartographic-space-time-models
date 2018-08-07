{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

import Control.Arrow
import Control.Monad.IO.Class
import Control.Monad.Logger
import Control.Monad.Trans.Reader
import Data.Monoid ((<>))
import Database.Persist
import Database.Persist.Postgresql
import Database.Persist.TH
import Data.Text (Text)
import qualified Data.Text.IO as TIO
import Data.Time.Clock
import Data.Time.LocalTime
import Data.Time.Calendar

{-
 Following data declarations can be used to *create* the Turnstile
 table, but the others are views created in Postgresql:

CREATE OR REPLACE VIEW public.station AS
 SELECT DISTINCT turnstile.station,
    turnstile.line_name
   FROM turnstile

CREATE OR REPLACE VIEW public.grouped_turnstile AS
 SELECT sum(turnstile.entries) AS sum_entries,
    sum(turnstile.exits) AS sum_exits,
    turnstile.date,
    turnstile."time",
    turnstile.station,
    turnstile.line_name
   FROM turnstile
  GROUP BY turnstile.date, turnstile."time", turnstile.station, turnstile.line_name

Here is how we import data directly from the CSV files downloaded from
http://web.mta.info/developers/turnstile.html :

\copy turnstile from turnstile_170812.txt csv header
\copy turnstile from turnstile_170819.txt csv header
\copy turnstile from turnstile_170826.txt csv header
etc.

Once the data are available in the database, commands like these can
be used in GHCI (Haskell REPL) to retrieve the preprocessed data.

ghci> runDB $ decumulateInt <$> listEntriesBy beach67A
[44,238,379,535,...]

-}


share [mkPersist sqlSettings] [persistLowerCase|
Turnstile
  controlArea  Text
  remoteUnit   Text
  subunitPos   Text
  station      Text
  lineName     Text
  division     Text
  descr        Text
  date         Day
  time         TimeOfDay
  entries      Int
  exits        Int
  Primary controlArea remoteUnit subunitPos date time
  deriving Show
GroupedTurnstile
  station      Text
  lineName     Text
  date         Day
  time         TimeOfDay
  sumEntries   Int
  sumExits     Int
  Primary station lineName date time
  deriving Show
Station
  station      Text
  lineName     Text
  Primary station lineName
  deriving Show
|]

type DB m a = ReaderT SqlBackend (LoggingT m) a

runDB :: DB IO a -> IO a
runDB act =
  runStderrLoggingT $
  filterLogger (const (>= LevelWarn)) $
  withPostgresqlConn "user=league dbname=turnstile" $
  runReaderT act

countRemote :: Text -> DB IO Int
countRemote r =
  count [TurnstileRemoteUnit ==. r]

listEntries :: Text -> Text -> Text -> DB IO [Int]
listEntries c r s =
  map (turnstileEntries . entityVal) <$>
  selectList [ TurnstileControlArea ==. c
             , TurnstileRemoteUnit ==. r
             , TurnstileSubunitPos ==. s
             ]
  [ Asc TurnstileDate
  , Asc TurnstileTime
  ]

decumulate :: (a -> a -> b) -> [a] -> [b]
decumulate sub xs =
  zipWith sub (tail xs) xs

decumulateInt :: [Int] -> [Int]
decumulateInt = decumulate (-)

decumulateTime :: [UTCTime] -> [NominalDiffTime]
decumulateTime = decumulate diffUTCTime

listEntriesBy :: Key Station -> DB IO [Int]
listEntriesBy st =
  map getSumEntries <$>
  listDataForStation st

getSumEntries :: Entity GroupedTurnstile -> Int
getSumEntries = groupedTurnstileSumEntries . entityVal

listTimestampsForStation :: Key Station -> DB IO [UTCTime]
listTimestampsForStation st =
  map getTime <$> listDataForStation st

getTime :: Entity GroupedTurnstile -> UTCTime
getTime = mkTime . (groupedTurnstileDate &&& groupedTurnstileTime) . entityVal

mkTime :: (Day, TimeOfDay) -> UTCTime
mkTime (d,t) = UTCTime d (timeOfDayToTime t)

listDataForStation :: Key Station -> DB IO [Entity GroupedTurnstile]
listDataForStation st =
  selectList [ GroupedTurnstileStation ==. stationKeystation st
             , GroupedTurnstileLineName ==. stationKeylineName st
             ]
  [ Asc GroupedTurnstileDate
  , Asc GroupedTurnstileTime
  ]

z1, z2, z3, z4 :: IO [Int]

z1 = runDB $ decumulateInt <$> listEntries "C021" "R212" "00-00-02"
z2 = runDB $ decumulateInt <$> listEntries "A002" "R051" "02-06-00"
z3 = runDB $ decumulateInt <$> listEntries "J025" "R003" "00-00-01"
z4 = runDB $ decumulateInt <$> listEntries "N006A" "R280" "00-00-01"

stations :: DB IO [Key Station]
stations =
  selectKeysList [] [Asc StationStation, Asc StationLineName]

printStation :: MonadIO m => Key Station -> m ()
printStation ks =
  liftIO $ TIO.putStrLn $ stationKeystation ks <> " " <> stationKeylineName ks

printAllStations :: IO ()
printAllStations = runDB (stations >>= mapM_ printStation)

isSuitable :: Key Station -> IO ()
isSuitable ks = do
  TIO.putStr $ stationKeystation ks <> " " <> stationKeylineName ks <> ": "
  es <- runDB $ decumulateInt <$> listEntriesBy ks
  if all (>= 0) es
    then
    do
      ts <- runDB $ decumulateTime <$> listTimestampsForStation ks
      if all (== 14400) ts
        then putStrLn $ "Checks out, " <> show (length es) <> " data points"
        else putStrLn "Oops, has irregular timing"
    else putStrLn "Oops, has negative entry count"

dekalb, unionSq :: Key Station
dekalb = StationKey "DEKALB AV" "BDNQR"
unionSq = StationKey "14 ST-UNION SQ" "456LNQRW"

springSt, atlanticL :: Key Station
springSt = StationKey "SPRING ST" "6"
atlanticL = StationKey "ATLANTIC AV" "L"

westchesterSq :: Key Station
westchesterSq = StationKey "WESTCHESTER SQ" "6"

st155BD       :: Key Station
av20D         :: Key Station
av20N         :: Key Station
st55D         :: Key Station
beach36A      :: Key Station
beach67A      :: Key Station
hewesSt       :: Key Station
pathWtc       :: Key Station
broadwayG     :: Key Station
clevelandSt   :: Key Station
bushwickAv    :: Key Station
cypressHills  :: Key Station
juniusSt      :: Key Station

st155BD       = StationKey "155 ST" "BD"
av20D         = StationKey "20 AV" "D"
av20N         = StationKey "20 AV" "N"
st55D         = StationKey "55 ST" "D"
beach36A      = StationKey "BEACH 36 ST" "A"
beach67A      = StationKey "BEACH 67 ST" "A"
hewesSt       = StationKey "HEWES ST" "JM"
pathWtc       = StationKey "PATH WTC" "1"
broadwayG     = StationKey "BROADWAY" "G"
clevelandSt   = StationKey "CLEVELAND ST" "J"
bushwickAv    = StationKey "BUSHWICK AV" "L"
cypressHills  = StationKey "CYPRESS HILLS" "J"
juniusSt      = StationKey "JUNIUS ST" "3"

-- leaving out pathWTC -- not enough data

suitableStations :: [Key Station]
suitableStations =
  [st155BD
  , av20D
  , av20N
  , st55D
  , beach36A
  , beach67A
  , hewesSt
  , broadwayG
  , clevelandSt
  , bushwickAv
  , cypressHills
  , juniusSt
  ]

-- Let's try to troubleshoot the twisting problem for av20D.
troubleshoot :: Key Station -> IO ()
troubleshoot st = do
  ts <- runDB $ listTimestampsForStation st
  putStrLn $ "Start: " <> show (head ts)
  let expected = iterate (addUTCTime 14400) (head ts)
      worst = maximum $ zipWith diffUTCTime expected ts
  putStrLn $ "Off by as much as: " <> show worst <> " = "
    <> show (realToFrac worst / 3600 :: Double) <> "h"

-- Instrumented to help with debugging
synchro :: [UTCTime] -> [Entity GroupedTurnstile] -> [(UTCTime, NominalDiffTime, UTCTime, Maybe Int)]
synchro _ [] = []
synchro [] _ = []
synchro (t:ts) (d:ds) =
  if abs deltaT < 7200
  then (t, deltaT, getTime d, Just (getSumEntries d)) : synchro ts ds
  else
    if getTime d < t
    then synchro (t:ts) ds
    else (t, deltaT, getTime d, Nothing) : synchro ts (d:ds)
  where
    deltaT = diffUTCTime t (getTime d)

synchroSums :: [UTCTime] -> [Entity GroupedTurnstile] -> [Maybe Int]
synchroSums ts es = map f $ synchro ts es
  where f (_, _, _, i) = i

interpolateSums :: [Maybe Int] -> [Int]
interpolateSums [] = []
interpolateSums [Just x] = [x]
interpolateSums (Just x : Nothing : Just y : rest) =
  x : ((x+y) `div` 2) : interpolateSums (Just y : rest)
interpolateSums (Just x : Nothing : Nothing : Just y : rest) =
  x : (x+oneThird) : (x+oneThird+oneThird) : interpolateSums (Just y : rest)
  where oneThird = (y - x) `div` 3
interpolateSums (Just x : rest) =
  x : interpolateSums rest
interpolateSums (Nothing : _) = error "interpolateSums unhandled"

synchroIO st = do
  dat <- runDB $ listDataForStation st
  let start = getTime $ head dat
      expected = iterate (addUTCTime 14400) start
  putStrLn $ "Start: " <> show start
  return $ decumulateInt $ interpolateSums $ synchroSums expected dat
