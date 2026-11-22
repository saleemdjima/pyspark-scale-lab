-- sql/athena_tables.sql


-- TABLE trades 
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.trades (
  symbol         string,
  trade_id       bigint,
  event_time_us  bigint,
  price          decimal(20,8),
  quantity       decimal(20,8)
)
PARTITIONED BY (trade_date string)
STORED AS PARQUET
LOCATION 's3://BUCKET/catalog/trades/';

-- TABLE bars_1m
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.bars_1m (
  symbol       string,
  minute       timestamp,
  open         decimal(20,8),
  high         decimal(20,8),
  low          decimal(20,8),
  close        decimal(20,8),
  volume       decimal(38,8),
  trade_count  bigint,
  vwap         decimal(38,18)
)
PARTITIONED BY (trade_date string)
STORED AS PARQUET
LOCATION 's3://BUCKET/catalog/bars_1m/';

-- Register the run partition at its exact S3 path.
-- Do not use MSCK REPAIR; it searches under catalog/, not runs/.
ALTER TABLE market_lab.trades ADD IF NOT EXISTS
  PARTITION (trade_date='2025-01-02')
  LOCATION 's3://BUCKET/runs/test/trades/trade_date=2025-01-02/';

ALTER TABLE market_lab.bars_1m ADD IF NOT EXISTS
  PARTITION (trade_date='2025-01-02')
  LOCATION 's3://BUCKET/runs/test/bars_1m/trade_date=2025-01-02/';

-- Query A: BTC trades from 00:00 to 00:15 UTC
SELECT count(*)                               AS trade_count,
       sum(quantity)                          AS volume,
       sum(price*quantity)/nullif(sum(quantity),0) AS vwap
FROM market_lab.trades
WHERE trade_date = '2025-01-02'
  AND symbol = 'BTCUSDT'
  AND event_time_us >= 1735776000000000   -- 2025-01-02 00:00:00 UTC (µs)
  AND event_time_us <  1735776900000000;  -- +15 min

-- Query B: daily activity for all symbols
SELECT symbol,
       count(*)      AS trades,
       sum(quantity) AS volume
FROM market_lab.trades
WHERE trade_date = '2025-01-02'
GROUP BY symbol
ORDER BY trades DESC;

-- After each query, note the "Data scanned" (bytes scanned) shown by Athena:
-- this is your cost/performance metric and the basis for the partitioning benchmark (Phase 4).