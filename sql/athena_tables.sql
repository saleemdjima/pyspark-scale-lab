-- Tables over the job output. Replace BUCKET with `terraform -chdir=infra output -raw bucket_name`.

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

-- Partitions live under runs/, outside the table LOCATION, so MSCK REPAIR cannot find them.
ALTER TABLE market_lab.trades ADD IF NOT EXISTS
  PARTITION (trade_date='2025-01-02')
  LOCATION 's3://BUCKET/runs/test/trades/trade_date=2025-01-02/';

ALTER TABLE market_lab.bars_1m ADD IF NOT EXISTS
  PARTITION (trade_date='2025-01-02')
  LOCATION 's3://BUCKET/runs/test/bars_1m/trade_date=2025-01-02/';

-- BTCUSDT, 00:00-00:15 UTC
SELECT count(*)                               AS trade_count,
       sum(quantity)                          AS volume,
       sum(price*quantity)/nullif(sum(quantity),0) AS vwap
FROM market_lab.trades
WHERE trade_date = '2025-01-02'
  AND symbol = 'BTCUSDT'
  AND event_time_us >= 1735776000000000
  AND event_time_us <  1735776900000000;

-- Daily activity per symbol
SELECT symbol,
       count(*)      AS trades,
       sum(quantity) AS volume
FROM market_lab.trades
WHERE trade_date = '2025-01-02'
GROUP BY symbol
ORDER BY trades DESC;
