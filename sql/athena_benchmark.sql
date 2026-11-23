-- Partitioning benchmark: layout A (trade_date) vs layout B (trade_date, symbol).
-- Run each statement separately in the market-lab-wg workgroup; compare "Data scanned".

-- Layout A
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.trades_a (
  symbol        string,
  trade_id      bigint,
  event_time_us bigint,
  price         decimal(20,8),
  quantity      decimal(20,8)
)
PARTITIONED BY (trade_date string)
STORED AS PARQUET
LOCATION 's3://BUCKET/runs/layout_a/trades/';

MSCK REPAIR TABLE market_lab.trades_a;

-- Layout B: symbol is a partition column, so it is not stored in the Parquet files
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.trades_b (
  trade_id      bigint,
  event_time_us bigint,
  price         decimal(20,8),
  quantity      decimal(20,8)
)
PARTITIONED BY (trade_date string, symbol string)
STORED AS PARQUET
LOCATION 's3://BUCKET/runs/layout_b/trades/';

MSCK REPAIR TABLE market_lab.trades_b;

-- Same query on both layouts
SELECT count(*) AS n, sum(quantity) AS volume
FROM market_lab.trades_a
WHERE trade_date = '2025-01-02' AND symbol = 'SOLUSDT';

SELECT count(*) AS n, sum(quantity) AS volume
FROM market_lab.trades_b
WHERE trade_date = '2025-01-02' AND symbol = 'SOLUSDT';
