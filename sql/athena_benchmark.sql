-- sql/athena_benchmark.sql
-- Partitioning benchmark: layout A (trade_date) vs. layout B (trade_date, symbol)
-- Athena settings: region eu-central-1, workgroup market-lab-wg, database market_lab
-- Run each statement separately.

-- ============================================================
-- Layout A: partition by trade_date; symbol remains a regular column
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.trades_a (
  symbol        string,
  trade_id      bigint,
  event_time_us bigint,
  price         decimal(20,8),
  quantity      decimal(20,8)
)
PARTITIONED BY (trade_date string)
STORED AS PARQUET
LOCATION 's3://market-lab-09e954a4/runs/layout_a/trades/';

MSCK REPAIR TABLE market_lab.trades_a;

-- ============================================================
-- Layout B: partition by trade_date and symbol
-- Note: symbol is a partition column, so it is omitted from the regular
-- column list and encoded in the S3 path rather than stored in Parquet files.
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS market_lab.trades_b (
  trade_id      bigint,
  event_time_us bigint,
  price         decimal(20,8),
  quantity      decimal(20,8)
)
PARTITIONED BY (trade_date string, symbol string)
STORED AS PARQUET
LOCATION 's3://market-lab-09e954a4/runs/layout_b/trades/';

MSCK REPAIR TABLE market_lab.trades_b;

-- ============================================================
-- Benchmark: run the same query for the smaller symbol (SOLUSDT) in both layouts.
-- Record the "Data scanned" value displayed for each result.
-- The results (n, volume) should match, while scanned bytes should differ:
-- A scans all symbols for the date; B prunes partitions to SOLUSDT.
-- ============================================================

-- A) Layout A: symbol is not a partition, so all symbols for the date are scanned.
SELECT count(*) AS n, sum(quantity) AS volume
FROM market_lab.trades_a
WHERE trade_date = '2025-01-02' AND symbol = 'SOLUSDT';

-- B) Layout B: symbol partitioning enables pruning to SOLUSDT only.
SELECT count(*) AS n, sum(quantity) AS volume
FROM market_lab.trades_b
WHERE trade_date = '2025-01-02' AND symbol = 'SOLUSDT';

-- ============================================================
-- For the README, record results in results/benchmark.md:
--   layout | Data scanned (SOLUSDT) | file count | time
--   A      | ...                    | ...         | ...
--   B      | ...                    | ...         | ...
-- Expected conclusion: B scans less data through partition pruning, at the cost
-- of more files and partitions (potential small-file issues with many symbols).
-- ============================================================