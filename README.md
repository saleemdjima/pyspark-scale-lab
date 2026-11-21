# PySpark Scale Lab

PySpark Scale Lab processes Binance spot trade data into normalized trades and
one-minute OHLCV bars. The same Spark job is intended to run locally for
development and on AWS EMR Serverless; only the input and output paths change.

## Project layout

- `data.sh`: downloads and verifies Binance daily trade archives.
- `data/daily/<date>/<symbol>/`: raw CSV data and its checksum.
- `src/pyspark_scale_lab/job.py`: PySpark market-trades transformation job.
- `runs/<attempt>/`: local Parquet job outputs.
- `infra/`: infrastructure definitions.
- `sql/`: SQL analysis and transformations.

## Requirements

- Python 3.14
- [uv](https://docs.astral.sh/uv/)
- Java for PySpark/Spark
- `curl`, `unzip`, and `sha256sum`

Install the Python dependencies:

```bash
uv sync
```

## Download trade data

The download script currently retrieves BTCUSDT, ETHUSDT, and SOLUSDT for
2025-01-02:

```bash
bash data.sh
```

It downloads the ZIP and `.CHECKSUM` files, verifies the ZIP checksum, and
extracts the CSV files under `data/daily/`.

## Run the Spark job locally

From the project root:

```bash
uv run spark-submit \
  src/pyspark_scale_lab/job.py \
  --input data/daily/2025-01-02/ \
  --output runs/test \
  --date 2025-01-02 \
  --layout date
```

The job validates the input before writing and creates two partitioned Parquet
datasets:

- `runs/test/trades/`
- `runs/test/bars_1m/`

## Transformation details

The raw Binance trade schema is interpreted as:

- trade ID
- price
- quantity
- quote quantity
- event time in microseconds
- buyer-maker flag
- best-match flag

The job derives UTC trade dates and minute buckets, rejects empty or invalid
input, and checks for duplicate `(symbol, trade_id)` records. It writes the
normalized trades and 1-minute OHLCV bars grouped by symbol and minute.

OHLCV fields include `open`, `high`, `low`, `close`, `volume`, `trade_count`,
and `vwap`.

## Partition layouts

Use `--layout date` for date-only partitions:

```text
trade_date=<YYYY-MM-DD>/
```

Use `--layout date_symbol` for date and symbol partitions:

```text
trade_date=<YYYY-MM-DD>/symbol=<SYMBOL>/
```

## AWS-compatible output

The processing code does not set a local Spark master. Use the same command
shape and pass an S3 output path when running on EMR Serverless:

```text
--input s3://<bucket>/<raw-prefix>/
--output s3://<bucket>/<output-prefix>/
```

