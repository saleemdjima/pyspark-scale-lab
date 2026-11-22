# PySpark Scale Lab

PySpark Scale Lab turns Binance spot-trade archives into typed trade records and
one-minute OHLCV bars. The Spark job writes partitioned Parquet files locally
or to S3, and Terraform defines the AWS resources used to run and query the
pipeline.

For 2025-01-02, the included download script retrieves three symbols:
`BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.

## How the data moves

```text
Binance daily archives
        |
        v
CSV files grouped by symbol
        |
        v
PySpark validation and transformation
        |
        +--> normalized trades
        |
        +--> 1-minute OHLCV bars
        |
        v
Partitioned Parquet in local storage or S3
        |
        v
Amazon Athena through a Glue database
```

## What the job produces

The `trades` dataset contains:

- `symbol`
- `trade_id`
- `event_time_us`
- `price`
- `quantity`
- `trade_date`

The `bars_1m` dataset contains:

- `symbol`
- `minute`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `trade_count`
- `vwap`

The job also performs a small set of data-quality checks before writing:

- the input is not empty
- prices and quantities are positive
- `event_time_us` is present
- `(symbol, trade_id)` pairs are not duplicated

## Requirements

The local setup assumes Linux or WSL with:

- Python 3.14
- [uv](https://docs.astral.sh/uv/)
- Java 17
- `curl`
- `unzip`
- `sha256sum`

Install the project dependencies:

```bash
uv sync
```

AWS workflows additionally require:

- AWS CLI v2
- Terraform 1.5 or newer
- credentials with permission to create and use the resources in `infra/`

## Quick start

From the project root, download the example daily data:

```bash
bash scripts/download_data.sh
```

The script verifies each downloaded archive against Binance's checksum before
extracting it. Extracted files are placed under:

```text
data/daily/2025-01-02/<SYMBOL>/
```

Run the Spark job locally:

```bash
uv run spark-submit \
  src/pyspark_scale_lab/job.py \
  --input data/daily/2025-01-02/ \
  --output runs/test \
  --layout date
```

Two datasets are written:

```text
runs/test/trades/
runs/test/bars_1m/
```

With `--layout date`, output is partitioned by trade date:

```text
trade_date=2025-01-02/
```

With `--layout date_symbol`, it is partitioned by trade date and symbol:

```text
trade_date=2025-01-02/symbol=BTCUSDT/
```

## Repository layout

```text
src/pyspark_scale_lab/job.py  Spark transformation and data-quality checks
scripts/download_data.sh       Download and verify one UTC day
scripts/download_monthly_data.sh
                               Download and verify the January 2025 month
scripts/publish_to_s3.sh       Copy a local run to S3
scripts/launch_job.sh          Start the configured EMR Serverless job
infra/                         Terraform resources and AWS outputs
sql/                           Athena table definitions and benchmark queries
data/                          Downloaded archives and extracted CSV files
runs/                          Local Parquet output
```

`data/` and `runs/` are generated directories and are ignored by Git.

## AWS setup

Initialize and create the infrastructure:

```bash
terraform -chdir=infra init
terraform -chdir=infra apply
```

The Terraform configuration creates:

- a private S3 bucket with server-side encryption
- a Glue database named `market_lab`
- an Athena workgroup named `market-lab-wg`
- an EMR Serverless Spark application
- an execution role for EMR jobs
- an Athena query-result location and a one-gigabyte per-query scan cutoff

The EMR application stops after 15 minutes of inactivity.

## Run on EMR Serverless

Copy the Spark source to S3, upload the raw CSV data, and update the entry-point
and input paths in `scripts/launch_job.sh` if needed. The script can then
submit the existing job:

```bash
bash scripts/launch_job.sh
```

It reads the EMR application ID and execution role from Terraform outputs,
starts `market-trades-emr`, and prints the returned job run ID.

To publish a local run:

```bash
bash scripts/publish_to_s3.sh test 2025-01-02
```

The first argument is the run directory under `runs/`; the second is recorded
for the command interface and is not used to select output files.

## Query with Athena

Replace `BUCKET` in `sql/athena_tables.sql` with the name returned by:

```bash
terraform -chdir=infra output -raw bucket_name
```

The SQL file creates `trades` and `bars_1m` external tables and registers
partition locations for `runs/test`. Run it in the `market-lab-wg` workgroup,
after the matching Parquet files have been uploaded to S3.

`sql/athena_benchmark.sql` contains separate table layouts for comparing
date-only and date-plus-symbol partitioning. It is a benchmark helper, not a
required step for querying the pipeline output.

## Implementation notes

- The raw CSV schema is explicit rather than inferred.
- Prices and quantities use Spark decimal types to avoid floating-point rounding
  drift.
- OHLCV open and close values use `event_time_us` and `trade_id` as an ordering
  key, making tied trades deterministic.
- The job derives UTC trade dates and minute buckets from Binance microsecond
  timestamps.
- S3 paths are passed through the same CLI flags as local paths.
- Athena table paths and bucket-specific values must be kept in sync with the
  Terraform and run outputs.
