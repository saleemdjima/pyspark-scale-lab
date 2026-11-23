# PySpark Scale Lab

![ci](https://github.com/saleemdjima/pyspark-scale-lab/actions/workflows/ci.yaml/badge.svg)

PySpark Scale Lab turns Binance spot-trade archives into typed trade records and
one-minute OHLCV bars. The Spark job writes partitioned Parquet files locally
or to S3, and Terraform defines the AWS resources used to run and query the
pipeline.

The pipeline covers three symbols, `BTCUSDT`, `ETHUSDT` and `SOLUSDT`, at two
scales: one day (2025-01-02) for local development, and the full month of
January 2025 (25.6 GB of CSV) processed on EMR Serverless.

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
airflow/dags/                  Airflow DAG that submits the job to EMR Serverless
tests/                         pytest unit tests for the transformations
infra/                         Terraform resources and AWS outputs
sql/                           Athena table definitions and benchmark queries
data/                          Downloaded archives and extracted CSV files
runs/                          Local Parquet output
```

`data/` and `runs/` are generated directories and are ignored by Git.

## Tests and CI

The transformations are covered by pytest unit tests that build small DataFrames
in memory, so no data download is needed:

```bash
uv run pytest -v
```

They check that tied timestamps produce deterministic open and close prices,
that VWAP is exact, that trades are bucketed by UTC minute and date, and that
each data-quality rule rejects bad input.

GitHub Actions runs the same checks on every pull request: `ruff check`,
`ruff format --check`, `mypy` in strict mode, `pytest`, and
`terraform fmt -check` plus `terraform validate`. The Terraform job uses
`init -backend=false` and needs no AWS credentials.

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

Copy `src/pyspark_scale_lab/job.py` to `s3://<bucket>/code/job.py` and upload
the raw CSV data, then submit the job:

```bash
bash scripts/launch_job.sh
```

By default the script reads the application ID, execution role and bucket from
Terraform outputs and processes the 2025-01-02 daily sample. Override `INPUT`,
`OUTPUT` and `NAME` to run another dataset. The script prints the job run ID and
its initial state; it does not wait for completion.

## Scaling to a full month

The same `job.py` runs unchanged on the January 2025 month. Download and verify
the archives, then upload only the extracted CSV files:

```bash
bash scripts/download_monthly_data.sh
aws s3 sync data/monthly/ s3://<bucket>/raw/monthly/ --exclude '*' --include '*.csv'
```

| Symbol | CSV size |
|---|---|
| BTCUSDT | 10.4 GB |
| SOLUSDT | 8.1 GB |
| ETHUSDT | 7.1 GB |
| **Total** | **25.6 GB** |

Submit it with:

```bash
INPUT=s3://<bucket>/raw/monthly/ OUTPUT=s3://<bucket>/runs/emr-monthly/ NAME=monthly \
  bash scripts/launch_job.sh
```

Baseline run on EMR Serverless with the default application settings:

| Metric | Value |
|---|---|
| Wall-clock duration | 553 s |
| vCPU-hours | 2.44 |
| Memory GB-hours | 9.74 |

The job reads the input through four separate actions (two data-quality checks
and two writes) without caching, so the CSV is likely parsed four times. That is
the first optimization to measure.

## Orchestrate with Airflow

`airflow/dags/market_trades_emr.py` submits the same job with
`EmrServerlessStartJobOperator` and waits for it to finish. Account-specific
values are not hardcoded: they are Jinja templates resolved from Airflow
Variables when the task runs. Provide them from Terraform outputs, for example
as environment variables:

```bash
export AIRFLOW_VAR_MARKET_LAB_BUCKET=$(terraform -chdir=infra output -raw bucket_name)
export AIRFLOW_VAR_EMR_APPLICATION_ID=$(terraform -chdir=infra output -raw emr_application_id)
export AIRFLOW_VAR_EMR_EXECUTION_ROLE_ARN=$(terraform -chdir=infra output -raw emr_execution_role_arn)
```

To publish a local run:

```bash
bash scripts/publish_to_s3.sh test
```

The argument is the run directory under `runs/` (default: `test`).

## Query with Athena

Replace `BUCKET` in `sql/athena_tables.sql` with the name returned by:

```bash
terraform -chdir=infra output -raw bucket_name
```

The SQL file creates `trades` and `bars_1m` external tables and registers
partition locations for `runs/test`. Run it in the `market-lab-wg` workgroup,
after the matching Parquet files have been uploaded to S3.

The trade and bar partitions are written under `runs/`, outside the tables'
`LOCATION`, so they are registered with `ALTER TABLE ... ADD PARTITION` rather
than `MSCK REPAIR TABLE`.

## Partitioning benchmark

`sql/athena_benchmark.sql` writes the same trades with two layouts and runs the
same single-symbol query on both (replace `BUCKET` there as well):

| Layout | Partitions | Data scanned for one symbol and one day |
|---|---|---|
| A | `trade_date` | 4.12 MB |
| B | `trade_date`, `symbol` | 4.15 MB |

Partitioning by symbol brought no gain. Partition pruning already skips the
other dates, and Athena uses the min/max statistics of each Parquet row group to
skip the row groups of other symbols. Layout A is kept: it is simpler and
produces fewer small files.

## Implementation notes

- The raw CSV schema is explicit rather than inferred.
- Prices and quantities use Spark decimal types to avoid floating-point rounding
  drift.
- OHLCV open and close values use `event_time_us` and `trade_id` as an ordering
  key, making tied trades deterministic.
- The job derives UTC trade dates and minute buckets from Binance microsecond
  timestamps, and pins `spark.sql.session.timeZone` to UTC: `to_date` and
  `date_trunc` follow the session time zone, so a 23:30 UTC trade would
  otherwise land in the next day's partition on a machine set to Europe/Paris.
- S3 paths are passed through the same CLI flags as local paths.
- Athena table paths and bucket-specific values must be kept in sync with the
  Terraform and run outputs.
