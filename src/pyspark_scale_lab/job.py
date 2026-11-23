from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StructField,
    StructType,
)

RAW_SCHEMA = StructType(
    [
        StructField("trade_id", LongType(), False),
        StructField("price", DecimalType(20, 8), False),
        StructField("quantity", DecimalType(20, 8), False),
        StructField("quote_qty", DecimalType(20, 8), True),
        StructField("event_time_us", LongType(), False),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("is_best_match", BooleanType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Binance trades -> trades + 1m OHLCV bars")
    p.add_argument("--input", required=True, help="raw CSV root; symbol is in the path")
    p.add_argument("--output", required=True, help="output root (local path or s3://...)")
    p.add_argument("--layout", choices=["date", "date_symbol"], default="date")
    p.add_argument("--has-header", action="store_true", help="set if the CSV has a header row")
    return p.parse_args()


def read_trades(spark: SparkSession, input_path: str, has_header: bool) -> DataFrame:
    """Read raw CSVs (recursively, one subdir per symbol) and derive time columns."""

    symbol_from_path = F.element_at(F.split(F.input_file_name(), "/"), -2)

    df = (
        spark.read.option("header", str(has_header).lower())
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(RAW_SCHEMA)
        .csv(input_path)
        .withColumn("symbol", symbol_from_path)
    )

    return with_time_columns(df)


def with_time_columns(df: DataFrame) -> DataFrame:
    """Derive the UTC trade date and the 1-minute bucket from event_time_us (microseconds).

    Both depend on spark.sql.session.timeZone, which main() pins to UTC.
    """
    event_ts = F.timestamp_micros(F.col("event_time_us"))
    return df.withColumn("trade_date", F.to_date(event_ts)).withColumn("minute", F.date_trunc("minute", event_ts))


def validate_trades(trades: DataFrame) -> None:
    """Fail the batch loudly if data-quality invariants are violated."""
    agg = trades.select(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("bad_price"),
        F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias("bad_qty"),
        F.sum(F.when(F.col("event_time_us").isNull(), 1).otherwise(0)).alias("null_time"),
    ).collect()[0]

    dup = trades.groupBy("symbol", "trade_id").count().where(F.col("count") > 1).count()

    problems = []
    if agg["n"] == 0:
        problems.append("empty input")
    if agg["bad_price"]:
        problems.append(f"{agg['bad_price']} rows with price<=0")
    if agg["bad_qty"]:
        problems.append(f"{agg['bad_qty']} rows with quantity<=0")
    if agg["null_time"]:
        problems.append(f"{agg['null_time']} null event_time_us")
    if dup:
        problems.append(f"{dup} duplicate (symbol, trade_id)")
    if problems:
        raise ValueError("DATA QUALITY FAILED: " + "; ".join(problems))


def build_bars(trades: DataFrame) -> DataFrame:
    order_key = F.struct("event_time_us", "trade_id")

    return trades.groupBy("symbol", "minute", "trade_date").agg(
        F.min_by("price", order_key).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order_key).alias("close"),
        F.sum("quantity").alias("volume"),
        F.count("*").alias("trade_count"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("market-trades")
        # to_date/date_trunc use the session time zone: pin UTC so every runtime agrees
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    trades = read_trades(spark, args.input, args.has_header)

    trades_out = trades.select(
        "symbol",
        "trade_id",
        "event_time_us",
        "price",
        "quantity",
        "trade_date",
    )
    validate_trades(trades_out)  # fail fast BEFORE writing anything

    bars_out = build_bars(trades).select(
        "symbol",
        "minute",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
    )

    partition_cols = ["trade_date"] if args.layout == "date" else ["trade_date", "symbol"]
    out = args.output.rstrip("/")

    (trades_out.write.mode("overwrite").partitionBy(*partition_cols).parquet(f"{out}/trades"))
    (bars_out.write.mode("overwrite").partitionBy(*partition_cols).parquet(f"{out}/bars_1m"))

    spark.stop()


if __name__ == "__main__":
    main()
