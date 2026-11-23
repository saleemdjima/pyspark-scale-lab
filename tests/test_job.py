"""Unit tests for the pure transformations in job.py (no file I/O)."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

from pyspark_scale_lab.job import build_bars, validate_trades, with_time_columns

# 2025-01-01T00:00:00Z in microseconds since epoch
DAY_START_US = 1_735_689_600_000_000
SECOND_US = 1_000_000

TRADES_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("trade_id", LongType(), False),
        StructField("event_time_us", LongType(), False),
        StructField("price", DecimalType(20, 8), False),
        StructField("quantity", DecimalType(20, 8), False),
    ]
)


def make_trades(spark: SparkSession, rows: list[tuple[str, int, int, str, str]]) -> DataFrame:
    """Build a trades DataFrame from (symbol, trade_id, event_time_us, price, quantity) tuples."""
    typed = [(s, tid, t, Decimal(p), Decimal(q)) for s, tid, t, p, q in rows]
    return with_time_columns(spark.createDataFrame(typed, TRADES_SCHEMA))


def test_ohlc_is_deterministic_on_timestamp_ties(spark: SparkSession) -> None:
    # Same minute, two pairs of identical timestamps, rows deliberately out of order.
    t1 = DAY_START_US + 1 * SECOND_US
    t2 = DAY_START_US + 2 * SECOND_US
    trades = make_trades(
        spark,
        [
            ("BTCUSDT", 4, t2, "102", "1"),
            ("BTCUSDT", 2, t1, "101", "1"),
            ("BTCUSDT", 3, t2, "99", "1"),
            ("BTCUSDT", 1, t1, "100", "1"),
        ],
    )

    [bar] = build_bars(trades).collect()

    assert bar["open"] == Decimal("100")  # (t1, trade_id=1) is the first trade, not trade 2
    assert bar["high"] == Decimal("102")
    assert bar["low"] == Decimal("99")
    assert bar["close"] == Decimal("102")  # (t2, trade_id=4) is the last trade, not trade 3
    assert bar["trade_count"] == 4


def test_vwap_is_exact(spark: SparkSession) -> None:
    trades = make_trades(
        spark,
        [
            ("ETHUSDT", 1, DAY_START_US, "100", "1"),
            ("ETHUSDT", 2, DAY_START_US + SECOND_US, "200", "3"),
        ],
    )

    [bar] = build_bars(trades).collect()

    # (100*1 + 200*3) / (1 + 3) = 175
    assert bar["vwap"] == Decimal("175")
    assert bar["volume"] == Decimal("4")


def test_bars_are_split_per_minute_and_symbol(spark: SparkSession) -> None:
    last_us_of_minute = DAY_START_US + 60 * SECOND_US - 1
    trades = make_trades(
        spark,
        [
            ("BTCUSDT", 1, DAY_START_US, "100", "1"),
            ("BTCUSDT", 2, last_us_of_minute, "101", "1"),  # still minute 00:00
            ("BTCUSDT", 3, last_us_of_minute + 1, "102", "1"),  # first µs of minute 00:01
            ("ETHUSDT", 1, DAY_START_US, "10", "1"),  # same trade_id, other symbol
        ],
    )

    # Format the minute inside Spark (session time zone = UTC). collect() would turn timestamps
    # into naive datetimes in the *machine's* local time zone, making the test machine-dependent.
    rows = build_bars(trades).select("symbol", F.date_format("minute", "HH:mm").alias("hhmm"), "trade_count").collect()
    bars = {(r["symbol"], r["hhmm"]): r["trade_count"] for r in rows}

    assert bars == {
        ("BTCUSDT", "00:00"): 2,
        ("BTCUSDT", "00:01"): 1,
        ("ETHUSDT", "00:00"): 1,
    }


def test_trade_date_is_utc(spark: SparkSession) -> None:
    # 23:30 UTC — would be the next day in Europe/Paris if the session time zone weren't UTC.
    late_evening_us = DAY_START_US + (23 * 3600 + 30 * 60) * SECOND_US
    [row] = make_trades(spark, [("BTCUSDT", 1, late_evening_us, "100", "1")]).collect()

    assert row["trade_date"] == date(2025, 1, 1)


def test_validate_accepts_clean_data(spark: SparkSession) -> None:
    trades = make_trades(
        spark,
        [
            ("BTCUSDT", 1, DAY_START_US, "100", "1"),
            ("ETHUSDT", 1, DAY_START_US, "10", "2"),  # same trade_id on another symbol is fine
        ],
    )

    validate_trades(trades)  # must not raise


def test_validate_rejects_duplicate_trade_id(spark: SparkSession) -> None:
    trades = make_trades(
        spark,
        [
            ("BTCUSDT", 1, DAY_START_US, "100", "1"),
            ("BTCUSDT", 1, DAY_START_US, "100", "1"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate"):
        validate_trades(trades)


@pytest.mark.parametrize(
    ("price", "quantity", "message"),
    [
        ("0", "1", "price<=0"),
        ("-5", "1", "price<=0"),
        ("100", "0", "quantity<=0"),
    ],
)
def test_validate_rejects_non_positive_values(spark: SparkSession, price: str, quantity: str, message: str) -> None:
    trades = make_trades(spark, [("BTCUSDT", 1, DAY_START_US, price, quantity)])

    with pytest.raises(ValueError, match=message):
        validate_trades(trades)


def test_validate_rejects_empty_input(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="empty input"):
        validate_trades(make_trades(spark, []))
