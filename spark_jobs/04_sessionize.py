"""
spark_jobs/04_sessionize.py
───────────────────────────
Stage : silver → gold (trade_sessions)
Input : silver/prices/
Output: gold/trade_sessions/

Groups consecutive trades on the same ticker into sessions.
A new session starts when:
  1. It is the first trade for the ticker on that day.
  2. The gap from the previous trade exceeds SESSION_GAP_MINUTES (25 min).
  3. The local calendar date changes (America/New_York).

This is the same sessionization logic used in hw2 Problem 1,
applied to intraday trade data.

Note: Yahoo Finance provides daily OHLCV, not tick-level data.
      This job demonstrates the sessionization pattern using
      synthetic intraday rows derived from daily bars.
      Swap silver/prices with a real tick feed in production.

Run:
    python spark_jobs/04_sessionize.py
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from spark_jobs.utils import get_spark
from config import DATA_SILVER, DATA_GOLD, NYSE_TZ, SESSION_GAP_MINUTES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_intraday_proxy(spark):
    """
    Since Yahoo Finance gives daily bars, we synthesize intraday
    timestamps (open, high, low, close) for demonstration.
    In production, replace this with a real tick/minute feed.
    """
    log.info("Reading silver/prices …")
    prices = spark.read.parquet(str(DATA_SILVER / "prices"))

    # Represent open (09:30), high (11:00), low (14:00), close (16:00) NYSE
    # as synthetic "trade" events within the trading day.
    open_rows = (
        prices
        .withColumn("trade_ts",
            F.to_timestamp(F.concat(F.col("trade_date").cast("string"), F.lit(" 09:30:00"))))
        .withColumn("price", F.col("open"))
        .withColumn("trade_type", F.lit("open"))
    )
    high_rows = (
        prices
        .withColumn("trade_ts",
            F.to_timestamp(F.concat(F.col("trade_date").cast("string"), F.lit(" 11:00:00"))))
        .withColumn("price", F.col("high"))
        .withColumn("trade_type", F.lit("high"))
    )
    low_rows = (
        prices
        .withColumn("trade_ts",
            F.to_timestamp(F.concat(F.col("trade_date").cast("string"), F.lit(" 14:00:00"))))
        .withColumn("price", F.col("low"))
        .withColumn("trade_type", F.lit("low"))
    )
    close_rows = (
        prices
        .withColumn("trade_ts",
            F.to_timestamp(F.concat(F.col("trade_date").cast("string"), F.lit(" 16:00:00"))))
        .withColumn("price", F.col("close"))
        .withColumn("trade_type", F.lit("close"))
    )

    cols = ["ticker", "trade_date", "trade_ts", "price", "trade_type", "cap_bucket"]
    trades = (
        open_rows.select(cols)
        .union(high_rows.select(cols))
        .union(low_rows.select(cols))
        .union(close_rows.select(cols))
        .filter(F.col("price").isNotNull())
    )

    return trades


def sessionize(trades):
    """
    Assign session boundaries using the same 3-condition logic as hw2:
      1. prev_trade_ts is null (first event for ticker)
      2. gap_minutes > SESSION_GAP_MINUTES (25)
      3. local_date != prev_local_date (NY calendar date change)

    Uses cumulative sum over a window to assign session_index,
    then constructs session_id as ticker#000001 format.
    """
    # Window ordered by trade timestamp within each ticker
    w_ticker = (
        Window
        .partitionBy("ticker")
        .orderBy("trade_ts")
    )
    # Cumulative window for session index
    w_ticker_cum = (
        Window
        .partitionBy("ticker")
        .orderBy("trade_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    result = (
        trades
        # Convert UTC trade_ts to NYSE local time
        .withColumn(
            "local_ts",
            F.from_utc_timestamp(F.col("trade_ts"), NYSE_TZ)
        )
        .withColumn("local_date", F.to_date(F.col("local_ts")))

        # Lag columns for gap calculation
        .withColumn("prev_trade_ts",  F.lag("trade_ts").over(w_ticker))
        .withColumn("prev_local_date", F.lag("local_date").over(w_ticker))

        # gap_minutes: seconds difference / 60
        .withColumn(
            "gap_minutes",
            (F.unix_timestamp("trade_ts") - F.unix_timestamp("prev_trade_ts")) / 60.0
        )

        # Session boundary: any of the 3 conditions
        .withColumn(
            "is_session_start",
            F.when(F.col("prev_trade_ts").isNull(), 1)          # condition 1
             .when(F.col("gap_minutes") > SESSION_GAP_MINUTES, 1)  # condition 2
             .when(F.col("local_date") != F.col("prev_local_date"), 1)  # condition 3
             .otherwise(0)
        )

        # session_index: cumulative count of session starts
        .withColumn(
            "session_index",
            F.sum("is_session_start").over(w_ticker_cum)
        )

        # session_id: ticker#000001 format (matches hw2 format)
        .withColumn(
            "session_id",
            F.concat(
                F.col("ticker"),
                F.lit("#"),
                F.lpad(F.col("session_index").cast("string"), 6, "0")
            )
        )
        .drop("prev_local_date")
    )

    return result


def main():
    spark = get_spark("04_sessionize")

    trades     = build_intraday_proxy(spark)
    sessionized = sessionize(trades)

    out = DATA_GOLD / "trade_sessions"
    (
        sessionized
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(str(out))
    )

    # Quick sanity check
    session_count = sessionized.select("session_id").distinct().count()
    log.info(
        "gold/trade_sessions written: %d rows, %d distinct sessions → %s",
        sessionized.count(),
        session_count,
        out,
    )

    spark.stop()
    log.info("04_sessionize complete.")


if __name__ == "__main__":
    main()
