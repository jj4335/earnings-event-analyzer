"""
spark_jobs/03_event_windows.py
──────────────────────────────
Stage : silver → gold (event_day_metrics)
Input : silver/prices/, silver/earnings/
Output: gold/event_day_metrics/

For each earnings event, generates one row per trading day in
[-EVENT_WINDOW_PRE, +EVENT_WINDOW_POST] and joins with price data.
This is an Event Study methodology implementation using
explode(sequence(...)).

Run:
    python spark_jobs/03_event_windows.py
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType
from pyspark.sql.window import Window

from spark_jobs.utils import get_spark
from config import (
    DATA_SILVER,
    DATA_GOLD,
    ANALYSIS_END,
    EVENT_WINDOW_PRE,
    EVENT_WINDOW_POST,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_event_windows(spark):
    """
    Step 1: Expand each earnings event into a [-5, +5] day window.
    Uses explode(sequence(...)) — same pattern as member_days in hw2.
    """
    log.info("Reading silver/earnings …")
    earnings = spark.read.parquet(str(DATA_SILVER / "earnings"))

    analysis_end = F.lit(ANALYSIS_END).cast(DateType())

    event_days = (
        earnings
        .withColumn("window_start", F.date_sub(F.col("event_date"), EVENT_WINDOW_PRE))
        .withColumn(
            "window_end",
            # Clamp to analysis end — same as clamping to report_end_excl in hw2
            F.least(
                F.date_add(F.col("event_date"), EVENT_WINDOW_POST),
                analysis_end
            )
        )
        # Only expand windows where start < end
        .filter(F.col("window_start") < F.col("window_end"))
        .withColumn(
            "day",
            F.explode(
                F.sequence(F.col("window_start"), F.col("window_end"))
            )
        )
        # relative_day: -5 to +5 (0 = event date)
        .withColumn(
            "relative_day",
            F.datediff(F.col("day"), F.col("event_date"))
        )
        .select(
            "event_id",
            "ticker",
            "event_date",
            "fiscal_quarter",
            "cap_bucket",
            "day",
            "relative_day",
        )
    )

    log.info("Event windows expanded: %d rows", event_days.count())
    return event_days


def join_prices(spark, event_days):
    """
    Step 2: Join event windows with daily price data.
    Left join so we keep all event-day rows even on non-trading days.
    """
    log.info("Reading silver/prices …")
    prices = spark.read.parquet(str(DATA_SILVER / "prices"))

    # Keep only the columns we need from prices
    prices_slim = prices.select(
        "ticker", "trade_date", "open", "high", "low", "close", "volume"
    )

    joined = event_days.join(
        prices_slim,
        on=(
            (event_days["ticker"] == prices_slim["ticker"]) &
            (event_days["day"] == prices_slim["trade_date"])
        ),
        how="left",
    ).drop(prices_slim["ticker"])  # avoid ambiguous column

    return joined


def compute_returns(df):
    """
    Step 3: Compute daily return and cumulative return per event.
    Uses lag() window function — ordered by day within each event.
    """
    w_event = (
        Window
        .partitionBy("event_id", "ticker")
        .orderBy("day")
    )
    w_event_cum = (
        Window
        .partitionBy("event_id", "ticker")
        .orderBy("day")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    result = (
        df
        .withColumn("prev_close", F.lag("close", 1).over(w_event))

        # daily_return: null if prev_close is null (first row) or close is null
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull() & F.col("close").isNotNull(),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close")
            ).otherwise(F.lit(None).cast(DecimalType(10, 6)))
        )

        # cum_return: cumulative sum of daily_return from window start
        .withColumn(
            "cum_return",
            F.sum("daily_return").over(w_event_cum)
        )

        # volume_ratio: volume vs. average volume over the entire event window
        .withColumn(
            "avg_window_volume",
            F.avg("volume").over(
                Window.partitionBy("event_id", "ticker")
            )
        )
        .withColumn(
            "volume_ratio",
            F.when(
                F.col("avg_window_volume").isNotNull() & (F.col("avg_window_volume") > 0),
                F.col("volume") / F.col("avg_window_volume")
            ).otherwise(F.lit(None).cast(DecimalType(10, 4)))
        )
        .drop("avg_window_volume", "prev_close")
    )

    return result


def main():
    spark = get_spark("03_event_windows")

    event_days   = build_event_windows(spark)
    joined       = join_prices(spark, event_days)
    event_metrics = compute_returns(joined)

    out = DATA_GOLD / "event_day_metrics"
    (
        event_metrics
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(str(out))
    )

    log.info(
        "gold/event_day_metrics written: %d rows → %s",
        event_metrics.count(),
        out,
    )

    spark.stop()
    log.info("03_event_windows complete.")


if __name__ == "__main__":
    main()
