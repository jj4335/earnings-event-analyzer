"""
spark_jobs/05_cohort_metrics.py
───────────────────────────────
Stage : gold → gold (daily_cohort_metrics, event_summary)
Input : gold/event_day_metrics/
Output: gold/daily_cohort_metrics/  — aggregated by (relative_day, cap_bucket)
        gold/event_summary/         — one row per (ticker, event_id)

Produces two final analytical tables:

1. daily_cohort_metrics
   Aggregates event reactions by relative_day × cap_bucket.
   Same structure as daily_cohort_metrics in hw2 Problem 2.

2. event_summary
   One row per earnings event: pre/post 5-day cumulative return,
   volume ratio, used directly by the API for ticker-level queries.

Run:
    python spark_jobs/05_cohort_metrics.py
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from spark_jobs.utils import get_spark
from config import DATA_GOLD, EVENT_WINDOW_PRE, EVENT_WINDOW_POST

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_cohort_metrics(spark):
    """
    Aggregate event reactions by (relative_day, cap_bucket).

    For each (relative_day, cap_bucket) combination, compute:
      - event_count       : number of earnings events contributing
      - avg_return        : mean daily_return
      - median_return     : 50th percentile daily_return
      - avg_volume_ratio  : mean volume vs. event-window average
      - positive_rate     : fraction of events with daily_return > 0

    This is the same groupBy structure as daily_cohort_metrics in hw2,
    extended with financial-domain metrics.
    """
    log.info("Reading gold/event_day_metrics …")
    edf = spark.read.parquet(str(DATA_GOLD / "event_day_metrics"))

    cohort = (
        edf
        .filter(F.col("close").isNotNull())       # trading days only
        .filter(F.col("relative_day").isNotNull())
        .groupBy("relative_day", "cap_bucket")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("daily_return").cast(DecimalType(10, 6)).alias("avg_return"),
            F.percentile_approx("daily_return", 0.5).cast(DecimalType(10, 6)).alias("median_return"),
            F.avg("volume_ratio").cast(DecimalType(10, 4)).alias("avg_volume_ratio"),
            F.avg(
                F.when(F.col("daily_return") > 0, 1).otherwise(0)
            ).cast(DecimalType(6, 4)).alias("positive_rate"),
        )
        .orderBy("relative_day", "cap_bucket")
    )

    return cohort


def build_event_summary(spark):
    """
    One row per (ticker, event_id) with pre/post aggregates.

    - pre_return:  cumulative return over [-5, -1] relative days
    - post_return: cumulative return over [+1, +5] relative days
    - day0_return: single-day return on event date (relative_day == 0)
    - avg_pre_volume_ratio:  average volume ratio before event
    - avg_post_volume_ratio: average volume ratio after event

    Used by the API endpoint GET /events/{ticker}.
    """
    log.info("Building event_summary …")
    edf = spark.read.parquet(str(DATA_GOLD / "event_day_metrics"))

    pre = (
        edf
        .filter(
            (F.col("relative_day") >= -EVENT_WINDOW_PRE) &
            (F.col("relative_day") < 0)
        )
        .groupBy("ticker", "event_id", "event_date", "fiscal_quarter", "cap_bucket")
        .agg(
            F.sum("daily_return").cast(DecimalType(10, 6)).alias("pre_return"),
            F.avg("volume_ratio").cast(DecimalType(10, 4)).alias("avg_pre_volume_ratio"),
        )
    )

    day0 = (
        edf
        .filter(F.col("relative_day") == 0)
        .select(
            "ticker", "event_id",
            F.col("daily_return").cast(DecimalType(10, 6)).alias("day0_return"),
            F.col("close").cast(DecimalType(12, 4)).alias("event_close"),
        )
    )

    post = (
        edf
        .filter(
            (F.col("relative_day") > 0) &
            (F.col("relative_day") <= EVENT_WINDOW_POST)
        )
        .groupBy("ticker", "event_id")
        .agg(
            F.sum("daily_return").cast(DecimalType(10, 6)).alias("post_return"),
            F.avg("volume_ratio").cast(DecimalType(10, 4)).alias("avg_post_volume_ratio"),
        )
    )

    summary = (
        pre
        .join(day0,  on=["ticker", "event_id"], how="left")
        .join(post,  on=["ticker", "event_id"], how="left")
        .orderBy("ticker", "event_date")
    )

    return summary


def main():
    spark = get_spark("05_cohort_metrics")

    cohort  = build_cohort_metrics(spark)
    summary = build_event_summary(spark)

    # Write cohort metrics
    cohort_out = DATA_GOLD / "daily_cohort_metrics"
    cohort.write.mode("overwrite").parquet(str(cohort_out))
    log.info(
        "gold/daily_cohort_metrics written: %d rows → %s",
        cohort.count(), cohort_out
    )

    # Write event summary
    summary_out = DATA_GOLD / "event_summary"
    (
        summary
        .write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(str(summary_out))
    )
    log.info(
        "gold/event_summary written: %d rows → %s",
        summary.count(), summary_out
    )

    spark.stop()
    log.info("05_cohort_metrics complete.")


if __name__ == "__main__":
    main()
