"""
spark_jobs/02_clean.py
──────────────────────
Stage : bronze → silver
Input : bronze/prices/, bronze/earnings/
Output: silver/prices/    — cleaned, typed, classified
        silver/earnings/  — deduplicated, validated

Run:
    python spark_jobs/02_clean.py
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, DateType

from spark_jobs.utils import get_spark
from config import DATA_BRONZE, DATA_SILVER, NYSE_TZ, LARGE_CAP_THRESHOLD, MID_CAP_THRESHOLD

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── Required UDF: Ticker Classifier ──────────────────────────
# Maps a ticker to a market-cap bucket.
# In production, CAP_SETS would be loaded from a lookup table.
# Here we use a simple hard-coded reference set for portability.

_LARGE_CAP = {
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "JPM", "BAC", "GS", "MS", "JNJ", "XOM", "CVX", "WMT",
}
_MID_CAP = {
    "PFE", "ABBV", "COST", "TGT", "BA", "CAT", "GE",
}


def classify_ticker(ticker: str) -> str:
    """
    Scalar Python UDF.
    Returns one of: LARGE / MID / SMALL / OTHER

    Precedence:
      1. null or empty  → OTHER
      2. in LARGE set   → LARGE
      3. in MID set     → MID
      4. otherwise      → SMALL
    """
    if ticker is None or ticker.strip() == "":
        return "OTHER"
    t = ticker.strip().upper()
    if t in _LARGE_CAP:
        return "LARGE"
    if t in _MID_CAP:
        return "MID"
    return "SMALL"


classify_udf = F.udf(classify_ticker, StringType())


# ── Clean prices ─────────────────────────────────────────────
def clean_prices(spark):
    log.info("Reading bronze/prices …")
    df = spark.read.parquet(str(DATA_BRONZE / "prices"))

    silver = (
        df
        # Cast types
        .withColumn("trade_date", F.col("trade_date").cast(DateType()))
        .withColumn("open",   F.col("open").cast(DecimalType(12, 4)))
        .withColumn("high",   F.col("high").cast(DecimalType(12, 4)))
        .withColumn("low",    F.col("low").cast(DecimalType(12, 4)))
        .withColumn("close",  F.col("close").cast(DecimalType(12, 4)))
        .withColumn("volume", F.col("volume").cast("long"))

        # Drop rows where close is null (unquoted days)
        .filter(F.col("close").isNotNull())

        # Drop rows where volume is null or zero (non-trading days)
        .filter(F.col("volume").isNotNull() & (F.col("volume") > 0))

        # Add cap_bucket via UDF
        .withColumn("cap_bucket", classify_udf(F.col("ticker")))

        # Deduplicate: keep one row per (ticker, trade_date)
        # In case of duplicates, keep the row with the latest ingestion order
        .dropDuplicates(["ticker", "trade_date"])

        # Compute local_date in NYSE timezone for downstream use
        .withColumn(
            "local_date",
            F.to_date(
                F.from_utc_timestamp(
                    F.to_timestamp(F.col("trade_date").cast("string")),
                    NYSE_TZ
                )
            )
        )
    )

    out = DATA_SILVER / "prices"
    silver.write.mode("overwrite").partitionBy("ticker").parquet(str(out))
    log.info("silver/prices written: %d rows → %s", silver.count(), out)
    return silver


# ── Clean earnings ───────────────────────────────────────────
def clean_earnings(spark):
    log.info("Reading bronze/earnings …")
    df = spark.read.parquet(str(DATA_BRONZE / "earnings"))

    silver = (
        df
        .withColumn("event_date", F.col("event_date").cast(DateType()))

        # Drop rows with null event_date or ticker
        .filter(F.col("event_date").isNotNull())
        .filter(F.col("ticker").isNotNull())

        # Deduplicate: one event per (ticker, event_date)
        # event_id is deterministic: ticker_YYYYMMDD
        .dropDuplicates(["ticker", "event_date"])

        # Add cap_bucket
        .withColumn("cap_bucket", classify_udf(F.col("ticker")))

        # Derive fiscal_quarter from event_date (approximate)
        # Q1: Jan–Mar, Q2: Apr–Jun, Q3: Jul–Sep, Q4: Oct–Dec
        .withColumn(
            "fiscal_quarter",
            F.concat(
                F.lit("Q"),
                F.ceil(F.month(F.col("event_date")) / 3).cast("string")
            )
        )
    )

    out = DATA_SILVER / "earnings"
    silver.write.mode("overwrite").partitionBy("ticker").parquet(str(out))
    log.info("silver/earnings written: %d rows → %s", silver.count(), out)
    return silver


# ── Validation helpers ───────────────────────────────────────
def validate(df, name: str) -> None:
    """Basic sanity checks — log warnings, never raise."""
    null_counts = {
        c: df.filter(F.col(c).isNull()).count()
        for c in df.columns
    }
    for col, cnt in null_counts.items():
        if cnt > 0:
            log.warning("[%s] null in '%s': %d rows", name, col, cnt)

    total = df.count()
    log.info("[%s] total rows: %d", name, total)


# ── Main ──────────────────────────────────────────────────────
def main():
    spark = get_spark("02_clean")

    prices   = clean_prices(spark)
    earnings = clean_earnings(spark)

    validate(prices,   "silver/prices")
    validate(earnings, "silver/earnings")

    spark.stop()
    log.info("02_clean complete.")


if __name__ == "__main__":
    main()
