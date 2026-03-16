"""
spark_jobs/utils.py — shared Spark session factory and helpers
used by every pipeline job.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "EarningsEventAnalyzer") -> SparkSession:
    """
    Return (or create) a SparkSession with sensible defaults.
    Runs locally in dev; swap master to a cluster URL in production.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
