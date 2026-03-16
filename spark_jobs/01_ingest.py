"""
spark_jobs/01_ingest.py
───────────────────────
Stage : raw → bronze
Input : Yahoo Finance API (yfinance)
Output: bronze/prices/         — OHLCV partitioned by ticker
        bronze/earnings/       — earnings announcement dates

Run:
    python spark_jobs/01_ingest.py
    python spark_jobs/01_ingest.py --tickers AAPL MSFT NVDA  # subset
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

import yfinance as yf

from spark_jobs.utils import get_spark
from config import (
    DATA_BRONZE,
    ANALYSIS_START,
    ANALYSIS_END,
    DEFAULT_TICKERS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── 1. Fetch OHLCV from Yahoo Finance ────────────────────────
def fetch_prices(tickers: list[str], start: str, end: str) -> list[dict]:
    rows = []
    for ticker in tickers:
        log.info("Fetching prices: %s", ticker)
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                multi_level_index=False,
            )
            if df.empty:
                log.warning("No price data returned for %s", ticker)
                continue
            for date, row in df.iterrows():
                rows.append({
                    "ticker":     ticker,
                    "trade_date": date.strftime("%Y-%m-%d"),
                    "open":       float(row["Open"])   if row["Open"]   == row["Open"]   else None,
                    "high":       float(row["High"])   if row["High"]   == row["High"]   else None,
                    "low":        float(row["Low"])    if row["Low"]    == row["Low"]    else None,
                    "close":      float(row["Close"])  if row["Close"]  == row["Close"]  else None,
                    "volume":     int(row["Volume"])   if row["Volume"] == row["Volume"] else None,
                })
        except Exception as e:
            log.error("Failed to fetch prices for %s: %s", ticker, e)
    log.info("Total price rows fetched: %d", len(rows))
    return rows


# ── 2. Fetch earnings calendar from Yahoo Finance ────────────
def fetch_earnings(tickers: list[str]) -> list[dict]:
    """
    Download earnings announcement dates for each ticker.
    Returns a flat list of row dicts.
    """
    rows = []
    for ticker in tickers:
        log.info("Fetching earnings dates: %s", ticker)
        try:
            t = yf.Ticker(ticker)
            cal = t.earnings_dates  # DataFrame indexed by earnings date
            if cal is None or cal.empty:
                log.warning("No earnings data for %s", ticker)
                continue

            for date, _ in cal.iterrows():
                rows.append({
                    "ticker":     ticker,
                    "event_date": date.strftime("%Y-%m-%d"),
                    "event_id":   f"{ticker}_{date.strftime('%Y%m%d')}",
                })
        except Exception as e:
            log.error("Failed to fetch earnings for %s: %s", ticker, e)

    # Filter to analysis window
    rows = [
        r for r in rows
        if ANALYSIS_START <= r["event_date"] <= ANALYSIS_END
    ]
    log.info("Total earnings rows fetched: %d", len(rows))
    return rows


# ── 3. Write to bronze layer as Parquet ──────────────────────
def write_bronze(spark, rows: list[dict], output_path: Path, partition_by: str = None) -> None:
    """
    Convert list of row dicts → Spark DataFrame → Parquet.
    Overwrites existing data for idempotent re-runs.
    """
    if not rows:
        log.warning("No rows to write to %s — skipping.", output_path)
        return

    df = spark.createDataFrame(rows)

    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(str(output_path))
    log.info("Written %d rows → %s", df.count(), output_path)


# ── 4. Save ingestion metadata ───────────────────────────────
def write_metadata(output_path: Path, tickers: list[str], row_counts: dict) -> None:
    meta = {
        "run_ts":      datetime.utcnow().isoformat(),
        "tickers":     tickers,
        "analysis_start": ANALYSIS_START,
        "analysis_end":   ANALYSIS_END,
        "row_counts":  row_counts,
    }
    meta_path = output_path / "_metadata.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2))
    log.info("Metadata written → %s", meta_path)


# ── Main ──────────────────────────────────────────────────────
def main(tickers: list[str]) -> None:
    spark = get_spark("01_ingest")

    prices_path   = DATA_BRONZE / "prices"
    earnings_path = DATA_BRONZE / "earnings"

    # Fetch
    price_rows   = fetch_prices(tickers, ANALYSIS_START, ANALYSIS_END)
    earning_rows = fetch_earnings(tickers)

    # Write
    write_bronze(spark, price_rows,   prices_path,   partition_by="ticker")
    write_bronze(spark, earning_rows, earnings_path, partition_by="ticker")

    # Metadata
    write_metadata(
        DATA_BRONZE,
        tickers,
        row_counts={
            "prices":   len(price_rows),
            "earnings": len(earning_rows),
        },
    )

    spark.stop()
    log.info("01_ingest complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest price + earnings data")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=DEFAULT_TICKERS,
        help="Space-separated list of tickers (default: all in config.py)",
    )
    args = parser.parse_args()
    main(args.tickers)
