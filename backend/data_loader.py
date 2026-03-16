"""
backend/data_loader.py
──────────────────────
Reads gold-layer Parquet files into pandas DataFrames.
Caches results in memory so repeated API calls are fast
without re-reading from disk on every request.

No Spark dependency here — the API only reads pre-computed
gold tables, keeping the backend lightweight.
"""

from __future__ import annotations
import logging
from functools import lru_cache
from pathlib import Path

import pandas as pd

from config import DATA_GOLD

log = logging.getLogger(__name__)


# ── Loaders (lru_cache = read once, serve many times) ────────

@lru_cache(maxsize=1)
def load_event_summary() -> pd.DataFrame:
    """
    Load gold/event_summary — one row per (ticker, event_id).
    Used by GET /events/{ticker} and GET /tickers.
    """
    path = DATA_GOLD / "event_summary"
    if not path.exists():
        log.warning("event_summary not found at %s — returning empty DataFrame", path)
        return pd.DataFrame()

    df = pd.read_parquet(path)
    # Ensure event_date is a string for JSON serialisation
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")
    log.info("Loaded event_summary: %d rows", len(df))
    return df


@lru_cache(maxsize=1)
def load_event_day_metrics() -> pd.DataFrame:
    """
    Load gold/event_day_metrics — one row per (ticker, event_id, relative_day).
    Used by GET /events/{ticker}/window.
    """
    path = DATA_GOLD / "event_day_metrics"
    if not path.exists():
        log.warning("event_day_metrics not found at %s — returning empty DataFrame", path)
        return pd.DataFrame()

    df = pd.read_parquet(
        path,
        columns=[
            "ticker", "event_id", "event_date",
            "relative_day", "daily_return", "cum_return",
            "volume_ratio", "close",
        ],
    )
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")
    log.info("Loaded event_day_metrics: %d rows", len(df))
    return df


@lru_cache(maxsize=1)
def load_cohort_metrics() -> pd.DataFrame:
    """
    Load gold/daily_cohort_metrics.
    Used by GET /cohorts.
    """
    path = DATA_GOLD / "daily_cohort_metrics"
    if not path.exists():
        log.warning("daily_cohort_metrics not found at %s — returning empty DataFrame", path)
        return pd.DataFrame()

    df = pd.read_parquet(path)
    log.info("Loaded daily_cohort_metrics: %d rows", len(df))
    return df


# ── Query helpers ─────────────────────────────────────────────

def get_events_for_ticker(ticker: str) -> pd.DataFrame:
    df = load_event_summary()
    if df.empty:
        return df
    return df[df["ticker"] == ticker.upper()].copy()


def get_window_for_ticker(ticker: str) -> pd.DataFrame:
    df = load_event_day_metrics()
    if df.empty:
        return df
    return df[df["ticker"] == ticker.upper()].copy()


def get_all_tickers() -> pd.DataFrame:
    """Return unique tickers with cap_bucket and event count."""
    df = load_event_summary()
    if df.empty:
        return pd.DataFrame(columns=["ticker", "cap_bucket", "event_count"])

    return (
        df.groupby("ticker")
        .agg(
            cap_bucket=("cap_bucket", "first"),
            event_count=("event_id", "count"),
        )
        .reset_index()
        .sort_values("ticker")
    )


def invalidate_cache() -> None:
    """Force reload on next request (call after pipeline re-run)."""
    load_event_summary.cache_clear()
    load_event_day_metrics.cache_clear()
    load_cohort_metrics.cache_clear()
    log.info("Data loader cache cleared.")
