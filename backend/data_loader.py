"""
backend/data_loader.py
──────────────────────
1. Parquet(gold) 먼저 조회
2. 없으면 yfinance 실시간 호출 → pandas로 계산
"""

from __future__ import annotations
import logging
from functools import lru_cache

import pandas as pd
import yfinance as yf

from config import DATA_GOLD

log = logging.getLogger(__name__)

EVENT_WINDOW   = 5
ANALYSIS_START = "2018-01-01"

_LARGE_CAP = {
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA",
    "JPM","BAC","GS","MS","JNJ","XOM","CVX","WMT",
}
_MID_CAP = {"PFE","ABBV","COST","TGT","BA","CAT","GE"}

def _cap_bucket(ticker: str) -> str:
    t = ticker.upper()
    if t in _LARGE_CAP: return "LARGE"
    if t in _MID_CAP:   return "MID"
    return "SMALL"


# ── Parquet loaders ───────────────────────────────────────────

@lru_cache(maxsize=1)
def load_event_summary() -> pd.DataFrame:
    path = DATA_GOLD / "event_summary"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")
    return df


@lru_cache(maxsize=1)
def load_event_day_metrics() -> pd.DataFrame:
    path = DATA_GOLD / "event_day_metrics"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path, columns=[
        "ticker","event_id","event_date",
        "relative_day","daily_return","cum_return","volume_ratio","close",
    ])
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")
    return df


@lru_cache(maxsize=1)
def load_cohort_metrics() -> pd.DataFrame:
    path = DATA_GOLD / "daily_cohort_metrics"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


# ── Real-time yfinance fallback ───────────────────────────────

def _fetch_realtime(ticker: str):
    ticker = ticker.upper()
    log.info("Fetching realtime data for %s", ticker)

    t = yf.Ticker(ticker)

    prices = t.history(start=ANALYSIS_START, auto_adjust=True)
    if prices.empty:
        return None, None

    prices.index = pd.to_datetime(prices.index).tz_localize(None)
    prices = prices[["Close","Volume"]].rename(columns={"Close":"close","Volume":"volume"})
    prices.index.name = "trade_date"

    try:
        cal = t.earnings_dates
        if cal is None or cal.empty:
            return None, None
        earnings_dates = pd.to_datetime(cal.index).tz_localize(None)
        earnings_dates = earnings_dates[
            (earnings_dates >= pd.Timestamp(ANALYSIS_START)) &
            (earnings_dates <= pd.Timestamp.today())
        ]
    except Exception as e:
        log.error("Failed to get earnings dates for %s: %s", ticker, e)
        return None, None

    cap = _cap_bucket(ticker)
    summary_rows = []
    window_rows  = []

    for event_date in earnings_dates:
        event_id  = f"{ticker}_{event_date.strftime('%Y%m%d')}"
        event_str = event_date.strftime("%Y-%m-%d")

        window = prices[
            (prices.index >= event_date - pd.Timedelta(days=14)) &
            (prices.index <= event_date + pd.Timedelta(days=14))
        ].copy()

        if len(window) < 3:
            continue

        trading_days = window.index.tolist()
        event_idx = min(
            range(len(trading_days)),
            key=lambda i: abs((trading_days[i] - event_date).days)
        )

        window = window.iloc[
            max(0, event_idx - EVENT_WINDOW):event_idx + EVENT_WINDOW + 1
        ].copy()

        avg_vol = window["volume"].mean()
        window["daily_return"] = window["close"].pct_change()
        window["cum_return"]   = window["daily_return"].cumsum()
        window["volume_ratio"] = window["volume"] / avg_vol if avg_vol > 0 else None
        start_rel = -min(event_idx, EVENT_WINDOW)
        window["relative_day"] = list(range(start_rel, start_rel + len(window)))

        for dt, row in window.iterrows():
            window_rows.append({
                "ticker":       ticker,
                "event_id":     event_id,
                "event_date":   event_str,
                "relative_day": int(row["relative_day"]),
                "daily_return": row["daily_return"],
                "cum_return":   row["cum_return"],
                "volume_ratio": row["volume_ratio"],
                "close":        row["close"],
            })

        pre  = window[window["relative_day"].between(-EVENT_WINDOW, -1)]
        post = window[window["relative_day"].between(1, EVENT_WINDOW)]
        day0 = window[window["relative_day"] == 0]

        summary_rows.append({
            "ticker":                ticker,
            "event_id":              event_id,
            "event_date":            event_str,
            "fiscal_quarter":        f"Q{((event_date.month - 1) // 3) + 1}",
            "cap_bucket":            cap,
            "pre_return":            pre["daily_return"].sum()   if not pre.empty  else None,
            "day0_return":           day0["daily_return"].iloc[0] if not day0.empty else None,
            "post_return":           post["daily_return"].sum()  if not post.empty else None,
            "event_close":           day0["close"].iloc[0]       if not day0.empty else None,
            "avg_pre_volume_ratio":  pre["volume_ratio"].mean()  if not pre.empty  else None,
            "avg_post_volume_ratio": post["volume_ratio"].mean() if not post.empty else None,
        })

    if not summary_rows:
        return None, None

    return pd.DataFrame(summary_rows), pd.DataFrame(window_rows)


# ── Public query helpers ──────────────────────────────────────

def get_events_for_ticker(ticker: str) -> pd.DataFrame:
    ticker = ticker.upper()
    df = load_event_summary()
    if not df.empty and ticker in df["ticker"].values:
        return df[df["ticker"] == ticker].copy()
    summary, _ = _fetch_realtime(ticker)
    return summary if summary is not None else pd.DataFrame()


def get_window_for_ticker(ticker: str) -> pd.DataFrame:
    ticker = ticker.upper()
    df = load_event_day_metrics()
    if not df.empty and ticker in df["ticker"].values:
        return df[df["ticker"] == ticker].copy()
    _, window = _fetch_realtime(ticker)
    return window if window is not None else pd.DataFrame()


def get_all_tickers() -> pd.DataFrame:
    df = load_event_summary()
    if df.empty:
        return pd.DataFrame(columns=["ticker","cap_bucket","event_count"])
    return (
        df.groupby("ticker")
        .agg(cap_bucket=("cap_bucket","first"), event_count=("event_id","count"))
        .reset_index()
        .sort_values("ticker")
    )


def invalidate_cache() -> None:
    load_event_summary.cache_clear()
    load_event_day_metrics.cache_clear()
    load_cohort_metrics.cache_clear()
    log.info("Cache cleared.")
