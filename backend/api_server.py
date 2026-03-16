"""
backend/api_server.py
─────────────────────
FastAPI application serving pre-computed gold-layer Parquet tables.

Endpoints
─────────
GET /tickers                      — list all available tickers
GET /events/{ticker}              — event summaries for one ticker
GET /events/{ticker}/window       — day-level data for chart rendering
GET /cohorts                      — aggregated cohort metrics (all tickers)
POST /admin/reload                — invalidate data cache after pipeline re-run

Run:
    uvicorn backend.api_server:app --reload --port 8000
"""

from __future__ import annotations
import logging
import math
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from backend.schemas import (
    TickersResponse, TickerInfo,
    TickerEventsResponse, EventSummary,
    TickerWindowResponse, EventDayRow,
    CohortsResponse, CohortRow,
)
from backend import data_loader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────
app = FastAPI(
    title="Earnings Event Analyzer API",
    description="Serves pre-computed earnings event analysis from PySpark gold tables.",
    version="1.0.0",
)

# Allow React dev server (localhost:3000) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Helper ────────────────────────────────────────────────────
def _safe(val):
    """Convert NaN/inf → None for JSON serialisation."""
    if val is None:
        return None
    try:
        if math.isnan(float(val)) or math.isinf(float(val)):
            return None
    except (TypeError, ValueError):
        pass
    return val


# ── GET /tickers ──────────────────────────────────────────────
@app.get("/tickers", response_model=TickersResponse, summary="List available tickers")
def list_tickers():
    """Return all tickers that have been through the pipeline."""
    df = data_loader.get_all_tickers()
    if df.empty:
        return TickersResponse(tickers=[])

    tickers = [
        TickerInfo(
            ticker=row["ticker"],
            cap_bucket=row.get("cap_bucket"),
            event_count=int(row["event_count"]),
        )
        for _, row in df.iterrows()
    ]
    return TickersResponse(tickers=tickers)


# ── GET /events/{ticker} ─────────────────────────────────────
@app.get(
    "/events/{ticker}",
    response_model=TickerEventsResponse,
    summary="Event summaries for a ticker",
)
def get_ticker_events(
    ticker: str,
    limit: Optional[int] = Query(default=50, ge=1, le=500, description="Max events to return"),
):
    """
    Returns one row per earnings event for the given ticker,
    including pre/post cumulative returns and volume ratios.
    """
    ticker = ticker.upper()
    df = data_loader.get_events_for_ticker(ticker)

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No earnings events found for ticker '{ticker}'. "
                   f"Run the pipeline first or check the ticker symbol.",
        )

    df = df.sort_values("event_date", ascending=False).head(limit)

    events = [
        EventSummary(
            ticker=row["ticker"],
            event_id=row["event_id"],
            event_date=row["event_date"],
            fiscal_quarter=row.get("fiscal_quarter"),
            cap_bucket=row.get("cap_bucket"),
            pre_return=_safe(row.get("pre_return")),
            day0_return=_safe(row.get("day0_return")),
            post_return=_safe(row.get("post_return")),
            event_close=_safe(row.get("event_close")),
            avg_pre_volume_ratio=_safe(row.get("avg_pre_volume_ratio")),
            avg_post_volume_ratio=_safe(row.get("avg_post_volume_ratio")),
        )
        for _, row in df.iterrows()
    ]

    cap_bucket = df["cap_bucket"].iloc[0] if "cap_bucket" in df.columns else None

    return TickerEventsResponse(
        ticker=ticker,
        cap_bucket=cap_bucket,
        event_count=len(events),
        events=events,
    )


# ── GET /events/{ticker}/window ───────────────────────────────
@app.get(
    "/events/{ticker}/window",
    response_model=TickerWindowResponse,
    summary="Day-level event window data for charts",
)
def get_ticker_window(
    ticker: str,
    event_id: Optional[str] = Query(default=None, description="Filter to a specific event"),
):
    """
    Returns day-level rows for the [-5, +5] event window.
    Used by the frontend to render the price/volume chart.
    If event_id is omitted, returns all events for the ticker.
    """
    ticker = ticker.upper()
    df = data_loader.get_window_for_ticker(ticker)

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No window data found for ticker '{ticker}'.",
        )

    if event_id:
        df = df[df["event_id"] == event_id]
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"event_id '{event_id}' not found for ticker '{ticker}'.",
            )

    df = df.sort_values(["event_id", "relative_day"])

    rows = [
        EventDayRow(
            event_id=row["event_id"],
            event_date=row["event_date"],
            relative_day=int(row["relative_day"]),
            daily_return=_safe(row.get("daily_return")),
            cum_return=_safe(row.get("cum_return")),
            volume_ratio=_safe(row.get("volume_ratio")),
            close=_safe(row.get("close")),
        )
        for _, row in df.iterrows()
    ]

    return TickerWindowResponse(ticker=ticker, rows=rows)


# ── GET /cohorts ──────────────────────────────────────────────
@app.get(
    "/cohorts",
    response_model=CohortsResponse,
    summary="Aggregated cohort metrics",
)
def get_cohorts(
    cap_bucket: Optional[str] = Query(
        default=None,
        description="Filter by cap bucket: LARGE, MID, SMALL",
    ),
):
    """
    Returns aggregated event reactions by (relative_day, cap_bucket).
    Used by the frontend to render the cohort comparison chart.
    """
    df = data_loader.load_cohort_metrics()

    if df.empty:
        return CohortsResponse(rows=[])

    if cap_bucket:
        df = df[df["cap_bucket"] == cap_bucket.upper()]

    df = df.sort_values(["relative_day", "cap_bucket"])

    rows = [
        CohortRow(
            relative_day=int(row["relative_day"]),
            cap_bucket=str(row["cap_bucket"]),
            event_count=int(row["event_count"]),
            avg_return=_safe(row.get("avg_return")),
            median_return=_safe(row.get("median_return")),
            avg_volume_ratio=_safe(row.get("avg_volume_ratio")),
            positive_rate=_safe(row.get("positive_rate")),
        )
        for _, row in df.iterrows()
    ]

    return CohortsResponse(rows=rows)


# ── POST /admin/reload ────────────────────────────────────────
@app.post("/admin/reload", summary="Invalidate data cache")
def reload_data():
    """
    Clears the in-memory cache so the next request re-reads
    from Parquet. Call this after running the Spark pipeline.
    """
    data_loader.invalidate_cache()
    return {"status": "cache cleared"}


# ── Health check ──────────────────────────────────────────────
@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}
