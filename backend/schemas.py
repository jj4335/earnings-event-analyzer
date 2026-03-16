"""
backend/schemas.py
──────────────────
Pydantic response models for all API endpoints.
"""

from __future__ import annotations
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel


# ── /events/{ticker} ─────────────────────────────────────────

class EventSummary(BaseModel):
    """One row per earnings event for a given ticker."""
    ticker:                 str
    event_id:               str
    event_date:             str
    fiscal_quarter:         Optional[str]
    cap_bucket:             Optional[str]
    pre_return:             Optional[Decimal]   # cumulative [-5, -1]
    day0_return:            Optional[Decimal]   # event-day return
    post_return:            Optional[Decimal]   # cumulative [+1, +5]
    event_close:            Optional[Decimal]
    avg_pre_volume_ratio:   Optional[Decimal]
    avg_post_volume_ratio:  Optional[Decimal]


class TickerEventsResponse(BaseModel):
    ticker:        str
    cap_bucket:    Optional[str]
    event_count:   int
    events:        list[EventSummary]


# ── /events/{ticker}/window ───────────────────────────────────

class EventDayRow(BaseModel):
    """One row per (event_id, relative_day) for chart rendering."""
    event_id:      str
    event_date:    str
    relative_day:  int
    daily_return:  Optional[Decimal]
    cum_return:    Optional[Decimal]
    volume_ratio:  Optional[Decimal]
    close:         Optional[Decimal]


class TickerWindowResponse(BaseModel):
    ticker:  str
    rows:    list[EventDayRow]


# ── /cohorts ─────────────────────────────────────────────────

class CohortRow(BaseModel):
    """Aggregated event reaction by (relative_day, cap_bucket)."""
    relative_day:       int
    cap_bucket:         str
    event_count:        int
    avg_return:         Optional[Decimal]
    median_return:      Optional[Decimal]
    avg_volume_ratio:   Optional[Decimal]
    positive_rate:      Optional[Decimal]


class CohortsResponse(BaseModel):
    rows: list[CohortRow]


# ── /tickers ──────────────────────────────────────────────────

class TickerInfo(BaseModel):
    ticker:      str
    cap_bucket:  Optional[str]
    event_count: int


class TickersResponse(BaseModel):
    tickers: list[TickerInfo]
