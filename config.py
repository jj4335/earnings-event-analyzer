"""
config.py — shared constants across the entire pipeline.
All paths and parameters are defined here so each spark_job
imports from one place.
"""

from pathlib import Path

# ── Project root ──────────────────────────────────────────────
ROOT = Path(__file__).parent

# ── Data layer paths ──────────────────────────────────────────
DATA_RAW    = ROOT / "data" / "raw"
DATA_BRONZE = ROOT / "data" / "bronze"
DATA_SILVER = ROOT / "data" / "silver"
DATA_GOLD   = ROOT / "data" / "gold"

# ── Analysis window ───────────────────────────────────────────
ANALYSIS_START = "2018-01-01"
ANALYSIS_END   = "2024-12-31"

# ── Event study window (trading days relative to event) ───────
EVENT_WINDOW_PRE  = 5   # days before earnings
EVENT_WINDOW_POST = 5   # days after earnings

# ── Sessionization threshold (minutes) ───────────────────────
SESSION_GAP_MINUTES = 25

# ── Market cap buckets (approximate USD billions) ─────────────
LARGE_CAP_THRESHOLD = 10   # >= $10B  → LARGE
MID_CAP_THRESHOLD   = 2    # >= $2B   → MID
                           # <  $2B   → SMALL

# ── Timezone ──────────────────────────────────────────────────
NYSE_TZ = "America/New_York"

# ── Tickers to include in the analysis ───────────────────────
# Extend this list or load from an external file as needed
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "JPM",  "BAC",  "GS",   "MS",
    "JNJ",  "PFE",  "ABBV",
    "XOM",  "CVX",
    "WMT",  "COST", "TGT",
    "BA",   "CAT",  "GE",
]
