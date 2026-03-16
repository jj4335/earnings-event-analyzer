# 📈 Earnings Event Analyzer

## Demo
https://github.com/user-attachments/assets/b6c7d2ad-007a-455f-909a-9d6536cfc16e

**PySpark-Based Earnings Event Analysis Pipeline**

Analyzes how stock prices, volume, and volatility react around earnings announcements.
Users enter a ticker on the web dashboard and instantly see historical event patterns.

---

## Spark Skills

| Technique | Applied To |
|---|---|
| Window functions (`lag`, `lead`) | Pre/post-event returns, session indexing |
| Scalar Python UDF | Ticker classifier (LARGE / MID / SMALL) |
| `explode(sequence(...))` | Expand event window to day-level rows |
| Sessionization | Group consecutive trades into sessions |
| Cohort analysis | Aggregate by sector × cap bucket |
| Timezone-aware computation | All dates in `America/New_York` |

---

## Architecture
```
Yahoo Finance / SEC EDGAR
        │
        ▼
  PySpark Pipeline
  ├── 01_ingest.py         raw → bronze
  ├── 02_clean.py          bronze → silver
  ├── 03_event_windows.py  explode(sequence)
  ├── 04_sessionize.py     Window + cumsum
  └── 05_cohort_metrics.py groupBy + Window
        │
        ▼
  Parquet Tables (gold/)
        │
        ▼
  FastAPI  →  React Dashboard
```

---

## Quick Start
```bash
git clone https://github.com/jj4335/earnings-event-analyzer
cd earnings-event-analyzer
pip install -r requirements.txt

# Run full pipeline
export PYSPARK_PYTHON=/opt/anaconda3/bin/python3.13
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python3.13
export PYTHONPATH=.
python spark_jobs/01_ingest.py && \
python spark_jobs/02_clean.py && \
python spark_jobs/03_event_windows.py && \
python spark_jobs/04_sessionize.py && \
python spark_jobs/05_cohort_metrics.py

# Start API + Dashboard
./run.sh
```

---

## Stack

- **Pipeline**: PySpark 3.5, Parquet
- **API**: FastAPI
- **Frontend**: React + Recharts
- **Data**: Yahoo Finance API

---

## Design Principles

- **Determinism** — explicit sort after `collect_list`; tie-break in every window spec
- **Timezone awareness** — all date logic in `America/New_York`
- **Null safety** — null handled in every UDF; `coalesce` guards for missing prices
- **No collect** — full pipeline runs inside Spark
