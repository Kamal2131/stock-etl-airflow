# 📈 Real-Time F&O Stock Market ETL Pipeline

Production-grade ETL pipeline for ingesting 1-minute F&O market data using **Apache Airflow (Astronomer)** and storing in a **partitioned Parquet data lake**.

## 🎯 What This Does

- Downloads **1-minute OHLCV + Open Interest** data from Zerodha Kite API
- Runs **daily after market close** via Airflow scheduler
- Stores data in a **partitioned Parquet lake** (idempotent, backfill-ready)
- Supports **BANKNIFTY** and **NIFTY** F&O instruments

## 🏗️ Architecture

```
Zerodha Kite API
        ↓
    [Extract]     → Raw JSON from API
        ↓
   [Transform]    → Clean, validate, enrich
        ↓
  [Load Raw]      → data/lake/fno/raw/
        ↓
   [Process]      → data/lake/fno/processed/
        ↓
[Quality Check]   → Validate data integrity
```

## 📁 Project Structure

```
stock-etl/
├── dags/
│   └── fno_etl_dag.py          # Main ETL DAG
├── include/
│   ├── extractors/
│   │   └── kite_extractor.py   # Kite API client
│   ├── transformers/
│   │   └── fno_transformer.py  # Data transformation
│   ├── loaders/
│   │   └── parquet_loader.py   # Parquet writer
│   └── utils/
│       └── data_quality.py     # Quality checks
├── data/lake/fno/
│   ├── raw/                    # Unprocessed data
│   ├── processed/              # Cleaned data
│   └── analytics/              # Indicators (future)
├── .env.example
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (running)
- Astro CLI (`winget install -e --id Astronomer.Astro`)
- Zerodha Kite API credentials

### Setup

```powershell
# 1. Configure environment
copy .env.example .env
# Edit .env with your Kite API credentials

# 2. Start Airflow
astro dev start

# 3. Access Airflow UI
# Open http://localhost:8080
# Login: admin / admin
```

### Trigger DAG

```powershell
# Manual trigger for today
astro dev run dags trigger fno_etl_daily

# Backfill historical date
astro dev run dags trigger fno_etl_daily --conf '{"ds": "2025-01-03"}'
```

## 📊 Data Lake Partitioning

```
data/lake/fno/processed/
└── underlying=BANKNIFTY/
    └── date=2025-01-06/
        └── data.parquet
```

Query with DuckDB:
```python
import duckdb
duckdb.sql("SELECT * FROM 'data/lake/fno/processed/**/*.parquet' LIMIT 10")
```

## ⏰ Schedule

| DAG | Schedule | Description |
|-----|----------|-------------|
| `fno_etl_daily` | `0 16 * * 1-5` | 4:00 PM IST, Mon-Fri |

## 🧪 Quality Checks

- Row count validation
- Unique `(symbol, timestamp)` check
- Market hours filtering
- Price validity (no negative/inverted OHLC)

## 📝 Commands Reference

```powershell
astro dev start     # Start Airflow
astro dev stop      # Stop Airflow
astro dev restart   # Restart Airflow
astro dev logs      # View logs
astro dev bash      # Shell into container
```

## 🔮 Future Extensions

- [ ] S3/GCS storage backend
- [ ] Analytics layer with indicators (VWAP, OI change)
- [ ] DuckDB dashboard
- [ ] Kafka streaming ingestion
- [ ] ML feature store

---

**Tech Stack**: Python 3.10 | Apache Airflow 2.x | Astronomer | PyArrow | DuckDB
