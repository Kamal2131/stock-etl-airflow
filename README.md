# 📈 Stock Market ETL Pipeline

Production-grade ETL pipeline for ingesting market data using **Apache Airflow (Astronomer)** and storing in a **partitioned Parquet data lake** with AWS S3 upload.

## 🎯 Features

| Pipeline | Data | Interval | Source |
|----------|------|----------|--------|
| **F&O ETL** | BANKNIFTY, NIFTY derivatives | 1-minute | Kite API |
| **Nifty 500 ETL** | 500 NSE equity stocks | 5-minute | Kite API |

- ✅ Daily automated runs at **4:00 PM IST**
- ✅ Partitioned **Parquet data lake** (idempotent, backfill-ready)
- ✅ **AWS S3** upload support
- ✅ **Master orchestrator** for coordinating pipelines

## 🏗️ Architecture

```
Zerodha Kite API
        ↓
    [Extract]     → Raw OHLCV from API
        ↓
   [Transform]    → Clean, validate, enrich
        ↓
  [Load Raw]      → data/lake/{fno|equity}/raw/
        ↓
   [Process]      → data/lake/{fno|equity}/processed/
        ↓
  [Upload S3]     → s3://bucket/nifty500/date=YYYY-MM-DD/
        ↓
[Quality Check]   → Validate data integrity
```

## 📁 Project Structure

```
stock-etl-airflow/
├── dags/
│   ├── fno_etl_dag.py              # F&O derivatives ETL
│   ├── nifty500_etl_dag.py         # Nifty 500 equity ETL
│   └── market_etl_orchestrator.py  # Master orchestrator
├── include/
│   ├── extractors/
│   │   ├── kite_extractor.py       # F&O Kite client
│   │   └── nifty500_extractor.py   # Nifty 500 Kite client
│   ├── transformers/
│   │   ├── fno_transformer.py      # F&O data cleaning
│   │   └── equity_transformer.py   # Equity data cleaning
│   ├── loaders/
│   │   ├── parquet_loader.py       # Local Parquet writer
│   │   └── s3_loader.py            # AWS S3 uploader
│   └── utils/
│       └── data_quality.py         # Quality checks
├── data/lake/
│   ├── fno/                        # F&O data
│   └── equity/                     # Equity data
├── .env.example
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop
- Astro CLI (`winget install -e --id Astronomer.Astro`)
- Zerodha Kite API credentials

### Setup

```powershell
# Configure environment
copy .env.example .env
# Edit .env with your credentials

# Start Airflow
astro dev start

# Access UI: http://localhost:8080 (admin/admin)
```

## ⚙️ Configuration

Edit `.env` file:

```bash
# Kite API (required)
KITE_API_KEY=your_api_key
KITE_ACCESS_TOKEN=your_token

# AWS S3 (optional)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET_NAME=your-bucket

# Orchestration
RUN_FNO_PIPELINE=true
RUN_EQUITY_PIPELINE=true
RUN_PIPELINES_PARALLEL=true

# Testing (limit stocks)
NIFTY500_MAX_INSTRUMENTS=20
```

## ⏰ DAG Schedule

| DAG | Schedule | Description |
|-----|----------|-------------|
| `market_etl_orchestrator` | 4:00 PM IST | Master - triggers both pipelines |
| `fno_etl_daily` | 4:00 PM IST | F&O derivatives only |
| `nifty500_etl_daily` | 4:00 PM IST | Nifty 500 equity only |

## 📊 Data Lake Structure

```
data/lake/
├── fno/processed/underlying=BANKNIFTY/date=2025-01-06/data.parquet
└── equity/processed/date=2025-01-06/data.parquet
```

Query with DuckDB:
```python
import duckdb
duckdb.sql("SELECT * FROM 'data/lake/equity/processed/**/*.parquet' LIMIT 10")
```

## 📝 Commands

```powershell
astro dev start       # Start Airflow
astro dev stop        # Stop Airflow
astro dev restart     # Restart
astro dev run dags trigger nifty500_etl_daily  # Manual trigger
```

## 🧪 Quality Checks

- Row count validation (75 candles/stock for 5-min)
- Unique `(symbol, timestamp)` constraint
- Market hours filtering (9:15 AM - 3:30 PM)
- OHLC relationship validation

---

**Tech Stack**: Python 3.10 | Apache Airflow 2.x | Astronomer | PyArrow | boto3 | DuckDB
