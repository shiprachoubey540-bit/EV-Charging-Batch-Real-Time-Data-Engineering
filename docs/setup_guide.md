# Setup Guide

## Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Apache Kafka (or Docker image `confluentinc/cp-kafka`)
- Apache Spark 3.x (via `pyspark` pip package for local dev)
- Git

## Quick Start

1. Clone the repository:
   `git clone https://github.com/shiprachoubey540-bit/EV-Charging-Batch-Real-Time-Data-Engineering.git`
2. Create and activate a virtual environment.
3. Install dependencies: `pip install -r requirements.txt`
4. Copy `.env.example` to `.env` and configure your local services.

## Per-Module Setup

### ingestion/ (Ananthan S)
- Place raw CSV files in `data/raw/`.
- Run: `python -m ingestion.extract`

### streaming/ (Harikrishnan SM)
- Start Kafka locally or via Docker.
- Run: `python -m streaming.producer`

### processing/ (Aleena Antony)
- Spark uses `local[*]` by default for development.
- Run batch: `python -m processing.batch_transform`
- Run streaming: `python -m processing.stream_transform`

### database/ (Bainty Kaur)
- Create the database: `createdb ev_charging`
- Apply schema: `psql -d ev_charging -f database/schema.sql`

### orchestration/ and dashboard/ (Shipra Choubey)
- Airflow: `airflow standalone` (after configuring AIRFLOW_HOME).
- Streamlit: `streamlit run dashboard/app.py`

## Running Tests

`pytest tests/ -v`
