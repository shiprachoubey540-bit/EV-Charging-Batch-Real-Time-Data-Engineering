# Architecture Overview

## Pipeline Diagram

`
ACN-Data (CSV)  ──▶  ingestion/  ──▶  processing/ (Spark Batch)  ──▶  database/ (PostgreSQL)  ──▶  dashboard/ (Streamlit)
                     extract.py       batch_transform.py               schema.sql                    app.py
                     transform.py
                     load.py

Simulated Events ──▶ streaming/  ──▶  processing/ (Structured Streaming) ──▶  database/
                     event_generator  stream_transform.py                     charging_events table
                     producer.py
                     (Kafka)

Airflow (orchestration/) coordinates the batch pipeline on a schedule.
`

## Module Communication

| Source Module | Output | Consumer Module |
|--------------|--------|----------------|
| ingestion/ | Cleaned CSV/Parquet | processing/ (batch) |
| streaming/ | Kafka topic: charging-events | processing/ (streaming) |
| processing/ | Transformed DataFrames | database/ |
| database/ | PostgreSQL tables | dashboard/ |

## Data Flow

1. **Batch path**: ACN-Data CSV → ingestion/ → processing/ (Spark) → database/ → dashboard/
2. **Stream path**: event_generator → Kafka → processing/ (Structured Streaming) → database/ → dashboard/
3. **Orchestration**: Airflow DAG triggers the batch path on schedule.

---

*To be expanded with component diagrams and deployment architecture.*
