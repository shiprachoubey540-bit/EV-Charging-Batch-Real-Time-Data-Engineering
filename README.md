# EV-Charging-Batch-Real-Time-Data-Engineering

End-to-end EV charging data engineering pipeline using Python, Apache Kafka, Apache Spark, PostgreSQL, Apache Airflow, and Streamlit for batch and simulated real-time data processing.

The project focuses on core Data Engineering concepts: batch and streaming processing, ETL/ELT, data quality, incremental loading, data modelling, SQL optimisation, fault tolerance, orchestration, monitoring, and scalable deployment.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Module Ownership](#module-ownership)
- [Technology Stack](#technology-stack)
- [Getting Started](#getting-started)
- [Data Contracts](#data-contracts)
- [Development Workflow](#development-workflow)
- [License](#license)

---

## Architecture Overview

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌────────────┐     ┌───────────┐
│  ACN-Data   │────▶│  ingestion/  │────▶│   processing/    │────▶│ database/  │────▶│ dashboard/│
│  (CSV/API)  │     │  Batch ETL   │     │  Spark Batch     │     │ PostgreSQL │     │ Streamlit │
└─────────────┘     └──────────────┘     │  + Streaming     │     └────────────┘     └───────────┘
                                         └──────────────────┘
                         ▲                        ▲
┌─────────────┐     ┌────┴─────────┐              │
│  Simulated  │────▶│  streaming/  │──────────────┘
│   Events    │     │  Kafka       │
└─────────────┘     └──────────────┘

                    ┌──────────────┐
                    │orchestration/│  ← Airflow DAGs coordinate the pipeline
                    │  Airflow     │
                    └──────────────┘
```

---

## Project Structure

```
ev-charging-data-engineering/
├── ingestion/            # Historical dataset ingestion and batch ETL
│   ├── __init__.py
│   ├── extract.py        # Download / load ACN-Data CSVs
│   ├── transform.py      # Clean, validate, reshape raw data
│   └── load.py           # Write to staging area or database
├── streaming/            # Kafka producer and simulated event generator
│   ├── __init__.py
│   ├── producer.py       # Kafka message producer
│   └── event_generator.py# Simulate real-time charging events
├── processing/           # Spark batch and streaming applications
│   ├── __init__.py
│   ├── batch_transform.py    # Spark batch transformations
│   └── stream_transform.py  # Structured Streaming jobs
├── database/             # PostgreSQL schema, SQL scripts, data-quality checks
│   ├── schema.sql        # Table DDL
│   ├── seed.sql          # Optional seed / reference data
│   ├── analytics.sql     # Analytical queries
│   └── quality_checks.sql# Data-quality validation queries
├── orchestration/        # Airflow DAGs
│   └── dags/
│       └── ev_pipeline_dag.py
├── dashboard/            # Streamlit application
│   ├── app.py
│   └── components/
│       └── __init__.py
├── data/
│   ├── sample/           # Small, non-sensitive sample datasets (committed)
│   │   └── README.md
│   └── raw/              # Local raw datasets (git-ignored)
│       └── .gitkeep
├── tests/                # Automated tests
│   ├── __init__.py
│   ├── test_ingestion.py
│   ├── test_streaming.py
│   ├── test_processing.py
│   └── test_database.py
├── docs/                 # Architecture, schemas, data dictionary, setup guide
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── setup_guide.md
├── config/               # Example configuration files (no secrets)
│   └── config.example.yaml
├── contracts/            # Shared data contracts
│   └── data_contracts.yaml
├── .env.example          # Placeholder environment variables
├── .gitattributes        # Line-ending normalisation
├── .gitignore            # Python + project ignores
├── CONTRIBUTING.md       # Branch, commit, and PR conventions
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

---

## Module Ownership

| Module | Owner | Scope |
|--------|-------|-------|
| `ingestion/` | **Ananthan S** | Historical data ingestion, Python ETL, Spark batch processing, project integration |
| `streaming/` | **Harikrishnan SM** | Python event generator, Apache Kafka, simulated real-time event ingestion |
| `processing/` | **Aleena Antony** | Apache Spark batch transformations, Structured Streaming, stream processing |
| `database/` | **Bainty Kaur** | PostgreSQL database modelling, SQL analytics, data-quality validation |
| `orchestration/` `dashboard/` | **Shipra Choubey** | Apache Airflow orchestration, Streamlit dashboard, end-to-end validation |

---

## Technology Stack

| Technology | Purpose |
|------------|---------|
| Python 3.10+ | Core language for ETL, producers, and application code |
| Apache Kafka | Message broker for simulated real-time event streaming |
| Apache Spark | Batch transformations and Structured Streaming |
| PostgreSQL | Analytical data warehouse |
| Apache Airflow | Workflow orchestration and scheduling |
| Streamlit | Interactive monitoring dashboard |
| Docker Compose | (Planned) Reproducible service deployment |
| Git / GitHub | Version control and collaboration |

---

## Getting Started

### Prerequisites

- Python 3.10 or later
- PostgreSQL 14+
- Apache Kafka (or Docker image)
- Apache Spark 3.x
- Git

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/shiprachoubey540-bit/EV-Charging-Batch-Real-Time-Data-Engineering.git
cd EV-Charging-Batch-Real-Time-Data-Engineering

# 2. Create and activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy the example environment file and fill in your local settings
cp .env.example .env
```

See [`docs/setup_guide.md`](docs/setup_guide.md) for detailed per-module setup.

---

## Database (PostgreSQL)

The warehouse is the PostgreSQL database `ev_charging_db`. Scripts are in `database/`.
Column-level details are in `docs/data_dictionary.md`, the table diagram is in
`docs/er_diagram.md` and the quality results are in `docs/data_quality_report.md`.

| Table | Contents | Data |
|-------|----------|------|
| `dim_station` | The 55 verified Caltech stations | Real |
| `charging_sessions` | 16,125 historical sessions (Jan 2019 to Sep 2021) | Real |
| `session_user_inputs` | 16,600 driver preference records | Real |
| `charging_events` | Stream events written by Spark (`is_synthetic` = TRUE) | Simulated |
| `station_window_metrics` | 10-minute windowed aggregates per station | Simulated |

### Create and load the database

```bash
# 1. Create the database and the tables
psql -U postgres -c "CREATE DATABASE ev_charging_db;"
psql -U postgres -d ev_charging_db -f database/schema.sql

# 2. Load the historical data (needs sessions.parquet and user_inputs.parquet in
#    data/processed/historical_batch/ and station_summary.csv in data/processed/validation/)
python -m database.load_historical
```

The loader reconciles its totals with the batch report (16,125 sessions, 16,600 user
inputs, 55 stations, 152,572.77 kWh) and loads nothing if they differ. It is safe to rerun.

### Run the checks and analytics

```bash
psql -U postgres -d ev_charging_db -f database/quality_checks.sql   # 15 data-quality checks
psql -U postgres -d ev_charging_db -f database/analytics.sql        # 7 analytical queries
```

Connection settings use the environment variables `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER` and `POSTGRES_PASSWORD`. Never commit the password.

### Limitations

- Real data covers 2019-01-01 to 2021-09-14 only; the Caltech site stopped operating in September 2021.
- `charging_events` and `station_window_metrics` are simulated and must not be reported as live observations.

## Data Contracts

Shared field definitions for historical sessions and streaming events are in [`contracts/data_contracts.yaml`](contracts/data_contracts.yaml). All modules must conform to these schemas.

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for the full data dictionary.

---

## Development Workflow

1. Create a feature branch: `git checkout -b feature/<module>-<description>`
2. Develop and test locally using sample data in `data/sample/`.
3. Open a pull request to `main` and request a review.
4. Merge only after approval.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for full conventions.

---

## License

To be determined — check dataset licence requirements before adding.
