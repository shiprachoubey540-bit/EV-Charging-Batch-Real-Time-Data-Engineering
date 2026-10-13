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
