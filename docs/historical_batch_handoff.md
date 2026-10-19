# Historical Batch Ingestion, ETL & Analytical Handoff Specification

**Module Ownership & Responsibility:**
- **Historical Ingestion, Dataset Validation & API Downloader:** Ananthan S
- **Batch Processing & Parquet Analytical Outputs (PySpark / PyArrow):** Aleena Antony, Ananthan S
- **Batch Verification & Quality Auditing:** Ananthan S
- **Batch Pipeline Orchestration (`run_historical_batch.py`):** Ananthan S

---

## 1. Reproducible Workflow Guide

### A. Environment Prerequisites
- **Python:** Python 3.10+ (tested on Python 3.10.11 and Python 3.13.7)
- **Dependencies:** `pyspark>=3.5.0`, `pyarrow>=14.0.0`, `pandas>=2.0.0`, `python-dotenv>=1.0.0`, `pytest>=8.0.0`
- **Java (for PySpark):** OpenJDK 17 (`JAVA_HOME` pointing to valid JDK)
- **Windows Hadoop Note:** On Windows without native Hadoop binaries (`winutils.exe`), the batch pipeline automatically executes distributed transformations in Spark memory and writes compressed Parquet tables via PyArrow, preventing NTFS file-locking errors.

### B. Individual Stage Commands

#### 1. Download Historical Dataset (Bounded or Full)
```powershell
# Bounded Smoke Test (2 pages)
python -m ingestion.download --site caltech --start "Tue, 01 Jan 2019 16:18:00 GMT" --end "Tue, 31 Dec 2024 16:19:00 GMT" --max-pages 2 --raw-dir data/raw/smoke_test

# Full Historical Retrieval (645 pages, 16,125 sessions)
python -m ingestion.download --site caltech --start "Tue, 01 Jan 2019 16:18:00 GMT" --end "Tue, 31 Dec 2024 16:19:00 GMT" --raw-dir data/raw/full_download
```

#### 2. Run Local Ingestion & Validation Pipeline
```powershell
# Ingestion extraction, field validation, and tabular conversion
python -m ingestion.run_pipeline
```

#### 3. Execute Batch ETL (PySpark / PyArrow)
```powershell
# Using default candidate input path and output directory
python -m processing.batch_transform --input data/raw/full_download/acndata_sessions_full_candidate.json --output-dir data/processed/historical_batch/ --engine auto
```

#### 4. Run Read-Only Verification & Analytics Audit
```powershell
python -m processing.verify_batch --historical-dir data/processed/historical_batch/ --candidate data/raw/full_download/acndata_sessions_full_candidate.json --validation-dir data/processed/validation/
```

#### 5. Verify PostgreSQL Integration & Reconciliation (Offline & Live)
```powershell
# Offline reconciliation math tests
pytest tests/test_database.py -k reconciliation -v

# Live database load and reconciliation (when PostgreSQL is running)
python -m database.load_historical
```

#### 6. Run Test Suite
```powershell
# Ingestion test suite (78 tests)
pytest tests/test_ingestion.py tests/test_download.py -v

# Batch and processing tests (24 passed, 1 skipped)
pytest tests/test_processing.py -v

# Historical batch workflow tests
pytest tests/test_orchestration.py -k historical -v
```

---

### C. Unified Orchestration Entry Point

To execute the entire batch ETL, deduplication, quarantine separation, and verification in a single automated step:

```powershell
python -m orchestration.run_historical_batch
```

**Guarantees:**
1. **Idempotency & Safe Reruns:** Destination Parquet tables are overwritten cleanly (`mode="overwrite"`). Re-running the command never appends duplicate rows or mixes old and new data.
2. **Fail-Fast Semantics:** If any validation check, schema constraint, or transformation fails, the script halts immediately with exit status `1`.
3. **Dataset Safety:** Protects and leaves `data/raw/acndata_sessions.json` 100% untouched. Distinctly reads `data/raw/full_download/acndata_sessions_full_candidate.json`.

---

## 2. Output Data Contracts & Schemas

### A. Primary Sessions Table (`sessions.parquet`)
- **Location:** `data/processed/historical_batch/sessions.parquet`
- **Format:** Apache Parquet (Snappy compressed)
- **Rows:** 16,125 | **Columns:** 17

| Column Name | Physical Type | Logical Type | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `VARCHAR(24)` | String | No | MongoDB ObjectId from source. Primary key. |
| `session_id` | `VARCHAR(64)` | String | No | Unique business session ID (`{stationID}_{datetime}`). |
| `site_id` | `VARCHAR(10)` | String | No | Caltech site ID (`"0002"`). |
| `cluster_id` | `VARCHAR(10)` | String | No | Cluster ID (`"0039"`). |
| `station_id` | `VARCHAR(20)` | String | No | Hardware charging station identifier. |
| `space_id` | `VARCHAR(10)` | String | No | Physical parking stall identifier. |
| `connection_time` | `VARCHAR(35)` | Timestamp (UTC) | No | Plug-in timestamp in ISO 8601 UTC format. |
| `disconnect_time` | `VARCHAR(35)` | Timestamp (UTC) | No | Plug-out timestamp in ISO 8601 UTC format. |
| `done_charging_time` | `VARCHAR(35)` | Timestamp (UTC) | Yes | Last non-zero power timestamp. Null in ~12.7% of records. |
| `kwh_delivered` | `DOUBLE` | Float | No | Metered cumulative energy delivered (kWh). |
| `user_id` | `VARCHAR(12)` | String | Yes | Zero-padded user ID. Null in ~13.8% (anonymous users). |
| `timezone` | `VARCHAR(30)` | String | No | Site IANA timezone (`"America/Los_Angeles"`). |
| `session_duration_min` | `DOUBLE` | Float | No | Total connected duration: `(disconnect - connection)` (min). |
| `charging_duration_min`| `DOUBLE` | Float | Yes | Active charge duration: `(done_charging - connection)` (min). |
| `idle_duration_min` | `DOUBLE` | Float | Yes | Post-charge idle duration: `(disconnect - done_charging)` (min). |
| `validation_flags` | `TEXT` | String | Yes | Comma-separated flags (e.g. `"done_after_disconnect"`). |
| `ingestion_timestamp` | `VARCHAR(35)` | Timestamp (UTC) | No | Pipeline execution audit timestamp in UTC. |

### B. User Inputs Table (`user_inputs.parquet`)
- **Location:** `data/processed/historical_batch/user_inputs.parquet`
- **Format:** Apache Parquet (Snappy compressed)
- **Rows:** 16,600 | **Columns:** 9

| Column Name | Physical Type | Logical Type | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `session_id` | `VARCHAR(64)` | String | No | Foreign Key $\to$ `sessions.session_id`. Zero orphans. |
| `user_id_numeric` | `BIGINT` | Integer | No | User ID parsed as numeric integer. |
| `wh_per_mile` | `DOUBLE` | Float | No | Vehicle efficiency rating (Wh/mi). |
| `kwh_requested` | `DOUBLE` | Float | No | Total energy requested by driver (kWh). |
| `miles_requested` | `DOUBLE` | Float | No | Mileage range requested by driver. |
| `minutes_available` | `DOUBLE` | Float | No | Planned parking dwell time (min). |
| `modified_at` | `VARCHAR(35)` | Timestamp (UTC) | No | Timestamp when preference was logged in app. |
| `payment_required` | `BOOLEAN` | Boolean | No | Payment requirement indicator. |
| `requested_departure` | `VARCHAR(35)` | Timestamp (UTC) | No | Driver expected departure timestamp in UTC. |

---

## 3. Data Quality & Anomaly Rules

### A. The `done_after_disconnect` Anomaly
- **Definition:** Records where `done_charging_time > disconnect_time` (~2.5% of records; 409 occurrences).
- **Physical Root Cause:** Caltech station firmware sometimes recorded final polling packet confirmations seconds to minutes after the vehicle cable unlatched.
- **Metric Nulling Rule:**
  - Naive subtraction would yield negative `idle_duration_min`, corrupting aggregations.
  - **Contract Safeguard:** When `done_after_disconnect` is present:
    - The record is retained in the clean table (connection, disconnect, and energy are valid).
    - It is tagged in `validation_flags = "done_after_disconnect"`.
    - Both `charging_duration_min` and `idle_duration_min` are set to `null` (`None`).

### B. Upstream Physical Date Coverage vs. API Completeness
- **Requested Query Window:** `2019-01-01 16:18:00 GMT` to `2024-12-31 16:19:00 GMT`.
- **Actual Data Range:** `2019-01-01T17:41:45Z` to `2021-09-14T01:52:37Z`.
- **Completeness Distinction:**
  - **API Retrieval Completeness:** **100% Complete** (all 16,125 matching documents returned by Caltech API were retrieved).
  - **Requested Date Coverage:** **Incomplete** (the Caltech site collection physically terminated operations in September 2021; 2022–2024 has zero sessions recorded upstream).
  - Per rigorous data contracts, `completeness_verified` remains `false` in audit reports with reason: `"Date range does not span the requested end timestamp"`.

---

## 4. Streaming Integration Handoff Specification

**Consumer:** Kafka Producer (`streaming/producer.py`), Event Generator (`streaming/event_generator.py`), and Spark Structured Streaming.

### A. Event Contract Specification
Streaming events published to Kafka topic `charging-events` conform to `contracts/data_contracts.yaml`:

```json
{
  "event_id": "evt-74b89f2a-8101-49b9-9d95-8b87201c1f51",
  "station_id": "2-39-81-4550",
  "event_type": "energy_update",
  "timestamp": "2026-10-19T17:45:00+00:00",
  "energy_kwh": 14.85
}
```

### B. Mapping Matrix (Historical Batch vs. Streaming Producer)

| Event Field | Source in Streaming Simulator | Mapping / Generation Rule | Historical Batch Source |
| :--- | :--- | :--- | :--- |
| `event_id` | **Synthetic UUID** | Generated via `f"evt-{uuid.uuid4()}"` per telemetry message | Primary key (unique per message) |
| `station_id` | **Direct Map** | Sampled from the **55 verified station IDs** in `data/processed/validation/station_summary.csv` | `sessions.station_id` |
| `event_type` | **Lifecycle State** | `session_start` $\to$ `energy_update` (periodic) $\to$ `session_end` | Maps to lifecycle of a charging session |
| `timestamp` | **Simulation Clock** | Current UTC timestamp formatted as ISO 8601 (`+00:00`) | Maps to `connection_time` / `disconnect_time` |
| `energy_kwh` | **Cumulative Meter** | Incremental meter readings bounded by max session energy (77.7 kWh) | Maps to `sessions.kwh_delivered` |

### C. Streaming Invariants & Duplicate Policy
1. **Identifier Realism:** The streaming generator should emit station identifiers matching the historical station fleet (`2-39-81-4550`, `2-39-139-28`, `2-39-131-30`, etc.) to allow cross-path historical vs. real-time station analytics.
2. **Timestamp Protocol:** Strict ISO 8601 UTC with explicit timezone offset (`+00:00` or `Z`).
3. **Kafka Deduplication:** Due to at-least-once delivery, the Spark Structured Streaming consumer must apply watermarking and deduplication on `event_id`:
   ```python
   events_df.withWatermark("timestamp", "10 minutes").dropDuplicates(["event_id"])
   ```
4. **Energy Monotonicity:** Within a given active session, successive `energy_update` events must have non-decreasing `energy_kwh`.

---

## 5. Verification & Audit Evidence

### A. Core Batch Metrics (Reference vs. Actual Observed)

| Metric | Reference Contract | Actual Verified Batch Value | Status |
| :--- | :--- | :--- | :--- |
| **Clean Session Records** | 16,125 rows | 16,125 rows (`sessions.parquet`) | PASS |
| **Normalized User Inputs** | 16,600 rows | 16,600 rows (`user_inputs.parquet`) | PASS |
| **Active Charging Stations** | 55 stations | 55 distinct stations | PASS |
| **Total Delivered Energy** | 152,572.77 kWh | 152,572.77 kWh | PASS |
| **Duplicate Session IDs** | 0 duplicates | 0 duplicate business session IDs | PASS |
| **Orphaned User Inputs** | 0 orphans | 0 orphaned foreign keys | PASS |
| **Quarantined Records** | 0 rows | 0 rows (valid historical candidate) | PASS |
| **Source Date Range** | 2019-01-01 to 2021-09-14 | `2019-01-01T17:41:45Z` to `2021-09-14T01:52:37Z` | PASS |

### B. Generated Verification Artifacts
- **ETL Audit Report:** `data/processed/historical_batch/etl_audit_report.json`
- **Quality & Verification Report:** `data/processed/validation/verification_report.json`
- **Monthly Summary:** `data/processed/validation/monthly_sessions_summary.csv`
- **Daily Summary:** `data/processed/validation/daily_sessions_summary.csv`
- **Station Summary:** `data/processed/validation/station_summary.csv`
- **Energy & Duration Metrics:** `data/processed/validation/energy_and_duration_metrics.json`

