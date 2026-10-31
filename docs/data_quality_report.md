# Data-Quality Report: PostgreSQL warehouse

Database: `ev_charging_db` (PostgreSQL 13). Checks: `database/quality_checks.sql`.
Loader: `database/load_historical.py`. Owner: Bainty Kaur.

## 1. Reconciliation with the batch report

Loaded from Ananthan's `sessions.parquet`, `user_inputs.parquet` and
`station_summary.csv`. The loader compares every figure and rolls back on a mismatch.

| Metric | Batch report | Loaded in PostgreSQL | Result |
|--------|--------------|----------------------|--------|
| Sessions | 16,125 | 16,125 | PASS |
| User inputs | 16,600 | 16,600 | PASS |
| Stations | 55 | 55 | PASS |
| Total energy (kWh) | 152,572.77 | 152,572.77 | PASS |

## 2. Quality checks on the historical data

| Check | Issues found | Expected | Result |
|-------|--------------|----------|--------|
| Duplicate session IDs | 0 | 0 | PASS |
| Connection after disconnect | 0 | 0 | PASS |
| Negative durations | 0 | 0 | PASS |
| Zero or negative energy | 0 | 0 | PASS |
| Sessions at unknown stations (foreign key) | 0 | 0 | PASS |
| Flagged rows still holding durations | 0 | 0 | PASS |
| User inputs without a session (foreign key) | 0 | 0 | PASS |
| Negative user-input values | 0 | 0 | PASS |
| `done_after_disconnect` flagged sessions | 409 | 409 | Known anomaly |

## 3. The known anomaly

409 sessions (about 2.5% of 16,125) have a done-charging time after the disconnect time.
The cause is station firmware recording a final packet after the cable was unplugged.
These rows are kept because their connection, disconnect and energy values are valid.
Their `charging_duration_min` and `idle_duration_min` are NULL so they cannot give
negative durations in averages.

## 4. Missing values (by design)

| Column | Null rows | Share | Reason |
|--------|-----------|-------|--------|
| done_charging_time | 2,047 | 12.7% | Vehicle never reported a final charging time |
| user_id | 2,233 | 13.8% | Anonymous sessions |
| idle_duration_min | 2,456 | 15.2% | No done-charging time (2,047) plus the 409 anomalies |

## 5. Time coverage

Real data runs from 2019-01-01 to 2021-09-14 only. The Caltech site stopped
operating in September 2021, so 2022 to 2024 has no sessions. Reports and the
dashboard must state this limit.

## 6. Streaming tables (simulated)

`charging_events` and `station_window_metrics` are simulated and labelled
`is_synthetic = TRUE`. The streaming checks (duplicate event IDs, unknown stations,
decreasing energy, non-zero energy at session start, missing synthetic label) ran
only on 3 test events and returned 0 issues. They must be run again after the
end-to-end test with the real Kafka and Spark stream.