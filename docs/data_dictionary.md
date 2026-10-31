# Data Dictionary (PostgreSQL warehouse)

Database: `ev_charging_db`. Schema script: `database/schema.sql`.
Machine-readable source contracts: `contracts/data_contracts.yaml`.
Owner of this document: Bainty Kaur.

**Data truthfulness rule:** `charging_sessions` and `session_user_inputs` hold real
historical ACN-Data (Caltech site, Jan 2019 to Sep 2021 only). `charging_events` and
`station_window_metrics` hold **simulated** data and are labelled `is_synthetic = TRUE`.

## Table overview

| Table | Rows come from | Real or simulated | Primary key |
|-------|----------------|-------------------|-------------|
| dim_station | station_summary.csv (55 stations) | Real | station_id |
| charging_sessions | sessions.parquet (batch) | Real | id (unique: session_id) |
| session_user_inputs | user_inputs.parquet (batch) | Real | id (serial) |
| charging_events | Kafka stream via Spark | Simulated | event_id |
| station_window_metrics | Spark windowed aggregation | Simulated | id (serial) |

## Relationships

- `charging_sessions.station_id` -> `dim_station.station_id`
- `session_user_inputs.session_id` -> `charging_sessions.session_id` (one session, many inputs)
- `charging_events.station_id` and `station_window_metrics.station_id` have no foreign key
  on purpose, so a wrong simulated station cannot crash the stream. Quality check Q11 catches them.

## dim_station

| Column | Type | Null | Description |
|--------|------|------|-------------|
| station_id | VARCHAR(20) | No | Charging station ID, e.g. 2-39-81-4550 |
| space_id | VARCHAR(10) | Yes | Parking space label, e.g. CA-322 |
| site_id | VARCHAR(10) | Yes | Site ID, "0002" for Caltech |
| cluster_id | VARCHAR(10) | Yes | Cluster within the site |

## charging_sessions (historical, real)

| Column | Type | Null | Description |
|--------|------|------|-------------|
| id | VARCHAR(24) | No | MongoDB ObjectId from ACN-Data |
| session_id | VARCHAR(64) | No | Unique business session ID |
| site_id | VARCHAR(10) | No | Site ID |
| cluster_id | VARCHAR(10) | No | Cluster ID |
| station_id | VARCHAR(20) | No | FK to dim_station |
| space_id | VARCHAR(10) | No | Parking space label |
| connection_time | TIMESTAMPTZ | No | Plug-in time (UTC) |
| disconnect_time | TIMESTAMPTZ | No | Plug-out time (UTC) |
| done_charging_time | TIMESTAMPTZ | Yes | Last non-zero charging power time |
| kwh_delivered | DOUBLE PRECISION | No | Energy delivered, kWh (must be >= 0) |
| user_id | VARCHAR(12) | Yes | Zero-padded user ID; null for anonymous sessions |
| timezone | VARCHAR(30) | No | Site IANA time zone, America/Los_Angeles |
| session_duration_min | DOUBLE PRECISION | Yes | disconnect minus connection, minutes |
| charging_duration_min | DOUBLE PRECISION | Yes | done_charging minus connection, minutes |
| idle_duration_min | DOUBLE PRECISION | Yes | disconnect minus done_charging, minutes |
| validation_flags | TEXT | Yes | Comma-separated flags, e.g. done_after_disconnect |
| ingestion_timestamp | TIMESTAMPTZ | No | When the ETL processed the row |

Rule: when `validation_flags` contains `done_after_disconnect`, both
`charging_duration_min` and `idle_duration_min` are NULL (they would be negative).

## session_user_inputs (historical, real)

| Column | Type | Null | Description |
|--------|------|------|-------------|
| id | SERIAL | No | Surrogate key |
| session_id | VARCHAR(64) | No | FK to charging_sessions.session_id |
| user_id_numeric | BIGINT | No | User ID as an integer |
| wh_per_mile | DOUBLE PRECISION | No | Vehicle efficiency, Wh per mile |
| kwh_requested | DOUBLE PRECISION | No | Energy the driver asked for, kWh |
| miles_requested | DOUBLE PRECISION | No | Range the driver asked for, miles |
| minutes_available | DOUBLE PRECISION | No | Time the driver expects to stay, minutes |
| modified_at | TIMESTAMPTZ | No | When the input was submitted |
| payment_required | BOOLEAN | No | Whether payment was required |
| requested_departure | TIMESTAMPTZ | No | Driver's requested departure time |

## charging_events (streaming, SIMULATED)

| Column | Type | Null | Description |
|--------|------|------|-------------|
| event_id | VARCHAR(64) | No | Unique event ID, format evt-<uuid4> |
| station_id | VARCHAR(20) | No | Station that emitted the event |
| session_id | VARCHAR(64) | No | Groups the events of one charging session |
| event_type | VARCHAR(20) | No | session_start, energy_update or session_end |
| event_timestamp | TIMESTAMPTZ | No | Event time (UTC). Called `timestamp` in the Kafka JSON |
| energy_kwh | DOUBLE PRECISION | No | Cumulative energy in the session, kWh (>= 0) |
| incremental_kwh | DOUBLE PRECISION | Yes | Energy since the previous event, kWh (>= 0) |
| is_synthetic | BOOLEAN | No | Always TRUE; default is filled automatically |
| created_at | TIMESTAMPTZ | Yes | Row insert time |

## station_window_metrics (streaming aggregates, SIMULATED)

| Column | Type | Null | Description |
|--------|------|------|-------------|
| id | SERIAL | No | Surrogate key |
| station_id | VARCHAR(20) | No | Station |
| window_start | TIMESTAMPTZ | No | Start of the aggregation window |
| window_end | TIMESTAMPTZ | No | End of the aggregation window |
| event_count | BIGINT | No | Events in the window |
| total_incremental_kwh | DOUBLE PRECISION | No | Sum of incremental energy in the window |
| max_cumulative_kwh | DOUBLE PRECISION | No | Highest cumulative energy seen in the window |
| is_synthetic | BOOLEAN | No | Always TRUE |
| created_at | TIMESTAMPTZ | Yes | Row insert time |

## Known limitations

- Historical data covers only 2019-01-01 to 2021-09-14. The site stopped operating in
  September 2021, so 2022 to 2024 has no sessions.
- Streaming data is simulated and must never be reported as live observations.
- Revenue is not stored; it cannot be derived from energy alone.