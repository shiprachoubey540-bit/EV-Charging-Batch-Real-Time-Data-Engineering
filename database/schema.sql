-- ============================================================
-- database/schema.sql — PostgreSQL warehouse schema
-- Owner: Bainty Kaur
-- Built from contracts/data_contracts.yaml and the batch
-- handoff spec (sessions.parquet: 17 cols, user_inputs.parquet: 9 cols)
-- ============================================================

-- Station dimension (55 verified Caltech stations)
CREATE TABLE IF NOT EXISTS dim_station (
    station_id      VARCHAR(20)  PRIMARY KEY,
    space_id        VARCHAR(10),
    site_id         VARCHAR(10),
    cluster_id      VARCHAR(10)
);

-- Historical charging sessions (batch-loaded from sessions.parquet)
CREATE TABLE IF NOT EXISTS charging_sessions (
    id                    VARCHAR(24)       PRIMARY KEY,
    session_id            VARCHAR(64)       NOT NULL UNIQUE,
    site_id               VARCHAR(10)       NOT NULL,
    cluster_id            VARCHAR(10)       NOT NULL,
    station_id            VARCHAR(20)       NOT NULL REFERENCES dim_station(station_id),
    space_id              VARCHAR(10)       NOT NULL,
    connection_time       TIMESTAMPTZ       NOT NULL,
    disconnect_time       TIMESTAMPTZ       NOT NULL,
    done_charging_time    TIMESTAMPTZ,
    kwh_delivered         DOUBLE PRECISION  NOT NULL CHECK (kwh_delivered >= 0),
    user_id               VARCHAR(12),
    timezone              VARCHAR(30)       NOT NULL,
    session_duration_min  DOUBLE PRECISION,
    charging_duration_min DOUBLE PRECISION,
    idle_duration_min     DOUBLE PRECISION,
    validation_flags      TEXT,
    ingestion_timestamp   TIMESTAMPTZ       NOT NULL
);

-- User inputs (batch-loaded from user_inputs.parquet), many per session
CREATE TABLE IF NOT EXISTS session_user_inputs (
    id                    SERIAL            PRIMARY KEY,
    session_id            VARCHAR(64)       NOT NULL REFERENCES charging_sessions(session_id),
    user_id_numeric       BIGINT            NOT NULL,
    wh_per_mile           DOUBLE PRECISION  NOT NULL,
    kwh_requested         DOUBLE PRECISION  NOT NULL,
    miles_requested       DOUBLE PRECISION  NOT NULL,
    minutes_available     DOUBLE PRECISION  NOT NULL,
    modified_at           TIMESTAMPTZ       NOT NULL,
    payment_required      BOOLEAN           NOT NULL,
    requested_departure   TIMESTAMPTZ       NOT NULL
);

-- Streaming events (stream-loaded). ALL rows are simulated.
CREATE TABLE IF NOT EXISTS charging_events (
    event_id          VARCHAR(64)       PRIMARY KEY,
    station_id        VARCHAR(20)       NOT NULL,
    session_id        VARCHAR(64)       NOT NULL,
    event_type        VARCHAR(20)       NOT NULL
                      CHECK (event_type IN ('session_start', 'energy_update', 'session_end')),
    event_timestamp   TIMESTAMPTZ       NOT NULL,
    energy_kwh        DOUBLE PRECISION  NOT NULL CHECK (energy_kwh >= 0),
    incremental_kwh   DOUBLE PRECISION  CHECK (incremental_kwh >= 0),
    is_synthetic      BOOLEAN           NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ       DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common analytical queries
CREATE INDEX IF NOT EXISTS idx_sessions_station    ON charging_sessions(station_id);
CREATE INDEX IF NOT EXISTS idx_sessions_connection ON charging_sessions(connection_time);
CREATE INDEX IF NOT EXISTS idx_inputs_session      ON session_user_inputs(session_id);
CREATE INDEX IF NOT EXISTS idx_events_station      ON charging_events(station_id);
CREATE INDEX IF NOT EXISTS idx_events_session      ON charging_events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp    ON charging_events(event_timestamp);


-- Windowed streaming aggregates (written by Spark Structured Streaming)
-- Columns match processing/stream_transform.py exactly. Simulated data.
CREATE TABLE IF NOT EXISTS station_window_metrics (
    id                     SERIAL            PRIMARY KEY,
    station_id             VARCHAR(20)       NOT NULL,
    window_start           TIMESTAMPTZ       NOT NULL,
    window_end             TIMESTAMPTZ       NOT NULL,
    event_count            BIGINT            NOT NULL,
    total_incremental_kwh  DOUBLE PRECISION  NOT NULL,
    max_cumulative_kwh     DOUBLE PRECISION  NOT NULL,
    is_synthetic           BOOLEAN           NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_window_station ON station_window_metrics(station_id);
CREATE INDEX IF NOT EXISTS idx_window_start   ON station_window_metrics(window_start);