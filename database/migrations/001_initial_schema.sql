-- ============================================================
-- Migration: 001_initial_schema.sql
-- Description: Initial PostgreSQL schema for EV Charging pipeline.
-- Owner: Bainty Kaur (database/), Aleena Antony (processing/)
--
-- Entities:
--   1. schema_migrations       - Tracks applied database migrations
--   2. stations                - Fleet stations dimension
--   3. charging_sessions       - Batch-loaded historical sessions
--   4. session_user_inputs     - Normalised user inputs linked to sessions
--   5. charging_events         - Real-time validated streaming events
--   6. streaming_quarantine    - Corrupted or contract-violating streaming events
--   7. station_streaming_metrics - Real-time aggregated station metrics
-- ============================================================

-- 1. Schema Migrations Audit Table
CREATE TABLE IF NOT EXISTS schema_migrations (
    version             VARCHAR(64)     PRIMARY KEY,
    description         TEXT            NOT NULL,
    applied_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Charging Stations Dimension
CREATE TABLE IF NOT EXISTS stations (
    station_id                  VARCHAR(32)     PRIMARY KEY,
    site_id                     VARCHAR(10)     NOT NULL DEFAULT '0002',
    cluster_id                  VARCHAR(10)     NOT NULL DEFAULT '0039',
    space_id                    VARCHAR(20),
    total_historical_sessions   INT             DEFAULT 0,
    total_historical_kwh        NUMERIC(12, 3)  DEFAULT 0.000,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Historical Charging Sessions (Batch Path)
CREATE TABLE IF NOT EXISTS charging_sessions (
    id                          VARCHAR(32)     PRIMARY KEY,
    session_id                  VARCHAR(64)     UNIQUE NOT NULL,
    site_id                     VARCHAR(10)     NOT NULL,
    cluster_id                  VARCHAR(10)     NOT NULL,
    station_id                  VARCHAR(32)     NOT NULL REFERENCES stations(station_id),
    space_id                    VARCHAR(20)     NOT NULL,
    connection_time             TIMESTAMPTZ     NOT NULL,
    disconnect_time             TIMESTAMPTZ     NOT NULL,
    done_charging_time          TIMESTAMPTZ,
    kwh_delivered               NUMERIC(10, 3)  NOT NULL CHECK (kwh_delivered >= 0.0),
    user_id                     VARCHAR(20),
    timezone                    VARCHAR(50)     NOT NULL DEFAULT 'America/Los_Angeles',
    session_duration_min        NUMERIC(10, 2),
    charging_duration_min       NUMERIC(10, 2),
    idle_duration_min           NUMERIC(10, 2),
    validation_flags            TEXT,
    ingestion_timestamp         TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Normalised Session User Inputs
CREATE TABLE IF NOT EXISTS session_user_inputs (
    id                          BIGSERIAL       PRIMARY KEY,
    session_id                  VARCHAR(64)     NOT NULL REFERENCES charging_sessions(session_id) ON DELETE CASCADE,
    input_index                 INT             NOT NULL DEFAULT 0,
    user_id_numeric             INT,
    wh_per_mile                 NUMERIC(8, 2),
    kwh_requested               NUMERIC(10, 3),
    miles_requested             NUMERIC(8, 2),
    minutes_available           NUMERIC(10, 2),
    modified_at                 TIMESTAMPTZ     NOT NULL,
    payment_required            BOOLEAN         NOT NULL DEFAULT TRUE,
    requested_departure         TIMESTAMPTZ     NOT NULL,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_session_user_input UNIQUE (session_id, input_index)
);

-- 5. Real-Time Validated Charging Events (Stream Path)
CREATE TABLE IF NOT EXISTS charging_events (
    event_id                    VARCHAR(64)     PRIMARY KEY,
    station_id                  VARCHAR(32)     NOT NULL REFERENCES stations(station_id),
    session_id                  VARCHAR(64)     NOT NULL,
    event_type                  VARCHAR(32)     NOT NULL CHECK (event_type IN ('session_start', 'energy_update', 'session_end')),
    event_timestamp             TIMESTAMPTZ     NOT NULL,
    energy_kwh                  NUMERIC(10, 3)  NOT NULL CHECK (energy_kwh >= 0.0),
    incremental_kwh             NUMERIC(10, 3)  NOT NULL DEFAULT 0.000 CHECK (incremental_kwh >= 0.0),
    ingested_at                 TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 6. Quarantined Streaming Events
CREATE TABLE IF NOT EXISTS streaming_quarantine (
    id                          BIGSERIAL       PRIMARY KEY,
    event_id                    VARCHAR(64),
    station_id                  VARCHAR(32),
    session_id                  VARCHAR(64),
    raw_payload                 TEXT            NOT NULL,
    quarantine_reason           TEXT            NOT NULL,
    ingested_at                 TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 7. Real-Time Station Aggregate Metrics
CREATE TABLE IF NOT EXISTS station_streaming_metrics (
    station_id                  VARCHAR(32)     PRIMARY KEY REFERENCES stations(station_id),
    event_count                 BIGINT          NOT NULL DEFAULT 0,
    total_incremental_kwh       NUMERIC(12, 3)  NOT NULL DEFAULT 0.000,
    max_cumulative_kwh          NUMERIC(10, 3)  NOT NULL DEFAULT 0.000,
    latest_event_timestamp      TIMESTAMPTZ,
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Indexes for Analytical & Relational Query Performance
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_sessions_station_id ON charging_sessions(station_id);
CREATE INDEX IF NOT EXISTS idx_sessions_connection_time ON charging_sessions(connection_time);
CREATE INDEX IF NOT EXISTS idx_sessions_disconnect_time ON charging_sessions(disconnect_time);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON charging_sessions(user_id);

CREATE INDEX IF NOT EXISTS idx_user_inputs_session_id ON session_user_inputs(session_id);
CREATE INDEX IF NOT EXISTS idx_user_inputs_modified_at ON session_user_inputs(modified_at);

CREATE INDEX IF NOT EXISTS idx_events_station_id ON charging_events(station_id);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON charging_events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON charging_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON charging_events(event_type);

CREATE INDEX IF NOT EXISTS idx_quarantine_ingested_at ON streaming_quarantine(ingested_at);
CREATE INDEX IF NOT EXISTS idx_quarantine_station_id ON streaming_quarantine(station_id);

-- Record migration version
INSERT INTO schema_migrations (version, description)
VALUES ('001_initial_schema', 'Initial DDL for stations, sessions, user inputs, events, quarantine, and metrics')
ON CONFLICT (version) DO NOTHING;
