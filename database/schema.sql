-- ============================================================
-- database/schema.sql — PostgreSQL table definitions
-- Owner: Bainty Kaur
--
-- Field names are placeholders. They will be finalised after
-- examining the actual ACN-Data dataset structure.
-- ============================================================

-- Historical charging sessions (batch-loaded)
CREATE TABLE IF NOT EXISTS charging_sessions (
    session_id          VARCHAR(64)     PRIMARY KEY,
    station_id          VARCHAR(64)     NOT NULL,
    -- TODO: Add remaining fields from ACN-Data after dataset inspection
    -- Examples may include: connection_time, done_charging_time,
    -- disconnect_time, kwh_delivered, user_id, etc.
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Real-time charging events (stream-loaded)
CREATE TABLE IF NOT EXISTS charging_events (
    event_id            VARCHAR(64)     PRIMARY KEY,
    station_id          VARCHAR(64)     NOT NULL,
    event_type          VARCHAR(32)     NOT NULL,
    event_timestamp     TIMESTAMP       NOT NULL,
    energy_kwh          NUMERIC(10, 2),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common analytical queries
CREATE INDEX IF NOT EXISTS idx_sessions_station ON charging_sessions(station_id);
CREATE INDEX IF NOT EXISTS idx_events_station ON charging_events(station_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON charging_events(event_timestamp);
