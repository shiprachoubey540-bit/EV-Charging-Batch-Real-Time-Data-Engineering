-- ============================================================
-- database/seed.sql — TEST DATA ONLY (not real ACN-Data)
-- Delete with: TRUNCATE session_user_inputs, charging_sessions,
--              charging_events, station_window_metrics, dim_station CASCADE;
-- ============================================================

INSERT INTO dim_station (station_id, space_id, site_id, cluster_id) VALUES
 ('2-39-81-4550', 'CA-322', '0002', '0039'),
 ('2-39-139-28',  'CA-489', '0002', '0039');

INSERT INTO charging_sessions
 (id, session_id, site_id, cluster_id, station_id, space_id,
  connection_time, disconnect_time, done_charging_time, kwh_delivered,
  user_id, timezone, session_duration_min, charging_duration_min,
  idle_duration_min, validation_flags, ingestion_timestamp)
VALUES
 ('test0000000000000000001', 'TEST_S1', '0002', '0039', '2-39-81-4550', 'CA-322',
  '2019-01-02 16:00:00+00', '2019-01-02 20:00:00+00', '2019-01-02 18:00:00+00', 10.5,
  '000000558', 'America/Los_Angeles', 240, 120, 120, NULL, now()),
 ('test0000000000000000002', 'TEST_S2', '0002', '0039', '2-39-139-28', 'CA-489',
  '2019-01-03 17:00:00+00', '2019-01-03 19:00:00+00', '2019-01-03 19:05:00+00', 5.0,
  NULL, 'America/Los_Angeles', 120, NULL, NULL, 'done_after_disconnect', now());

INSERT INTO session_user_inputs
 (session_id, user_id_numeric, wh_per_mile, kwh_requested, miles_requested,
  minutes_available, modified_at, payment_required, requested_departure)
VALUES
 ('TEST_S1', 558, 300, 12.0, 40, 240,
  '2019-01-02 16:01:00+00', FALSE, '2019-01-02 20:00:00+00');

INSERT INTO charging_events
 (event_id, station_id, session_id, event_type, event_timestamp, energy_kwh, incremental_kwh)
VALUES
 ('evt-test-1', '2-39-81-4550', 'SIM_S1', 'session_start',  '2026-10-31 10:00:00+00', 0.0, 0.0),
 ('evt-test-2', '2-39-81-4550', 'SIM_S1', 'energy_update',  '2026-10-31 10:05:00+00', 1.5, 1.5),
 ('evt-test-3', '2-39-81-4550', 'SIM_S1', 'session_end',    '2026-10-31 10:10:00+00', 3.0, 1.5);

INSERT INTO station_window_metrics
 (station_id, window_start, window_end, event_count, total_incremental_kwh, max_cumulative_kwh)
VALUES
 ('2-39-81-4550', '2026-10-31 10:00:00+00', '2026-10-31 10:15:00+00', 3, 3.0, 3.0);