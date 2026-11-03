-- ============================================================
-- database/analytics.sql — Analytical queries
-- Owner: Bainty Kaur
-- Historical queries use charging_sessions (real ACN-Data,
-- Jan 2019 - Sep 2021). Stream queries use simulated data only.
-- ============================================================

-- 1. Total energy and session count per station
SELECT station_id,
       COUNT(*)                     AS sessions,
       ROUND(SUM(kwh_delivered)::numeric, 2) AS total_kwh
FROM charging_sessions
GROUP BY station_id
ORDER BY total_kwh DESC;

-- 2. Monthly sessions and energy
SELECT DATE_TRUNC('month', connection_time AT TIME ZONE 'America/Los_Angeles') AS month,
       COUNT(*)                             AS sessions,
       ROUND(SUM(kwh_delivered)::numeric, 2) AS total_kwh
FROM charging_sessions
GROUP BY month
ORDER BY month;

-- 3. Sessions by hour of day (Los Angeles local time)
SELECT EXTRACT(HOUR FROM connection_time AT TIME ZONE 'America/Los_Angeles') AS hour_of_day,
       COUNT(*) AS sessions
FROM charging_sessions
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 4. Average session duration and energy per station
SELECT station_id,
       ROUND(AVG(session_duration_min)::numeric, 1) AS avg_session_min,
       ROUND(AVG(kwh_delivered)::numeric, 2)        AS avg_kwh
FROM charging_sessions
GROUP BY station_id
ORDER BY avg_kwh DESC;

-- 5. Overall totals (compare with Ananthan: 16,125 sessions, 152,572.77 kWh)
SELECT COUNT(*) AS total_sessions,
       ROUND(SUM(kwh_delivered)::numeric, 2) AS total_kwh,
       MIN(connection_time) AS first_session,
       MAX(connection_time) AS last_session
FROM charging_sessions;

-- 6. SIMULATED stream: events per type
SELECT event_type, COUNT(*) AS events
FROM charging_events
WHERE is_synthetic
GROUP BY event_type;

-- 7. SIMULATED stream: energy per station per window
SELECT station_id, window_start, window_end,
       event_count, total_incremental_kwh
FROM station_window_metrics
ORDER BY window_start DESC
LIMIT 50;