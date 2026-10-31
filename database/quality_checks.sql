-- ============================================================
-- database/quality_checks.sql — Data-quality validation queries
-- Owner: Bainty Kaur
-- Every check returns a count. Expected result: 0 unless noted.
-- ============================================================

-- ---------- HISTORICAL SESSIONS ----------

-- Q1. Duplicate business session IDs (expected 0)
SELECT 'duplicate_session_ids' AS check_name, COUNT(*) AS issues
FROM (SELECT session_id FROM charging_sessions
      GROUP BY session_id HAVING COUNT(*) > 1) d;

-- Q2. Sessions whose connection time is after disconnect time (expected 0)
SELECT 'connection_after_disconnect' AS check_name, COUNT(*) AS issues
FROM charging_sessions
WHERE connection_time > disconnect_time;

-- Q3. Negative durations (expected 0; nulls are allowed)
SELECT 'negative_durations' AS check_name, COUNT(*) AS issues
FROM charging_sessions
WHERE session_duration_min < 0
   OR charging_duration_min < 0
   OR idle_duration_min < 0;

-- Q4. Zero or negative energy (expected 0)
SELECT 'non_positive_energy' AS check_name, COUNT(*) AS issues
FROM charging_sessions
WHERE kwh_delivered <= 0;

-- Q5. Sessions at stations missing from dim_station (expected 0)
SELECT 'orphan_session_stations' AS check_name, COUNT(*) AS issues
FROM charging_sessions s
LEFT JOIN dim_station d ON s.station_id = d.station_id
WHERE d.station_id IS NULL;

-- Q6. Known anomaly: done_charging after disconnect (informational; ~409 expected)
SELECT 'done_after_disconnect_flagged' AS check_name, COUNT(*) AS issues
FROM charging_sessions
WHERE validation_flags LIKE '%done_after_disconnect%';

-- Q7. Flagged rows that still have charging/idle durations (expected 0)
SELECT 'flagged_but_not_nulled' AS check_name, COUNT(*) AS issues
FROM charging_sessions
WHERE validation_flags LIKE '%done_after_disconnect%'
  AND (charging_duration_min IS NOT NULL OR idle_duration_min IS NOT NULL);

-- ---------- USER INPUTS ----------

-- Q8. User inputs without a matching session (expected 0)
SELECT 'orphan_user_inputs' AS check_name, COUNT(*) AS issues
FROM session_user_inputs i
LEFT JOIN charging_sessions s ON i.session_id = s.session_id
WHERE s.session_id IS NULL;

-- Q9. Negative user-input values (expected 0)
SELECT 'negative_user_input_values' AS check_name, COUNT(*) AS issues
FROM session_user_inputs
WHERE wh_per_mile < 0 OR kwh_requested < 0
   OR miles_requested < 0 OR minutes_available < 0;

-- ---------- STREAMING EVENTS (SIMULATED) ----------

-- Q10. Duplicate event IDs (expected 0; primary key also blocks these)
SELECT 'duplicate_event_ids' AS check_name, COUNT(*) AS issues
FROM (SELECT event_id FROM charging_events
      GROUP BY event_id HAVING COUNT(*) > 1) d;

-- Q11. Event stations not among the 55 real stations (expected 0)
SELECT 'event_unknown_station' AS check_name, COUNT(*) AS issues
FROM charging_events e
LEFT JOIN dim_station d ON e.station_id = d.station_id
WHERE d.station_id IS NULL;

-- Q12. Energy that decreases within a session (expected 0)
SELECT 'energy_decreasing' AS check_name, COUNT(*) AS issues
FROM (
    SELECT energy_kwh,
           LAG(energy_kwh) OVER (PARTITION BY session_id
                                 ORDER BY event_timestamp) AS prev_kwh
    FROM charging_events
) t
WHERE prev_kwh IS NOT NULL AND energy_kwh < prev_kwh;

-- Q13. session_start events with non-zero energy (expected 0)
SELECT 'start_nonzero_energy' AS check_name, COUNT(*) AS issues
FROM charging_events
WHERE event_type = 'session_start' AND energy_kwh <> 0;

-- Q14. Events not labelled synthetic (expected 0)
SELECT 'event_not_labelled_synthetic' AS check_name, COUNT(*) AS issues
FROM charging_events
WHERE is_synthetic = FALSE;

-- ---------- RECONCILIATION WITH ANANTHAN'S BATCH REPORT ----------

-- Q15. Totals must equal 16,125 sessions / 55 stations / 152,572.77 kWh
SELECT COUNT(*) AS total_sessions,
       COUNT(DISTINCT station_id) AS stations,
       ROUND(SUM(kwh_delivered)::numeric, 2) AS total_kwh
FROM charging_sessions;