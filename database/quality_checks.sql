-- ============================================================
-- database/quality_checks.sql — Data-quality validation queries
-- Owner: Bainty Kaur
-- ============================================================

-- Check for null session IDs
-- SELECT COUNT(*) AS null_session_ids
-- FROM charging_sessions
-- WHERE session_id IS NULL;

-- Check for duplicate events
-- SELECT event_id, COUNT(*) AS cnt
-- FROM charging_events
-- GROUP BY event_id
-- HAVING COUNT(*) > 1;

-- TODO: Add more quality checks after schema is finalised
