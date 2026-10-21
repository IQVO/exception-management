-- 0001_exception_management.down.sql: recreates the previous schema
-- (empty) exactly.

DROP INDEX IF EXISTS idx_outbox_attempts_next_attempt;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS exception_policies;
DROP INDEX IF EXISTS idx_exception_cases_open;
DROP INDEX IF EXISTS idx_exception_cases_key;
DROP INDEX IF EXISTS uq_exception_cases_one_open_per_key;
DROP TABLE IF EXISTS exception_cases;
