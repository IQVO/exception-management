-- The analytical schema is a derived copy (ADR 0006): dropping it and
-- replaying the analytics topic under a new consumer group rebuilds it.
DROP TABLE IF EXISTS exception_policy_facts;
DROP TABLE IF EXISTS exception_case_facts;
DROP TABLE IF EXISTS analytics_processed_events;
