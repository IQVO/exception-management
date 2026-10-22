-- exception-management analytics read model (ADR 0006).
--
-- This is the ANALYTICAL database, separate from the OLTP database. It is
-- written only by cmd/exceptions-projector and read (read-only) by
-- cmd/exceptions-reports. Everything here is a projection derived from
-- warehouse.exception-management.analytics, not a source of truth: it can be
-- dropped and rebuilt by replaying the topic under a new consumer group.
--
-- Facts, not aggregates: every number of the three reports is counted from
-- these rows over the requested window. Each milestone column keeps the
-- CloudEvents time of the EARLIEST such event seen (LEAST ignores NULLs), so
-- arrival order never matters; the descriptive columns keep the first
-- non-NULL value, and severity/queue/assignee keep the LATEST (a triage or
-- an assignment supersedes the opened values; a redelivered old event is
-- stopped by the dedupe claim before it can reach the upsert).

-- Idempotency: every applied CloudEvents id is recorded here exactly once, in
-- the SAME transaction as its effect. occurred_at is the event's CloudEvents
-- `time`; max(occurred_at) is the freshness of the projection.
CREATE TABLE analytics_processed_events (
    event_id    TEXT        PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_analytics_processed_events_occurred_at ON analytics_processed_events (occurred_at);

-- exception_case_facts: one row per exception case the projection has seen
-- (any of its events). A case whose OPENED event has not been seen yet (an
-- out-of-order redelivery) has NULL source columns until it arrives; the
-- reports count such a case only where its milestones allow it, exactly like
-- a completion without a seen request has no latency.
--   opened_at / sla_due_at   ExceptionOpened (queue, severity, the source ids)
--   triaged_at               ExceptionTriaged (latest severity/queue win)
--   assigned_at / assignee   ExceptionAssigned (latest assignee wins)
--   escalated_at             ExceptionEscalated (also refreshes sla_due_at)
--   resolved_at              ExceptionResolved (resolution_code, root_cause, note)
--   auto_resolved_at         ExceptionAutoResolved (rule_id)
CREATE TABLE exception_case_facts (
    case_id          TEXT COLLATE "C" PRIMARY KEY,
    source_type      TEXT COLLATE "C",
    source_subject   TEXT COLLATE "C",
    site_id          TEXT COLLATE "C",
    severity         TEXT COLLATE "C",
    queue            TEXT COLLATE "C",
    opened_at        TIMESTAMPTZ,
    sla_due_at       TIMESTAMPTZ,
    triaged_at       TIMESTAMPTZ,
    assigned_at      TIMESTAMPTZ,
    assignee         TEXT COLLATE "C",
    escalated_at     TIMESTAMPTZ,
    resolved_at      TIMESTAMPTZ,
    auto_resolved_at TIMESTAMPTZ,
    resolution_code  TEXT COLLATE "C",
    root_cause       TEXT COLLATE "C",
    resolution_note  TEXT COLLATE "C",
    rule_id          TEXT COLLATE "C"
);

CREATE INDEX idx_exception_case_facts_source_type ON exception_case_facts (source_type);
CREATE INDEX idx_exception_case_facts_queue       ON exception_case_facts (queue);
CREATE INDEX idx_exception_case_facts_opened      ON exception_case_facts (opened_at) WHERE opened_at IS NOT NULL;
CREATE INDEX idx_exception_case_facts_resolved    ON exception_case_facts (resolved_at) WHERE resolved_at IS NOT NULL;
CREATE INDEX idx_exception_case_facts_escalated   ON exception_case_facts (escalated_at) WHERE escalated_at IS NOT NULL;

-- exception_policy_facts: one row per declared policy VERSION of a source
-- type (ExceptionPolicyDeclared). declared_at keeps the earliest time seen
-- for the version.
CREATE TABLE exception_policy_facts (
    source_type                  TEXT COLLATE "C" NOT NULL,
    version                      BIGINT      NOT NULL,
    severity                     TEXT COLLATE "C" NOT NULL,
    queue                        TEXT COLLATE "C" NOT NULL,
    sla_minutes                  BIGINT      NOT NULL,
    auto_resolve_on_type         TEXT COLLATE "C",
    auto_resolve_within_minutes  BIGINT,
    declared_at                  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_type, version)
);

CREATE INDEX idx_exception_policy_facts_declared_at ON exception_policy_facts (declared_at);
