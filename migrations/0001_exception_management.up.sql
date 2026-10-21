-- 0001_exception_management.up.sql: exception-management's OLTP schema
-- (ADR 0002 aggregates, ADR 0004 facts). No read-model tables: the case
-- FREEZES the source payload at open and the correlation reads the arriving
-- event, so there is no consumed-facts local copy to keep.
--
-- Text keys that are compared or ordered in Go (case ids, dedup key parts,
-- queues, assignees) use the "C" collation so ORDER BY follows byte order,
-- the same order the in-memory adapter uses.

-- exception_cases: one row per ExceptionCase aggregate (the whole snapshot).
-- version starts at 1, is incremented by exactly one per accepted
-- transition, and guards every UPDATE
-- (... WHERE exception_cases.version = excluded.version - 1).
CREATE TABLE exception_cases (
    case_id                   TEXT COLLATE "C" PRIMARY KEY,
    source_type               TEXT COLLATE "C" NOT NULL,
    source_subject            TEXT COLLATE "C" NOT NULL,
    site_id                   TEXT COLLATE "C" NOT NULL DEFAULT '',
    status                    TEXT        NOT NULL CHECK (status IN ('OPEN', 'TRIAGED', 'ASSIGNED', 'RESOLVED')),
    severity                  TEXT        NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    queue                     TEXT COLLATE "C" NOT NULL,
    assignee                  TEXT COLLATE "C" NOT NULL DEFAULT '',
    escalated                 BOOLEAN     NOT NULL DEFAULT false,
    occurrence                BIGINT      NOT NULL CHECK (occurrence >= 1),
    opened_at                 TIMESTAMPTZ NOT NULL,
    last_seen_at              TIMESTAMPTZ NOT NULL,
    sla_minutes               BIGINT      NOT NULL CHECK (sla_minutes >= 1),
    source_event_id           TEXT COLLATE "C" NOT NULL,
    source_payload            JSONB       NOT NULL,
    resolution_code           TEXT COLLATE "C" NOT NULL DEFAULT '',
    root_cause                TEXT COLLATE "C" NOT NULL DEFAULT '',
    note                      TEXT        NOT NULL DEFAULT '',
    resolved_at               TIMESTAMPTZ,
    auto_resolved             BOOLEAN     NOT NULL DEFAULT false,
    auto_rule_id              TEXT COLLATE "C" NOT NULL DEFAULT '',
    auto_correlated_type      TEXT COLLATE "C" NOT NULL DEFAULT '',
    auto_correlated_event_id  TEXT COLLATE "C" NOT NULL DEFAULT '',
    version                   BIGINT      NOT NULL CHECK (version >= 1),
    audit                     JSONB       NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (last_seen_at >= opened_at)
);

-- Invariant (ADR 0002): at most ONE open case per dedup key
-- (source type, source subject, site). The use case decides open-or-touch
-- and this index backs the rule: the losing writer of a race fails here and
-- becomes usecases.ErrOpenCaseExists. RESOLVED is terminal, so a resolved
-- case does NOT hold the key -- but the domain never reopens: a later fact
-- touches the resolved case (occurrence+1), it never opens a second case.
CREATE UNIQUE INDEX uq_exception_cases_one_open_per_key
    ON exception_cases (source_type, source_subject, site_id)
    WHERE status <> 'RESOLVED';

-- The owner lookup of a dedup key (the open case first, else the resolved
-- owner) and the operator queue view / SLA sweep (open cases, opened-at
-- then case id).
CREATE INDEX idx_exception_cases_key
    ON exception_cases (source_type, source_subject, site_id, status);

CREATE INDEX idx_exception_cases_open
    ON exception_cases (opened_at, case_id)
    WHERE status <> 'RESOLVED';

-- exception_policies: one row per ExceptionPolicy VERSION, append-only
-- (ADR 0002: history is never rewritten; Get returns the latest version of
-- the source type). The version is part of the primary key, so a redeclare
-- races another writer of the same next version into a unique violation
-- (usecases.ErrConcurrentModification).
CREATE TABLE exception_policies (
    source_type                 TEXT COLLATE "C" NOT NULL,
    version                     BIGINT      NOT NULL CHECK (version >= 1),
    severity                    TEXT        NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    queue                       TEXT COLLATE "C" NOT NULL,
    sla_minutes                 BIGINT      NOT NULL CHECK (sla_minutes >= 1),
    auto_resolve_on_type        TEXT COLLATE "C",
    auto_resolve_within_minutes BIGINT      CHECK (auto_resolve_within_minutes >= 1),
    declared_at                 TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_type, version),
    CHECK (
        (auto_resolve_on_type IS NULL AND auto_resolve_within_minutes IS NULL)
        OR
        (auto_resolve_on_type IS NOT NULL AND auto_resolve_within_minutes IS NOT NULL)
    )
);

-- processed_events: the inbound consumers' dedupe guard (fleet rule: the
-- CloudEvents id is claimed in the SAME transaction as the effect, so a
-- rollback un-claims it). One row per processed event id across consumers:
-- every consumed type funnels into the same open-or-touch / correlation
-- effect, so one guard suffices.
CREATE TABLE processed_events (
    event_id     TEXT COLLATE "C" PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- outbox: the transactional outbox (the relay table of the fleet template).
-- One row per DOMAIN fact, inserted in the SAME transaction as the
-- aggregate's Save; the relay (a later step) drains unpublished rows,
-- encodes them as CloudEvents 1.0 and produces to Kafka.
--   event_id  the CloudEvents id (UUID v4), minted ONCE here at enqueue and
--             persisted: a relay retry republishes the same id.
--   payload   the domain fact, JSON (the relay maps it to the wire shapes).
--   attempts / next_attempt_at  the retry state; the claim scan reads
--             (attempts, next_attempt_at).
CREATE TABLE outbox (
    id              BIGSERIAL   PRIMARY KEY,
    event_id        TEXT COLLATE "C" NOT NULL UNIQUE,
    aggreg_type     TEXT COLLATE "C" NOT NULL,
    aggreg_id       TEXT COLLATE "C" NOT NULL,
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_attempts_next_attempt ON outbox (attempts, next_attempt_at);
