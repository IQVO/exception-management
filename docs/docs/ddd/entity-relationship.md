---
id: entity-relationship
title: Entity-relationship
sidebar_label: Entity-relationship
sidebar_position: 18
description: The Postgres schema behind the aggregates — migrations 0001 and 0002, every table, constraint and index, with the aggregate each belongs to.
---

# Entity-relationship

The whole OLTP schema is two migrations
(`migrations/0001_exception_management.up.sql`,
`migrations/0002_idempotency_keys.up.sql`), run embedded at boot by both
binaries. No read-model tables exist by design: the case freezes its own
evidence (ADR 0003).

```mermaid
erDiagram
    exception_cases {
        TEXT case_id PK "case-<uuid>, COLLATE C"
        TEXT source_type "dedup key 1/3"
        TEXT source_subject "dedup key 2/3"
        TEXT site_id "dedup key 3/3, '' when the fact has no site"
        TEXT status "OPEN | TRIAGED | ASSIGNED | RESOLVED"
        TEXT severity "LOW | MEDIUM | HIGH | CRITICAL"
        TEXT queue
        TEXT assignee "'' until assigned"
        BOOLEAN escalated "set once by the sweeper"
        BIGINT occurrence ">= 1"
        TIMESTAMPTZ opened_at
        TIMESTAMPTZ last_seen_at ">= opened_at"
        BIGINT sla_minutes ">= 1"
        TEXT source_event_id "the CloudEvents id that opened it"
        JSONB source_payload "frozen at open"
        TEXT resolution_code "'' until resolved"
        TEXT root_cause
        TEXT note
        TIMESTAMPTZ resolved_at
        BOOLEAN auto_resolved
        TEXT auto_rule_id
        TEXT auto_correlated_type
        TEXT auto_correlated_event_id
        BIGINT version "optimistic concurrency, +1 per transition"
        JSONB audit "one entry per accepted transition"
    }
    exception_policies {
        TEXT source_type PK "full CloudEvents type"
        BIGINT version PK "append-only, >= 1"
        TEXT severity
        TEXT queue
        BIGINT sla_minutes ">= 1"
        TEXT auto_resolve_on_type "NULL pair or both set"
        BIGINT auto_resolve_within_minutes
        TIMESTAMPTZ declared_at
    }
    processed_events {
        TEXT event_id PK "CloudEvents id, claimed with the effect"
        TIMESTAMPTZ processed_at
    }
    outbox {
        BIGSERIAL id PK
        TEXT event_id UK "CloudEvents id, minted once at enqueue"
        TEXT aggreg_type
        TEXT aggreg_id
        JSONB payload "the domain fact"
        TIMESTAMPTZ occurred_at
        INTEGER attempts
        TIMESTAMPTZ next_attempt_at
        TIMESTAMPTZ created_at
    }
    idempotency_keys {
        TEXT key PK "the Idempotency-Key header"
        TEXT fingerprint "hash of method, path and body"
        INTEGER status
        TEXT content_type
        BYTEA body "the stored answer"
        TIMESTAMPTZ created_at
    }
```

(The `idempotency_keys` column list follows migration 0002 and the
middleware's store contract in `internal/adapters/inbound/http/idempotency.go`.)

## Analytical database

The exception-analytics read model is a separate database
(`exception_management_analytics`, `ANALYTICS_DATABASE_URL`), written only by
`cmd/exceptions-projector` from `warehouse.exception-management.analytics` and
read by `cmd/exceptions-reports`. Facts, not aggregates: it can be dropped and
rebuilt by replaying the topic under a new consumer group. No foreign keys.
Source: `analytics/migrations/0001_exception_analytics.up.sql`.

```mermaid
erDiagram
    analytics_processed_events {
        text event_id PK "every applied CloudEvents id, claimed with its effect"
        text event_type
        timestamptz occurred_at "indexed; max() is the projection's freshness"
        timestamptz applied_at
    }
    exception_case_facts {
        text case_id PK
        text source_type "nullable until the OPENED event is seen"
        text source_subject
        text site_id
        text severity
        text queue
        timestamptz opened_at "nullable"
        timestamptz sla_due_at "nullable; ExceptionEscalated refreshes it"
        timestamptz triaged_at "nullable; latest wins"
        timestamptz assigned_at "nullable"
        text assignee "nullable; latest wins"
        timestamptz escalated_at "nullable"
        timestamptz resolved_at "nullable"
        timestamptz auto_resolved_at "nullable"
        text resolution_code "nullable"
        text root_cause "nullable"
    }
    exception_policy_facts {
        text source_type PK
        bigint version PK
        text severity
        text queue
        bigint sla_minutes
        text auto_resolve_on_type "nullable"
        bigint auto_resolve_within_minutes "nullable"
        timestamptz declared_at "earliest time seen for the version"
    }
```

## The constraints that matter

| Constraint | Enforces |
| --- | --- |
| `uq_exception_cases_one_open_per_key` — UNIQUE `(source_type, source_subject, site_id) WHERE status <> 'RESOLVED'` | The one-open-case-per-dedup-key invariant across cases; the loser of a race becomes `ErrOpenCaseExists`. A resolved case does not hold the key — but the domain never reopens: a later fact touches the resolved case. |
| `CHECK (last_seen_at >= opened_at)` | Occurrence semantics; `occurrence >= 1` likewise. |
| `PRIMARY KEY (source_type, version)` on `exception_policies` | Append-only versions; two writers racing the same next version collide into `ErrConcurrentModification`. |
| `CHECK` on `auto_resolve_*` — both NULL or both set | The rule is a pair, never half a rule. |
| `event_id UNIQUE` in `outbox` | A relay retry republishes the same CloudEvents id byte-identically; consumers dedupe on it. |
| `version` on `exception_cases` | Every UPDATE guards `WHERE version = excluded.version - 1` — optimistic concurrency, exactly one accepted transition per version. |

## Indexes

- `idx_exception_cases_key (source_type, source_subject, site_id, status)`
  — the dedup-key owner lookup (open case first, else the resolved owner).
- `idx_exception_cases_open (opened_at, case_id) WHERE status <> 'RESOLVED'`
  — the operator listing and the SLA sweep.
- `idx_outbox_attempts_next_attempt (attempts, next_attempt_at)` — the
  relay's claim scan (`FOR UPDATE SKIP LOCKED`, up to 100 due rows).
- `idx_idempotency_keys_created_at` — store cleanup.

Text keys compared or ordered in Go (case ids, dedup key parts, queues,
assignees) use the `C` collation so `ORDER BY` follows byte order, the
same order the in-memory adapter uses.
