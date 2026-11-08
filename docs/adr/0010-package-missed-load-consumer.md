# ADR 0010: PackageMissedLoad consumer

## Status

Accepted (2026-11-06).

## Context

outbound-shipping publishes `PackageMissedLoad` for every package that was
not loaded by its cutoff or by the trailer's departure (raised at most once
per package and load; a re-run sweep or a repeated departure publishes
nothing new). A missed package needs a problem-solver — decide its next
load or its return to stock — while order-management re-promises the order
from the same fact. exception-management owns the case. Constraints from
ADR 0003 (no live call to a sibling context; consumed contracts restated
locally) and ADR 0004 (CloudEvents 1.0 structured JSON, full `type`
dispatch).

## Decision

1. **One consumer, one type.** `warehouse.outbound-shipping.events` is read
   under its own group `OUTBOUND_SHIPPING_EXCEPTIONS_CONSUMER_GROUP` (env,
   no default, boot error when unset; rendered by the Helm chart). Only
   `com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad`
   is acted on; the other types on the topic are ignored.
2. **Payload (snake_case, as shipped):** `package_id`, `load_id`,
   `site_id`, `source_order_id?`, `cutoff`, `detected_at`, `version`.
   `source_order_id` is omitted when the manifest carried none; unknown
   additive fields are ignored (no `DisallowUnknownFields`).
3. **Natural key — one case per missed package.** The dedup key is
   `(type, package_id, site_id)`: the producer's subject is the package,
   and the payload's `site_id` is the site part of the key (the same
   package id at another site would be another case). A replayed
   CloudEvents id is a no-op (processed-event claim in the same
   transaction as the effect). The producer flags at most once per package
   AND load, so a re-flag under a new id — the same package missing its
   NEXT load — TOUCHES the open case (occurrence+1, `lastSeenAt`), it
   never opens a second one: the case is per package, not per load.
4. **Operator references stay frozen.** The case freezes the producer's
   payload exactly as consumed: the package, load, order, cutoff and
   detection instant the operator works from are on the case, never
   re-fetched (ADR 0003).
5. **Routing.** One policy source type, seeded by migration 0004
   (version 1, redeclarable through the policy API):
   `...PackageMissedLoad` (HIGH, queue `problem-solve`, 60 min) — a
   package that missed its truck is a problem-solver's case, in step with
   the carton `#problem-solve` route of ADR 0006.
6. **Poison handling is unchanged.** A payload of the handled type that
   can never be applied (data that does not decode, no `package_id`)
   is dead-lettered to `<topic>.dlq` and committed past; a transient
   failure retries the same message with capped backoff.

## Consequences

- The case lifecycle is exactly the existing one: the cases appear in the
  REST list (filters `sourceType`, `queue`, `site`), detail,
  triage/assign/resolve, the MCP tools and the SLA sweep.
- order-management's re-promise (its own consumption of the same fact) is
  NOT the case's auto-resolve: resolving a missed-load case is an
  operator decision, so no auto-resolve rule is declared for this source
  type. If a later fleet decision wants a correlate (e.g. the package
  loading on its next load), that is a new correlated fact and a policy
  rule, added then.
- If the policy row is absent the message is skipped with a WARN and
  committed past (ADR 0002 policy rule).
