# ADR 0010: yard exceptions consumers and routing

## Status

Accepted (2026-11-05).

## Context

Two new producers join the fleet: yard-management (gate, trailers, yard
audits) and dock-scheduling (appointments and door calendar). Both raise
facts a person has to act on: a visit turned away at the gate, a trailer
whose seal is not the one the carrier declared, a trailer that overstayed,
a yard audit that found trailers missing or in the wrong place, and a
carrier that never showed for its appointment. Their services are built in
parallel to this change and are not on `develop` yet, so the contracts are
pinned consumer-first from the fleet's cross-context contract list (the
same way ADR 0003 pinned returns and inventory). The constraints are the
existing ones: no live call to a sibling (ADR 0003), CloudEvents 1.0
structured JSON with dispatch on the FULL `type` (ADR 0004), one routed case
per natural key with the processed-event claim in the same transaction as
the effect (ADR 0006).

## Decision

1. **Two consumers, two topics, two groups.** Both read under their own
   group from env (no default, boot error when unset, rendered by the Helm
   chart; ADR 0006 pattern):
   - `YARD_MANAGEMENT_EXCEPTIONS_CONSUMER_GROUP` reads
     `warehouse.yard-management.events`.
   - `DOCK_SCHEDULING_EXCEPTIONS_CONSUMER_GROUP` reads
     `warehouse.dock-scheduling.events`.

   Other types on those topics (most of yard-management's surface is
   analytics) are ignored.
2. **Consumed types and payloads** (snake_case, as the producers publish;
   CloudEvents `source` `/warehouse/yard-management` and
   `/warehouse/dock-scheduling`):

   | Type | Fields relied on |
   | --- | --- |
   | `com.warehouse.wms.yard-management.yardvisit.VisitRejected` | `visit_id`, `site_id`, `carrier_scac`, `appointment_id?`, `reason`, `rejected_at` |
   | `com.warehouse.wms.yard-management.trailer.SealExceptionRaised` | `trailer_id`, `site_id`, `appointment_id?`, `expected_seal_number?`, `found_seal_number?`, `raised_at` |
   | `com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded` | `trailer_id`, `site_id`, `kind`, `dwell_minutes`, `threshold_minutes`, `location_code` |
   | `com.warehouse.wms.yard-management.yardcheck.YardCheckClosed` | `yard_check_id`, `site_id`, `closed_at`, `discrepancies[]` of `{kind MISSING\|UNEXPECTED\|WRONG_LOCATION, location_code, trailer_id?, unit_number?}` |
   | `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow` | `appointment_id`, `site_id`, `door_code`, `direction`, `carrier_scac`, `window_end`, `version` |

   Only the fields that make up the dedup key are required; the rest are
   frozen on the case with the whole payload (ADR 0003). A discrepancy `kind`
   is accepted as ANY non-blank string.
3. **Natural keys** (`site_id` is the site part of every key; the case's
   `dedupKey.sourceType` is the event type):
   - VisitRejected: `visit_id`.
   - SealExceptionRaised: `<trailer_id>/<raised_at>`.
   - TrailerDwellExceeded: `<trailer_id>/<CloudEvents time>` (the payload
     has no instant of its own).
   - YardCheckClosed, per discrepancy:
     `<yard_check_id>/<kind>/<location_code>/<trailer_id, else unit_number, else ->`.
   - DockAppointmentNoShow: `appointment_id` (NO_SHOW is terminal in
     dock-scheduling, so one case per appointment).

   A trailer can raise again on a later visit and a resolved case never
   reopens (ADR 0002: a fact for a key that has a case touches it), so the
   instant is part of the seal and dwell keys. The same raise re-published
   under another CloudEvents id carries the same instant and touches the open
   case; a replayed id is a no-op.
4. **One case per discrepancy, one claim per event.**
   `usecases.OpenYardCheckDiscrepancies` claims the CloudEvents id ONCE and
   opens or touches every discrepancy's case in the same unit of work.
   `ConsumeFact` cannot serve: it claims per fact, so the second discrepancy
   of the same event would read as a duplicate. Every discrepancy is
   validated before the claim (a blank kind or location poisons the whole
   closure, which is dead-lettered); a failure while opening rolls back the
   claim and every case, so the redelivery re-applies the whole check. A
   check with no discrepancies claims the id and opens nothing. Each case
   freezes `{yard_check_id, site_id, closed_at, discrepancy}`: the check's
   header and its own finding, not its siblings.
5. **Routing is data, per source type.** Migration 0004 seeds one policy
   (version 1, redeclarable through the policy API) per type; the queue,
   severity and SLA of a case come from it:

   | Source type | Severity | Queue | SLA |
   | --- | --- | --- | --- |
   | VisitRejected | MEDIUM | `yard-supervisor` | 120 min |
   | SealExceptionRaised | HIGH | `yard-supervisor` | 30 min |
   | TrailerDwellExceeded | MEDIUM | `yard-supervisor` | 240 min |
   | YardCheckClosed | MEDIUM | `yard-supervisor` | 480 min |
   | DockAppointmentNoShow | MEDIUM | `dock-planner` | 240 min |

   A seal mismatch is the only HIGH: it can mean tampering and the trailer
   may be about to dock. A policy that is absent skips the message with a
   WARN and commits past (ADR 0002 policy rule).
6. **Poison.** A payload that does not decode, or lacks the id its key needs
   (`visit_id`, `trailer_id`, `yard_check_id`, `appointment_id`), is
   dead-lettered to `<topic>.dlq` and committed past; it never blocks the
   partition.
7. **No new aggregate and no new local copy.** Everything fits the existing
   ExceptionCase and ExceptionPolicy (ADR 0002); the frozen payload is the
   only copy of the producer's data.
8. **Visibility** is unchanged: the cases appear in the existing REST list
   (`sourceType`, `queue`, `siteId` filters), detail, triage/assign/resolve
   and the MCP and A2A reads.

## Consequences

- yard-management and dock-scheduling can ship in either order: the
  consumers ignore a topic that does not exist yet (the reader retries the
  join; the service stays ready).
- Routing is per source type, not per payload field. Sending MISSING
  discrepancies to a different queue than WRONG_LOCATION, or a rejection
  `reason` to a different SLA, needs a policy source type per value (the
  ADR 0006 `#route` mechanism); deferred until asked for.
- The seal and dwell keys include an instant, so a producer that re-raised
  the same crossing with a different instant would open a second case. The
  yard contract raises once per crossing, so that is accepted.
- Every deployment must set the two new group ids or `cmd/api` exits at boot
  naming them (fail-closed, as for the seven existing groups).
- If a producer's published contract differs from the pinned one above, only
  the payload structs and `apis/asyncapi.yaml` change; the routing and the
  keys do not.
