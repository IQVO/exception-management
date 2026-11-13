# ADR 0012: held-stock exception cases — one case per held unit, auto-resolved on disposition

## Status

Accepted (2026-11-12).

## Context

inventory-storage ADR 0052 books a `Damaged` receipt line as HELD stock
(`StockHeld`) instead of dropping it: the units stay visible in its
held-stock list and REST disposition, but nobody owns the follow-up —
decide the disposition (release, return to vendor, scrap, rework) and
chase it to done. The inbound plan's IF-6 follow-up names exactly this
gap: "exception case per hold (exception-management consumer of
StockHeld)". Every unit of held stock must correspond to an OPEN
exception case, so disposition work is visible, queueable and
escalatable in one place; the case closes when the hold ends.

The producer's contract (inventory-storage ADR 0052, restated here per
ADR 0003; field names verified against its `apis/asyncapi.yaml` on
origin/develop): all three types on `warehouse.inventory.events`,
CloudEvents `source` `/warehouse/inventory-storage`, subject = Kafka
key = the held unit id.

| type | `data` (required) | optional |
|---|---|---|
| `com.warehouse.wms.inventory-storage.stock.StockHeld` | `held_unit_id, sku, quantity, reason (DAMAGED\|QC_PENDING\|SUSPECT\|OTHER), receipt_id, asn_number, line_no, held_at` | `owner_ref, sscc` |
| `com.warehouse.wms.inventory-storage.stock.StockReleased` | `held_unit_id, sku, quantity, receipt_id, asn_number, line_no, released_at` | — |
| `com.warehouse.wms.inventory-storage.stock.StockDispositioned` | `held_unit_id, sku, quantity, disposition (RELEASE\|RETURN_TO_VENDOR\|SCRAP\|REWORK), reason, receipt_id, asn_number, line_no, decided_at` | — |

What each fact MEANS comes from the producer's own ADR 0052 table:
`RELEASE` leaves the hold (state REMOVED) and the quantity re-enters
stock as a fresh staged receipt; `RETURN_TO_VENDOR` and `SCRAP` leave
the hold and the units leave the building; `REWORK` KEEPS the unit
HELD (reason updated, rework count +1). `StockDispositioned` is raised
for all four dispositions; `StockReleased` is raised IN ADDITION, only
on `RELEASE`.

## Decision

1. **A second consumer group on the topic this repo already reads.**
   `warehouse.inventory.events` is already consumed by
   `INVENTORY_EXCEPTIONS_CONSUMER_GROUP` (DiscrepancyDetected,
   ItemUnlocated — ADR 0004). The held-stock consumer is its own
   consumer under a NEW group `HELD_STOCK_EXCEPTIONS_CONSUMER_GROUP`
   (env, no default, boot error when unset; chart values/configmap/
   deployment + render test), following the repo's one-group-per-
   concern pattern: independent offsets and lag, and failure isolation
   in both directions — a poison or wedged held-stock partition must
   not stall discrepancy/unlocated handling, and a replay of the
   inventory group must not replay held-stock effects under a foreign
   offset regime. Adding the three types to the EXISTING group would
   couple the two concerns' progress for no gain: the handlers share
   only the topic, not the semantics (open-or-touch vs
   open-per-unit + targeted close).

2. **Semantics per type** (idempotent, exactly-once per CloudEvents id
   via the processed-events claim in the same UnitOfWork as the
   effect — the established `ConsumeFact` mechanism):

   | type | effect on the case of that held unit id |
   |---|---|
   | `StockHeld` | OPEN one case per held unit id — the dedup key is `(StockHeld type, held_unit_id, "")` (the payload carries no site; the key still dedupes, like the inventory consumer's v1 payloads). A redelivered id is a no-op (claim); a RE-EMITTED `StockHeld` under a new id for the same unit TOUCHES the open case (occurrence+1, lastSeenAt) — the case is per held unit, not per emission — and if that case is already RESOLVED it stays resolved (the domain never reopens; the touch is recorded). Exactly one open case per held unit, ever. |
   | `StockDispositioned` with `RELEASE`, `RETURN_TO_VENDOR` or `SCRAP` | AUTO-RESOLVE the open case (`ExceptionAutoResolved`) — the hold has ended, so the follow-up work this case exists for is done. The disposition outcome is recorded in the resolution note (see 5). A case already RESOLVED stays resolved (no-op); the claim still commits. |
   | `StockDispositioned` with `REWORK` | the case STAYS OPEN — ADR 0052 keeps the unit HELD on rework, and the invariant "every held unit has an open case" would break if the case closed while the stock is still held. The arrival is recorded as a TOUCH (occurrence+1, lastSeenAt): each rework cycle is visible on the case that still owns the pending work. |
   | `StockReleased` | AUTO-RESOLVE with a released note, same as a closing disposition. Justification from ADR 0052: `StockReleased` is raised ONLY on a `RELEASE` disposition, in addition to `StockDispositioned` — so in practice it arrives as the second fact of a pair whose first fact already closed the case, and this handler is the idempotent no-op; handling it as a close in its own right covers either arrival order and any future producer that releases without a disposition row. Resolving (not leaving it for triage) is right because a release means the quantity re-entered usable stock — there is nothing left to chase. |

3. **An unknown held unit id commits past, WARN, never dead-letter.** A
   `StockDispositioned`/`StockReleased` for a unit whose case does not
   exist (facts from before this feature, or a hold this service never
   saw open) is a WELL-FORMED fact about state we never tracked: it is
   logged at WARN and its offset committed. The DLQ is for messages
   that can never be applied (a payload that does not decode, a blank
   `held_unit_id`) — parking a well-formed disposition there would
   hide exactly the recovery path (its `StockHeld` was the anomaly).
   A `StockHeld` with no declared policy stays the ADR 0002 skip
   (WARN, commit past): a configuration gap, not the message's fault.

4. **Routing: the existing policy engine, seeded.** One policy source
   type, seeded by migration 0006 (version 1, redeclarable through the
   policy API): `...stock.StockHeld` — MEDIUM severity (the middle
   band; a held unit is real money on the floor but not a
   ship-stopping emergency), queue `problem-solve` (the disposition
   decision is a problem-solver's call, in step with ADR 0011), SLA
   1440 minutes (a held unit should be dispositioned within a day of
   booking). Cases open UNASSIGNED — assignment stays a human action.
   No auto-resolve RULE is declared for the source type: the existing
   declarative rule shape matches order-level correlations through the
   work-unit formula (ADR 0002), which cannot express "the same held
   unit"; the disposition close is instead DEFINITIONAL (point 2) —
   the closing half of this feature's invariant, not an
   operator-declarable choice. It reuses the aggregate's existing
   `AutoResolve` transition (audit actor = the rule id, terminal
   RESOLVED, `ExceptionAutoResolved` raised); the rule id is derived
   in the established `<source type>-><closing type>` shape. No new
   policy mechanism exists or is needed.

5. **The disposition outcome rides the resolution note.** The case's
   `AutoResolve` gains an optional note (additive; existing callers
   unchanged), recorded in the existing `note` column and surfaced by
   the existing REST read (nullable, absent until RESOLVED). The wire
   payload of `ExceptionAutoResolved` gains the same OPTIONAL
   `note` (null when unset) — additive, v1 stays v1. Notes:
   `disposition RELEASE` / `disposition RETURN_TO_VENDOR` /
   `disposition SCRAP` for dispositions; `stock released` for
   `StockReleased`.

6. **No backfill.** Held units booked before this consumer's group
   first joins get NO case: the group starts at the earliest offset
   still retained (a fresh group reads the topic from the start, per
   the reader contract), which replays whatever the cluster still
   holds — but anything retention has already dropped is gone, and
   nothing here reconstructs it. The honest cutoff is the group's
   first committed offset; the cluster carries essentially no live
   traffic today (plan Wave 0 notes), so the practical gap is the
   e2e fixtures at most. Pre-cutoff held units remain visible in
   inventory-storage's own held-stock list; when they are
   dispositioned, point 3 handles the unknown-unit fact.

7. **Migration 0006** is additive only: the seeded policy row (point
   4). The case-per-held-unit link needs no new persistence — the
   dedup key column already carries `(StockHeld type, held unit id)`
   and its partial unique index already enforces one open case per
   key; the closing path looks the case up by exactly that key.

## Consequences

- Held stock becomes owned follow-up work: cases appear in the REST
  list (filter `sourceType` = the StockHeld type), detail, the MCP
  read tools, the console and the SLA sweep like every other source;
  the disposition outcome is on the resolved case record.
- Two consumer groups now read `warehouse.inventory.events`
  independently (eleven consumers total: ten fact + one correlation).
  warehouse-infra's helm values gain `HELD_STOCK_EXCEPTIONS_CONSUMER_GROUP`
  (this PR does not touch that repo).
- A rework loop leaves a growing occurrence count on an open case —
  visible churn by design; if it ever becomes noise, a REWORK-aware
  note is an additive follow-up.
- `apis/asyncapi.yaml` restates the three consumed payloads (ADR 0003
  local-copy rule); inventory-storage's `x-no-consumer-reason`
  annotations on the three messages can be dropped by ITS next
  contract PR — not this repo's to edit.
- The additive optional `note` on `ExceptionAutoResolved` (point 5)
  is the only published-payload change; ADR 0004's catalogue rows are
  superseded here for the consumed set (its body is immutable; the
  consumed table there already lags the later consumer ADRs by
  convention).
