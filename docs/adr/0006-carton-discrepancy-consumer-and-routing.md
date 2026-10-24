# ADR 0006: carton-discrepancy consumer and routing

## Status

Accepted (2026-10-24).

## Context

inbound-receiving publishes `CartonDiscrepancyRaised` for every carton that
does not match its ASN or fails a receiving check (mastercase contract R5,
R12). exception-management turns each into a routed case. Constraints from
ADR 0003 (no live call to a sibling context; consumed contracts restated
locally) and ADR 0004 (CloudEvents 1.0 structured JSON, full `type` dispatch).

## Decision

1. **One consumer, one type.** `warehouse.inbound-receiving.events` is read
   under its own group `CARTON_DISCREPANCY_CONSUMER_GROUP` (env, no default,
   boot error when unset; rendered by the Helm chart). Only
   `com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised` is
   acted on; the other types on the topic are ignored.
2. **Payload (snake_case, as shipped):** `receipt_id`, `sscc?`, `kind`,
   `expected?`, `actual?`, `at`. `kind` is accepted as ANY string.
3. **Natural key.** One case per `(receipt_id, sscc | "-", kind)`: the
   dedup key is `(type, "<receipt_id>/<sscc|->/<kind>", "")`. A re-raise under
   another CloudEvents id touches the open case; a replayed id is a no-op
   (processed-event claim in the same transaction as the effect).
4. **Routing is data.** Two policy source types, seeded by migration 0003
   (version 1, redeclarable through the policy API):
   `...CartonDiscrepancyRaised#problem-solve` (HIGH, queue `problem-solve`,
   60 min) and `...#receiving-lead` (LOW, queue `receiving-lead`, 480 min,
   informational, blocks nothing). The routing table lives in
   `usecases.RouteForCartonKind`:
   - problem-solve: UnexpectedCarton, MissingCarton, DuplicateSscc,
     UnknownGtin, CaseQtyMismatch, MixedCaseDeclaredAsCasePack,
     MixedCartonContentMismatch, and every unknown kind (never dropped,
     never silently demoted).
   - receiving-lead: UnlabelledCarton, CartonOverLimit.

   The case's `dedupKey.sourceType` stays the event type; only the policy
   lookup uses the route (`Fact.PolicyType`).
5. **Visibility** is unchanged: the cases appear in the existing REST list
   (`sourceType`, `queue` filters), detail, triage/assign/resolve and the MCP
   tools; the two routes appear in the policy list.
6. **Shutdown budget.** kafka-go group readers use `MaxWait: 500ms` so
   seven consumers stop inside the 40s graceful-stop budget.

## Consequences

- A new inbound-receiving kind needs no code change to be tracked; moving it
  to receiving-lead is a one-line table change.
- Operators tune severity/queue/SLA per route, not per kind (per-kind policy
  would need a source type per kind; deferred until asked for).
- If a route policy is absent the message is skipped with a WARN and
  committed past (ADR 0002 policy rule).
