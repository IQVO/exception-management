---
id: subdomain-classification
title: Subdomain classification
sidebar_label: Subdomain classification
sidebar_position: 2
---

# Subdomain classification

| Question | Answer for exception-management |
| --- | --- |
| Tier | **WES** (`com.warehouse.wes.exception-management.*`) — it shapes operator flow; it holds no stock, layout or master data. |
| Classification today | **Supporting subdomain.** The lifecycle (open, triage, assign, resolve, root cause) is bookkeeping around other contexts' facts: necessary for the operation to run, but not what the warehouse wins or loses on. |
| Candidate future | **Core, conditionally.** ADR 0001 classifies it "candidate Core once auto-resolution policies and agent recommendations run against it" — when the accuracy of auto-resolution at scale measurably moves labor cost, the classification flips. Today auto-resolve is one declarative rule (order re-promises closing missed-CPT cases); that is supporting-grade. |
| Build vs buy | Build. The state machine is deliberately small and fixed; a generic workflow engine was explicitly rejected (ADR 0001, "Explicitly excluded (v1)"). |
| Differentiation | None claimed in v1. Every input is a fleet fact; every output is a fleet event. |

## Reasoning

- **It owns no physics.** Inventory, layout, orders, tasks — the truths this
  context reasons about all live upstream. What it owns is the *workflow
  over* those truths: who looks at the anomaly, how fast, with what outcome.
  That is the signature of a supporting subdomain.
- **Its data is derivative.** Delete every case and the warehouse still
  ships; the facts to rebuild them keep arriving. The value is in the
  *resolution record* (root causes), which becomes strategic only when
  someone acts on it at scale — the auto-resolve policies and the ops-agent
  recommendations of ADR 0001's Core candidacy.
- **Its cost of failure is real but bounded.** A stuck problem-solve queue
  means slower humans, not lost stock. That is why the SLA sweeper flags and
  never blocks, and why the whole lifecycle is idempotent and replayable.

## Neighbour classifications

| Context | Tier | Classification |
| --- | --- | --- |
| order-management | WES | Core |
| fulfillment-execution | WES | Core |
| work-planning | WES | Core |
| inventory-storage | WMS | Core |
| returns-processing | WMS | Supporting |
| exception-management | WES | Supporting (this context) |
