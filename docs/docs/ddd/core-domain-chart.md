---
id: core-domain-chart
title: Core domain chart
sidebar_label: Core domain chart
sidebar_position: 11
description: Where exception-management sits in the warehouse-systems core/supporting/generic split today, and what flips it to core.
---

# Core domain chart

The ddd-crew Core Domain Chart, drawn for the contexts exception-management
touches. Colour says classification today; the arrow says the declared
path (ADR 0001).

```mermaid
flowchart LR
    subgraph CORE["Core - what the warehouse wins or loses on"]
        OM["order-management"]
        FE["fulfillment-execution"]
        WP["work-planning"]
        IS["inventory-storage"]
    end
    subgraph SUP["Supporting - necessary, bookkeeping over others' facts"]
        EM["exception-management<br/>(candidate Core)"]
        RP["returns-processing"]
    end
    subgraph GEN["Generic / planned"]
        WM["workforce-management<br/>(source, roadmap)"]
        NIP["network-inventory-planning<br/>(source, roadmap)"]
    end
    EM -. "auto-resolve at scale<br/>+ agent recommendations" .-> CORE
```

## Why supporting today

- Every input is another context's fact; every output is bookkeeping about
  it (who owns it, how old it is, why it happened).
- Delete the case rows and nothing physical is lost — the facts keep
  arriving; the record rebuilds.
- The differentiator is the *resolution record* (root causes), and acting
  on it at scale is exactly what v1 does not do yet: auto-resolve is one
  declarative rule, the ops-agent reads but never recommends into effect.

## What flips it to core (ADR 0001's condition)

1. Auto-resolution policies running against the caseload at scale — more
   declarative rules than the one `OrderRepromised` rule shipped, with
   measured precision.
2. Agent recommendations the operation actually follows, powered by the
   read-only MCP surface (`cmd/mcp`, built) and the policy data the agent
   can read.

Until then: invest for supporting — simple, fixed, well-tested; do not
gold-plate.
