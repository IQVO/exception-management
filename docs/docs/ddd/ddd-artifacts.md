---
id: ddd-artifacts
title: DDD artifacts (ddd-crew)
sidebar_label: DDD artifacts index
sidebar_position: 10
---

# DDD artifacts (ddd-crew)

The strategic and tactical design artifacts for exception-management, in
the shape of the [ddd-crew](https://github.com/ddd-crew) templates. Each
page is written from what is **built** on `develop`; planned items are
marked *roadmap* where they appear.

| Artifact | Page | One-line summary |
| --- | --- | --- |
| Core Domain Chart | [Core domain chart](/docs/ddd/core-domain-chart) | Where exception-management sits in the fleet's core/supporting/generic split, and what would make it core. |
| Bounded Context Canvas | [Bounded context canvas](/docs/ddd/bounded-context-canvas) | The full canvas: name, purpose, classification, inbound/outbound communication, ubiquitous language, decisions, assumptions, verification. |
| Context Map | [Context map](/docs/ddd/context-map) | The nine upstream producers, the conformist stance, the downstream readers, and the two planned edges. |
| Aggregate Design Canvas | [Aggregate design canvas](/docs/ddd/aggregate-design-canvas) | The ExceptionCase and ExceptionPolicy aggregates: invariants, boundaries, consistency, lifecycle. |
| Domain Message Flow | [Domain message flow](/docs/ddd/domain-message-flow) | A fact's journey from a producer's topic to a resolved case and the events that announce it. |
| EventStorming | [EventStorming](/docs/ddd/eventstorming) | Big-picture timeline of the domain events this context owns and consumes. |
| Class diagrams | [Class diagram](/docs/ddd/class-diagram) | The domain packages' types and relationships. |
| Entity-relationship | [Entity-relationship](/docs/ddd/entity-relationship) | The Postgres schema (migrations 0001 and 0002) behind the aggregates. |
| Sequence diagrams | [Sequence diagrams](/docs/ddd/sequence-diagrams) | The end-to-end flows: open/touch, triage/assign/resolve, auto-resolve, SLA sweep. |
| Domain events | [Domain events](/docs/ddd/domain-events) | Every published and consumed event with its full payload. |
| Ubiquitous language | [Ubiquitous language](/docs/ddd/ubiquitous-language) | The terms, exactly as the code uses them. |
| Use cases | [Use cases](/docs/ddd/use-cases) | The eleven application use cases, REST and consumer-driven. |
| Subdomain classification | [Subdomain classification](/docs/ddd/subdomain-classification) | Why this is a Supporting subdomain today. |
