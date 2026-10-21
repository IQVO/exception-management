---
id: index
title: Architecture decision records
sidebar_label: ADR index
slug: /
---

# Architecture decision records

The decisions of `exception-management`, in the order they were taken. The
ADR bodies are immutable; a changed decision gets a new ADR that supersedes
the old one. The *Status* column is copied from each ADR's own `## Status`
section.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](/docs/adr/0001-exception-management-bounded-context) | exception-management as the owner of the problem-solve lifecycle | Accepted (2026-10-20) |
| [0002](/docs/adr/0002-aggregates-and-policy) | the ExceptionCase and ExceptionPolicy aggregates, their invariants and state machines | Accepted (2026-10-21) |
| [0003](/docs/adr/0003-local-copies-and-consumed-contracts) | local copies and the consumed contracts | Accepted (2026-10-21) |
| [0004](/docs/adr/0004-cloudevents-envelope-and-type-catalogue) | CloudEvents envelope and type catalogue | Accepted (2026-10-21) |
| [0005](/docs/adr/0005-outbox-relay-attempt-cap-and-dlq) | outbox relay — delete-on-publish, attempt cap, dead-letter topic | Accepted (2026-10-21) |
