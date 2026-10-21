---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
sidebar_position: 4
---

# Quickstart

Run the service by hand against real Postgres and Kafka, then walk one anomaly the whole way:
fact → case → triage → resolve. Everything here uses the real runtime — there is no in-memory
mode.

## 1. Infrastructure

```bash
docker run -d --name em-pg -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=exception_management -p 5432:5432 postgres:17
docker run -d --name em-kafka -p 9092:9092 apache/kafka:3.9.0
```

## 2. Boot the API

`cmd/api` fails fast unless every mandatory variable is set
([Configuration](/docs/operations/configuration)):

```bash
export DATABASE_URL="postgres://postgres:dev@localhost:5432/exception_management?sslmode=disable"
export KAFKA_BROKERS="localhost:9092"
export FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP="local-fulfillment"
export WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP="local-work-planning"
export NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP="local-network-fulfillment"
export ORDER_CORRELATION_CONSUMER_GROUP="local-order-correlation"
export INVENTORY_EXCEPTIONS_CONSUMER_GROUP="local-inventory"
export RETURNS_EXCEPTIONS_CONSUMER_GROUP="local-returns"
go run ./cmd/api
```

Migrations are embedded and run at boot (idempotent; safe to race with a second start). When the
log shows the listener on `:8080`, check it:

```bash
curl -s localhost:8080/readyz   # {"status":"ready"} — checks Postgres AND a Kafka broker
```

## 3. Declare a policy first

A consumed fact whose source type has **no declared policy** is WARN-logged and committed past —
no case is opened. So declare one before producing the fact (the source type below is the real
CloudEvents type of a missed CPT):

```bash
curl -s -X PUT localhost:8080/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: policy-cpt-1' \
  -d '{"severity":"HIGH","queue":"cpt-missed","slaMinutes":30,"autoResolve":{"onType":"com.warehouse.wes.order-management.order.OrderRepromised","withinMinutes":120}}'
```

The four REST writes require an `Idempotency-Key`
([API conventions](/docs/api-reference)).

## 4. Produce the anomaly fact

Publish the CloudEvent exactly as fulfillment-execution does (structured mode, the envelope as the
Kafka value):

```bash
kcat -P -b localhost:9092 -t warehouse.fulfillment.events \
  -H 'content-type: application/cloudevents+json; charset=UTF-8' <<'EOF'
{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/fulfillment-execution","type":"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed","subject":"task-8a1f","time":"2026-10-21T09:15:04Z","datacontenttype":"application/json","data":{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-10-21T09:10:00Z"}}
EOF
```

## 5. Work the case

```bash
curl -s 'localhost:8080/exceptions?status=OPEN' | jq '.items[0] | {case_id, source_subject, severity, queue, occurrence_count, sla_due_at}'
curl -s -X POST localhost:8080/exceptions/<caseId>/assign  -H 'Content-Type: application/json' -H 'Idempotency-Key: a-1' -d '{"assignee":"jamie"}'
curl -s -X POST localhost:8080/exceptions/<caseId>/resolve -H 'Content-Type: application/json' -H 'Idempotency-Key: r-1' -d '{"resolutionCode":"CPT_REPROMISED","rootCause":"CARRIER_DELAY","note":"carrier picked up late"}'
```

Reproduce the same fact (change only the CloudEvents `id`) — the case stays one:
`occurrence_count` grows, status stays `RESOLVED` (a fact after resolution touches, never
reopens).

## 6. Watch the lifecycle leave the outbox

```bash
kcat -C -b localhost:9092 -t warehouse.exception-management.events -o beginning -q
```

You will see `ExceptionOpened`, `ExceptionAssigned`, `ExceptionResolved` — each a CloudEvents 1.0
envelope whose `id` was minted once, at enqueue, inside the same database transaction as the state
change ([ADR 0005](/docs/adr/0005-outbox-relay-attempt-cap-and-dlq)).

## The docs site itself

```bash
cd docs && npm ci && npm run build   # also what the Docs workflow runs on every PR
```
