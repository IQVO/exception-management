//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	outboundkafka "github.com/claudioed/exception-management/internal/adapters/outbound/kafka"
	relay "github.com/claudioed/exception-management/internal/adapters/outbound/outbox"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The outbox path end to end against REAL infrastructure, both started with
// testcontainers (never skip-gated, never a hardcoded broker address, topics
// created explicitly with the leader waited for): the production
// OutboxPublisher writes the facts inside real UnitOfWork transactions, the
// production relay + RelaySink drain them to a real broker on the REAL
// topic names, and the consumed CloudEvents are compared — every attribute
// and the whole data object — with the encoder's golden files, the single
// source of truth of the wire contract. The DLQ path is proven the same way
// on a poison row.

// The broker comes from internal/testing/kafkatest, which starts it through
// testcontainers-go/modules/kafka (referenced here so the module import
// stays reachable from this Kafka-touching file).
var _ = tckafka.Run

const (
	goldenCaseID = "case-6b1e4a2c-8d3f-4e5a-9c7b-2f1d8e4a5c63"
	taskCEType   = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromType   = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func goldenAt(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }

// golden loads one of the kafka encoder's golden envelopes, so the wire
// contract has ONE source of truth (these are the same bytes its unit test
// pins).
func golden(t *testing.T, name string) any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "kafka", "testdata", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return decodeJSON(t, name, raw)
}

func decodeJSON(t *testing.T, what string, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s is not JSON: %v\n%s", what, err, raw)
	}
	return v
}

// assertEnvelope checks one consumed message: the Kafka key, exactly the
// CloudEvents content-type header, a valid CloudEvents 1.0 value, and the
// whole envelope against its golden file.
func assertEnvelope(t *testing.T, msg kafkatest.Message, wantKey, goldenName string) {
	t.Helper()
	if msg.Key != wantKey {
		t.Errorf("%s: key = %q, want %q", goldenName, msg.Key, wantKey)
	}
	if len(msg.Headers) != 1 || msg.Headers["content-type"] != cloudevents.MediaType {
		t.Errorf("%s: headers = %v, want exactly the content-type %q", goldenName, msg.Headers, cloudevents.MediaType)
	}
	if _, err := cloudevents.Decode(msg.Value); err != nil {
		t.Fatalf("%s: not a valid CloudEvent: %v", goldenName, err)
	}
	if got, want := decodeJSON(t, goldenName, msg.Value), golden(t, goldenName); !reflect.DeepEqual(got, want) {
		t.Errorf("%s: envelope differs from the golden\n got: %s\nwant: %v", goldenName, msg.Value, want)
	}
}

// ids hands out the golden files' CloudEvents ids, in publication order.
func ids(list ...string) func() string {
	i := 0
	return func() string {
		id := list[i%len(list)]
		i++
		return id
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// runRelay starts the relay's Run loop and stops it (and closes the sink)
// when the test ends.
func runRelay(t *testing.T, r *relay.Relay, sink *outboundkafka.RelaySink) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
		if err := sink.Close(); err != nil {
			t.Errorf("close relay sink: %v", err)
		}
	})
}

// waitFor polls until cond is true, failing the test at the deadline.
func waitFor(t *testing.T, what string, deadline time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// goldenCase builds the case whose facts match the golden files: opened from
// a site-less TaskCPTMissed fact at 09:30 (SLA 30m), triaged to
// MEDIUM/ps-expedite at 09:41, resolved at 10:15 with no remark.
func goldenCase(t *testing.T) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(taskCEType, "task-8a1f", "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := exceptioncase.Open(exceptioncase.OpenInput{
		CaseID: goldenCaseID, Key: key, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, SourceEventID: "0e8d7c6b-5a4f-4c3b-9e2d-1f0a9b8c7d6e",
		Payload: []byte(`{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}`),
		At:      goldenAt(9, 30),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestRelay_RealPostgresAndKafka_PublishesTheExactEnvelopes: a policy
// declaration (version 2, with an auto-resolve rule) and one case's whole
// lifecycle are written through the production OutboxPublisher inside real
// unit-of-work transactions; the production relay and RelaySink publish
// them, in outbox order, on warehouse.exception-management.events. Every
// message matches its golden envelope exactly — both aggregates of ADR 0004
// (exceptioncase and exceptionpolicy) are covered.
func TestRelay_RealPostgresAndKafka_PublishesTheExactEnvelopes(t *testing.T) {
	ctx := context.Background()
	brokers := kafkatest.Brokers(t)
	pool := pgtest.NewPool(t)
	eventsTopic := outboundkafka.TopicEvents
	kafkatest.CreateTopic(t, eventsTopic)
	// The relay writes every fact's analytics twin too (ADR 0006); the test
	// broker has auto-creation OFF, so the topic must exist before the drain.
	kafkatest.CreateTopic(t, outboundkafka.TopicAnalytics)

	pub := postgres.NewOutboxPublisher(pool)
	pub.NewID = ids(
		"d4e5f6a7-b8c9-4d0e-8f2a-3b4c5d6e7f80", // ExceptionPolicyDeclared v2
		"6f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e5f", // ExceptionOpened
		"0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d", // ExceptionTriaged
		"8c7d6e5f-4a3b-4c2d-1e0f-9a8b7c6d5e4f", // ExceptionResolved
	)
	uow := postgres.NewUnitOfWork(pool)
	policies := postgres.NewPolicyRepo(pool)
	cases := postgres.NewCaseRepo(pool)

	// The policy: version 1 then a Redeclare to version 2 (the golden's
	// version), both saved; only the v2 declaration fact is published.
	rule, err := exceptionpolicy.NewRule(repromType, 10)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := exceptionpolicy.Declare(exceptionpolicy.DeclareInput{
		SourceType: taskCEType, Severity: shared.SeverityMedium, Queue: "ps-later",
		SLAMinutes: 480, At: goldenAt(8, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := p1.Redeclare(exceptionpolicy.DeclareInput{
		SourceType: taskCEType, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, AutoResolve: &rule, At: goldenAt(9, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.Do(ctx, func(ctx context.Context) error {
		if err := policies.Save(ctx, p1); err != nil {
			return err
		}
		if err := policies.Save(ctx, p2); err != nil {
			return err
		}
		return pub.PublishPolicyDeclared(ctx, p2.Declaration())
	}); err != nil {
		t.Fatalf("policy unit of work: %v", err)
	}

	// The case lifecycle: open, triage, resolve — each transition's facts
	// published in the same unit of work as its save.
	c := goldenCase(t)
	persist := func(eventsFrom int) func(context.Context) error {
		return func(ctx context.Context) error {
			if err := cases.Save(ctx, c); err != nil {
				return err
			}
			return pub.PublishCaseEvents(ctx, c.DomainEvents()[eventsFrom:]...)
		}
	}
	if err := uow.Do(ctx, persist(0)); err != nil {
		t.Fatalf("open unit of work: %v", err)
	}
	if err := c.Triage(shared.SeverityMedium, "ps-expedite", shared.Actor{Kind: shared.ActorOperator, Name: "ops-amy"}, goldenAt(9, 41)); err != nil {
		t.Fatal(err)
	}
	if err := uow.Do(ctx, persist(1)); err != nil {
		t.Fatalf("triage unit of work: %v", err)
	}
	if err := c.Resolve("STOCK_FOUND", "COUNT_ERROR", "", shared.Actor{Kind: shared.ActorOperator, Name: "jlee"}, goldenAt(10, 15)); err != nil {
		t.Fatal(err)
	}
	if err := uow.Do(ctx, persist(2)); err != nil {
		t.Fatalf("resolve unit of work: %v", err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	r := relay.NewOutboxRelay(postgres.NewOutboxStore(pool), sink, quiet(),
		relay.WithMaxAttempts(relay.DefaultMaxAttempts))
	runRelay(t, r, sink)

	got := kafkatest.ReadN(t, eventsTopic, 4, 60*time.Second)
	assertEnvelope(t, got[0], taskCEType, "exception_policy_declared.golden.json")
	assertEnvelope(t, got[1], goldenCaseID, "exception_opened.golden.json")
	assertEnvelope(t, got[2], goldenCaseID, "exception_triaged.golden.json")
	assertEnvelope(t, got[3], goldenCaseID, "exception_resolved.golden.json")

	// Every published row left the outbox (delete-on-publish, ADR 0005).
	waitFor(t, "the outbox to drain", 10*time.Second, func() bool {
		return pgtest.Count(t, pool, "outbox") == 0
	})
}

// TestRelay_PoisonRowDeadLettersOnRealKafka: a row whose fact has no wire
// shape can never publish; the relay parks its raw payload on
// warehouse.exception-management.events.dlq with the x-dlq-* headers and
// disposes of the row.
func TestRelay_PoisonRowDeadLettersOnRealKafka(t *testing.T) {
	ctx := context.Background()
	brokers := kafkatest.Brokers(t)
	pool := pgtest.NewPool(t)
	dlqTopic := outboundkafka.DeadLetterTopic
	kafkatest.CreateTopic(t, dlqTopic)

	payload := `{"event":"ExceptionVanished","aggregate_id":"case-poison-1","occurred_at":"2026-10-21T09:30:00Z","data":{"case_id":"case-poison-1"}}`
	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO outbox (event_id, aggreg_type, aggreg_id, payload, occurred_at)
		VALUES ('evt-poison-1', 'exceptioncase', 'case-poison-1', $1::jsonb, now())
		RETURNING id
	`, payload).Scan(&id); err != nil {
		t.Fatal(err)
	}

	sink := outboundkafka.NewRelaySink(brokers)
	r := relay.NewOutboxRelay(postgres.NewOutboxStore(pool), sink, quiet(),
		relay.WithMaxAttempts(relay.DefaultMaxAttempts))
	runRelay(t, r, sink)

	got := kafkatest.ReadN(t, dlqTopic, 1, 60*time.Second)[0]
	if got.Key != "case-poison-1" {
		t.Errorf("key = %q, want the aggregate id", got.Key)
	}
	if gotValue, want := decodeJSON(t, "dlq value", got.Value), decodeJSON(t, "payload", []byte(payload)); !reflect.DeepEqual(gotValue, want) {
		t.Errorf("dlq value = %s, want the raw row payload %s", got.Value, payload)
	}
	for _, h := range []string{"content-type", "x-dlq-error", "x-dlq-outbox-row", "x-dlq-aggregate", "x-dlq-failed-at"} {
		if got.Headers[h] == "" {
			t.Errorf("header %q missing: %v", h, got.Headers)
		}
	}
	if got.Headers["x-dlq-outbox-row"] != jsonInt(id) || got.Headers["x-dlq-aggregate"] != "case-poison-1" {
		t.Errorf("x-dlq headers = %s / %s", got.Headers["x-dlq-outbox-row"], got.Headers["x-dlq-aggregate"])
	}

	// The disposed row is gone from the outbox (the DLQ write disposes, like
	// a publish).
	waitFor(t, "the poison row to be disposed", 10*time.Second, func() bool {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 0
	})
}

func jsonInt(n int64) string {
	b, err := json.Marshal(n)
	if err != nil {
		return ""
	}
	return string(b)
}
