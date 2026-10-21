//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/kafkatest"
)

// legacyFlat is a flat-envelope message (the retired pre-CloudEvents shape):
// not a valid CloudEvent, WARNed and committed past, never handled.
const legacyFlat = `{"event_id":"e-flat","event_type":"TaskCPTMissed","occurred_at":"2026-10-21T09:00:00Z"}`

// TestConsumers_RealKafkaAndPostgres: the whole inbound contract on a real
// broker and a real database — open and touch under the declared policy,
// the id dedupe (a replayed CloudEvents id with a DIFFERENT payload changes
// nothing), an unknown type ignored, an invalid envelope committed past, a
// poison payload dead-lettered, and every offset moved only after its
// effect was durable.
func TestConsumers_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	w.declarePolicy(t, kafka.TypeTaskCPTMissed, "", 0)
	topic := kafkatest.Topic(t, kafka.FulfillmentTopic)
	dlq := topic + kafka.DLQSuffix
	kafkatest.CreateTopic(t, dlq)

	unit := itID("wu") // ord-it-line-1 shaped work unit
	cptPayload := func(id string) string {
		return `{"task_id":"task-` + id + `","order_ref":"` + unit + `-line-1","task_type":"PICK","cpt":"2026-10-21T09:00:00Z"}`
	}
	// The whole fulfillment topic of the test, in order:
	//  1. e-1: the first CPT miss opens the case
	//  2. e-2: the re-fired sweep fact (another id) TOUCHES it
	//  3. e-1 again with ANOTHER payload: deduped on the id
	//  4. an unknown type of the topic: ignored
	//  5. a legacy flat message: WARNed, committed past
	//  6. a poison payload (no order_ref): dead-lettered
	//  7. e-3: another work unit opens the second case (the sentinel)
	messages := keyed("k",
		produced(t, "e-1", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed, unit+"-line-1", cptPayload("1")),
		produced(t, "e-2", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed, unit+"-line-1", cptPayload("2")),
		produced(t, "e-1", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed, unit+"-line-1", cptPayload("tampered")),
		produced(t, "e-4", "/warehouse/order-management",
			"com.warehouse.wes.order-management.order.OrderAllocated", "ord-1", `{"order_id":"ord-1"}`),
		[]byte(legacyFlat),
		produced(t, "e-5", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed, unit+"-line-1",
			`{"task_id":"t-5","task_type":"PICK","cpt":"2026-10-21T09:00:00Z"}`),
		produced(t, "e-3", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed, "wu-other-line-2",
			`{"task_id":"task-3","order_ref":"wu-other-line-2","task_type":"PICK","cpt":"2026-10-21T09:00:00Z"}`),
	)
	kafkatest.Produce(t, topic, messages...)

	group := theGroup("fulfillment")
	runConsumer(t, kafka.NewFulfillmentExceptionsConsumerForTopic(kafkatest.Brokers(t), topic, group, w.consume, itLogger(t)))

	// The sentinel: the second case exists and carries its own policy.
	eventually(t, "the second case to open", func() bool {
		_, err := w.lookupCase(t, kafka.TypeTaskCPTMissed, "wu-other-line-2", "")
		return err == nil
	})
	assertCommittedPast(t, group, topic, int64(len(messages)))

	// OPEN under the declared policy, then TOUCHED exactly once.
	c := w.openCase(t, kafka.TypeTaskCPTMissed, unit+"-line-1", "")
	if c.Status() != exceptioncase.StatusOpen || c.Queue() != "ps-main" || c.SLAMinutes() != 30 {
		t.Fatalf("the case was not opened under the declared policy: %s %s %d", c.Status(), c.Queue(), c.SLAMinutes())
	}
	if c.Occurrence() != 2 {
		t.Fatalf("occurrence = %d, want 2: the touch applied, the replayed id did not", c.Occurrence())
	}
	if c.SourceEventID() != "e-1" || len(c.SourcePayload()) == 0 {
		t.Fatalf("frozen source = %s %q, want the opening id and payload", c.SourceEventID(), c.SourcePayload())
	}
	if n := w.countTable("exception_cases"); n != 2 {
		t.Fatalf("cases = %d, want 2: nothing else may open one", n)
	}
	// The declaration fact + two ExceptionOpened (one per case) and nothing
	// else: a touch raises no fact, the replays and skips raise nothing.
	if n := w.countTable("outbox"); n != 3 {
		t.Fatalf("outbox rows = %d, want 3 (the declaration + two openings)", n)
	}
	// e-1, e-2, e-3 claimed; the replay, the unknown type, the flat
	// message and the poison claim nothing.
	if n := w.countTable("processed_events"); n != 3 {
		t.Fatalf("processed_events rows = %d, want 3", n)
	}

	// The poison message is the only one on the dead letter topic, with its
	// source topic and cause in the headers.
	dead := kafkatest.ReadN(t, dlq, 1, 90*time.Second)
	if !strings.Contains(string(dead[0].Value), `"id":"e-5"`) {
		t.Fatalf("dead letter = %s, want the poison occurrence e-5", dead[0].Value)
	}
	if dead[0].Headers["x-dlq-source-topic"] != topic || dead[0].Headers["x-dlq-cause"] == "" {
		t.Fatalf("dead letter headers = %v, want the source topic and the cause", dead[0].Headers)
	}
}

// TestOrderCorrelation_RealKafkaAndPostgres: the end-to-end auto-resolve —
// a TaskCPTMissed case opened by the fulfillment consumer is closed by an
// OrderRepromised arriving on ORDER-MANAGEMENT's topic under the
// correlation consumer's own group: the case becomes RESOLVED with the
// derived rule id, exactly one ExceptionAutoResolved is queued for the
// relay, and the repromise's replay changes nothing.
func TestOrderCorrelation_RealKafkaAndPostgres(t *testing.T) {
	w := newPGWorld(t)
	w.declarePolicy(t, kafka.TypeTaskCPTMissed, kafka.TypeOrderRepromised, 10)

	fulTopic := kafkatest.Topic(t, kafka.FulfillmentTopic)
	ordTopic := kafkatest.Topic(t, kafka.OrderManagementTopic)
	kafkatest.Produce(t, fulTopic, keyed("k",
		produced(t, "e-cpt", "/warehouse/fulfillment-execution", kafka.TypeTaskCPTMissed,
			"ord-8a1f-line-1",
			`{"task_id":"task-8a1f","order_ref":"ord-8a1f-line-1","task_type":"PICK","cpt":"2026-10-21T09:00:00Z"}`),
	)...)
	fulGroup := theGroup("fulfillment")
	runConsumer(t, kafka.NewFulfillmentExceptionsConsumerForTopic(kafkatest.Brokers(t), fulTopic, fulGroup, w.consume, itLogger(t)))
	eventually(t, "the CPT-missed case to open", func() bool {
		_, err := w.lookupCase(t, kafka.TypeTaskCPTMissed, "ord-8a1f-line-1", "")
		return err == nil
	})

	ordGroup := theGroup("correlation")
	runConsumer(t, kafka.NewOrderCorrelationConsumerForTopic(kafkatest.Brokers(t), ordTopic, ordGroup, w.correlate, itLogger(t)))
	repromise := produced(t, "e-rep", "/warehouse/order-management", kafka.TypeOrderRepromised,
		"ord-8a1f", `{"order_id":"ord-8a1f","cpt_id_old":"sp1-1200","cpt_id_new":"sp1-1800","reason":"TaskCPTMissed"}`)
	kafkatest.Produce(t, ordTopic, keyed("k", repromise, repromise)...) // the replay rides along

	eventually(t, "the case to be auto-resolved", func() bool {
		c, err := w.lookupCase(t, kafka.TypeTaskCPTMissed, "ord-8a1f-line-1", "")
		return err == nil && c.Status() == exceptioncase.StatusResolved
	})
	assertCommittedPast(t, ordGroup, ordTopic, 2)

	c := w.openCase(t, kafka.TypeTaskCPTMissed, "ord-8a1f-line-1", "")
	if c.ResolutionCode() != "AUTO_RESOLVED" || c.RootCause() != kafka.TypeOrderRepromised {
		t.Fatalf("resolution = %s / %s, want AUTO_RESOLVED of the correlated type", c.ResolutionCode(), c.RootCause())
	}
	// The declaration fact + Opened + AutoResolved, each exactly once: the
	// repromise replay is a duplicate claim and the terminal case refuses
	// everything anyway.
	if n := w.countTable("outbox"); n != 3 {
		t.Fatalf("outbox rows = %d, want the declaration, the opening and exactly one auto-resolve", n)
	}
	if n := w.countTable("processed_events"); n != 2 {
		t.Fatalf("processed_events rows = %d, want the CPT fact and the repromise", n)
	}
}

// TestTheClaimRollsBackWithTheEffect_OnRealPostgres: the fleet dedupe rule
// on a real database — a claim that shares a unit of work with a FAILING
// effect is un-claimed by the rollback, so the redelivery is processed, not
// skipped.
func TestTheClaimRollsBackWithTheEffect_OnRealPostgres(t *testing.T) {
	w := newPGWorld(t)
	ctx := context.Background()
	boom := errors.New("effect failed")
	err := w.uow.Do(ctx, func(ctx context.Context) error {
		if _, err := w.processed.Claim(ctx, "evt-rollback"); err != nil {
			t.Fatalf("claim: %v", err)
		}
		return boom // the effect fails after the claim
	})
	if !errors.Is(err, boom) {
		t.Fatalf("uow = %v, want the effect error", err)
	}
	claimed, err := w.processed.Claim(ctx, "evt-rollback")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("the claim survived the rollback: a redelivery would be skipped")
	}
}
