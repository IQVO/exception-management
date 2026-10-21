package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	kafkain "github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
)

// producerEvent builds a sibling context's occurrence on its own topic: its
// source, its type — never this service's cloudevents.New, which exists for
// what THIS service publishes.
func producerEvent(t *testing.T, id, source, typ, subject string, data string) ce.Event {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource(source)
	e.SetType(typ)
	e.SetSubject(subject)
	e.SetTime(time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC))
	if err := e.SetData("application/json", []byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("producer event invalid: %v", err)
	}
	return e
}

func encode(t *testing.T, e ce.Event) []byte {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// consumedSpy records what a handler saw.
type consumedSpy struct {
	seen []string
	fail error // returned by the handler when set
}

func (s *consumedSpy) handler(_ context.Context, e ce.Event) error {
	s.seen = append(s.seen, e.ID())
	return s.fail
}

// consumerWith builds a Consumer over a handler map, without a broker
// (HandleMessage never touches the Reader).
func consumerWith(handlers map[string]kafkain.HandlerFunc) *kafkain.Consumer {
	return &kafkain.Consumer{Name: "test consumer", Topic: "t", Handlers: handlers, Logger: slog.Default()}
}

func TestHandleMessage_SkipsInvalidCloudEvents(t *testing.T) {
	// A legacy flat envelope: not a CloudEvent — WARN and commit past
	// (nil), never an error, never the handler.
	spy := &consumedSpy{}
	c := consumerWith(map[string]kafkain.HandlerFunc{
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed": spy.handler,
	})
	flat := []byte(`{"event_id":"e-1","event_type":"TaskCPTMissed","occurred_at":"2026-10-21T09:00:00Z"}`)
	if err := c.HandleMessage(context.Background(), flat); err != nil {
		t.Fatalf("invalid envelope = %v, want nil (WARN + commit past)", err)
	}
	if len(spy.seen) != 0 {
		t.Fatalf("handler ran for an invalid envelope: %v", spy.seen)
	}
}

func TestHandleMessage_IgnoresUnknownTypes(t *testing.T) {
	spy := &consumedSpy{}
	c := consumerWith(map[string]kafkain.HandlerFunc{
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed": spy.handler,
	})
	e := producerEvent(t, "e-1", "/warehouse/order-management",
		"com.warehouse.wes.order-management.order.OrderAllocated", "ord-1", `{"order_id":"ord-1"}`)
	if err := c.HandleMessage(context.Background(), encode(t, e)); err != nil {
		t.Fatalf("unknown type = %v, want nil", err)
	}
	if len(spy.seen) != 0 {
		t.Fatalf("handler ran for an unknown type: %v", spy.seen)
	}
}

func TestHandleMessage_DispatchesOnFullType(t *testing.T) {
	spy := &consumedSpy{}
	c := consumerWith(map[string]kafkain.HandlerFunc{
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed": spy.handler,
	})
	e := producerEvent(t, "e-1", "/warehouse/fulfillment-execution",
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", "wu-8a1f",
		`{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}`)
	if err := c.HandleMessage(context.Background(), encode(t, e)); err != nil {
		t.Fatalf("handled type = %v, want nil", err)
	}
	if len(spy.seen) != 1 || spy.seen[0] != "e-1" {
		t.Fatalf("handler saw %v, want the e-1 event", spy.seen)
	}
	// A suffix match must NOT dispatch: another type that ends the same.
	e2 := producerEvent(t, "e-2", "/warehouse/other", "com.warehouse.other.ctx.task.TaskCPTMissed", "wu-1", `{}`)
	if err := c.HandleMessage(context.Background(), encode(t, e2)); err != nil {
		t.Fatalf("suffix type = %v, want nil", err)
	}
	if len(spy.seen) != 1 {
		t.Fatalf("suffix-matched type dispatched: %v", spy.seen)
	}
}

func TestHandleMessage_TransientErrorIsReturned(t *testing.T) {
	spy := &consumedSpy{fail: errors.New("postgres down")}
	c := consumerWith(map[string]kafkain.HandlerFunc{"t.any.Thing": spy.handler})
	e := producerEvent(t, "e-1", "/warehouse/fulfillment-execution", "t.any.Thing", "s-1", `{}`)
	if err := c.HandleMessage(context.Background(), encode(t, e)); !errors.Is(err, spy.fail) {
		t.Fatalf("err = %v, want the transient error (the loop retries)", err)
	}
}
