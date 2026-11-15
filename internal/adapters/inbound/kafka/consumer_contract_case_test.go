package kafka_test

// Case harness for the event consumer contract tests (E1-E4): loading a
// routed type's golden examples, running them through the REAL
// HandleMessage decode+dispatch against a fresh consumeWorld, asserting
// the open-or-touch contract (one case under the payload's natural key,
// the wire's own event id, at-least-once redelivery creating nothing),
// the CloudEvents envelope invariants, and this repo's documented
// unknown-tolerance / dead-letter policy. Split from
// consumer_contract_spec_test.go to stay under the file-length gate.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
)

// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// goldenExamples returns the pinned spec's own examples of the message
// documenting typ on topic — the producer's documented wire format.
func goldenExamples(t *testing.T, spec, topic, typ string) []pinnedExample {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, spec)
	msg, ok := doc.messageForType(t, topic, typ)
	if !ok {
		t.Fatalf("pinned spec does not document %s on %s", typ, topic)
	}
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned spec documents %s without examples (producer example backfill pending)", typ)
	}
	return msg.Examples
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}

// dataOf returns a golden payload's data object.
func dataOf(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("golden payload carries no data object: %v", payload)
	}
	return data
}

// dataString reads a string field of a golden data object.
func dataString(data map[string]any, key string) string {
	s, _ := data[key].(string)
	return s
}

// factKey is how a golden payload maps onto the dedup key this repo's
// fact() builds: the payload's natural id, and the site it carries (empty
// for the producers whose v1 payloads have no site).
type factKey struct {
	subject func(data map[string]any, e ce.Event) string
	site    func(data map[string]any) string
}

// keyBy builds a factKey from plain field names.
func keyBy(subjectField string, siteField string) factKey {
	return factKey{
		subject: func(data map[string]any, _ ce.Event) string { return dataString(data, subjectField) },
		site:    func(data map[string]any) string { return dataString(data, siteField) },
	}
}

// assertGoldenFact runs every pinned golden example of typ through the
// REAL HandleMessage decode+dispatch and asserts the open-or-touch
// contract: one case under (typ, payload natural id, site) carrying the
// wire's own event id and payload, and an at-least-once redelivery of the
// same CloudEvents id (the ProcessedEvents claim key) creating nothing
// more. newWorld builds a FRESH world+consumer per type: several
// producers' specs illustrate their messages with one SHARED CloudEvents
// id (fulfillment-execution and inventory-storage reuse a single uuid
// across all their examples), and the id claim is global per world — a
// shared world would read the second type's golden fact as a duplicate.
func assertGoldenFact(t *testing.T, newWorld func(t *testing.T) (*consumeWorld, *kafka.Consumer), spec, topic, typ string, key factKey) {
	t.Helper()
	for _, ex := range goldenExamples(t, spec, topic, typ) {
		t.Run(ex.Name, func(t *testing.T) {
			w, c := newWorld(t)
			raw := goldenJSON(t, ex.Payload)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			evt, err := cloudevents.Decode(raw)
			if err != nil {
				t.Fatalf("re-decode golden example: %v", err)
			}
			data := dataOf(t, ex.Payload)
			cse := openCase(t, w, typ, key.subject(data, evt), key.site(data))
			if cse.SourceEventID() != evt.ID() {
				t.Errorf("frozen source event id %q, wire id is %q", cse.SourceEventID(), evt.ID())
			}
			if len(cse.SourcePayload()) == 0 {
				t.Errorf("case froze an empty source payload")
			}
			before := openCaseCount(t, w)
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			if after := openCaseCount(t, w); after != before {
				t.Errorf("replayed CloudEvents id %q changed the open-case set: %d -> %d", evt.ID(), before, after)
			}
		})
	}
}

// assertEnvelope asserts the fleet-mandatory CloudEvents 1.0 envelope on
// one golden raw document: specversion 1.0, the id/source/subject/time
// triple, datacontenttype application/json, the producer's own source
// path, and a dataschema urn naming the producer and the type's own .vN
// version (the exact urn comes from the spec's per-message const when it
// pins one; the regex keeps the check honest for the specs that document
// the value only in examples).
func assertEnvelope(t *testing.T, name string, raw []byte, producer string, evtName string) {
	t.Helper()
	evt, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("%s: decode golden example: %v", name, err)
	}
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/subject/time must all be present (got %q/%q/%v)", name, evt.ID(), evt.Subject(), evt.Time())
	}
	if want := "/warehouse/" + producer; evt.Source() != want {
		t.Errorf("%s: source %q, want %q", name, evt.Source(), want)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	want := regexp.MustCompile(
		`^urn:warehouse:` + producer + `:(events|analytics):` + evtName + `:v[0-9]+$`)
	if !want.MatchString(evt.DataSchema()) {
		t.Errorf("%s: dataschema %q, want urn:warehouse:%s:(events|analytics):%s:vN", name, evt.DataSchema(), producer, evtName)
	}
}

// assertEnvelopeRequired asserts the pinned spec's envelope schema for
// typ requires the full CloudEvents attribute set the fleet mandates.
func assertEnvelopeRequired(t *testing.T, spec, topic, typ string) {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, spec)
	msg, ok := doc.messageForType(t, topic, typ)
	if !ok {
		t.Fatalf("pinned spec does not document %s on %s", typ, topic)
	}
	required := doc.envelopeRequired(t, msg.Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's %s envelope does not require %q (required: %v)", typ, attr, required)
		}
	}
}

// contractConsumer builds a Consumer around handlers the way the
// composition root does, without dialing anything: HandleMessage only
// decodes and dispatches.
func contractConsumer(name, topic string, handlers map[string]kafka.HandlerFunc) *kafka.Consumer {
	return &kafka.Consumer{Name: name, Topic: topic, Handlers: handlers, Logger: contractLogger()}
}

// contractLogger is the logger the contract tests share.
func contractLogger() *slog.Logger { return slog.Default() }

// eventNameOf returns a CloudEvents type's event-name segment (the last
// path segment), for building dataschema expectations.
func eventNameOf(typ string) string {
	if i := strings.LastIndex(typ, "."); i >= 0 {
		return typ[i+1:]
	}
	return typ
}

// recordingDLQ (consumer_test.go) doubles as the contract tests' DLQ
// double: a poison assertion wires it as Consumer.DLQ and asserts
// HandleMessage surfaces the dead-letter refusal.

// assertUnknownTolerance asserts this repo's documented tolerance policy
// on one golden fixture of a routed type: unknown ADDITIVE data keys must
// not change the effect, an unknown event TYPE is silently skipped, a
// message that is not a CloudEvent at all is WARNed and committed past
// (never dead-lettered — the type is unknowable, nothing to replay), and
// a payload of a HANDLED type that can never be applied (its natural id
// is blank) is the one deterministic refusal the run loop dead-letters.
func assertUnknownTolerance(t *testing.T, c *kafka.Consumer, w *consumeWorld, spec, topic, typ string, key factKey, blank func(data map[string]any)) {
	t.Helper()
	golden := clonePayload(goldenExamples(t, spec, topic, typ)[0].Payload)

	tolerant := clonePayload(golden)
	data := dataOf(t, tolerant)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	if err := c.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if got := openCaseCount(t, w); got != 1 {
		t.Errorf("unknown data attributes changed the outcome: %d open cases", got)
	}

	otherType := clonePayload(golden)
	otherType["type"] = typ + "SomethingElse"
	if err := c.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := openCaseCount(t, w); got != 1 {
		t.Errorf("unknown event type was acted on (%d open cases)", got)
	}

	if err := c.HandleMessage(context.Background(), goldenJSON(t, map[string]any{"path_id": "PICK", "legacy": true})); err != nil {
		t.Fatalf("a non-CloudEvent must be WARNed and committed past, not an error: %v", err)
	}
	if _, err := cloudevents.Decode(goldenJSON(t, map[string]any{"flat": "envelope"})); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope decoded as %v, want ErrNotCloudEvent", err)
	}

	poison := clonePayload(golden)
	pdata := dataOf(t, poison)
	blank(pdata)
	poisonDLQ := &recordingDLQ{}
	pc := contractConsumer(c.Name, topic, c.Handlers)
	pc.DLQ = poisonDLQ
	if err := pc.HandleMessage(context.Background(), goldenJSON(t, poison)); err == nil {
		t.Fatal("a poison payload of a handled type must surface the dead-letter refusal")
	}
}

// occurredAt mirrors yard_exceptions_consumer.go's instant choice: the
// payload's own instant when present, else the CloudEvents time.
func occurredAt(payloadInstant string, e ce.Event) string {
	if s := strings.TrimSpace(payloadInstant); s != "" {
		return s
	}
	if tv := e.Time(); !tv.IsZero() {
		return tv.UTC().Format(time.RFC3339Nano)
	}
	return "-"
}
