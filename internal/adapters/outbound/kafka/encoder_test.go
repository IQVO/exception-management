package kafka_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/exception-management/internal/application/outbox"
)

// Golden exact-JSON tests, one per published type (fleet Event Standard §6):
// every CloudEvents attribute, the whole data object, the Kafka key, the
// topic and exactly the content-type header are pinned by
// testdata/<type>.golden.json. The data objects are byte-for-byte the wire
// JSON the transactional outbox persists (postgres.OutboxPublisher), so these
// goldens are the single source of truth for the integration test too.

const (
	caseID = "case-6b1e4a2c-8d3f-4e5a-9c7b-2f1d8e4a5c63"
	taskCE = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	reprom = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func at(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }

func data(t *testing.T, raw string) json.RawMessage {
	t.Helper()
	if !json.Valid([]byte(raw)) {
		t.Fatalf("fixture data is not JSON: %s", raw)
	}
	return json.RawMessage(raw)
}

// goldenCase is one published type: the fact the relay encodes and the golden
// file pinning the exact CloudEvents JSON.
type goldenCase struct {
	name string
	fact outbox.Fact
}

func goldenCases(t *testing.T) []goldenCase {
	t.Helper()
	return []goldenCase{
		{
			name: "ExceptionOpened",
			fact: outbox.Fact{
				EventID: "6f1d2c3b-4a5e-4b7c-8d9e-0a1b2c3d4e5f", Entity: "exceptioncase", Event: "ExceptionOpened",
				AggregateID: caseID, OccurredAt: at(9, 30),
				Data: data(t, `{"case_id":"`+caseID+`","source_type":"`+taskCE+`","source_subject":"task-8a1f","site_id":null,"severity":"HIGH","queue":"ps-main","opened_at":"2026-10-21T09:30:00Z","sla_due_at":"2026-10-21T10:00:00Z","source_event_id":"0e8d7c6b-5a4f-4c3b-9e2d-1f0a9b8c7d6e","source_payload":{"task_id":"task-8a1f","order_ref":"wu-8a1f","task_type":"PICK","cpt":"2026-09-30T14:55:00Z"}}`),
			},
		},
		{
			name: "ExceptionTriaged",
			fact: outbox.Fact{
				EventID: "0b9a8c7d-6e5f-4a3b-9c2d-1e0f9a8b7c6d", Entity: "exceptioncase", Event: "ExceptionTriaged",
				AggregateID: caseID, OccurredAt: at(9, 41),
				Data: data(t, `{"case_id":"`+caseID+`","severity":"MEDIUM","queue":"ps-expedite","actor":"ops-amy","at":"2026-10-21T09:41:00Z"}`),
			},
		},
		{
			name: "ExceptionAssigned",
			fact: outbox.Fact{
				EventID: "2b1a0f9e-8d7c-4b6a-a594-837261504f3e", Entity: "exceptioncase", Event: "ExceptionAssigned",
				AggregateID: caseID, OccurredAt: at(9, 52),
				Data: data(t, `{"case_id":"`+caseID+`","assignee":"jlee","actor":"ops-amy","at":"2026-10-21T09:52:00Z"}`),
			},
		},
		{
			name: "ExceptionEscalated",
			fact: outbox.Fact{
				EventID: "4d3c2b1a-0f9e-4d8c-8b7a-6f5e4d3c2b1a", Entity: "exceptioncase", Event: "ExceptionEscalated",
				AggregateID: caseID, OccurredAt: at(10, 1),
				Data: data(t, `{"case_id":"`+caseID+`","sla_due_at":"2026-10-21T10:00:00Z","at":"2026-10-21T10:01:00Z"}`),
			},
		},
		{
			name: "ExceptionResolved",
			fact: outbox.Fact{
				EventID: "8c7d6e5f-4a3b-4c2d-1e0f-9a8b7c6d5e4f", Entity: "exceptioncase", Event: "ExceptionResolved",
				AggregateID: caseID, OccurredAt: at(10, 15),
				Data: data(t, `{"case_id":"`+caseID+`","resolution_code":"STOCK_FOUND","root_cause":"COUNT_ERROR","note":null,"actor":"jlee","at":"2026-10-21T10:15:00Z"}`),
			},
		},
		{
			name: "ExceptionAutoResolved",
			fact: outbox.Fact{
				EventID: "a5948372-6150-4f3e-8d7c-4b6a2b1a0f9e", Entity: "exceptioncase", Event: "ExceptionAutoResolved",
				AggregateID: caseID, OccurredAt: at(9, 35),
				Data: data(t, `{"case_id":"`+caseID+`","rule_id":"`+taskCE+`->`+reprom+`","correlated_type":"`+reprom+`","correlated_event_id":"f3e2d1c0-9b8a-4c5d-6e7f-8a9b0c1d2e3f","at":"2026-10-21T09:35:00Z"}`),
			},
		},
		{
			name: "ExceptionPolicyDeclared",
			fact: outbox.Fact{
				EventID: "d4e5f6a7-b8c9-4d0e-8f2a-3b4c5d6e7f80", Entity: "exceptionpolicy", Event: "ExceptionPolicyDeclared",
				AggregateID: taskCE, OccurredAt: at(9, 0),
				Data: data(t, `{"source_type":"`+taskCE+`","version":2,"severity":"HIGH","queue":"ps-main","sla_minutes":30,"auto_resolve":{"on_type":"`+reprom+`","within_minutes":10}}`),
			},
		},
	}
}

// goldenName maps an event name to its golden file: ExceptionOpened ->
// exception_opened.golden.json.
func goldenName(name string) string {
	out := make([]rune, 0, len(name)+8)
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				out = append(out, '_')
			}
			r = r - 'A' + 'a'
		}
		out = append(out, r)
	}
	return string(out) + ".golden.json"
}

func decodeJSON(t *testing.T, what string, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s is not JSON: %v\n%s", what, err, raw)
	}
	return v
}

// assertMessage checks one encoded message against its golden file: topic,
// key, the full CloudEvents value (every attribute and the whole data
// object), and exactly the content-type header.
func assertMessage(t *testing.T, c goldenCase, got outbox.Message) {
	t.Helper()
	goldenFile := goldenName(c.name)
	if got.Topic != kafka.TopicEvents {
		t.Errorf("%s: topic = %q, want %q", c.name, got.Topic, kafka.TopicEvents)
	}
	if got.EventType != cloudevents.Type(c.fact.Entity, c.name) {
		t.Errorf("%s: type = %q, want %q", c.name, got.EventType, cloudevents.Type(c.fact.Entity, c.name))
	}
	if got.Subject != c.fact.AggregateID || string(got.Key) != c.fact.AggregateID {
		t.Errorf("%s: subject %q key %q, want the aggregate id %q", c.name, got.Subject, got.Key, c.fact.AggregateID)
	}
	if got.DataSchema != cloudevents.DataSchema(cloudevents.StreamEvents, c.name, 1) {
		t.Errorf("%s: dataschema = %q", c.name, got.DataSchema)
	}
	wantHeaders := []outbox.Header{{Key: "content-type", Value: cloudevents.MediaType}}
	if !reflect.DeepEqual(got.Headers, wantHeaders) {
		t.Errorf("%s: headers = %+v, want %+v", c.name, got.Headers, wantHeaders)
	}
	raw, err := os.ReadFile(filepath.Join("testdata", goldenFile))
	if err != nil {
		t.Fatalf("read golden %s: %v", goldenFile, err)
	}
	if g, w := decodeJSON(t, "value", got.Value), decodeJSON(t, goldenFile, raw); !reflect.DeepEqual(g, w) {
		t.Errorf("%s: value differs from the golden\n got: %s\nwant: %s", c.name, got.Value, raw)
	}
}

func TestEncode_GoldenJSONPerType(t *testing.T) {
	for _, c := range goldenCases(t) {
		t.Run(c.name, func(t *testing.T) {
			msgs, err := kafka.Encode(c.fact)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(msgs) != 1 {
				t.Fatalf("%d messages, want 1 (the events stream; the analytics mirror ships with the projector)", len(msgs))
			}
			assertMessage(t, c, msgs[0])
		})
	}
}

// Every encoded value is a valid CloudEvents 1.0 event the repo's own
// decoder accepts, with the service source and the persisted id.
func TestEncode_ValuesDecodeAsCloudEvents(t *testing.T) {
	for _, c := range goldenCases(t) {
		msgs, err := kafka.Encode(c.fact)
		if err != nil {
			t.Fatalf("%s: Encode: %v", c.name, err)
		}
		e, err := cloudevents.Decode(msgs[0].Value)
		if err != nil {
			t.Fatalf("%s: decode: %v", c.name, err)
		}
		if e.Source() != cloudevents.Source || e.DataContentType() != "application/json" || e.ID() != c.fact.EventID {
			t.Errorf("%s: source %q datacontenttype %q id %q", c.name, e.Source(), e.DataContentType(), e.ID())
		}
		if len(e.Extensions()) != 0 {
			t.Errorf("%s: unexpected extension attributes: %v", c.name, e.Extensions())
		}
	}
}

// The id is the one minted at enqueue and persisted with the outbox row, not
// a fresh one per encode: a relay retry republishes the same id (consumers
// dedupe on it).
func TestEncode_IDStableAcrossCalls(t *testing.T) {
	first, err := kafka.Encode(goldenCases(t)[0].fact)
	if err != nil {
		t.Fatal(err)
	}
	second, err := kafka.Encode(goldenCases(t)[0].fact)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].EventID != second[0].EventID || string(first[0].Value) != string(second[0].Value) {
		t.Fatal("two encodes of one fact differ: the id/value must be a function of the persisted row")
	}
}

// Unknown domain events yield NOTHING, never an error: the relay dead-letters
// the row instead of failing or retrying forever.
func TestEncode_UnknownYieldsNothing(t *testing.T) {
	for _, f := range []outbox.Fact{
		{Entity: "exceptioncase", Event: "ExceptionReopened", AggregateID: caseID, OccurredAt: at(9, 30), Data: data(t, `{}`)},
		{Entity: "exceptionpolicy", Event: "ExceptionOpened", AggregateID: taskCE, OccurredAt: at(9, 30), Data: data(t, `{}`)},
		{Entity: "returnreceipt", Event: "ReturnLineInspected", AggregateID: "rcpt-1", OccurredAt: at(9, 30), Data: data(t, `{}`)},
		{Entity: "", Event: "", AggregateID: "x", OccurredAt: at(9, 30), Data: data(t, `{}`)}, // undecodable payload row
	} {
		msgs, err := kafka.Encode(f)
		if err != nil || len(msgs) != 0 {
			t.Errorf("Encode(%s.%s) = %d msgs, err %v; want nothing, no error", f.Entity, f.Event, len(msgs), err)
		}
	}
}

// A fact the service publishes but cannot build into a valid envelope is a
// deterministic error (dead-lettered by the relay, never retried).
func TestEncode_InvalidFactIsAnError(t *testing.T) {
	f := goldenCases(t)[0].fact
	f.AggregateID = "" // empty subject: no CloudEvent can be built
	if _, err := kafka.Encode(f); err == nil {
		t.Fatal("Encode with empty aggregate id: want error, got nil")
	}
	f = goldenCases(t)[0].fact
	f.Data = json.RawMessage(`{"case_id":`) // data JSON that does not parse
	if _, err := kafka.Encode(f); err == nil {
		t.Fatal("Encode with broken data JSON: want error, got nil")
	}
}

// Numbers keep their exact lexical form through the envelope (json.Number):
// an int64 field is not rounded by a float64 round-trip.
func TestEncode_Int64DataSurvivesExactly(t *testing.T) {
	f := goldenCases(t)[0].fact
	f.EventID, f.OccurredAt = "11111111-2222-4333-8444-555555555555", at(9, 30)
	f.Data = data(t, `{"case_id":"`+caseID+`","big":9007199254740993}`)
	msgs, err := kafka.Encode(f)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Data struct {
			Big json.Number `json:"big"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msgs[0].Value, &v); err != nil {
		t.Fatal(err)
	}
	if v.Data.Big.String() != "9007199254740993" {
		t.Fatalf("big = %s, want 9007199254740993 exactly", v.Data.Big)
	}
}

// The nullable fields are pinned as PRESENT and null (never omitted, never
// ""): site_id, note and auto_resolve follow the AsyncAPI schemas.
func TestEncode_NullableFieldsAreNullNotOmitted(t *testing.T) {
	policyNoRule := outbox.Fact{
		EventID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", Entity: "exceptionpolicy", Event: "ExceptionPolicyDeclared",
		AggregateID: taskCE, OccurredAt: at(9, 0),
		Data: data(t, `{"source_type":"`+taskCE+`","version":1,"severity":"LOW","queue":"ps-later","sla_minutes":480,"auto_resolve":null}`),
	}
	msgs, err := kafka.Encode(policyNoRule)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Data struct {
			SiteID      *string `json:"site_id"`
			Note        *string `json:"note"`
			AutoResolve *struct {
				OnType string `json:"on_type"`
			} `json:"auto_resolve"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msgs[0].Value, &v); err != nil {
		t.Fatal(err)
	}
	if v.Data.AutoResolve != nil {
		t.Errorf("auto_resolve = %+v, want null", v.Data.AutoResolve)
	}

	resolved := outbox.Fact{
		EventID: "cccccccc-dddd-4eee-8fff-000000000000", Entity: "exceptioncase", Event: "ExceptionResolved",
		AggregateID: caseID, OccurredAt: at(10, 15),
		Data: data(t, `{"case_id":"`+caseID+`","resolution_code":"DONE","root_cause":"FIXED","note":null,"actor":"jlee","at":"2026-10-21T10:15:00Z"}`),
	}
	msgs, err = kafka.Encode(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(msgs[0].Value, &v); err != nil {
		t.Fatal(err)
	}
	if v.Data.Note != nil {
		t.Errorf("note = %v, want null", *v.Data.Note)
	}
}
