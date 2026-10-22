package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/analytics/report"
)

// newTestAnalyticsConsumer builds the consumer over brokers that are never
// dialled (only HandleMessage runs in these tests).
func newTestAnalyticsConsumer(proj report.Projection) *Consumer {
	return NewAnalyticsConsumer([]string{"b:9092"}, "t.analytics", "g", proj, slog.Default())
}

// fakeProjection records the events it was handed and answers per event id.
type fakeProjection struct {
	applied []report.Event
	errBy   map[string]error
}

func (f *fakeProjection) Apply(_ context.Context, e report.Event) (bool, error) {
	if err, ok := f.errBy[e.EventID]; ok {
		return false, err
	}
	f.applied = append(f.applied, e)
	return true, nil
}

const (
	taskCE   = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromCE = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func analyticsEvent(t *testing.T, entity, name, subject string, payload string) []byte {
	t.Helper()
	var data any
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		t.Fatalf("fixture payload is not JSON: %v", err)
	}
	raw, err := cloudevents.New(cloudevents.Spec{
		ID: "ev-" + name, Entity: entity, EventName: name, Subject: subject,
		Time:   time.Date(2026, 10, 21, 9, 30, 0, 0, time.UTC),
		Stream: cloudevents.StreamAnalytics, Version: 1,
		Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Every published kind maps onto the projection with the fields its payload
// carries and the envelope's id/time; one check per kind keeps the loop
// shallow.
func TestAnalyticsConsumerProjectsEveryKind(t *testing.T) {
	for _, tc := range []struct {
		name    string
		event   string
		entity  string
		subject string
		payload string
		check   func(t *testing.T, e report.Event)
	}{
		{"opened", "ExceptionOpened", "exceptioncase", "case-1",
			`{"case_id":"case-1","source_type":"` + taskCE + `","source_subject":"task-8a1f","site_id":"SITE-1","severity":"HIGH","queue":"ps-main","opened_at":"2026-10-21T09:30:00Z","sla_due_at":"2026-10-21T10:00:00Z","source_event_id":"src-1","source_payload":{}}`,
			checkOpened},
		{"triaged", "ExceptionTriaged", "exceptioncase", "case-1",
			`{"case_id":"case-1","severity":"MEDIUM","queue":"ps-expedite","actor":"ops-amy","at":"2026-10-21T09:41:00Z"}`,
			checkTriaged},
		{"assigned", "ExceptionAssigned", "exceptioncase", "case-1",
			`{"case_id":"case-1","assignee":"jlee","actor":"ops-amy","at":"2026-10-21T09:52:00Z"}`,
			func(t *testing.T, e report.Event) {
				if e.Assignee != "jlee" {
					t.Fatalf("assignee = %q", e.Assignee)
				}
			}},
		{"escalated", "ExceptionEscalated", "exceptioncase", "case-1",
			`{"case_id":"case-1","sla_due_at":"2026-10-21T10:00:00Z","at":"2026-10-21T10:01:00Z"}`,
			checkEscalated},
		{"resolved", "ExceptionResolved", "exceptioncase", "case-1",
			`{"case_id":"case-1","resolution_code":"STOCK_FOUND","root_cause":"COUNT_ERROR","note":"adjusted","actor":"jlee","at":"2026-10-21T10:15:00Z"}`,
			checkResolved},
		{"auto-resolved", "ExceptionAutoResolved", "exceptioncase", "case-1",
			`{"case_id":"case-1","rule_id":"` + taskCE + `->` + repromCE + `","correlated_type":"` + repromCE + `","correlated_event_id":"corr-1","at":"2026-10-21T09:35:00Z"}`,
			checkAutoResolved},
		{"policy declared", "ExceptionPolicyDeclared", "exceptionpolicy", taskCE,
			`{"source_type":"` + taskCE + `","version":1,"severity":"HIGH","queue":"ps-main","sla_minutes":30,"auto_resolve":{"on_type":"` + repromCE + `","within_minutes":10}}`,
			checkPolicyDeclared},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proj := &fakeProjection{}
			c := newTestAnalyticsConsumer(proj)
			if err := c.HandleMessage(context.Background(), analyticsEvent(t, tc.entity, tc.event, tc.subject, tc.payload)); err != nil {
				t.Fatal(err)
			}
			if len(proj.applied) != 1 {
				t.Fatalf("%d events applied, want 1", len(proj.applied))
			}
			got := proj.applied[0]
			if got.EventID != "ev-"+tc.event || !got.At.Equal(time.Date(2026, 10, 21, 9, 30, 0, 0, time.UTC)) {
				t.Fatalf("envelope fields = %q %v", got.EventID, got.At)
			}
			tc.check(t, got)
		})
	}
}

func checkOpened(t *testing.T, e report.Event) {
	t.Helper()
	if e.SourceType != taskCE || e.SourceSubject != "task-8a1f" || e.SiteID == nil || *e.SiteID != "SITE-1" ||
		e.SLADueAt != time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC).UTC() {
		t.Fatalf("opened = %+v", e)
	}
}

func checkTriaged(t *testing.T, e report.Event) {
	t.Helper()
	if e.Severity != "MEDIUM" || e.Queue != "ps-expedite" {
		t.Fatalf("triaged = %+v", e)
	}
}

func checkEscalated(t *testing.T, e report.Event) {
	t.Helper()
	if e.SLADueAt != time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC).UTC() {
		t.Fatalf("escalated = %+v", e)
	}
}

func checkResolved(t *testing.T, e report.Event) {
	t.Helper()
	if e.ResolutionCode != "STOCK_FOUND" || e.RootCause != "COUNT_ERROR" || e.Note == nil || *e.Note != "adjusted" {
		t.Fatalf("resolved = %+v", e)
	}
}

func checkAutoResolved(t *testing.T, e report.Event) {
	t.Helper()
	if e.RuleID != taskCE+"->"+repromCE || e.CorrelatedType != repromCE {
		t.Fatalf("auto-resolved = %+v", e)
	}
}

func checkPolicyDeclared(t *testing.T, e report.Event) {
	t.Helper()
	if e.Version != 1 || e.SLAMinutes != 30 || e.AutoResolve == nil || e.AutoResolve.OnType != repromCE || e.AutoResolve.WithinMinutes != 10 {
		t.Fatalf("policy = %+v", e)
	}
}

// A type the projector does not know is ignored, not an error.
func TestAnalyticsConsumerIgnoresUnknownTypes(t *testing.T) {
	proj := &fakeProjection{}
	c := newTestAnalyticsConsumer(proj)
	other := analyticsEvent(t, "exceptioncase", "ExceptionReopened", "case-1", `{"case_id":"case-1"}`)
	if err := c.HandleMessage(context.Background(), other); err != nil {
		t.Fatalf("unknown type: %v", err)
	}
	if len(proj.applied) != 0 {
		t.Fatalf("%d applied, want none", len(proj.applied))
	}
}

// A message that is not a CloudEvents 1.0 event is skipped (WARN), never an
// error: the run loop commits past it.
func TestAnalyticsConsumerSkipsNonCloudEvents(t *testing.T) {
	proj := &fakeProjection{}
	c := newTestAnalyticsConsumer(proj)
	if err := c.HandleMessage(context.Background(), []byte(`{"legacy":true}`)); err != nil {
		t.Fatalf("not a CloudEvent: %v", err)
	}
	if len(proj.applied) != 0 {
		t.Fatalf("%d applied, want none", len(proj.applied))
	}
}

// Poison: a payload whose subject does not match, one that fails validation,
// and one the store deterministically rejects. All are errDeadLetter markers
// so the run loop parks them on the DLQ and commits past.
func TestAnalyticsConsumerDeadLettersDeterministicRefusals(t *testing.T) {
	cases := []struct {
		name    string
		entity  string
		event   string
		subject string
		payload string
	}{
		{"subject mismatch", "exceptioncase", "ExceptionResolved", "case-OTHER", `{"case_id":"case-1","resolution_code":"DONE","root_cause":"X","note":null,"actor":"a","at":"2026-10-21T10:15:00Z"}`},
		{"missing root cause", "exceptioncase", "ExceptionResolved", "case-1", `{"case_id":"case-1","resolution_code":"DONE","note":null,"actor":"a","at":"2026-10-21T10:15:00Z"}`},
		{"broken sla_due_at", "exceptioncase", "ExceptionOpened", "case-1", `{"case_id":"case-1","source_type":"` + taskCE + `","source_subject":"s","severity":"HIGH","queue":"q","opened_at":"2026-10-21T09:30:00Z","sla_due_at":"soon"}`},
		{"policy without a version", "exceptionpolicy", "ExceptionPolicyDeclared", taskCE, `{"source_type":"` + taskCE + `","version":0,"severity":"HIGH","queue":"q","sla_minutes":30}`},
	}
	for _, c := range cases {
		proj := &fakeProjection{errBy: map[string]error{}}
		consumer := newTestAnalyticsConsumer(proj)
		err := consumer.HandleMessage(context.Background(),
			analyticsEvent(t, c.entity, c.event, c.subject, c.payload))
		var poison *errDeadLetter
		if !errors.As(err, &poison) {
			t.Errorf("%s: %v, want the dead-letter marker", c.name, err)
		}
	}
}

// A store rejection (report.ErrRejected) is poison; a transient failure is
// returned for the capped retry, never dead-lettered.
func TestAnalyticsConsumerSeparatesRejectionFromTransient(t *testing.T) {
	rejected := &fakeProjection{errBy: map[string]error{"ev-ExceptionResolved": errRejectedForTest()}}
	c := newTestAnalyticsConsumer(rejected)
	good := analyticsEvent(t, "exceptioncase", "ExceptionResolved", "case-1",
		`{"case_id":"case-1","resolution_code":"DONE","root_cause":"X","note":null,"actor":"a","at":"2026-10-21T10:15:00Z"}`)
	err := c.HandleMessage(context.Background(), good)
	var poison *errDeadLetter
	if !errors.As(err, &poison) || !errors.Is(err, report.ErrRejected) {
		t.Fatalf("rejected: %v, want the dead-letter marker wrapping ErrRejected", err)
	}

	transient := &fakeProjection{errBy: map[string]error{"ev-ExceptionResolved": context.DeadlineExceeded}}
	c2 := newTestAnalyticsConsumer(transient)
	err = c2.HandleMessage(context.Background(), good)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transient: %v, want it returned for the retry", err)
	}
	var marker *errDeadLetter
	if errors.As(err, &marker) {
		t.Fatal("a transient failure must not carry the dead-letter marker")
	}
}

func errRejectedForTest() error {
	return errors.Join(report.ErrRejected, errors.New("sqlstate 23505"))
}
