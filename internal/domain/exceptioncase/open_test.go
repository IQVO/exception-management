package exceptioncase

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

var (
	openedAt  = time.Date(2026, 10, 21, 9, 0, 5, 0, time.UTC)
	cptMissed = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	payload   = []byte(`{"task_id":"task-8a1f","order_ref":"ord-7-line-3","cpt":"2026-09-30T14:55:00Z"}`)
)

func testKey() shared.DedupKey {
	return shared.DedupKey{SourceType: cptMissed, SourceSubject: "task-8a1f", SiteID: ""}
}

func testOpenInput() OpenInput {
	return OpenInput{
		CaseID:        "case-1",
		Key:           testKey(),
		Severity:      shared.SeverityHigh,
		Queue:         "cps-front",
		SLAMinutes:    30,
		SourceEventID: "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
		Payload:       payload,
		At:            openedAt,
	}
}

func openedCase(t *testing.T) *Case {
	t.Helper()
	c, err := Open(testOpenInput())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return c
}

// Invariant: Open rejects every invalid input whole (the failing paths).
func TestOpenRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mut  func(*OpenInput)
		want error
	}{
		{"blank case id", func(in *OpenInput) { in.CaseID = " " }, ErrBlankCaseID},
		{"NUL case id", func(in *OpenInput) { in.CaseID = "case\x001" }, ErrBlankCaseID},
		{"blank source type", func(in *OpenInput) { in.Key.SourceType = "" }, ErrInvalidDedupKey},
		{"blank source subject", func(in *OpenInput) { in.Key.SourceSubject = " " }, ErrInvalidDedupKey},
		{"blank-ish site", func(in *OpenInput) { in.Key.SiteID = " " }, ErrInvalidDedupKey},
		{"unknown severity", func(in *OpenInput) { in.Severity = "SEV1" }, ErrInvalidSeverity},
		{"blank queue", func(in *OpenInput) { in.Queue = "" }, ErrBlankQueue},
		{"zero SLA", func(in *OpenInput) { in.SLAMinutes = 0 }, ErrInvalidSLA},
		{"blank source event id", func(in *OpenInput) { in.SourceEventID = " " }, ErrBlankSourceEventID},
		{"empty payload", func(in *OpenInput) { in.Payload = nil }, ErrEmptySourcePayload},
		{"zero timestamp", func(in *OpenInput) { in.At = time.Time{} }, ErrNoTimestamp},
	}
	for _, tc := range cases {
		in := testOpenInput()
		tc.mut(&in)
		if _, err := Open(in); !errors.Is(err, tc.want) {
			t.Errorf("%s: Open err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

// Invariant: a source fact creates an OPEN case at version 1 with the
// policy's triage baked in.
func TestOpenRecordsThePolicyTriageAtVersion1(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	if c.Status() != StatusOpen || c.Version() != 1 || c.Occurrence() != 1 {
		t.Fatalf("status=%s version=%d occurrence=%d", c.Status(), c.Version(), c.Occurrence())
	}
	if c.Severity() != shared.SeverityHigh || c.Queue() != "cps-front" || c.SLAMinutes() != 30 {
		t.Error("the policy defaults must be recorded at open")
	}
	if c.OpenedAt() != openedAt || c.LastSeenAt() != openedAt {
		t.Error("lastSeenAt starts at openedAt")
	}
	if c.Assignee() != "" || c.Escalated() || c.ResolutionCode() != "" {
		t.Error("a fresh case has no assignee, no flag, no resolution")
	}
}

// Invariant: Open raises exactly one ExceptionOpened carrying the whole
// source fact.
func TestOpenRaisesExceptionOpenedCarryingTheSourceFact(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	evts := c.DomainEvents()
	if len(evts) != 1 || evts[0].EventName() != "ExceptionOpened" {
		t.Fatalf("events = %v", evts)
	}
	opened := evts[0].(ExceptionOpened)
	if opened.CaseID() != "case-1" || opened.CaseVersion() != 1 || opened.OccurredAt() != openedAt {
		t.Errorf("ExceptionOpened header = %+v", opened.Header)
	}
	if opened.Key != testKey() || opened.Severity != shared.SeverityHigh || opened.Queue != "cps-front" {
		t.Errorf("ExceptionOpened body = %+v", opened)
	}
	if opened.SLADueAt != c.SLADueAt() || opened.SourceEventID != "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b" {
		t.Errorf("ExceptionOpened SLA/source = %+v", opened)
	}
	if string(opened.SourcePayload) != string(payload) {
		t.Error("ExceptionOpened carries the source payload")
	}
}

// Invariant: DomainEvents hands out a copy — appending to the returned slice
// must not touch the case.
func TestDomainEventsReturnsACopy(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	evts := c.DomainEvents()
	_ = append(evts, nil)
	if got := c.DomainEvents(); len(got) != 1 {
		t.Error("DomainEvents must return a copy")
	}
}
