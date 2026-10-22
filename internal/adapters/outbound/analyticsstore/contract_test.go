package analyticsstore

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// The contract both stores must satisfy identically: the Postgres fold
// semantics and the report definitions, exercised by one script of events.
// The unit test runs it against Memory; the integration test runs it against
// real Postgres (ADR 0006: the two can never drift).

const (
	sourceTaskCE = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	sourceInvCE  = "com.warehouse.wes.inventory-storage.bin.DiscrepancyDetected"
	onRepromised = "com.warehouse.wes.order-management.order.OrderRepromised"
)

func tat(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }

func strp(s string) *string { return &s }

// contractStore is one store under test: the writer and the reader are the
// same model in both implementations.
type contractStore struct {
	name       string
	projection report.Projection
	reader     report.Reader
}

// script is the shared event script: three cases and a policy declaration.
// case-1 breaches (escalated, then resolved late-ish); case-2 resolves
// cleanly in another queue and source; case-3 auto-resolves in the source-A
// queue. case-4 pins the out-of-order fold: its resolution is applied
// BEFORE its opened event.
func script() []report.Event {
	return []report.Event{
		{Kind: report.KindExceptionPolicyDeclared, EventID: "pol-1", At: tat(11, 0),
			SourceType: sourceTaskCE, Version: 1, Severity: "HIGH", Queue: "ps-main", SLAMinutes: 30,
			AutoResolve: &report.AutoResolveRule{OnType: onRepromised, WithinMinutes: 10}},
		{Kind: report.KindExceptionOpened, EventID: "c1-open", At: tat(9, 0), CaseID: "case-1",
			SourceType: sourceTaskCE, SourceSubject: "task-8a1f", Severity: "HIGH", Queue: "ps-main",
			SLADueAt: tat(10, 0)},
		{Kind: report.KindExceptionTriaged, EventID: "c1-tri", At: tat(9, 10), CaseID: "case-1",
			Severity: "MEDIUM", Queue: "ps-expedite"},
		{Kind: report.KindExceptionAssigned, EventID: "c1-asg", At: tat(9, 20), CaseID: "case-1", Assignee: "jlee"},
		{Kind: report.KindExceptionEscalated, EventID: "c1-esc", At: tat(10, 5), CaseID: "case-1", SLADueAt: tat(10, 0)},
		{Kind: report.KindExceptionResolved, EventID: "c1-res", At: tat(10, 15), CaseID: "case-1",
			ResolutionCode: "STOCK_FOUND", RootCause: "COUNT_ERROR"},
		{Kind: report.KindExceptionOpened, EventID: "c2-open", At: tat(9, 30), CaseID: "case-2",
			SourceType: sourceInvCE, SourceSubject: "A-12-3", SiteID: strp("SITE-1"), Severity: "HIGH", Queue: "ps-back",
			SLADueAt: tat(11, 30)},
		{Kind: report.KindExceptionResolved, EventID: "c2-res", At: tat(10, 0), CaseID: "case-2",
			ResolutionCode: "STOCK_ADJUSTED", RootCause: "BIN_MISMATCH", Note: strp("counted 47")},
		{Kind: report.KindExceptionOpened, EventID: "c3-open", At: tat(9, 45), CaseID: "case-3",
			SourceType: sourceTaskCE, SourceSubject: "task-9b2c", Severity: "LOW", Queue: "ps-main",
			SLADueAt: tat(13, 45)},
		{Kind: report.KindExceptionAutoResolved, EventID: "c3-auto", At: tat(9, 50), CaseID: "case-3",
			RuleID: sourceTaskCE + "->" + onRepromised, CorrelatedType: onRepromised},
		// case-4: resolved BEFORE its opened event is seen.
		{Kind: report.KindExceptionResolved, EventID: "c4-res", At: tat(10, 30), CaseID: "case-4",
			ResolutionCode: "STOCK_FOUND", RootCause: "COUNT_ERROR"},
		{Kind: report.KindExceptionOpened, EventID: "c4-open", At: tat(9, 55), CaseID: "case-4",
			SourceType: sourceInvCE, SourceSubject: "A-13-1", Severity: "MEDIUM", Queue: "ps-back",
			SLADueAt: tat(13, 55)},
	}
}

func window() report.Query {
	return report.Query{Range: report.Range{From: tat(8, 0), To: tat(20, 0)}}
}

func approx(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

// runStoreContract applies the script and runs every report check as its
// own subtest (one function per check keeps the complexity gate honest).
func runStoreContract(t *testing.T, s contractStore) {
	t.Helper()
	ctx := context.Background()
	for _, e := range script() {
		applied, err := s.projection.Apply(ctx, e)
		if err != nil || !applied {
			t.Fatalf("apply %s %s: applied=%v err=%v", e.Kind, e.EventID, applied, err)
		}
	}
	for _, check := range []struct {
		name string
		run  func(t *testing.T, s contractStore)
	}{
		{"root cause frequency", checkRootCauseFrequency},
		{"mean time to resolve per queue", checkMeanTimeToResolve},
		{"sla breach rate per source type", checkSLABreachRate},
		{"a replayed event changes nothing", checkReplayIsANoop},
		{"an invalid event is rejected deterministically", checkInvalidEventRejected},
		{"freshness", checkFreshness},
		{"an empty window answers empty, not null", checkEmptyWindow},
	} {
		t.Run(check.name, func(t *testing.T) { check.run(t, s) })
	}
}

func checkRootCauseFrequency(t *testing.T, s contractStore) {
	rows, err := s.reader.RootCauseRows(context.Background(), window())
	if err != nil {
		t.Fatal(err)
	}
	// Four manual resolutions in the window (case-1, case-2, case-4 and
	// none for the auto-resolved case-3); case-4 shares sourceInvCE. Every
	// source type totals two resolutions, so every share is one half.
	want := []report.RootCauseRow{
		{SourceType: sourceTaskCE, RootCause: "COUNT_ERROR", Resolved: 1, Share: 0.5},
		{SourceType: sourceInvCE, RootCause: "BIN_MISMATCH", Resolved: 1, Share: 0.5},
		{SourceType: sourceInvCE, RootCause: "COUNT_ERROR", Resolved: 1, Share: 0.5},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows = %+v, want %d rows", rows, len(want))
	}
	for i, w := range want {
		if rows[i].SourceType != w.SourceType || rows[i].RootCause != w.RootCause || rows[i].Resolved != w.Resolved {
			t.Fatalf("row %d = %+v, want %+v", i, rows[i], w)
		}
		approx(t, "share", rows[i].Share, w.Share)
	}
}

func checkMeanTimeToResolve(t *testing.T, s contractStore) {
	rows, err := s.reader.QueueTimingRows(context.Background(), window())
	if err != nil {
		t.Fatal(err)
	}
	// ps-back: case-2 (30m) and case-4 (35m); ps-expedite: case-1 (75m,
	// triaged into ps-expedite); ps-main: case-3 (5m, auto).
	if len(rows) != 3 {
		t.Fatalf("rows = %+v, want 3 queues", rows)
	}
	byQueue := map[string]report.QueueTimingRow{}
	for _, r := range rows {
		byQueue[r.Queue] = r
	}
	back, expedite, main := byQueue["ps-back"], byQueue["ps-expedite"], byQueue["ps-main"]
	if back.Resolved != 2 || expedite.Resolved != 1 || main.Resolved != 1 {
		t.Fatalf("resolved counts: %+v", rows)
	}
	approx(t, "ps-back mean", *back.MeanSeconds, 1950) // (1800+2100)/2
	approx(t, "ps-back p50", *back.P50Seconds, 1950)
	approx(t, "ps-back p95", *back.P95Seconds, 2085)
	approx(t, "ps-expedite mean", *expedite.MeanSeconds, 4500)
	approx(t, "ps-main mean", *main.MeanSeconds, 300)
}

func checkSLABreachRate(t *testing.T, s contractStore) {
	rows, err := s.reader.SLABreachRows(context.Background(), window())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want 2 source types", rows)
	}
	task, inv := rows[0], rows[1] // ordered by source type
	if task.SourceType != sourceTaskCE || inv.SourceType != sourceInvCE {
		t.Fatalf("order = %+v", rows)
	}
	// source A: case-1 (escalated -> breached) and case-3 (auto-resolved in
	// time); source B: case-2 and case-4 both resolved before due.
	if task.Opened != 2 || task.Escalated != 1 || task.Breached != 1 {
		t.Fatalf("task row = %+v", task)
	}
	approx(t, "task breach rate", task.BreachRate, 0.5)
	approx(t, "task escalation rate", task.EscalationRate, 0.5)
	if inv.Opened != 2 || inv.Escalated != 0 || inv.Breached != 0 {
		t.Fatalf("inventory row = %+v", inv)
	}
}

func checkReplayIsANoop(t *testing.T, s contractStore) {
	replay := script()[5] // case-1 resolved
	applied, err := s.projection.Apply(context.Background(), replay)
	if err != nil || applied {
		t.Fatalf("replay: applied=%v err=%v", applied, err)
	}
	rows, err := s.reader.RootCauseRows(context.Background(), window())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("root cause rows after the replay = %+v", rows)
	}
}

func checkInvalidEventRejected(t *testing.T, s contractStore) {
	bad := script()[0]
	bad.Version = 0
	if _, err := s.projection.Apply(context.Background(), bad); !errors.Is(err, report.ErrRejected) {
		t.Fatalf("apply invalid: %v, want ErrRejected", err)
	}
}

func checkFreshness(t *testing.T, s contractStore) {
	at, err := s.reader.LastEventAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if at == nil || !at.Equal(tat(11, 0)) {
		t.Fatalf("last event at = %v, want the 11:00 policy declaration", at)
	}
}

func checkEmptyWindow(t *testing.T, s contractStore) {
	q := report.Query{Range: report.Range{From: tat(20, 0), To: tat(23, 0)}}
	for _, check := range []func() (int, error){
		func() (int, error) { r, err := s.reader.RootCauseRows(context.Background(), q); return len(r), err },
		func() (int, error) { r, err := s.reader.QueueTimingRows(context.Background(), q); return len(r), err },
		func() (int, error) { r, err := s.reader.SLABreachRows(context.Background(), q); return len(r), err },
	} {
		n, err := check()
		if err != nil || n != 0 {
			t.Fatalf("empty window: n=%d err=%v", n, err)
		}
	}
}

func TestMemoryStoreSatisfiesTheContract(t *testing.T) {
	m := NewMemory()
	runStoreContract(t, contractStore{name: "memory", projection: m, reader: m})
}

// The store must refuse an event whose data Postgres rejects as a data
// exception or integrity violation with report.ErrRejected (dead-lettered
// at once, never retried).
func TestClassifyMapsDataAndIntegrityClassesToRejected(t *testing.T) {
	for _, code := range []string{"22001", "22P02", "23000", "23505"} {
		err := classify(&pgconn.PgError{Code: code, Message: "boom"})
		if !errors.Is(err, report.ErrRejected) {
			t.Errorf("code %s: %v, want ErrRejected", code, err)
		}
	}
	for _, code := range []string{"08006", "40001", "57014", "P0001"} {
		if err := classify(&pgconn.PgError{Code: code, Message: "boom"}); errors.Is(err, report.ErrRejected) {
			t.Errorf("code %s: must stay transient", code)
		}
	}
}
