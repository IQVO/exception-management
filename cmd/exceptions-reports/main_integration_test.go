//go:build integration

// Integration proof of cmd/exceptions-reports: the real run() over an EMPTY
// analytical database this test migrates and seeds through the repo's own
// analytics writer (analyticsstore.Projection, the writer the projector
// consumer drives). The booted binary answers its documented probe and the
// documented reports from the seeded facts, and stops cleanly on its only
// stop path: SIGTERM -> graceful drain (run takes no context).
package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/exception-management/internal/analytics/report"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// seedEvents is the two-case lifecycle the analytics stream carries (ADR
// 0004): case-1 opens at 09:00 on a 10:00 clock, is triaged into ps-expedite,
// assigned, escalated and manually resolved at 10:15 with root cause
// COUNT_ERROR; case-2 opens at 09:45 and auto-resolves at 09:50. Every
// number the test asserts below follows from these instants.
func seedEvents() []report.Event {
	taskCE := "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromCE := "com.warehouse.wes.order-management.order.OrderRepromised"
	at := func(h, m int) time.Time { return time.Date(2026, 10, 21, h, m, 0, 0, time.UTC) }
	site := "SIM1"
	return []report.Event{
		{Kind: report.KindExceptionOpened, EventID: "it-o1", At: at(9, 0), CaseID: "case-reports-it-1",
			SourceType: taskCE, SourceSubject: "task-r1", SiteID: &site, Severity: "HIGH", Queue: "ps-main", SLADueAt: at(10, 0)},
		{Kind: report.KindExceptionTriaged, EventID: "it-t1", At: at(9, 10), CaseID: "case-reports-it-1", Severity: "MEDIUM", Queue: "ps-expedite"},
		{Kind: report.KindExceptionAssigned, EventID: "it-a1", At: at(9, 20), CaseID: "case-reports-it-1", Assignee: "jlee"},
		{Kind: report.KindExceptionEscalated, EventID: "it-e1", At: at(10, 5), CaseID: "case-reports-it-1", SLADueAt: at(10, 0)},
		{Kind: report.KindExceptionResolved, EventID: "it-r1", At: at(10, 15), CaseID: "case-reports-it-1", ResolutionCode: "STOCK_FOUND", RootCause: "COUNT_ERROR"},
		{Kind: report.KindExceptionOpened, EventID: "it-o2", At: at(9, 45), CaseID: "case-reports-it-2",
			SourceType: taskCE, SourceSubject: "task-r2", SiteID: &site, Severity: "LOW", Queue: "ps-main", SLADueAt: at(13, 45)},
		{Kind: report.KindExceptionAutoResolved, EventID: "it-ar2", At: at(9, 50), CaseID: "case-reports-it-2", RuleID: taskCE + "->" + repromCE, CorrelatedType: repromCE},
	}
}

// seedProjection folds seedEvents through the repo's own analytics writer on
// a database this test migrated itself.
func seedProjection(t *testing.T, url string) {
	t.Helper()
	ctx := context.Background()
	writer, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	projection := analyticsstore.NewProjection(writer)
	for _, e := range seedEvents() {
		applied, err := projection.Apply(ctx, e)
		if err != nil || !applied {
			t.Fatalf("apply %s %s: applied=%v err=%v", e.Kind, e.EventID, applied, err)
		}
	}
}

// freeAddr reserves a loopback port and releases it for the binary to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// getJSON GETs url into dest, requiring 200.
func getJSON(t *testing.T, url string, dest any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
}

// waitHealthy polls the binary's documented probe (GET /healthz) until it
// answers 200.
func waitHealthy(t *testing.T, base string) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			continue
		}
		code := resp.StatusCode
		_ = resp.Body.Close()
		if code == http.StatusOK {
			return
		}
	}
	t.Fatal("/healthz never answered 200")
}

// TestReportsServeTheSeededProjectionAndStopCleanly boots the real run()
// over the seeded analytical database and proves the whole documented
// surface of the binary: the probe, the three reports with their exact
// numbers from the seeded facts, the freshness of the projection, and a
// clean drain on SIGTERM.
func TestReportsServeTheSeededProjectionAndStopCleanly(t *testing.T) {
	url := pgtest.EmptyURL(t) // a fresh database with NO schema: this test owns the migration step
	if err := analyticsstore.RunMigrations(url); err != nil {
		t.Fatalf("analytics migrations: %v", err)
	}
	seedProjection(t, url)

	addr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")

	// Catch SIGTERM from the start so the stop signal below can never kill
	// the test process if it races run()'s own registration.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	defer signal.Stop(sig)

	done := make(chan error, 1)
	go func() { done <- run() }()
	base := "http://" + addr
	waitHealthy(t, base)

	from, to := "2026-10-21T08:00:00Z", "2026-10-21T20:00:00Z"
	assertMeanTimeToResolve(t, base, from, to)
	assertRootCauseFrequency(t, base, from, to)
	assertSLABreachRate(t, base, from, to)
	assertFreshness(t, base)

	// run() takes no context; SIGTERM is the binary's stop path. A retried
	// signal makes the drain deterministic even if the first one lands
	// before run() installs its own handler.
	for range 5 {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a SIGTERM'd reports server must drain cleanly, got %v", err)
			}
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatal("the reports server did not stop on SIGTERM")
}

// assertMeanTimeToResolve proves the per-queue timing: ps-expedite resolved
// one case in 4500s (opened 09:00, resolved 10:15) and ps-main one in 300s
// (opened 09:45, auto-resolved 09:50).
func assertMeanTimeToResolve(t *testing.T, base, from, to string) {
	t.Helper()
	var mttr struct {
		Queues []struct {
			Queue  string `json:"queue"`
			Timing struct {
				Resolved    int      `json:"resolved"`
				MeanSeconds *float64 `json:"mean_seconds"`
			} `json:"timing"`
		} `json:"queues"`
	}
	getJSON(t, base+"/reports/mean-time-to-resolve?from="+from+"&to="+to, &mttr)
	counts, means := map[string]int{}, map[string]float64{}
	for _, q := range mttr.Queues {
		if q.Timing.MeanSeconds == nil {
			t.Fatalf("%s has %d resolutions but no mean", q.Queue, q.Timing.Resolved)
		}
		counts[q.Queue], means[q.Queue] = q.Timing.Resolved, *q.Timing.MeanSeconds
	}
	if len(mttr.Queues) != 2 || counts["ps-expedite"] != 1 || means["ps-expedite"] != 4500 {
		t.Fatalf("mean-time-to-resolve = %+v, want ps-expedite 1 case at 4500s", mttr.Queues)
	}
	if counts["ps-main"] != 1 || means["ps-main"] != 300 {
		t.Fatalf("mean-time-to-resolve = %+v, want ps-main 1 case at 300s", mttr.Queues)
	}
}

// assertRootCauseFrequency proves the manual-resolution grouping: one
// COUNT_ERROR resolution of the missed-CPT type, half of that type's two
// resolutions in the window (the auto-resolved one carries no root cause).
func assertRootCauseFrequency(t *testing.T, base, from, to string) {
	t.Helper()
	var rc struct {
		RootCauses []struct {
			SourceType string  `json:"source_type"`
			RootCause  string  `json:"root_cause"`
			Resolved   int64   `json:"resolved"`
			Share      float64 `json:"share"`
		} `json:"root_causes"`
	}
	getJSON(t, base+"/reports/root-cause-frequency?from="+from+"&to="+to, &rc)
	if len(rc.RootCauses) != 1 || rc.RootCauses[0].RootCause != "COUNT_ERROR" || rc.RootCauses[0].Resolved != 1 {
		t.Fatalf("root causes = %+v", rc.RootCauses)
	}
	if rc.RootCauses[0].Share != 0.5 {
		t.Fatalf("share = %v, want 0.5", rc.RootCauses[0].Share)
	}
}

// assertSLABreachRate proves the per-source-type shares over the cases
// opened in the window: two opened, one breached (resolved past its 10:00
// clock), one escalated.
func assertSLABreachRate(t *testing.T, base, from, to string) {
	t.Helper()
	var breach struct {
		SourceTypes []struct {
			Opened         int64   `json:"opened"`
			Escalated      int64   `json:"escalated"`
			Breached       int64   `json:"breached"`
			BreachRate     float64 `json:"breach_rate"`
			EscalationRate float64 `json:"escalation_rate"`
		} `json:"source_types"`
	}
	getJSON(t, base+"/reports/sla-breach-rate?from="+from+"&to="+to, &breach)
	if len(breach.SourceTypes) != 1 {
		t.Fatalf("sla breach rows = %+v", breach.SourceTypes)
	}
	row := breach.SourceTypes[0]
	if row.Opened != 2 || row.Breached != 1 || row.BreachRate != 0.5 || row.Escalated != 1 || row.EscalationRate != 0.5 {
		t.Fatalf("sla breach row = %+v, want 2 opened, 1 breached/escalated", row)
	}
}

// assertFreshness proves the projection freshness: the newest applied fact
// is the 10:15 resolution, and the lag behind the real clock is positive.
func assertFreshness(t *testing.T, base string) {
	t.Helper()
	var fresh struct {
		AsOf       *string  `json:"as_of"`
		LagSeconds *float64 `json:"lag_seconds"`
	}
	getJSON(t, base+"/reports/freshness", &fresh)
	if fresh.AsOf == nil || *fresh.AsOf != "2026-10-21T10:15:00Z" {
		t.Fatalf("freshness = %+v, want the 10:15 resolution as the newest fact", fresh)
	}
	if fresh.LagSeconds == nil || *fresh.LagSeconds <= 0 {
		t.Fatalf("freshness lag = %+v, want a positive lag behind the real clock", fresh)
	}
}
