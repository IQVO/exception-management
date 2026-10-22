package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// fakeReader serves canned rows/errors per report.
type fakeReader struct {
	rootCause []report.RootCauseRow
	timing    []report.QueueTimingRow
	breach    []report.SLABreachRow
	lastAt    *time.Time
	err       error
}

func (f *fakeReader) RootCauseRows(context.Context, report.Query) ([]report.RootCauseRow, error) {
	return f.rootCause, f.err
}
func (f *fakeReader) QueueTimingRows(context.Context, report.Query) ([]report.QueueTimingRow, error) {
	return f.timing, f.err
}
func (f *fakeReader) SLABreachRows(context.Context, report.Query) ([]report.SLABreachRow, error) {
	return f.breach, f.err
}
func (f *fakeReader) LastEventAt(context.Context) (*time.Time, error) { return f.lastAt, f.err }

func reportsServer() *ReportsServer {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	return &ReportsServer{Reader: &fakeReader{}, Now: func() time.Time { return now }}
}

// The reports router serves exactly the four documented report paths plus
// /healthz — nothing else (cmd/api serves no /reports/* route).
func TestReportsRouterServesTheDocumentedReports(t *testing.T) {
	srv := httptest.NewServer(NewReportsRouter(reportsServer()))
	defer srv.Close()
	for _, path := range []string{
		"/healthz",
		"/reports/root-cause-frequency",
		"/reports/mean-time-to-resolve",
		"/reports/sla-breach-rate",
		"/reports/freshness",
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, resp.StatusCode)
		}
	}
	for _, path := range []string{"/reports/", "/reports/unknown", "/"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}
}

// Every report echoes the resolved window and answers empty arrays (never
// null) when the analytical model holds nothing.
func TestReportsAnswerEmptyArraysAndEchoTheWindow(t *testing.T) {
	srv := httptest.NewServer(NewReportsRouter(reportsServer()))
	defer srv.Close()
	for _, c := range []struct {
		path string
		list string
	}{
		{"/reports/root-cause-frequency", "root_causes"},
		{"/reports/mean-time-to-resolve", "queues"},
		{"/reports/sla-breach-rate", "source_types"},
	} {
		resp, err := http.Get(srv.URL + c.path + "?from=2026-10-01T00:00:00Z&to=2026-10-21T00:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("%s: %v", c.path, err)
		}
		if body["from"] != "2026-10-01T00:00:00Z" || body["to"] != "2026-10-21T00:00:00Z" {
			t.Errorf("%s: window = %v..%v", c.path, body["from"], body["to"])
		}
		if list, ok := body[c.list].([]any); !ok || list == nil {
			t.Errorf("%s: %s = %v, want []", c.path, c.list, body[c.list])
		}
	}
}

func TestRootCauseFrequencyWireShape(t *testing.T) {
	share := 0.5
	s := reportsServer()
	s.Reader = &fakeReader{rootCause: []report.RootCauseRow{
		{SourceType: "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", RootCause: "COUNT_ERROR", Resolved: 3, Share: share},
	}}
	srv := httptest.NewServer(NewReportsRouter(s))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/reports/root-cause-frequency")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		RootCauses []struct {
			SourceType string  `json:"source_type"`
			RootCause  string  `json:"root_cause"`
			Resolved   int64   `json:"resolved"`
			Share      float64 `json:"share"`
		} `json:"root_causes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.RootCauses) != 1 || body.RootCauses[0].Resolved != 3 || body.RootCauses[0].Share != share {
		t.Fatalf("root causes = %+v", body.RootCauses)
	}
}

func TestMeanTimeToResolveWireShape(t *testing.T) {
	mean, p50, p95 := 1950.0, 1950.0, 2085.0
	s := reportsServer()
	s.Reader = &fakeReader{timing: []report.QueueTimingRow{
		{Queue: "ps-back", Resolved: 2, MeanSeconds: &mean, P50Seconds: &p50, P95Seconds: &p95},
	}}
	srv := httptest.NewServer(NewReportsRouter(s))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/reports/mean-time-to-resolve")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Queues []struct {
			Queue  string `json:"queue"`
			Timing struct {
				Resolved    int      `json:"resolved"`
				MeanSeconds *float64 `json:"mean_seconds"`
				P50Seconds  *float64 `json:"p50_seconds"`
				P95Seconds  *float64 `json:"p95_seconds"`
			} `json:"timing"`
		} `json:"queues"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Queues) != 1 || body.Queues[0].Queue != "ps-back" || body.Queues[0].Timing.Resolved != 2 ||
		*body.Queues[0].Timing.MeanSeconds != mean || *body.Queues[0].Timing.P95Seconds != p95 {
		t.Fatalf("queues = %+v", body.Queues)
	}
}

func TestSLABreachRateWireShape(t *testing.T) {
	s := reportsServer()
	s.Reader = &fakeReader{breach: []report.SLABreachRow{
		{SourceType: "src", Opened: 4, Escalated: 1, Breached: 2, BreachRate: 0.5, EscalationRate: 0.25},
	}}
	srv := httptest.NewServer(NewReportsRouter(s))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/reports/sla-breach-rate")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		SourceTypes []map[string]any `json:"source_types"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"source_type": "src", "opened": float64(4), "escalated": float64(1),
		"breached": float64(2), "breach_rate": 0.5, "escalation_rate": 0.25}
	if len(body.SourceTypes) != 1 || !reflect.DeepEqual(body.SourceTypes[0], want) {
		t.Fatalf("source types = %+v, want %+v", body.SourceTypes, want)
	}
}

func TestFreshnessWireShape(t *testing.T) {
	s := reportsServer()
	srv := httptest.NewServer(NewReportsRouter(s))
	defer srv.Close()
	// Before the first event both fields are null.
	resp, err := http.Get(srv.URL + "/reports/freshness")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if body["as_of"] != nil || body["lag_seconds"] != nil {
		t.Fatalf("empty freshness = %v, want both null", body)
	}
	// After one event the lag is now - as_of.
	asOf := time.Date(2026, 10, 21, 11, 0, 0, 0, time.UTC)
	s.Reader = &fakeReader{lastAt: &asOf}
	resp, err = http.Get(srv.URL + "/reports/freshness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body = nil
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["as_of"] != "2026-10-21T11:00:00Z" || body["lag_seconds"] != float64(3600) {
		t.Fatalf("freshness = %v", body)
	}
}

// A bad window is the documented 400 on every report; a failed store query
// is the 500 that never echoes the cause.
func TestReportsProblemMap(t *testing.T) {
	s := reportsServer()
	srv := httptest.NewServer(NewReportsRouter(s))
	defer srv.Close()
	for _, path := range []string{"/reports/root-cause-frequency", "/reports/mean-time-to-resolve", "/reports/sla-breach-rate"} {
		resp, err := http.Get(srv.URL + path + "?from=bogus")
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Type string `json:"type"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.HasSuffix(body.Type, "invalid-report-range") {
			t.Errorf("%s: %d %s, want 400 invalid-report-range", path, resp.StatusCode, body.Type)
		}
	}
	s.Reader = &fakeReader{err: errors.New("connection refused")}
	for _, path := range []string{"/reports/root-cause-frequency", "/reports/mean-time-to-resolve", "/reports/sla-breach-rate", "/reports/freshness"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Type string `json:"type"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError || !strings.HasSuffix(body.Type, "report-store-error") {
			t.Errorf("%s: %d %s, want 500 report-store-error", path, resp.StatusCode, body.Type)
		}
	}
}
