package http

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestHealthzIsAlways200(t *testing.T) {
	f := newFixture(t)
	rec := mustCall(t, f.router(), http.MethodGet, "/healthz", "", http.StatusOK, nil)
	if rec.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("healthz = %q", rec.Body.String())
	}
}

func TestReadyzReadyWhenNothingFails(t *testing.T) {
	f := newFixture(t)
	f.srv.Readiness = NewReadiness(0, 0)
	rec := mustCall(t, f.router(), http.MethodGet, "/readyz", "", http.StatusOK, nil)
	if rec.Body.String() != "{\"status\":\"ready\"}\n" {
		t.Fatalf("readyz = %q", rec.Body.String())
	}
}

func TestReadyzNilGateMeansReady(t *testing.T) {
	f := newFixture(t) // no Readiness wired
	mustCall(t, f.router(), http.MethodGet, "/readyz", "", http.StatusOK, nil)
}

func TestReadyzDrainGateFlipsTo503NotReady(t *testing.T) {
	f := newFixture(t)
	f.srv.Readiness = NewReadiness(time.Hour, 0) // deps cached as passing
	f.srv.Readiness.SetNotReady()
	rec := mustCall(t, f.router(), http.MethodGet, "/readyz", "", http.StatusServiceUnavailable, nil)
	if rec.Body.String() != "{\"status\":\"not_ready\"}\n" {
		t.Fatalf("draining readyz = %q", rec.Body.String())
	}
	// Liveness is never flipped by the drain gate.
	mustCall(t, f.router(), http.MethodGet, "/healthz", "", http.StatusOK, nil)
}

func TestReadyzNamesTheFailingDependency(t *testing.T) {
	f := newFixture(t)
	f.srv.Readiness = NewReadiness(0, 0,
		Dependency{Name: "postgres", Check: func(context.Context) error { return nil }},
		Dependency{Name: "kafka", Check: func(context.Context) error { return errors.New("dial tcp: refused") }},
	)
	rec := mustCall(t, f.router(), http.MethodGet, "/readyz", "", http.StatusServiceUnavailable, nil)
	wantProblem(t, rec, http.StatusServiceUnavailable, "dependency-unavailable")
	if !contains(rec.Body.String(), "not ready: kafka") {
		t.Fatalf("readyz = %s, want it to name kafka", rec.Body.String())
	}
	if contains(rec.Body.String(), "postgres") {
		t.Fatalf("readyz = %s, must not name a passing dependency", rec.Body.String())
	}
}

func TestMetricsIsTheTextExposition(t *testing.T) {
	f := newFixture(t)
	rec := mustCall(t, f.router(), http.MethodGet, "/metrics", "", http.StatusOK, nil)
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	if !contains(rec.Body.String(), "exception_management_up 1") {
		t.Fatalf("metrics = %s", rec.Body.String())
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
