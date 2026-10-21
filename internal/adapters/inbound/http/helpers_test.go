package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// The handler fixture: the real use cases over the in-memory doubles, wired
// exactly the way cmd/api wires them over Postgres (minus the outbound
// adapters). Idempotency is left nil unless a test wraps it.

const (
	testSourceType = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	testQueue      = "ps-main"
)

type fixture struct {
	cases *memory.Cases
	pol   *memory.Policies
	ev    *memory.Events
	ids   *memory.IDGen
	now   time.Time
	srv   *Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		cases: memory.NewCases(), pol: memory.NewPolicies(), ev: &memory.Events{},
		ids: memory.NewIDGen(), now: time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC),
	}
	f.srv = &Server{
		List:         &usecases.ListOpenExceptions{Cases: f.cases},
		Get:          &usecases.GetException{Cases: f.cases},
		Triage:       &usecases.TriageException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}},
		Assign:       &usecases.AssignException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}},
		Resolve:      &usecases.ResolveException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now}},
		Declare:      &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}},
		ListPolicies: &usecases.ListExceptionPolicies{Policies: f.pol},
	}
	return f
}

func (f *fixture) router() http.Handler { return NewRouter(f.srv) }

// withIdempotency wraps the command routes with the real middleware over
// the in-memory store (the double of the Postgres wiring in cmd/api).
func (f *fixture) withIdempotency(t *testing.T) (http.Handler, *memory.IdempotencyStore) {
	t.Helper()
	store := memory.NewIdempotencyStore()
	f.srv.Idempotency = NewIdempotency(store, uowFn(func(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }), memory.Clock{T: f.now}).Middleware
	return NewRouter(f.srv), store
}

// uowFn adapts a plain function to ports.UnitOfWork (the unwrapped double).
type uowFn func(ctx context.Context, fn func(context.Context) error) error

func (u uowFn) Do(ctx context.Context, fn func(context.Context) error) error { return u(ctx, fn) }

var _ ports.UnitOfWork = uowFn(nil)

// declarePolicy seeds one policy version through the use case.
func (f *fixture) declarePolicy(t *testing.T, sourceType string, sla int64) {
	t.Helper()
	_, err := f.srv.Declare.Execute(context.Background(), usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: "HIGH", Queue: testQueue, SLAMinutes: sla, Actor: "op-1",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// openCase seeds one case from a consumed fact (opened at f.now + offset)
// and returns its id.
func (f *fixture) openCase(t *testing.T, subject, site string, offset time.Duration) string {
	t.Helper()
	at := f.now.Add(offset)
	uc := &usecases.OpenOrTouchException{Cases: f.cases, Policies: f.pol, Events: f.ev, IDs: f.ids, Clock: memory.Clock{T: at}}
	c, err := uc.Execute(context.Background(), usecases.Fact{
		SourceType: testSourceType, SourceSubject: subject, SiteID: site,
		SourceEventID: "evt-" + subject, Payload: []byte(`{"task_id":"task-8a1f","order_ref":"wu-8a1f"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c.ID()
}

// resolveAt resolves the case through a use case stamped at f.now + offset
// (the operator path, with a real actor).
func (f *fixture) resolveAt(t *testing.T, id string, offset time.Duration) {
	t.Helper()
	uc := &usecases.ResolveException{Cases: f.cases, Events: f.ev, Clock: memory.Clock{T: f.now.Add(offset)}}
	if _, err := uc.Execute(context.Background(), id, "WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", "op-7"); err != nil {
		t.Fatal(err)
	}
}

// call runs one request against handler and returns the recorded response.
// The request is built by hand (not httptest.NewRequest) so targets with
// spaces, NUL bytes and raw percent-encoding reach the router intact.
func call(t *testing.T, handler http.Handler, method, target, body string, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		// A target with raw control characters (the NUL tests) cannot be
		// parsed; carry it as the decoded path, which is what chi matches on.
		u = &url.URL{Path: target}
	}
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req := &http.Request{
		Method: method, URL: u, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{}, Host: "api.example",
		RemoteAddr: "192.0.2.1:1234",
	}
	if bodyReader != nil {
		req.Body = io.NopCloser(bodyReader)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// mustCall requires a status code and returns the recorder.
func mustCall(t *testing.T, handler http.Handler, method, target, body string, want int, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	rec := call(t, handler, method, target, body, header)
	if rec.Code != want {
		t.Fatalf("%s %s = %d %s, want %d", method, target, rec.Code, rec.Body.String(), want)
	}
	return rec
}

// decodeBody unmarshals a recorder's JSON body into dest.
func decodeBody(t *testing.T, rec *httptest.ResponseRecorder, dest any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), dest); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
}

// wantProblem asserts the recorder holds the given problem slug and status.
func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, slug string) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	var p problemDetail
	decodeBody(t, rec, &p)
	if p.Status != status || !strings.HasSuffix(p.Type, slug) {
		t.Fatalf("problem = %+v, want status %d slug %s", p, status, slug)
	}
}

// idemKey is the header set of a well-formed idempotent request.
func idemKey(key string) map[string]string { return map[string]string{IdempotencyKeyHeader: key} }

// Shared test vocabulary.
var bgCtx = context.Background()

// mustAutoResolve auto-resolves the stored case through the domain (the
// correlation path) so a test can read it back over HTTP.
func mustAutoResolve(t *testing.T, f *fixture, id string) *exceptioncase.Case {
	t.Helper()
	c, err := f.cases.FindByID(bgCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AutoResolve("TaskCPTMissed->OrderRepromised",
		"com.warehouse.wes.order-management.order.OrderRepromised", "evt-corr", f.now); err != nil {
		t.Fatal(err)
	}
	return c
}
