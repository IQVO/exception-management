//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
	"github.com/claudioed/exception-management/internal/testing/repocontract"
)

// The Idempotency-Key middleware over the REAL router and REAL Postgres: the
// key row, the aggregate change and the outbox rows commit together, a replay
// is byte-identical and marked, a 4xx is stored, a 5xx never is.

// outboxDown fails the case events while armed, AFTER the aggregate was saved
// in the same unit of work: the 5xx the middleware must roll back and never
// store.
type outboxDown struct {
	ports.EventPublisher
	armed atomic.Bool
}

func (o *outboxDown) PublishCaseEvents(ctx context.Context, evs ...exceptioncase.Event) error {
	if o.armed.Load() {
		return errors.New("outbox down")
	}
	return o.EventPublisher.PublishCaseEvents(ctx, evs...)
}

// idemService is the real router over a fresh Postgres, with one OPEN case.
type idemService struct {
	t     *testing.T
	srv   *httptest.Server
	down  *outboxDown
	count func(table string) int
}

func bootIdem(t *testing.T) *idemService {
	t.Helper()
	pool := pgtest.NewPool(t)
	server := buildServer(pool, postgres.NewUnitOfWork(pool), system.Clock{}, nil)
	down := &outboxDown{EventPublisher: server.Assign.Events}
	server.Assign.Events = down
	srv := httptest.NewServer(inboundhttp.NewRouter(server))
	t.Cleanup(srv.Close)

	repo := postgres.NewCaseRepo(pool)
	if err := repo.Save(context.Background(), repocontract.Opened(t, "case-1", "ord-1-line-1", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	return &idemService{t: t, srv: srv, down: down, count: func(table string) int { return pgtest.Count(t, pool, table) }}
}

// post sends one POST with an optional key and returns status, body, headers.
func (s *idemService) post(path, body, key string) (int, string, http.Header) {
	s.t.Helper()
	req, err := http.NewRequest(http.MethodPost, s.srv.URL+path, strings.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), resp.Header
}

func (s *idemService) version() int64 {
	s.t.Helper()
	resp, err := http.Get(s.srv.URL + "/exceptions/case-1")
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var c struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		s.t.Fatal(err)
	}
	return c.Version
}

const triageBody = `{"severity":"CRITICAL","queue":"cps-inbound"}`

// TestPostgresPostsAreKeyedReplayedAndConflict: missing key 400, a fresh key
// runs once, the replay is byte-identical with Idempotency-Replayed: true, the
// key cannot be reused on another body or another route, and POST /exceptions
// (the permanent 405) is keyed like every POST.
func TestPostgresPostsAreKeyedReplayedAndConflict(t *testing.T) {
	s := bootIdem(t)
	for _, path := range []string{"/exceptions", "/exceptions/case-1/triage-decisions", "/exceptions/case-1/assignments", "/exceptions/case-1/resolutions"} {
		if status, body, _ := s.post(path, triageBody, ""); status != http.StatusBadRequest || !strings.Contains(body, "/idempotency-key-required") {
			t.Fatalf("POST %s without a key = %d %s, want 400 idempotency-key-required", path, status, body)
		}
	}
	if n := s.count("idempotency_keys"); n != 0 {
		t.Fatalf("idempotency rows = %d after keyless requests, want 0", n)
	}

	status, body, first := s.post("/exceptions/case-1/triage-decisions", triageBody, "k-triage")
	if status != http.StatusOK || first.Get("Idempotency-Replayed") != "" {
		t.Fatalf("fresh triage = %d %s (headers %v), want 200 without Idempotency-Replayed", status, body, first)
	}
	status2, body2, again := s.post("/exceptions/case-1/triage-decisions", triageBody, "k-triage")
	if status2 != status || body2 != body || again.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay = %d %s (headers %v), want the identical answer with Idempotency-Replayed: true", status2, body2, again)
	}
	if v := s.version(); v != 2 {
		t.Fatalf("case version = %d after a replayed triage, want 2 (one transition)", v)
	}
	if status, body, _ := s.post("/exceptions/case-1/triage-decisions", `{"severity":"LOW","queue":"cps-inbound"}`, "k-triage"); status != http.StatusConflict || !strings.Contains(body, "/idempotency-key-reused") {
		t.Fatalf("same key, other body = %d %s, want 409 idempotency-key-reused", status, body)
	}
	if status, body, _ := s.post("/exceptions/case-1/assignments", `{"assignee":"op-a"}`, "k-triage"); status != http.StatusConflict || !strings.Contains(body, "/idempotency-key-reused") {
		t.Fatalf("same key, other route = %d %s, want 409 idempotency-key-reused", status, body)
	}

	// POST /exceptions: the keyed, stored, replayed 405.
	status, body, first = s.post("/exceptions", `{}`, "k-create")
	if status != http.StatusMethodNotAllowed || first.Get("Idempotency-Replayed") != "" {
		t.Fatalf("POST /exceptions = %d %s (headers %v), want the 405 as a first execution", status, body, first)
	}
	status2, body2, again = s.post("/exceptions", `{}`, "k-create")
	if status2 != status || body2 != body || again.Get("Idempotency-Replayed") != "true" || again.Get("Allow") != "GET" {
		t.Fatalf("POST /exceptions replay = %d %s (headers %v)", status2, body2, again)
	}
	if n := s.count("idempotency_keys"); n != 2 {
		t.Fatalf("idempotency rows = %d, want 2 (one per logical submit)", n)
	}
}

// TestPostgresA4xxIsStoredAndReplayed: a rejection is a deterministic answer
// and replays; it leaves no effect and no outbox row behind.
func TestPostgresA4xxIsStoredAndReplayed(t *testing.T) {
	s := bootIdem(t)
	const resolve = `{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`
	if status, body, _ := s.post("/exceptions/case-1/resolutions", resolve, "k-ok"); status != http.StatusOK {
		t.Fatalf("resolve = %d %s", status, body)
	}
	outbox := s.count("outbox")
	status, body, first := s.post("/exceptions/case-1/resolutions", resolve, "k-409")
	if status != http.StatusConflict || !strings.Contains(body, "/case-already-resolved") || first.Get("Idempotency-Replayed") != "" {
		t.Fatalf("second resolve = %d %s (headers %v), want 409 case-already-resolved", status, body, first)
	}
	status2, body2, again := s.post("/exceptions/case-1/resolutions", resolve, "k-409")
	if status2 != status || body2 != body || again.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replayed 409 = %d %s (headers %v)", status2, body2, again)
	}
	if n := s.count("outbox"); n != outbox {
		t.Fatalf("outbox rows = %d after a rejection, want %d", n, outbox)
	}
}

// TestPostgresConcurrentSameKeyRunsOnce: N concurrent requests with one key
// produce exactly one transition, one key row and one outbox row; the losers
// replay the winner's answer.
func TestPostgresConcurrentSameKeyRunsOnce(t *testing.T) {
	s := bootIdem(t)
	outbox := s.count("outbox")
	const n = 12
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		bodies  = map[string]int{}
		fresh   int
		failure []string
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body, hdr := s.post("/exceptions/case-1/assignments", `{"assignee":"op-a"}`, "k-race")
			mu.Lock()
			defer mu.Unlock()
			if status != http.StatusOK {
				failure = append(failure, body)
			}
			bodies[body]++
			if hdr.Get("Idempotency-Replayed") == "" {
				fresh++
			}
		}()
	}
	wg.Wait()
	if len(failure) > 0 {
		t.Fatalf("concurrent same-key requests failed: %v", failure)
	}
	if len(bodies) != 1 || fresh != 1 {
		t.Fatalf("%d distinct bodies and %d first executions, want 1 and 1 (losers replay the winner)", len(bodies), fresh)
	}
	if v := s.version(); v != 2 {
		t.Fatalf("case version = %d, want 2 (assigned exactly once)", v)
	}
	if got := s.count("idempotency_keys"); got != 1 {
		t.Fatalf("idempotency rows = %d, want 1", got)
	}
	if got := s.count("outbox"); got != outbox+1 {
		t.Fatalf("outbox rows = %d, want %d (one ExceptionAssigned)", got, outbox+1)
	}
}

// TestPostgresA5xxIsRolledBackAndNeverStored: a failure after the aggregate
// was saved rolls the whole unit of work back, stores nothing, and the same
// key then runs the real work.
func TestPostgresA5xxIsRolledBackAndNeverStored(t *testing.T) {
	s := bootIdem(t)
	outbox := s.count("outbox")
	s.down.armed.Store(true)
	status, body, hdr := s.post("/exceptions/case-1/assignments", `{"assignee":"op-a"}`, "k-5xx")
	if status != http.StatusInternalServerError || !strings.Contains(body, "/internal-error") || hdr.Get("Idempotency-Replayed") != "" {
		t.Fatalf("assign while the outbox is down = %d %s (headers %v), want 500 internal-error", status, body, hdr)
	}
	if v := s.version(); v != 1 {
		t.Fatalf("case version = %d after a 5xx, want 1 (rolled back)", v)
	}
	if n := s.count("idempotency_keys"); n != 0 {
		t.Fatalf("idempotency rows = %d after a 5xx, want none stored", n)
	}
	if n := s.count("outbox"); n != outbox {
		t.Fatalf("outbox rows = %d after a 5xx, want %d", n, outbox)
	}

	s.down.armed.Store(false)
	status, body, hdr = s.post("/exceptions/case-1/assignments", `{"assignee":"op-a"}`, "k-5xx")
	if status != http.StatusOK || hdr.Get("Idempotency-Replayed") != "" {
		t.Fatalf("retry with the same key = %d %s (headers %v), want the work to run (200, not a replay)", status, body, hdr)
	}
	if v := s.version(); v != 2 {
		t.Fatalf("case version = %d after the retry, want 2", v)
	}
}

// TestPostgresRetiredRoutesAreGone: the verb sub-paths are gone from the real
// router and never reach the middleware.
func TestPostgresRetiredRoutesAreGone(t *testing.T) {
	s := bootIdem(t)
	for _, path := range []string{"/exceptions/case-1/triage", "/exceptions/case-1/assign", "/exceptions/case-1/resolve"} {
		status, body, _ := s.post(path, triageBody, "k-retired")
		if status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s = %d %s, want 404 or 405", path, status, body)
		}
	}
	if n := s.count("idempotency_keys"); n != 0 {
		t.Fatalf("idempotency rows = %d: a retired route reached the middleware", n)
	}
}
