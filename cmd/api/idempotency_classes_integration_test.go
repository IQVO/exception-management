//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
	"github.com/claudioed/exception-management/internal/testing/repocontract"
)

// The fleet replay-class rule over the REAL router and REAL Postgres: a
// RETRYABLE refusal is not stored. The loser of an optimistic-lock race
// (409 concurrent-modification) leaves no key row, so its retry under the
// same key executes again and is then stored and replayed; a 404 on a case
// whose opening fact has not been consumed yet (case-not-found) releases
// the key the same way (ADR 0015).

// classesService is the real API over a fresh testcontainers Postgres, with
// one OPEN case (case-1, version 1) and the triage command reading through
// stale (the version-bumping repo decorator below).
type classesService struct {
	t     *testing.T
	srv   *httptest.Server
	pool  *pgxpool.Pool
	stale *staleVersionCases
}

func bootClasses(t *testing.T) *classesService {
	t.Helper()
	pool := pgtest.NewPool(t)
	uow := postgres.NewUnitOfWork(pool)
	clock := system.Clock{}
	cases := postgres.NewCaseRepo(pool)
	events := postgres.NewOutboxPublisher(pool)
	stale := &staleVersionCases{ExceptionCaseRepo: cases, pool: pool}
	server := &inboundhttp.Server{
		Get:         &usecases.GetException{Cases: cases},
		Triage:      &usecases.TriageException{Cases: stale, Events: events, Clock: clock, UnitOfWork: uow},
		Idempotency: inboundhttp.NewIdempotency(postgres.NewIdempotencyStore(pool), uow, clock).Middleware,
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(server))
	t.Cleanup(srv.Close)

	if err := cases.Save(context.Background(), repocontract.Opened(t, "case-1", "ord-1-line-1", time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	return &classesService{t: t, srv: srv, pool: pool, stale: stale}
}

// post sends one POST with an Idempotency-Key and returns status, body and
// headers.
func (s *classesService) post(path, body, key string) (int, string, http.Header) {
	s.t.Helper()
	req, err := http.NewRequest(http.MethodPost, s.srv.URL+path, strings.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw := make([]byte, 0, 1024)
	buf := make([]byte, 1024)
	for {
		n, err := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if err != nil {
			break
		}
	}
	return resp.StatusCode, string(raw), resp.Header
}

// version reports the case's committed version through the real GET.
func (s *classesService) version(id string) int64 {
	s.t.Helper()
	resp, err := http.Get(s.srv.URL + "/exceptions/" + id)
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

// staleVersionCases wraps the case repo: while armed, the first FindByID
// returns the case it loaded and THEN bumps the stored version on a second
// connection (context.Background carries no transaction), so the caller
// saves a stale version and the use case answers the REAL
// ErrConcurrentModification — the committed competing write of another
// request, made deterministic.
type staleVersionCases struct {
	ports.ExceptionCaseRepo
	pool  *pgxpool.Pool
	armed atomic.Bool
	fired atomic.Bool
}

func (v *staleVersionCases) FindByID(ctx context.Context, id string) (*exceptioncase.Case, error) {
	c, err := v.ExceptionCaseRepo.FindByID(ctx, id)
	if err != nil || !v.armed.Load() || v.fired.Swap(true) {
		return c, err
	}
	if _, err := v.pool.Exec(context.Background(),
		`UPDATE exception_cases SET version = version + 1 WHERE case_id = $1`, id); err != nil {
		return nil, err
	}
	return c, nil
}

const triageCritical = `{"severity":"CRITICAL","queue":"cps-inbound"}`

// TestPostgresConcurrentModificationRefusalIsNotStored: the loser of the
// optimistic-lock race is answered 409 concurrent-modification, nothing is
// stored under its key, its retry under the same key executes the real work
// and is then stored and replayed.
func TestPostgresConcurrentModificationRefusalIsNotStored(t *testing.T) {
	s := bootClasses(t)
	s.stale.armed.Store(true)

	status, body, hdr := s.post("/exceptions/case-1/triage-decisions", triageCritical, "k-cm")
	if status != http.StatusConflict || !strings.Contains(body, "/concurrent-modification") {
		t.Fatalf("the stale save = %d %s, want 409 concurrent-modification", status, body)
	}
	if hdr.Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("the first answer carries %s: the refusal was replayed, not answered", inboundhttp.IdempotencyReplayedHeader)
	}
	if n := countKey(t, s.pool, "k-cm"); n != 0 {
		t.Fatalf("key rows after the concurrent-modification refusal = %d, want 0: a RETRYABLE refusal releases the key", n)
	}

	// The competing write is committed; the retry under the SAME key runs.
	s.stale.armed.Store(false)
	status, body, hdr = s.post("/exceptions/case-1/triage-decisions", triageCritical, "k-cm")
	if status != http.StatusOK || hdr.Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry under the released key = %d %s (replayed=%q), want the work to run (200, no replay header)",
			status, body, hdr.Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if v := s.version("case-1"); v != 3 {
		t.Fatalf("case version = %d, want 3 (one competing bump, one triage)", v)
	}
	if n := countKey(t, s.pool, "k-cm"); n != 1 {
		t.Fatalf("key rows after the retry = %d, want 1: the success is stored", n)
	}

	// The stored success replays; the handler does not run again.
	status, _, hdr = s.post("/exceptions/case-1/triage-decisions", triageCritical, "k-cm")
	if status != http.StatusOK || hdr.Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("the retry's answer must replay: %d (replayed=%q)", status, hdr.Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if v := s.version("case-1"); v != 3 {
		t.Fatalf("case version = %d after the replay, want 3 (a replay records nothing new)", v)
	}
}

// TestPostgresCaseNotFoundRefusalIsNotStored: a keyed command on a case
// whose opening fact has not been consumed yet answers 404 case-not-found
// and stores nothing, so the same key retries (the fact may land any
// moment) and a different body under the released key is a NEW request, not
// a reuse conflict.
func TestPostgresCaseNotFoundRefusalIsNotStored(t *testing.T) {
	s := bootClasses(t)
	const path = "/exceptions/case-factnotconsumedyet/triage-decisions"

	status, body, hdr := s.post(path, triageCritical, "k-404")
	if status != http.StatusNotFound || !strings.Contains(body, "/case-not-found") {
		t.Fatalf("triage of an unknown case = %d %s, want 404 case-not-found", status, body)
	}
	if hdr.Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("the first answer carries %s", inboundhttp.IdempotencyReplayedHeader)
	}
	if n := countKey(t, s.pool, "k-404"); n != 0 {
		t.Fatalf("key rows after the 404 = %d, want 0: the fact that opens the case may arrive yet (RETRYABLE)", n)
	}

	// The same key and body executes again (the fact has not landed: a fresh 404).
	status, body, hdr = s.post(path, triageCritical, "k-404")
	if status != http.StatusNotFound || hdr.Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry under the released key = %d %s (replayed=%q), want a fresh 404",
			status, body, hdr.Get(inboundhttp.IdempotencyReplayedHeader))
	}

	// A different body under the released key is a NEW request, not a reuse.
	status, body, _ = s.post(path, `{"severity":"LOW","queue":"cps-inbound"}`, "k-404")
	if status != http.StatusNotFound || strings.Contains(body, "/idempotency-key-reused") {
		t.Fatalf("a different body under the released key = %d %s, want a new request (404, not the reuse conflict)", status, body)
	}
}
