//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// The tcpostgres reference keeps the testcontainers postgres module import
// visible in this file (internal/architecture requires it of every Postgres
// integration test file); the database itself comes from pgtest.NewPool.
var _ = tcpostgres.Run

// fleet audit 2026-11-21 (idempotency in-flight duplicate): the key Lookup
// runs BEFORE the unit of work and the case row lock is taken INSIDE the use
// case (the version-guarded upsert, postgres/case_repository.go). A same-key,
// same-body copy that arrives while the original holds the row passes the
// Lookup, reads the case at the old version, queues on the row lock, and once
// the original commits its upsert matches 0 rows: concurrent-modification,
// RETRYABLE, not stored. The client must get the original's stored 2xx as a
// replay instead: N copies are one run and N-1 replays.

// gatedEvents parks the FIRST PublishCaseEvents call — reached only after the
// use case's Save took the case row lock, inside the open transaction — until
// release is closed, so the original holds the lock while the duplicate runs.
type gatedEvents struct {
	ports.EventPublisher
	once    sync.Once
	parked  chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (g *gatedEvents) PublishCaseEvents(ctx context.Context, events ...exceptioncase.Event) error {
	g.calls.Add(1)
	first := false
	g.once.Do(func() { first = true })
	if first {
		close(g.parked)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.EventPublisher.PublishCaseEvents(ctx, events...)
}

type answer struct {
	status int
	body   string
	header http.Header
}

func TestAnInFlightDuplicateTriageReplaysTheOriginalOverPostgres(t *testing.T) {
	s, gate := bootInflight(t)
	const path = "/exceptions/case-1/triage-decisions"

	originalDone := make(chan answer, 1)
	go func() { originalDone <- s.send(path, triageCritical, "k-inflight") }()
	select {
	case <-gate.parked:
	case <-time.After(30 * time.Second):
		t.Fatal("the original never reached its first PublishCaseEvents (it should hold the case row lock there)")
	}

	dupDone := make(chan answer, 1)
	go func() { dupDone <- s.send(path, triageCritical, "k-inflight") }()
	waitForLockWaiter(t, s.pool)
	close(gate.release)

	first := await(t, originalDone, "original")
	dup := await(t, dupDone, "in-flight duplicate")
	if first.status != http.StatusOK || first.header.Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("original = %d replayed=%q %s, want a fresh 200", first.status, first.header.Get(inboundhttp.IdempotencyReplayedHeader), first.body)
	}
	if dup.status != http.StatusOK || dup.header.Get(inboundhttp.IdempotencyReplayedHeader) != "true" || dup.body != first.body {
		t.Fatalf("in-flight duplicate = %d replayed=%q %s, want the original's 200 replayed byte-identically (%s)",
			dup.status, dup.header.Get(inboundhttp.IdempotencyReplayedHeader), dup.body, first.body)
	}
	if n := countKey(t, s.pool, "k-inflight"); n != 1 {
		t.Fatalf("key rows = %d, want 1", n)
	}
	if v := s.version("case-1"); v != 2 {
		t.Fatalf("case version = %d, want 2 (one triage; the duplicate wrote nothing)", v)
	}
	if n := countOutbox(t, s.pool, "case-1"); n != s.outboxBefore+1 {
		t.Fatalf("outbox rows for case-1 = %d, want %d (one triage fact)", n, s.outboxBefore+1)
	}
}

type inflightService struct {
	*classesService
	outboxBefore int
}

func bootInflight(t *testing.T) (*inflightService, *gatedEvents) {
	t.Helper()
	base := bootClasses(t) // case-1 OPEN at version 1
	uow := postgres.NewUnitOfWork(base.pool)
	clock := system.Clock{}
	cases := postgres.NewCaseRepo(base.pool)
	gate := &gatedEvents{EventPublisher: postgres.NewOutboxPublisher(base.pool), parked: make(chan struct{}), release: make(chan struct{})}
	server := &inboundhttp.Server{
		Get:         &usecases.GetException{Cases: cases},
		Triage:      &usecases.TriageException{Cases: cases, Events: gate, Clock: clock, UnitOfWork: uow},
		Idempotency: inboundhttp.NewIdempotency(postgres.NewIdempotencyStore(base.pool), uow, clock).Middleware,
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(server))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})
	base.srv = srv
	return &inflightService{classesService: base, outboxBefore: countOutbox(t, base.pool, "case-1")}, gate
}

// send is post without t.Fatal, so it may run on a goroutine.
func (s *inflightService) send(path, body, key string) answer {
	req, err := http.NewRequest(http.MethodPost, s.srv.URL+path, strings.NewReader(body))
	if err != nil {
		return answer{status: -1, body: err.Error()}
	}
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return answer{status: -1, body: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	var sb strings.Builder
	buf := make([]byte, 1024)
	for {
		n, rerr := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if rerr != nil {
			break
		}
	}
	return answer{status: resp.StatusCode, body: sb.String(), header: resp.Header}
}

func await(t *testing.T, ch <-chan answer, who string) answer {
	t.Helper()
	select {
	case a := <-ch:
		return a
	case <-time.After(30 * time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		t.Fatalf("the %s never answered", who)
		return answer{}
	}
}

// waitForLockWaiter waits until a backend of this database waits on a lock:
// the duplicate queued on the case row the original holds.
func waitForLockWaiter(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var states []string
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, err := pool.Query(t.Context(), `SELECT coalesce(state,''), coalesce(wait_event_type,''), coalesce(wait_event,''), left(query, 80)
		FROM pg_stat_activity WHERE datname = current_database()`)
	if err == nil {
		for rows.Next() {
			var st, wt, we, q string
			_ = rows.Scan(&st, &wt, &we, &q)
			states = append(states, st+"/"+wt+"/"+we+": "+q)
		}
		rows.Close()
	}
	_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
	t.Fatalf("no backend ever waited on a lock (the duplicate should queue on the case row); pg_stat_activity: %v", states)
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, caseID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE aggreg_id = $1`, caseID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
