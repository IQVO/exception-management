//go:build integration

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The tcpostgres reference keeps the testcontainers postgres module import
// visible in this file (internal/architecture requires it of every Postgres
// integration test file); the database itself comes from pgtest.NewPool.
var _ = tcpostgres.Run

// These tests pin the invariant behind the aborted-transaction fix of the
// sibling services, black-box through the middleware over real Postgres:
//
//   - a mapped 4xx answered over an ABORTED transaction is still recorded and
//     returned (here the middleware rolls the unit of work back and stores
//     the 4xx on its own, so no SAVEPOINT is needed);
//   - a 2xx answered over an aborted transaction is never returned nor cached.

// poisonTx makes a statement fail on the server inside the middleware's
// transaction (a duplicate primary key, 23505), which aborts it, exactly like
// a lost race on a unique index a handler then maps to 409.
func poisonTx(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.From(r.Context())
	if !ok {
		t.Fatal("the middleware did not bind a transaction into the request context")
	}
	const ins = `INSERT INTO idempotency_keys (key, fingerprint, status, content_type, body, created_at) VALUES ('dup', 'f', 200, '', ''::bytea, now())`
	for i := 0; i < 2; i++ {
		if _, err := tx.Exec(r.Context(), ins); err != nil {
			return // the second insert aborts the transaction
		}
	}
	t.Fatal("the duplicate insert did not abort the transaction")
}

func poisonMiddleware(pool *pgxpool.Pool, h http.Handler) http.Handler {
	m := inboundhttp.NewIdempotency(postgres.NewIdempotencyStore(pool), postgres.NewUnitOfWork(pool), system.Clock{})
	return m.Middleware(h)
}

func keyedPost(key string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/exceptions/case-1/assignments", strings.NewReader(`{}`))
	req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	return req
}

func countKey(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIdempotencyAbortedTransaction409IsRecordedAndReplayed(t *testing.T) {
	pool := pgtest.NewPool(t)
	var calls atomic.Int64
	mw := poisonMiddleware(pool, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":409}`))
	}))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedPost("k-poison"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the truthful 409", first.Code, first.Body.String())
	}
	var status int
	if err := pool.QueryRow(t.Context(), `SELECT status FROM idempotency_keys WHERE key = 'k-poison'`).Scan(&status); err != nil {
		t.Fatalf("the 409 outcome was not committed: %v", err)
	}
	if status != http.StatusConflict {
		t.Fatalf("stored status = %d", status)
	}
	if n := countKey(t, pool, "dup"); n != 0 {
		t.Fatalf("the handler's partial write survived (%d rows)", n)
	}

	replay := httptest.NewRecorder()
	mw.ServeHTTP(replay, keyedPost("k-poison"))
	if replay.Code != http.StatusConflict || replay.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "true" {
		t.Fatalf("replay = %d replayed=%q, want the cached 409", replay.Code, replay.Header().Get(inboundhttp.IdempotencyReplayedHeader))
	}
	if replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s, want %s", replay.Body.String(), first.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1 (the replay must not re-run it)", calls.Load())
	}
}

func TestIdempotencyAbortedTransaction2xxIsNeverCached(t *testing.T) {
	pool := pgtest.NewPool(t)
	var calls atomic.Int64
	mw := poisonMiddleware(pool, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			poisonTx(t, r) // only the first attempt dies inside the transaction
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, keyedPost("k-lie"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500 (the writes never happened)", first.Code, first.Body.String())
	}
	if n := countKey(t, pool, "k-lie"); n != 0 {
		t.Fatalf("a failed success left %d idempotency rows: the key could not retry", n)
	}

	second := httptest.NewRecorder()
	mw.ServeHTTP(second, keyedPost("k-lie"))
	if second.Code != http.StatusCreated {
		t.Fatalf("retry = %d %s, want the real work to succeed", second.Code, second.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", calls.Load())
	}
}
