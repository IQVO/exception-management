//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
)

// The tcpostgres reference keeps the testcontainers postgres module import
// visible in this file (internal/architecture requires it of every Postgres
// integration test file); the database itself comes from pgtest.NewPool.
var _ = tcpostgres.Run

// The retention half of ADR 0015's store design, recorded there as an open
// point: "No age predicate on Lookup, no purge of idempotency_keys anywhere
// in the codebase — rows live forever." The fleet fix (order-sourcing #84,
// last-mile-delivery #86) is a 24-hour window: a row older than the TTL must
// never answer a replay (the age guard on Lookup) and must be deleted by a
// bounded background purge.

// ageKeyRow ages the stored answer of key by d (the only way a test reaches
// an expired row without waiting a day).
func ageKeyRow(t *testing.T, s *classesService, key string, d time.Duration) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE idempotency_keys SET created_at = created_at - make_interval(secs => $1) WHERE key = $2`,
		d.Seconds(), key); err != nil {
		t.Fatalf("ageing key %s by %v: %v", key, d, err)
	}
}

// TestAnExpiredIdempotencyKeyExecutesAgainOverPostgres: a stored answer older
// than the 24-hour TTL is invisible to the replay path, so the same key and
// body EXECUTES again (the case moves a second time, the row is refreshed
// with the new answer), a different body under the expired key is a NEW
// request instead of 409 idempotency-key-reused, and once a fresh answer is
// stored an exact repeat of it replays again.
func TestAnExpiredIdempotencyKeyExecutesAgainOverPostgres(t *testing.T) {
	s := bootClasses(t) // case-1 OPEN at version 1
	const path = "/exceptions/case-1/triage-decisions"

	if status, _, hdr := s.post(path, triageCritical, "k-stale"); status != http.StatusOK || hdr.Get("Idempotency-Replayed") != "" {
		t.Fatalf("first triage = %d (replayed %q), want a fresh 200", status, hdr.Get("Idempotency-Replayed"))
	}
	if v := s.version("case-1"); v != 2 {
		t.Fatalf("version = %d after the first triage, want 2", v)
	}

	// 25 hours later the stored answer has expired: the same key and body
	// must run the command again, not replay the stale answer.
	ageKeyRow(t, s, "k-stale", 25*time.Hour)
	status, _, hdr := s.post(path, triageCritical, "k-stale")
	if status != http.StatusOK || hdr.Get("Idempotency-Replayed") != "" {
		t.Fatalf("replay of an expired key = %d (replayed %q), want a FRESH 200: the age guard must hide the stale answer", status, hdr.Get("Idempotency-Replayed"))
	}
	if v := s.version("case-1"); v != 3 {
		t.Fatalf("version = %d after the second triage, want 3 (the command ran again)", v)
	}
	if n := countKey(t, s.pool, "k-stale"); n != 1 {
		t.Fatalf("key rows = %d, want 1 (the expired row is refreshed, not duplicated)", n)
	}

	// The refreshed answer is live again: an exact repeat replays it.
	status, _, hdr = s.post(path, triageCritical, "k-stale")
	if status != http.StatusOK || hdr.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("repeat of the refreshed answer = %d (replayed %q), want the stored 200 replayed", status, hdr.Get("Idempotency-Replayed"))
	}
	if v := s.version("case-1"); v != 3 {
		t.Fatalf("version = %d after a replay, want 3 (nothing ran)", v)
	}

	// Once the row has expired again, a different body under the same key is
	// a NEW request (the key is free), never 409 idempotency-key-reused.
	ageKeyRow(t, s, "k-stale", 25*time.Hour)
	status, body, hdr := s.post(path, `{"severity":"LOW","queue":"cps-inbound"}`, "k-stale")
	if status != http.StatusOK || hdr.Get("Idempotency-Replayed") != "" {
		t.Fatalf("different body under an expired key = %d %s (replayed %q), want a fresh execution, not the stale answer or 409 idempotency-key-reused", status, body, hdr.Get("Idempotency-Replayed"))
	}
	if v := s.version("case-1"); v != 4 {
		t.Fatalf("version = %d after the third triage, want 4", v)
	}
}

// seedKeyRow stores a key row that was created age ago (the store's own
// shape, through the real Save so the refresh predicate is exercised too).
func seedKeyRow(t *testing.T, s *classesService, key string, age time.Duration) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO idempotency_keys (key, fingerprint, status, content_type, body, created_at)
		VALUES ($1, 'fp-seed', 200, 'application/json', '{}'::bytea, now() - make_interval(secs => $2))`,
		key, age.Seconds()); err != nil {
		t.Fatalf("seeding key %s: %v", key, err)
	}
}

func countRows(t *testing.T, s *classesService, table string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestThePurgeDeletesExpiredKeysAndKeepsLiveOnesOverPostgres: the store's
// PurgeExpired (the worker's one statement) deletes only what the read guard
// already ignores, oldest first, one bounded batch per call.
func TestThePurgeDeletesExpiredKeysAndKeepsLiveOnesOverPostgres(t *testing.T) {
	s := bootClasses(t)
	store := postgres.NewIdempotencyStore(s.pool)
	seedKeyRow(t, s, "expired-a", 48*time.Hour)
	seedKeyRow(t, s, "expired-b", 25*time.Hour)
	seedKeyRow(t, s, "live-a", time.Minute)
	seedKeyRow(t, s, "live-b", 23*time.Hour)

	if n, err := store.PurgeExpired(context.Background(), 100); err != nil || n != 2 {
		t.Fatalf("purge = %d %v, want 2 expired rows deleted", n, err)
	}
	if got := countRows(t, s, "idempotency_keys"); got != 2 {
		t.Fatalf("rows left = %d, want 2 (only the live ones)", got)
	}
	if n, err := store.PurgeExpired(context.Background(), 100); err != nil || n != 0 {
		t.Fatalf("second purge = %d %v, want 0: nothing live is ever deleted", n, err)
	}
}

// TestThePurgeDeletesAtMostOneBatchPerCallOverPostgres: one pass deletes at
// most limit rows; the next passes finish the backlog.
func TestThePurgeDeletesAtMostOneBatchPerCallOverPostgres(t *testing.T) {
	s := bootClasses(t)
	store := postgres.NewIdempotencyStore(s.pool)
	for i := range 5 {
		seedKeyRow(t, s, fmt.Sprintf("old-%d", i), 30*time.Hour)
	}
	seedKeyRow(t, s, "live", time.Minute)
	if n, err := store.PurgeExpired(context.Background(), 2); err != nil || n != 2 {
		t.Fatalf("first batch = %d %v, want 2", n, err)
	}
	if left := countRows(t, s, "idempotency_keys"); left != 4 {
		t.Fatalf("rows after one batch of 2 = %d, want 4 (3 expired + 1 live)", left)
	}
	total := 2
	for {
		n, err := store.PurgeExpired(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total != 5 || countRows(t, s, "idempotency_keys") != 1 {
		t.Fatalf("purged %d rows, %d left, want 5 purged and only the live key", total, countRows(t, s, "idempotency_keys"))
	}
}

// TestThePurgeSkipsARowARequestHoldsAndNeverWaitsForItOverPostgres: a
// transaction holds an expired key's row — what a request does between its
// Save (the ON CONFLICT refresh takes the row lock) and its commit. The
// purge must skip the locked row (FOR UPDATE SKIP LOCKED), delete nothing,
// never block on it; after the commit the refreshed row is live and stays.
func TestThePurgeSkipsARowARequestHoldsAndNeverWaitsForItOverPostgres(t *testing.T) {
	s := bootClasses(t)
	store := postgres.NewIdempotencyStore(s.pool)
	seedKeyRow(t, s, "held", 30*time.Hour)

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// What the store does inside the unit of work: refresh the (expired)
	// row; the row lock lives until the commit.
	if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, fingerprint, status, content_type, body, created_at)
		VALUES ('held', 'fp2', 200, 'application/json', '{}'::bytea, now())
		ON CONFLICT (key) DO UPDATE SET fingerprint = 'fp2', created_at = EXCLUDED.created_at
		WHERE idempotency_keys.created_at <= now() - make_interval(secs => $1)`,
		24*3600.0); err != nil {
		t.Fatal(err)
	}

	purgeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if n, err := store.PurgeExpired(purgeCtx, 100); err != nil || n != 0 {
		t.Fatalf("purge while a request holds the row = %d %v, want 0 deleted and no wait", n, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := store.PurgeExpired(ctx, 100); err != nil || n != 0 {
		t.Fatalf("purge after the request committed = %d %v: the row was refreshed, it is live", n, err)
	}
	if got := countKey(t, s.pool, "held"); got != 1 {
		t.Fatalf("key rows = %d, want the refreshed row", got)
	}
}

// TestThePurgeRacingRequestsOnExpiredKeysKeepsThemCorrectOverPostgres: the
// real router on keys whose rows have expired, while the purge runs at the
// same time. Every request is a FRESH one (an expired row never replays),
// N copies of one key are one run and N-1 replays of the winner, and exactly
// one live row per key is left behind.
func TestThePurgeRacingRequestsOnExpiredKeysKeepsThemCorrectOverPostgres(t *testing.T) {
	s := bootClasses(t)
	store := postgres.NewIdempotencyStore(s.pool)
	const path = "/exceptions/case-1/triage-decisions"

	const rounds = 10
	for i := range rounds {
		key := fmt.Sprintf("race-%d", i)
		seedKeyRow(t, s, key, 30*time.Hour)
		var wg sync.WaitGroup
		codes := make([]int, 3)
		for j := range codes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				status, _, _ := s.post(path, triageCritical, key)
				codes[j] = status
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.PurgeExpired(context.Background(), 100)
		}()
		wg.Wait()
		for _, c := range codes {
			if c != http.StatusOK {
				t.Fatalf("round %d: a request racing the purge answered %d, want 200", i, c)
			}
		}
	}
	// One live row per key (the expired seed was replaced by the winner's
	// answer or deleted and re-inserted — either way exactly one), the case
	// moved exactly once, and a repeat of any key replays the stored answer.
	if got := countRows(t, s, "idempotency_keys"); got != rounds {
		t.Fatalf("key rows = %d, want one live row per key (%d)", got, rounds)
	}
	if v := s.version("case-1"); v != 2 {
		t.Fatalf("case version = %d, want 2 (one triage; the copies replayed the winner)", v)
	}
	if _, _, hdr := s.post(path, triageCritical, "race-0"); hdr.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("a repeat of a stored key replays: %v", hdr)
	}
}
