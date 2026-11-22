//go:build integration

package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
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
