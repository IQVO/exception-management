package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// IdempotencyStore is the in-memory ports.IdempotencyStore: the answers of
// POSTs and PUTs made with an Idempotency-Key (the double of the
// idempotency_keys table), with the same 24-hour retention the Postgres
// store enforces — an answer older than repository.IdempotencyTTL is not
// replayed, a Save past the window replaces the expired answer, and
// PurgeExpired deletes what the read already ignores (ADR 0015, addendum
// 2026-11-22). One mutex serialises every call, the double of the row locks
// and the SKIP LOCKED of the real store.
type IdempotencyStore struct {
	mu   sync.Mutex
	rows map[string]repository.IdempotencyRecord
	now  func() time.Time
}

// NewIdempotencyStore constructs an empty IdempotencyStore on the wall
// clock: the retention is measured against time.Now, so a record stamped
// "now" is live for the next repository.IdempotencyTTL.
func NewIdempotencyStore() *IdempotencyStore {
	return newIdempotencyStore(time.Now)
}

// NewIdempotencyStoreClock constructs an empty IdempotencyStore measured
// against clock — the middleware's own clock, so a test that advances it
// advances the replay window with it.
func NewIdempotencyStoreClock(clock ports.Clock) *IdempotencyStore {
	return newIdempotencyStore(clock.Now)
}

func newIdempotencyStore(now func() time.Time) *IdempotencyStore {
	return &IdempotencyStore{rows: map[string]repository.IdempotencyRecord{}, now: now}
}

// live reports whether rec is still inside the replay window at t.
func live(rec repository.IdempotencyRecord, t time.Time) bool {
	return t.Sub(rec.CreatedAt) < repository.IdempotencyTTL
}

// Lookup implements ports.IdempotencyStore: the stored answer while it is
// younger than repository.IdempotencyTTL, not-found once it has expired
// (the age guard: an expired answer never replays, between expiry and the
// sweep like after it).
func (s *IdempotencyStore) Lookup(_ context.Context, key string) (repository.IdempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.rows[key]
	if !ok || !live(rec, s.now()) {
		return repository.IdempotencyRecord{}, false, nil
	}
	return rec, true, nil
}

// Save implements ports.IdempotencyStore; the first writer of a LIVE key
// wins, and the answer of a key whose previous answer has expired replaces
// it (the key is free again).
func (s *IdempotencyStore) Save(_ context.Context, rec repository.IdempotencyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, exists := s.rows[rec.Key]; !exists || !live(cur, s.now()) {
		s.rows[rec.Key] = rec
	}
	return nil
}

// PurgeExpired implements ports.IdempotencyStore: it deletes at most limit
// answers that are no longer replayed (age >= repository.IdempotencyTTL),
// oldest first, and returns how many. One mutex covers Lookup and Save too,
// so a purge never touches an answer a caller is storing under.
func (s *IdempotencyStore) PurgeExpired(_ context.Context, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		return 0, nil
	}
	var expired []string
	for key, rec := range s.rows {
		if !live(rec, s.now()) {
			expired = append(expired, key)
		}
	}
	sort.Slice(expired, func(i, j int) bool { return s.rows[expired[i]].CreatedAt.Before(s.rows[expired[j]].CreatedAt) })
	if len(expired) > limit {
		expired = expired[:limit]
	}
	for _, key := range expired {
		delete(s.rows, key)
	}
	return len(expired), nil
}

// Stored returns how many answers are stored (assertions in tests).
func (s *IdempotencyStore) Stored() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}
