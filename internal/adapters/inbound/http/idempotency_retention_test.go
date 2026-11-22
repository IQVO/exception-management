package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// The 24-hour replay window of repository.IdempotencyTTL through the real
// middleware over the in-memory double (the Postgres twin lives in cmd/api,
// TestAnExpiredIdempotencyKeyExecutesAgainOverPostgres): an answer past the
// window never replays, the key is free again, and the new answer is the one
// that replays while it stays live (ADR 0015, addendum 2026-11-22).

// retentionRig is the middleware over a clock-driven store: advancing the
// clock ages every stored answer.
type retentionRig struct {
	store *memory.IdempotencyStore
	clock *movingClock
	h     *scriptedHandler
	mw    http.Handler
}

func newRetentionRig(script ...answerFn) *retentionRig {
	clock := &movingClock{t: time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)}
	r := &retentionRig{
		store: memory.NewIdempotencyStoreClock(clock),
		clock: clock, h: &scriptedHandler{script: script},
	}
	m := NewIdempotency(r.store, uowFn(func(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }), clock)
	r.mw = m.Middleware(r.h)
	return r
}

func (r *retentionRig) post(t *testing.T, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/exceptions/case-1/triage-decisions", strings.NewReader(body))
	req.Header.Set(IdempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	r.mw.ServeHTTP(rec, req)
	return rec
}

func TestAnAnswerPastTheTTLReleasesTheKey(t *testing.T) {
	r := newRetentionRig(triaged)
	if first := r.post(t, "k", `{"n":1}`); first.Code != http.StatusOK || isReplay(first) {
		t.Fatalf("first = %d replayed=%v, want a fresh 200", first.Code, isReplay(first))
	}
	if r.h.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", r.h.calls)
	}

	// A microsecond short of the window: the stored answer still replays.
	r.clock.advance(repository.IdempotencyTTL - time.Microsecond)
	if again := r.post(t, "k", `{"n":1}`); again.Code != http.StatusOK || !isReplay(again) {
		t.Fatalf("a microsecond short of the TTL = %d replayed=%v, want the stored 200 replayed", again.Code, isReplay(again))
	}

	// Past the window: the key is free. The same request executes again...
	r.clock.advance(2 * time.Microsecond)
	if fresh := r.post(t, "k", `{"n":1}`); fresh.Code != http.StatusOK || isReplay(fresh) {
		t.Fatalf("past the TTL = %d replayed=%v, want a FRESH execution", fresh.Code, isReplay(fresh))
	}
	// ...and the refreshed answer is live: an exact repeat replays it, while
	// a different body under it is 409 again — until IT expires too.
	if again := r.post(t, "k", `{"n":1}`); again.Code != http.StatusOK || !isReplay(again) {
		t.Fatalf("repeat of the refreshed answer = %d replayed=%v, want the stored 200 replayed", again.Code, isReplay(again))
	}
	r.clock.advance(repository.IdempotencyTTL + time.Microsecond)
	if other := r.post(t, "k", `{"n":2}`); other.Code != http.StatusOK || isReplay(other) {
		t.Fatalf("another body after the refreshed answer expired = %d replayed=%v %s, want a fresh execution, not 409 idempotency-key-reused",
			other.Code, isReplay(other), other.Body)
	}
	if r.h.calls != 3 {
		t.Fatalf("handler calls = %d, want 3 (one per execution)", r.h.calls)
	}
	if n := r.store.Stored(); n != 1 {
		t.Fatalf("stored = %d, want 1 (the expired answer was replaced)", n)
	}
}
