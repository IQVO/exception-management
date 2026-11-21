package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

var inflightT0 = time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

// blindStore stands for the key Lookup an in-flight duplicate made BEFORE the
// original committed: while blind > 0 a Lookup reports "not found" (and
// counts down); every later one reads the store. failAfter > 0 makes every
// Lookup after the first failAfter ones fail, the way a database that dropped
// between the request and the refusal's re-read does.
type blindStore struct {
	ports.IdempotencyStore
	mu        sync.Mutex
	blind     int
	failAfter int
	lookups   int
}

func (s *blindStore) Lookup(ctx context.Context, key string) (repository.IdempotencyRecord, bool, error) {
	s.mu.Lock()
	s.lookups++
	n, blind := s.lookups, s.blind > 0
	if blind {
		s.blind--
	}
	s.mu.Unlock()
	if s.failAfter > 0 && n > s.failAfter {
		return repository.IdempotencyRecord{}, false, errors.New("idempotency store unreachable")
	}
	if blind {
		return repository.IdempotencyRecord{}, false, nil
	}
	return s.IdempotencyStore.Lookup(ctx, key)
}

// movingClock is a ports.Clock the test advances.
type movingClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *movingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *movingClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type answerFn func(w http.ResponseWriter, r *http.Request)

// scriptedHandler answers the n-th call with script[n] (the last entry
// repeats) and counts the calls.
type scriptedHandler struct {
	mu     sync.Mutex
	script []answerFn
	calls  int
}

func (h *scriptedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	i := min(h.calls, len(h.script)-1)
	h.calls++
	fn := h.script[i]
	h.mu.Unlock()
	fn(w, r)
}

type inflightRig struct {
	store *blindStore
	clock *movingClock
	h     *scriptedHandler
	mw    http.Handler
	logs  *bytes.Buffer
}

func newInflightRig(script ...answerFn) *inflightRig {
	r := &inflightRig{
		store: &blindStore{IdempotencyStore: memory.NewIdempotencyStore()},
		clock: &movingClock{t: inflightT0}, h: &scriptedHandler{script: script}, logs: &bytes.Buffer{},
	}
	m := NewIdempotency(r.store, uowFn(func(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }), r.clock)
	m.Logger = slog.New(slog.NewJSONHandler(r.logs, nil))
	r.mw = m.Middleware(r.h)
	return r
}

func triaged(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"caseId":"case-1","status":"TRIAGED","version":2}`)
}

func refusingWith(err error) answerFn {
	return func(w http.ResponseWriter, r *http.Request) { writeError(w, r, err) }
}

func (r *inflightRig) post(t *testing.T, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/exceptions/case-1/triage-decisions", strings.NewReader(body))
	req.Header.Set(IdempotencyKeyHeader, key)
	rec := httptest.NewRecorder()
	r.mw.ServeHTTP(rec, req)
	return rec
}

func isReplay(rec *httptest.ResponseRecorder) bool {
	return rec.Header().Get(IdempotencyReplayedHeader) == "true"
}

func slugOf(body string) string {
	var p struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal([]byte(body), &p)
	return p.Type[strings.LastIndex(p.Type, "/")+1:]
}

const triagedBody = `{"caseId":"case-1","status":"TRIAGED","version":2}`

// fleet audit 2026-11-21 (in-flight duplicate): a duplicate that passed the
// key Lookup while the original was in flight (the blind Lookup stands for
// it), queued on the case row lock and then decided a RETRYABLE refusal
// (concurrent-modification: the original's version is committed now) answers
// the stored 200 as a replay, not its own 409. A 5xx it reaches the same way
// replays too, and a copy with another body answers 409
// idempotency-key-reused. The Postgres twin over the real router is
// cmd/api TestAnInFlightDuplicateTriageReplaysTheOriginalOverPostgres.
func TestAnInFlightDuplicateThatDecidesAnUnstoredRefusalReplaysTheStoredAnswer(t *testing.T) {
	r := newInflightRig(triaged, refusingWith(usecases.ErrConcurrentModification))
	if first := r.post(t, "k-dup", `{"n":1}`); first.Code != http.StatusOK || isReplay(first) {
		t.Fatalf("first = %d replayed=%v %s, want a fresh 200", first.Code, isReplay(first), first.Body)
	}
	r.store.blind = 1
	dup := r.post(t, "k-dup", `{"n":1}`)
	if dup.Code != http.StatusOK || !isReplay(dup) || dup.Body.String() != triagedBody {
		t.Fatalf("in-flight duplicate = %d replayed=%v %s, want the first 200 replayed", dup.Code, isReplay(dup), dup.Body)
	}
	r.h.script = append(r.h.script, refusingWith(errors.New("database gone")))
	r.store.blind = 1
	if five := r.post(t, "k-dup", `{"n":1}`); five.Code != http.StatusOK || !isReplay(five) || five.Body.String() != triagedBody {
		t.Fatalf("in-flight duplicate answering a 5xx = %d replayed=%v %s, want the first 200 replayed", five.Code, isReplay(five), five.Body)
	}
	r.store.blind = 1
	other := r.post(t, "k-dup", `{"n":2}`)
	if other.Code != http.StatusConflict || slugOf(other.Body.String()) != "idempotency-key-reused" {
		t.Fatalf("in-flight copy with another body = %d %s, want 409 idempotency-key-reused", other.Code, other.Body)
	}
	if r.h.calls != 4 {
		t.Fatalf("handler calls = %d, want 4 (every blind copy ran its use case)", r.h.calls)
	}
}

// fleet audit 2026-11-21: the re-read uses the SAME Lookup as the up-front
// check, so it honours the same replay window. This store has no age bound
// (ADR 0015, "TTL/retention: none" — an open point, not this fix), so an
// answer exactly 24 h old and one a microsecond older both replay, through
// the up-front Lookup AND through the re-read before an unstored refusal. A
// re-read that applied its own window (a 24 h age check only on this path)
// would answer the duplicate's own 409 past 24 h while the up-front Lookup
// still replays: the two paths must not disagree.
func TestTheReReadBeforeAnUnstoredRefusalAgreesWithTheUpFrontLookupAt24Hours(t *testing.T) {
	r := newInflightRig(triaged, refusingWith(usecases.ErrConcurrentModification))
	if first := r.post(t, "k-win", `{"n":1}`); first.Code != http.StatusOK {
		t.Fatalf("first = %d %s", first.Code, first.Body)
	}
	for _, age := range []time.Duration{24 * time.Hour, time.Microsecond} {
		r.clock.advance(age)
		if up := r.post(t, "k-win", `{"n":1}`); up.Code != http.StatusOK || !isReplay(up) {
			t.Fatalf("up-front Lookup at +%v = %d replayed=%v %s, want the stored 200 replayed", r.clock.Now().Sub(inflightT0), up.Code, isReplay(up), up.Body)
		}
		r.store.blind = 1
		if dup := r.post(t, "k-win", `{"n":1}`); dup.Code != http.StatusOK || !isReplay(dup) || dup.Body.String() != triagedBody {
			t.Fatalf("re-read at +%v = %d replayed=%v %s, want the stored 200 replayed (same window as the up-front Lookup)",
				r.clock.Now().Sub(inflightT0), dup.Code, isReplay(dup), dup.Body)
		}
	}
}

// fleet audit 2026-11-21 (mfc f4b80e1): a failed re-read before an unstored
// refusal is logged at WARN, and the request's own refusal still goes out
// (the key stays released).
func TestAFailedReReadBeforeAnUnstoredRefusalIsLoggedAndTheRefusalAnswered(t *testing.T) {
	r := newInflightRig(refusingWith(usecases.ErrConcurrentModification))
	r.store.failAfter = 1
	rec := r.post(t, "k-warn", `{"n":1}`)
	if rec.Code != http.StatusConflict || isReplay(rec) || slugOf(rec.Body.String()) != "concurrent-modification" {
		t.Fatalf("answer = %d replayed=%v %s, want the request's own 409 concurrent-modification", rec.Code, isReplay(rec), rec.Body)
	}
	if r.store.lookups != 2 {
		t.Fatalf("lookups = %d, want the first Lookup and the re-read", r.store.lookups)
	}
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(r.logs.Bytes()), &entry); err != nil {
		t.Fatalf("log = %q, want one JSON line: %v", r.logs.String(), err)
	}
	msg, _ := entry["error"].(string)
	if entry["level"] != "WARN" || entry["msg"] != "idempotency: re-read before an unstored refusal failed" ||
		!strings.Contains(msg, "idempotency store unreachable") {
		t.Fatalf("log entry = %v", entry)
	}
}

// A refusal with no stored answer behind it (the ordinary, not-in-flight
// RETRYABLE) still answers itself after the re-read finds nothing, and
// leaves the key released.
func TestAnUnstoredRefusalWithNothingStoredAnswersItselfAfterTheReRead(t *testing.T) {
	r := newInflightRig(refusingWith(usecases.ErrConcurrentModification), triaged)
	rec := r.post(t, "k-none", `{"n":1}`)
	if rec.Code != http.StatusConflict || isReplay(rec) || slugOf(rec.Body.String()) != "concurrent-modification" {
		t.Fatalf("answer = %d replayed=%v %s, want the request's own 409", rec.Code, isReplay(rec), rec.Body)
	}
	if r.store.lookups != 2 {
		t.Fatalf("lookups = %d, want the first Lookup and the re-read", r.store.lookups)
	}
	if retry := r.post(t, "k-none", `{"n":1}`); retry.Code != http.StatusOK || isReplay(retry) {
		t.Fatalf("retry under the released key = %d replayed=%v %s, want the work to run", retry.Code, isReplay(retry), retry.Body)
	}
}
