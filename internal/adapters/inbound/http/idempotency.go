package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// IdempotencyKeyHeader is the request header a caller supplies on every POST
// and PUT wrapped by Idempotency.Middleware.
const IdempotencyKeyHeader = "Idempotency-Key"

// IdempotencyReplayedHeader is the response header ("true") of every replayed
// answer, and only of a replay: the first execution never carries it.
const IdempotencyReplayedHeader = "Idempotency-Replayed"

// maxIdempotencyKeyLen is the OpenAPI maxLength of the key.
const maxIdempotencyKeyLen = 128

// Control flow of the fresh path; neither escapes the middleware.
var (
	errRollback = errors.New("response is not stored: roll the request back")
	errLostRace = errors.New("another request stored an answer for this idempotency key first")
)

// Idempotency is the fleet's Idempotency-Key middleware for the POSTs and
// the PUT of this API, built on the application's ports so the adapter
// imports no sibling adapter. The argument in short:
//
//   - A key that already has a stored answer replays it (same fingerprint:
//     method, path and SHA-256 of the body) with the response header
//     Idempotency-Replayed: true, or is 409 idempotency-key-reused
//     (different fingerprint). Nothing runs. The store only serves answers
//     younger than repository.IdempotencyTTL (24 h, ADR 0015's 2026-11-22
//     amendment): past the window the key is free again and the request
//     executes anew.
//   - A new key runs the handler INSIDE one unit of work. A 2xx answer is
//     saved in that same transaction, so the effects, their outbox rows and
//     the stored answer commit or roll back together.
//   - A 4xx answer rolls the unit of work back (a rejected request leaves no
//     effect, and a Postgres transaction that hit a constraint is aborted
//     anyway) and is then stored on its own when its problem type is FINAL
//     (or OUTCOME), so the same key and body replay the same rejection. A
//     RETRYABLE 4xx (optimistic-lock conflict, a fact not consumed yet) and
//     an unclassified one are NOT stored: the key is released, exactly like
//     the 5xx branch, and a retry executes again (ADR 0015). A 5xx answer is
//     rolled back and NEVER stored, so the same key can retry the real work.
//     Before answering an unstored refusal the key is re-read: a same-key
//     copy that waited on the case row while the original was in flight
//     replays the original's stored answer instead (see finishRejected).
//   - Two concurrent requests with one new key both run, but the store keeps
//     the first writer's answer (ports.IdempotencyStore.Save): after saving,
//     a request re-reads the key in its own transaction, and the one that
//     finds another answer rolls back its effects and replays the winner's.
//     Under Postgres the second Save blocks on the key's primary key until
//     the first transaction ends, so the re-read sees the committed winner.
type Idempotency struct {
	Store  ports.IdempotencyStore
	UoW    ports.UnitOfWork
	Clock  ports.Clock
	Logger *slog.Logger
}

// NewIdempotency builds the middleware over the idempotency store, the unit
// of work the handlers' use cases also use, and the clock.
func NewIdempotency(store ports.IdempotencyStore, uow ports.UnitOfWork, clock ports.Clock) *Idempotency {
	return &Idempotency{Store: store, UoW: uow, Clock: clock, Logger: slog.Default()}
}

// Middleware wraps next.
func (m *Idempotency) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := requestKey(w, r)
		if !ok {
			return
		}
		body, ok := bufferBody(w, r)
		if !ok {
			return
		}
		fp := fingerprint(r, body)
		rec, found, err := m.Store.Lookup(r.Context(), key)
		if err != nil {
			writeError(w, r, err)
			return
		}
		if found {
			replay(w, r, rec, fp)
			return
		}
		m.serveFresh(w, r, next, key, fp)
	})
}

// requestKey reads the Idempotency-Key; a missing key is answered 400
// idempotency-key-required and a key that is longer than 128 characters or
// not printable ASCII 400 idempotency-key-invalid, both reported as not ok.
func requestKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.Header.Get(IdempotencyKeyHeader)
	if key == "" {
		writeProblem(w, r, idempotencyKeyRequired, "send an Idempotency-Key header with a unique value per logical request")
		return "", false
	}
	if !printableKey(key) {
		writeProblem(w, r, idempotencyKeyInvalid, "the Idempotency-Key header must be 1 to 128 printable ASCII characters")
		return "", false
	}
	return key, true
}

// printableKey reports whether key is 1 to 128 printable ASCII characters.
func printableKey(key string) bool {
	if len(key) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7e {
			return false
		}
	}
	return true
}

// bufferBody reads the body once so it can be fingerprinted and still be
// read by the handler.
func bufferBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	_ = r.Body.Close()
	if err != nil || len(body) > maxBodyBytes {
		writeError(w, r, errMalformedRequest)
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

// fingerprint identifies a request: the same key may only ever carry the
// same method, path and body.
func fingerprint(r *http.Request, body []byte) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\n%s\n", r.Method, r.URL.EscapedPath())
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// storedAnswer is what the record's Body holds: the response headers
// (Content-Type) with the body, so a replay is byte-identical.
type storedAnswer struct {
	Header http.Header `json:"header,omitempty"`
	Body   []byte      `json:"body"`
}

// capture records the handler's response so it can be stored (and committed)
// BEFORE anything reaches the client.
type capture struct {
	header http.Header
	code   int
	body   bytes.Buffer
	// replay is the class of the problem the handler answered with
	// (markReplay, called by writeProblem); "" when the handler wrote a
	// status by hand. The class decides whether a refusal is stored.
	replay replayClass
}

// replayMarker is what writeProblem tells the middleware's capturing writer:
// the replay class of the problem it answers with. Any other writer (the
// real client connection, a test recorder) ignores it.
type replayMarker interface{ markReplay(replayClass) }

func markReplay(w http.ResponseWriter, class replayClass) {
	if m, ok := w.(replayMarker); ok {
		m.markReplay(class)
	}
}

func (c *capture) markReplay(class replayClass) { c.replay = class }

func (c *capture) Header() http.Header { return c.header }

func (c *capture) WriteHeader(code int) {
	if c.code == 0 {
		c.code = code
	}
}

func (c *capture) Write(b []byte) (int, error) {
	if c.code == 0 {
		c.code = http.StatusOK
	}
	return c.body.Write(b)
}

func (m *Idempotency) serveFresh(w http.ResponseWriter, r *http.Request, next http.Handler, key, fp string) {
	var (
		out    *capture
		winner repository.IdempotencyRecord
	)
	err := m.UoW.Do(r.Context(), func(ctx context.Context) error {
		out = serveCaptured(next, r.WithContext(ctx))
		if out.code >= http.StatusBadRequest {
			return errRollback
		}
		rec, err := m.record(key, fp, out)
		if err != nil {
			return err
		}
		stored, err := m.saveAndReread(ctx, rec)
		if err != nil {
			return err
		}
		if !sameAnswer(stored, rec) {
			winner = stored
			return errLostRace
		}
		return nil
	})
	if err == nil {
		flush(w, out)
		return
	}
	if errors.Is(err, errLostRace) {
		replay(w, r, winner, fp)
		return
	}
	if errors.Is(err, errRollback) {
		m.finishRejected(w, r, key, fp, out)
		return
	}
	writeError(w, r, err)
}

// serveCaptured runs next and returns what it answered (200 when it wrote
// nothing).
func serveCaptured(next http.Handler, r *http.Request) *capture {
	out := &capture{header: http.Header{}}
	next.ServeHTTP(out, r)
	if out.code == 0 {
		out.code = http.StatusOK
	}
	return out
}

func (m *Idempotency) record(key, fp string, out *capture) (repository.IdempotencyRecord, error) {
	body, err := json.Marshal(storedAnswer{Header: out.header, Body: out.body.Bytes()})
	if err != nil {
		return repository.IdempotencyRecord{}, fmt.Errorf("encode idempotency answer: %w", err)
	}
	return repository.IdempotencyRecord{
		Key: key, Fingerprint: fp, Status: out.code, ContentType: out.header.Get("Content-Type"), Body: body,
		// Microseconds: the precision Postgres keeps, so the re-read compares equal.
		CreatedAt: m.Clock.Now().UTC().Truncate(time.Microsecond),
	}, nil
}

// saveAndReread saves rec and returns the record the store holds for its key
// afterwards: rec itself, or the answer of the request that won the race.
func (m *Idempotency) saveAndReread(ctx context.Context, rec repository.IdempotencyRecord) (repository.IdempotencyRecord, error) {
	if err := m.Store.Save(ctx, rec); err != nil {
		return repository.IdempotencyRecord{}, err
	}
	stored, found, err := m.Store.Lookup(ctx, rec.Key)
	if err != nil {
		return repository.IdempotencyRecord{}, err
	}
	if !found {
		return repository.IdempotencyRecord{}, errors.New("idempotency answer vanished after it was saved")
	}
	return stored, nil
}

func sameAnswer(a, b repository.IdempotencyRecord) bool {
	return a.Fingerprint == b.Fingerprint && a.Status == b.Status &&
		bytes.Equal(a.Body, b.Body) && a.CreatedAt.Equal(b.CreatedAt)
}

// finishRejected answers a request whose unit of work was rolled back: a 5xx
// goes out as it is and is never stored; a 4xx whose problem type is FINAL
// (or OUTCOME — none exists here, the unit of work was rolled back) is
// stored on its own first so a retry with the same key and body replays it;
// a RETRYABLE or unclassified 4xx is not stored either — the key is
// released, exactly like the 5xx branch, so a retry executes again (ADR
// 0015). If another request stored an answer for the key first (it won a
// race this one lost with a storable 4xx), that answer is replayed instead.
//
// An unstored refusal re-reads the key first (fleet audit 2026-11-21,
// in-flight duplicates). The key Lookup runs before the unit of work and the
// case row lock is taken inside the use case (the version-guarded upsert of
// the case repository), so a same-key copy that arrived while the first copy
// was in flight passed that Lookup, read the case at the old version, queued
// on the row lock and matched 0 rows once the first committed its triage,
// assignment or resolution together with its key row. The RETRYABLE refusal
// it then reached (concurrent-modification), or a 5xx, is answered by the
// stored answer instead: N copies are one run and N-1 replays (another body:
// idempotency-key-reused). Re-reading here is equivalent to re-reading under
// the lock: the refusal rolled back every write. A re-read at the start of
// the unit of work would not do: the unit takes no lock at begin, so under
// READ COMMITTED it runs before the original commits and finds nothing. The
// re-read is the same Lookup as the up-front check, so it honours the same
// replay window. A failed re-read is logged at WARN and the request's own
// refusal goes out (the key stays released).
func (m *Idempotency) finishRejected(w http.ResponseWriter, r *http.Request, key, fp string, out *capture) {
	if out.code >= http.StatusInternalServerError || !out.replay.stored() {
		stored, found, err := m.Store.Lookup(r.Context(), key)
		if err != nil {
			m.Logger.WarnContext(r.Context(), "idempotency: re-read before an unstored refusal failed", "error", logSafe(err.Error()))
		} else if found {
			replay(w, r, stored, fp)
			return
		}
		flush(w, out)
		return
	}
	rec, err := m.record(key, fp, out)
	if err == nil {
		err = m.Store.Save(r.Context(), rec)
	}
	if err != nil {
		m.Logger.WarnContext(r.Context(), "idempotency: answer not stored", "error", logSafe(err.Error()))
		flush(w, out)
		return
	}
	stored, found, err := m.Store.Lookup(r.Context(), key)
	if err == nil && found && !sameAnswer(stored, rec) {
		replay(w, r, stored, fp)
		return
	}
	flush(w, out)
}

// replay answers a request whose key already has a stored answer: that
// answer (marked Idempotency-Replayed: true) when the fingerprint matches,
// 409 when it does not.
func replay(w http.ResponseWriter, r *http.Request, rec repository.IdempotencyRecord, fp string) {
	if rec.Fingerprint != fp {
		writeProblem(w, r, idempotencyKeyReused, "the Idempotency-Key was used with a different request (method, path or body)")
		return
	}
	var answer storedAnswer
	if err := json.Unmarshal(rec.Body, &answer); err != nil {
		writeError(w, r, fmt.Errorf("decode stored idempotency answer: %w", err))
		return
	}
	for k, vs := range answer.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set(IdempotencyReplayedHeader, "true")
	w.WriteHeader(rec.Status)
	_, _ = w.Write(answer.Body)
}

// flush copies the captured response onto the real client connection.
func flush(w http.ResponseWriter, out *capture) {
	for k, vs := range out.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(out.code)
	_, _ = w.Write(out.body.Bytes())
}
