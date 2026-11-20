// Rendered from warehouse-harness-template
// templates/idempotency/replay_classes_wiring_test.go.tmpl: the repo-specific
// half of the idempotency replay-class test. This service's store sits
// behind a port (ports.IdempotencyStore) with an in-memory double the
// middleware's own unit tests use, so the closures run over that double
// (docs/idempotency-replay-classes.md, "an in-memory one is enough for a
// design whose store is behind a port"); the real-Postgres proofs of the
// two RETRYABLE problem types live in cmd/api (integration). The unit of
// work is a staging double of postgres.UnitOfWork: a write made through the
// request's context is staged and committed only when the request's unit of
// work commits — a refusal rolls the whole request back, staged writes
// included, exactly like the transaction the middleware opens in
// production.
package http_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	httpadapter "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
)

// stagingUoW is the unit-of-work double: writes staged inside Do reach the
// committed set only when Do returns nil.
type stagingUoW struct {
	mu        sync.Mutex
	committed map[string]int
	staged    map[string]bool
	inDo      bool
}

func newStagingUoW() *stagingUoW { return &stagingUoW{committed: map[string]int{}} }

func (u *stagingUoW) Do(ctx context.Context, fn func(context.Context) error) error {
	u.mu.Lock()
	staged := map[string]bool{}
	u.staged, u.inDo = staged, true
	u.mu.Unlock()
	err := fn(ctx)
	u.mu.Lock()
	u.inDo, u.staged = false, nil
	if err == nil {
		for tag := range staged {
			u.committed[tag]++
		}
	}
	u.mu.Unlock()
	return err
}

func (u *stagingUoW) write(_ context.Context, tag string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.inDo {
		return nil
	}
	u.staged[tag] = true
	return nil
}

// newReplayOps returns the six operations over ONE clean store (a fresh
// in-memory idempotency store per call).
func newReplayOps(t *testing.T) replayOps {
	t.Helper()
	store := memory.NewIdempotencyStore()
	uow := newStagingUoW()
	clock := memory.Clock{T: time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)}
	idem := httpadapter.NewIdempotency(store, uow, clock)
	errs := map[string]error{}
	for _, row := range httpadapter.Catalogue() {
		errs[row.Slug] = row.Err
	}
	return replayOps{
		Catalogue: func() []problemEntry {
			var out []problemEntry
			for _, row := range httpadapter.Catalogue() {
				out = append(out, problemEntry{Slug: row.Slug, Status: row.Status, Class: row.Class})
			}
			return out
		},
		Middleware: func(_ *testing.T, next http.Handler) http.Handler { return idem.Middleware(next) },
		// Refuse goes through the production error writer, so the class the
		// catalogue entry declares is what marks the capturing writer.
		Refuse: func(w http.ResponseWriter, r *http.Request, e problemEntry) {
			if err := errs[e.Slug]; err != nil {
				httpadapter.WriteError(w, r, err)
				return
			}
			httpadapter.WriteProblemFor(w, r, e.Slug)
		},
		// Write is one write through the request's context, inside the unit
		// of work the middleware opened for the request.
		Write: func(ctx context.Context, tag string) error { return uow.write(ctx, tag) },
		Committed: func(t *testing.T, tag string) int {
			t.Helper()
			uow.mu.Lock()
			defer uow.mu.Unlock()
			return uow.committed[tag]
		},
		KeyRows: func(t *testing.T, key string) int {
			t.Helper()
			_, found, err := store.Lookup(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				return 1
			}
			return 0
		},
	}
}
