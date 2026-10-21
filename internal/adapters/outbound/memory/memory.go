// Package memory holds the in-memory test doubles for the application ports
// (unit tests and BDD wiring only). The shipped binary never imports it:
// TestRuntimeBinaryHasNoInMemoryAdapters enforces that, and the runtime is
// real Postgres + real Kafka only.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The doubles return the PORT sentinel (ports.ErrNotFound) on a miss so
// use-case errors.Is checks hold against every adapter, memory and Postgres.
// They also enforce the STORAGE invariants the Postgres adapter enforces
// (optimistic-concurrency version guard, one open case per dedup key,
// append-only policy versions), so the repocontract suite proves the two
// adapters equivalent.

// Cases is the ExceptionCaseRepo double. Like the Postgres adapter, it
// round-trips through the aggregate snapshot: a saved case is rehydrated on
// read, so pending domain events are consumed by Save and never re-published
// by a later read (a live-pointer map would replay them).
type Cases struct {
	mu    sync.Mutex
	byID  map[string]exceptioncase.Snapshot
	order []string
}

// NewCases returns an empty double.
func NewCases() *Cases { return &Cases{byID: map[string]exceptioncase.Snapshot{}} }

// Save stores the latest snapshot of c, under the same guards as the
// Postgres upsert: an update is legal only when the stored version is the
// saved version minus one (optimistic concurrency), and a FIRST save of a
// case whose dedup key already has an open case is refused — the double of
// the partial unique index uq_exception_cases_one_open_per_key.
func (r *Cases) Save(_ context.Context, c *exceptioncase.Case) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := c.Snapshot()
	if cur, ok := r.byID[s.ID]; ok {
		if cur.Version != s.Version-1 {
			return usecases.ErrConcurrentModification
		}
	} else {
		for _, id := range r.order {
			other := r.byID[id]
			if other.Key == s.Key && other.Status != exceptioncase.StatusResolved {
				return usecases.ErrOpenCaseExists
			}
		}
		r.order = append(r.order, s.ID)
	}
	r.byID[s.ID] = s
	return nil
}

func (r *Cases) restore(id string) (*exceptioncase.Case, error) {
	s, ok := r.byID[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	return exceptioncase.Restore(s)
}

// FindByID returns the case or not-found.
func (r *Cases) FindByID(_ context.Context, id string) (*exceptioncase.Case, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restore(id)
}

// FindOpenByDedupKey returns the case owning the key — the open case when
// one exists, else the most recent RESOLVED case for the key (a later fact
// touches it and never reopens it) — or not-found. The same owner rule the
// Postgres adapter's ORDER BY (status <> 'RESOLVED') DESC, opened_at DESC
// encodes.
func (r *Cases) FindOpenByDedupKey(_ context.Context, key shared.DedupKey) (*exceptioncase.Case, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var open, resolved *exceptioncase.Case
	for _, id := range r.order {
		c, err := r.restore(id)
		if err != nil {
			return nil, err
		}
		if !c.Key().Equal(key) {
			continue
		}
		if c.Status() != exceptioncase.StatusResolved {
			open = c // at most one open case per key exists (the index)
			break
		}
		if resolved == nil || c.OpenedAt().After(resolved.OpenedAt()) {
			resolved = c
		}
	}
	if open != nil {
		return open, nil
	}
	if resolved != nil {
		return resolved, nil
	}
	return nil, ports.ErrNotFound
}

// ListOpen returns non-resolved cases, oldest-opened first, narrowed by the
// non-blank filters.
func (r *Cases) ListOpen(_ context.Context, queue, siteID, sourceType string) ([]*exceptioncase.Case, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*exceptioncase.Case
	for _, id := range r.order {
		c, err := r.restore(id)
		if err != nil {
			return nil, err
		}
		if string(c.Status()) == "RESOLVED" {
			continue
		}
		k := c.Key()
		if queue != "" && c.Queue() != queue {
			continue
		}
		if siteID != "" && k.SiteID != siteID {
			continue
		}
		if sourceType != "" && k.SourceType != sourceType {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt().Equal(out[j].OpenedAt()) {
			return out[i].ID() < out[j].ID()
		}
		return out[i].OpenedAt().Before(out[j].OpenedAt())
	})
	return out, nil
}

// Policies is the ExceptionPolicyRepo double (latest version per source type).
type Policies struct {
	mu     sync.Mutex
	latest map[string]*exceptionpolicy.Policy
}

// NewPolicies returns an empty double.
func NewPolicies() *Policies { return &Policies{latest: map[string]*exceptionpolicy.Policy{}} }

// Save stores the version and marks it latest for its source type, under
// the same guard as the Postgres INSERT: the source type already has THIS
// version (or a newer one), so another writer declared it first — the double
// of the (source_type, version) primary key.
func (r *Policies) Save(_ context.Context, p *exceptionpolicy.Policy) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.latest[p.SourceType()]; ok && cur.Version() >= p.Version() {
		return usecases.ErrConcurrentModification
	}
	r.latest[p.SourceType()] = p
	return nil
}

// Get returns the latest version for the source type, or not-found.
func (r *Policies) Get(_ context.Context, sourceType string) (*exceptionpolicy.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.latest[sourceType]; ok {
		return p, nil
	}
	return nil, ports.ErrNotFound
}

// Events collects everything published, for assertions.
type Events struct {
	mu     sync.Mutex
	Case   []exceptioncase.Event
	Policy []exceptionpolicy.PolicyDeclared
}

// PublishCaseEvents records case facts.
func (e *Events) PublishCaseEvents(_ context.Context, events ...exceptioncase.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Case = append(e.Case, events...)
	return nil
}

// PublishPolicyDeclared records a declaration fact.
func (e *Events) PublishPolicyDeclared(_ context.Context, d exceptionpolicy.PolicyDeclared) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Policy = append(e.Policy, d)
	return nil
}

// CaseEventNames returns the recorded case fact names in order.
func (e *Events) CaseEventNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.Case))
	for _, ev := range e.Case {
		out = append(out, ev.EventName())
	}
	return out
}

// IDGen mints sequential case ids.
type IDGen struct {
	mu sync.Mutex
	n  int
}

// NewIDGen returns a generator counting from 1.
func NewIDGen() *IDGen { return &IDGen{} }

// NewCaseID returns case-000N.
func (g *IDGen) NewCaseID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return fmt.Sprintf("case-%04d", g.n)
}

// Clock is a fixed clock.
type Clock struct{ T time.Time }

// Now returns the fixed instant.
func (c Clock) Now() time.Time { return c.T }
