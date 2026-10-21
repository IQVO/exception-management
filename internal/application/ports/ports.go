// Package ports declares the application's OUT ports: the interfaces the use
// cases drive and the outbound adapters (Postgres, the transactional outbox,
// the in-memory doubles) implement. The package holds interfaces and the
// ErrNotFound sentinel only — the architecture fitness test enforces
// interfaces-only here — and the read DTOs the inbound adapters consume live
// one package over, in internal/application/views.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// ErrNotFound is the typed miss every Find*/Get port returns: the requested
// case or policy does not exist. The HTTP adapter maps it to 404, the
// correlation and sweep paths treat it as fail-soft; errors.Is is the test.
var ErrNotFound = errors.New("ports: not found")

// ExceptionCaseRepo persists ExceptionCase aggregates by case id and carries
// the one-case-per-dedup-key invariant (ADR 0002). Inside a UnitOfWork it
// joins the transaction carried in ctx.
type ExceptionCaseRepo interface {
	// Save persists c. The Postgres adapter guards the write by the aggregate
	// version the caller loaded (optimistic concurrency); the in-memory double
	// stores the latest state.
	Save(ctx context.Context, c *exceptioncase.Case) error
	// FindByID returns the case, or ErrNotFound.
	FindByID(ctx context.Context, id string) (*exceptioncase.Case, error)
	// FindOpenByDedupKey returns the case that owns key. The owner is the
	// open case when one exists; a RESOLVED case still owns its key, because
	// a later fact for the key TOUCHES it (occurrence+1, lastSeenAt) and
	// never reopens it. ErrNotFound means no case exists for the key at all —
	// the only situation in which a fact opens one.
	FindOpenByDedupKey(ctx context.Context, key shared.DedupKey) (*exceptioncase.Case, error)
	// ListOpen returns every non-resolved case, narrowed by the non-blank
	// filters (exact match on queue, site id and source type; "" narrows
	// nothing), ordered by opened-at then case id. Serves the SLA sweeper
	// and the correlation paths.
	ListOpen(ctx context.Context, queue, siteID, sourceType string) ([]*exceptioncase.Case, error)
	// List serves the operator listing (GET /exceptions): every case whose
	// status matches status ("" = EVERY status, open and resolved alike —
	// resolve activity is part of the record, per apis/openapi.yaml),
	// narrowed by the non-blank filters, NEWEST opened first (opened-at
	// descending, then case id descending — the order the in-memory adapter
	// returns too).
	List(ctx context.Context, status, queue, siteID, sourceType string) ([]*exceptioncase.Case, error)
}

// ExceptionPolicyRepo persists the append-only ExceptionPolicy versions by
// source type. Inside a UnitOfWork it joins the transaction carried in ctx.
type ExceptionPolicyRepo interface {
	// Save persists one policy version.
	Save(ctx context.Context, p *exceptionpolicy.Policy) error
	// Get returns the LATEST declared version of the source type's policy, or
	// ErrNotFound when the type was never declared.
	Get(ctx context.Context, sourceType string) (*exceptionpolicy.Policy, error)
	// List returns the latest declared version of EVERY source type,
	// ordered by source type (GET /exception-policies).
	List(ctx context.Context) ([]*exceptionpolicy.Policy, error)
}

// IdempotencyStore keeps the answers of requests made with an
// Idempotency-Key. The first writer of a key wins: a later Save of the same
// key changes nothing, so two concurrent requests with one key cannot both
// record their answer. Lookup runs inside the middleware's unit of work on
// the fresh path (the race re-read), and standalone on the replay path.
type IdempotencyStore interface {
	// Lookup returns the stored answer of key, or found=false when the key
	// has none.
	Lookup(ctx context.Context, key string) (repository.IdempotencyRecord, bool, error)
	// Save stores rec unless the key already has an answer.
	Save(ctx context.Context, rec repository.IdempotencyRecord) error
}

// EventPublisher is the write side of the transactional outbox, in domain
// terms: it is called in the same UnitOfWork as the aggregate's Save, so a
// state change and its facts commit or roll back together. The outbound
// adapter maps the facts to CloudEvents on the wire (ADR 0004).
type EventPublisher interface {
	// PublishCaseEvents enqueues the facts an ExceptionCase raised.
	PublishCaseEvents(ctx context.Context, events ...exceptioncase.Event) error
	// PublishPolicyDeclared enqueues the analytics-only declaration fact.
	PublishPolicyDeclared(ctx context.Context, d exceptionpolicy.PolicyDeclared) error
}

// IDGen mints aggregate ids. The real generator's shape (prefixed, sortable)
// is an adapter concern; use cases only need a fresh unique string.
type IDGen interface {
	// NewCaseID returns a fresh case id.
	NewCaseID() string
}

// Clock is the service's notion of "now". Use cases stamp every transition,
// fact and report with it; the domain never reads a clock.
type Clock interface {
	Now() time.Time
}

// UnitOfWork runs fn as ONE atomic unit: every port call fn makes with the
// ctx it is handed commits together when fn returns nil, or rolls back
// together when fn returns an error (or panics). Use cases tolerate a NIL
// UnitOfWork — nil means the calls run unwrapped (in-memory wiring, tests).
//
// Contract:
//   - fn MUST use the ctx passed to it (not the outer ctx) for every port
//     call that should participate; the unit of work travels in that ctx.
//   - Do is re-entrant: if ctx already carries an open unit of work, Do JOINS
//     it and the outermost Do owns commit/rollback.
//   - The error fn returns is returned from Do unchanged (errors.Is keeps
//     working); a commit failure is returned wrapped.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}
