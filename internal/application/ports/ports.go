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
	// ListPage serves the operator listing (GET /exceptions), one keyset page
	// at a time: at most limit cases matching the filter (Status "" = EVERY
	// status, open and resolved alike — resolve activity is part of the
	// record, per apis/openapi.yaml), NEWEST opened first (opened-at
	// descending, then case id descending — the order the in-memory adapter
	// returns too), strictly after the position `after` (nil = the first
	// page). Callers ask for limit+1 rows to learn whether another page
	// exists.
	ListPage(ctx context.Context, f repository.CaseFilter, after *repository.CaseAfter, limit int) ([]*exceptioncase.Case, error)
}

// ExceptionPolicyRepo persists the append-only ExceptionPolicy versions by
// source type. Inside a UnitOfWork it joins the transaction carried in ctx.
type ExceptionPolicyRepo interface {
	// Save persists one policy version.
	Save(ctx context.Context, p *exceptionpolicy.Policy) error
	// Get returns the LATEST declared version of the source type's policy, or
	// ErrNotFound when the type was never declared.
	Get(ctx context.Context, sourceType string) (*exceptionpolicy.Policy, error)
	// ListPage returns at most limit policies — the latest declared version
	// of each source type — ordered by source type, strictly after the
	// source type `after` ("" = the first page). Callers ask for limit+1 rows
	// to learn whether another page exists (GET /exception-policies).
	ListPage(ctx context.Context, after string, limit int) ([]*exceptionpolicy.Policy, error)
}

// ProcessedEvents is the inbound consumers' dedupe guard (the fleet rule:
// one row per CloudEvents id). Claim runs inside the SAME UnitOfWork as the
// effect it guards, so a rollback un-claims the id and a redelivery is
// processed, not skipped. The Postgres adapter backs the processed_events
// table; the in-memory double keeps the map.
type ProcessedEvents interface {
	// Claim records eventID as processed, returning false when an earlier
	// call already recorded it (the duplicate outcome).
	Claim(ctx context.Context, eventID string) (bool, error)
}

// IdempotencyStore keeps the answers of requests made with an
// Idempotency-Key. The first writer of a key wins, for as long as its answer
// is live: a row older than repository.IdempotencyTTL answers nothing (the
// Lookup age guard), so two concurrent requests with one key cannot both
// record their answer, and a key past its window is free again. Lookup runs
// inside the middleware's unit of work on the fresh path (the race re-read),
// and standalone on the replay path.
type IdempotencyStore interface {
	// Lookup returns the stored answer of key while it is younger than
	// repository.IdempotencyTTL, or found=false when the key has none the
	// replay window still honours.
	Lookup(ctx context.Context, key string) (repository.IdempotencyRecord, bool, error)
	// Save stores rec unless the key already has a LIVE answer (the first
	// writer wins within the window); the answer of a key whose previous
	// answer has expired replaces it, so the key serves the new answer.
	Save(ctx context.Context, rec repository.IdempotencyRecord) error
	// PurgeExpired is the retention: it deletes at most limit rows older
	// than repository.IdempotencyTTL and returns how many it deleted; a
	// result equal to limit means more may remain. It never deletes a live
	// row, never waits for a row a request holds (SKIP LOCKED on Postgres),
	// and never changes what a concurrent request answers: an expired row is
	// already invisible to Lookup, so deleting it is not observable.
	PurgeExpired(ctx context.Context, limit int) (int, error)
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

// CaseMetrics records the business outcome of every consumed fact the
// open-or-touch decision handles: the Tier 2 counter of the fleet metrics
// convention (ADR 0014). OpenOrTouchException calls it exactly once per
// Execute, after the decision and its writes returned; a use case built
// without one records nothing. The adapter (internal/adapters/outbound/
// telemetry) maps the call to the exception.cases.opened counter; it is never
// called from the domain.
//
// A decision taken inside an enclosing unit of work that later fails to
// commit has already been counted, and its redelivery is counted again: the
// counter measures decisions, not durable commits.
type CaseMetrics interface {
	// CaseOpened counts a fact that opened a new case of the given severity.
	CaseOpened(ctx context.Context, severity string)
	// CaseTouched counts a fact that touched an existing case (occurrence+1).
	CaseTouched(ctx context.Context, severity string)
	// CaseReplayed counts the replay of a case's opening event: the
	// idempotent no-op.
	CaseReplayed(ctx context.Context, severity string)
	// CaseRejected counts a fact that opened or touched nothing, with the
	// problem slug of the failure as the reason.
	CaseRejected(ctx context.Context, reason string)
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
