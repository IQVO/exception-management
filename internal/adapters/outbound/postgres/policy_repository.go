package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// PolicyRepo is the pgxpool-backed ports.ExceptionPolicyRepo: versions are
// append-only rows keyed by (source type, version); Get returns the latest.
type PolicyRepo struct {
	pool *pgxpool.Pool
}

// NewPolicyRepo constructs a PolicyRepo over pool.
func NewPolicyRepo(pool *pgxpool.Pool) *PolicyRepo { return &PolicyRepo{pool: pool} }

const policyColumns = `source_type, version, severity, queue, sla_minutes,
	auto_resolve_on_type, auto_resolve_within_minutes, declared_at`

// Save implements ports.ExceptionPolicyRepo: an append-only INSERT of one
// version. A duplicate (source type, version) means another writer declared
// the same next version first (two concurrent redeclares raced) and maps to
// usecases.ErrConcurrentModification: re-fetch and retry.
func (r *PolicyRepo) Save(ctx context.Context, p *exceptionpolicy.Policy) error {
	s := p.Snapshot()
	var onType any
	var within any
	if s.AutoResolve != nil {
		onType, within = s.AutoResolve.OnType(), s.AutoResolve.WithinMinutes()
	}
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO exception_policies (`+policyColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, s.SourceType, s.Version, string(s.Severity), s.Queue, s.SLAMinutes, onType, within, s.DeclaredAt.UTC())
	if err != nil {
		if isUniqueViolation(err, "exception_policies_pkey") {
			return usecases.ErrConcurrentModification
		}
		return fmt.Errorf("save exception policy %s v%d: %w", s.SourceType, s.Version, err)
	}
	return nil
}

// Get implements ports.ExceptionPolicyRepo: the LATEST declared version of
// the source type's policy.
func (r *PolicyRepo) Get(ctx context.Context, sourceType string) (*exceptionpolicy.Policy, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT `+policyColumns+` FROM exception_policies
		WHERE source_type = $1
		ORDER BY version DESC
		LIMIT 1
	`, sourceType)
	var (
		s        exceptionpolicy.Snapshot
		severity string
		onType   *string
		within   *int64
	)
	if err := row.Scan(&s.SourceType, &s.Version, &severity, &s.Queue, &s.SLAMinutes, &onType, &within, &s.DeclaredAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ports.ErrNotFound
		}
		return nil, err
	}
	s.Severity = shared.Severity(severity)
	if onType != nil && within != nil {
		rule, err := exceptionpolicy.NewRule(*onType, *within)
		if err != nil {
			return nil, fmt.Errorf("stored policy %s v%d has an invalid rule: %w", s.SourceType, s.Version, err)
		}
		s.AutoResolve = &rule
	}
	p, err := exceptionpolicy.Restore(s)
	if err != nil {
		return nil, fmt.Errorf("stored policy %s v%d is invalid: %w", s.SourceType, s.Version, err)
	}
	return p, nil
}

// ListPage implements ports.ExceptionPolicyRepo: the LATEST declared version
// of every source type, ordered by source type, strictly after the source
// type `after` ("" = from the start), at most limit rows (GET
// /exception-policies). The source type is the keyset: one row per type, so
// it is its own tiebreaker.
func (r *PolicyRepo) ListPage(ctx context.Context, after string, limit int) ([]*exceptionpolicy.Policy, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT DISTINCT ON (source_type) `+policyColumns+`
		FROM exception_policies
		WHERE ($1 = '' OR source_type > $1)
		ORDER BY source_type, version DESC
		LIMIT $2
	`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list exception policies: %w", err)
	}
	defer rows.Close()
	out := []*exceptionpolicy.Policy{}
	for rows.Next() {
		var (
			s        exceptionpolicy.Snapshot
			severity string
			onType   *string
			within   *int64
		)
		if err := rows.Scan(&s.SourceType, &s.Version, &severity, &s.Queue, &s.SLAMinutes, &onType, &within, &s.DeclaredAt); err != nil {
			return nil, err
		}
		s.Severity = shared.Severity(severity)
		if onType != nil && within != nil {
			rule, err := exceptionpolicy.NewRule(*onType, *within)
			if err != nil {
				return nil, fmt.Errorf("stored policy %s v%d has an invalid rule: %w", s.SourceType, s.Version, err)
			}
			s.AutoResolve = &rule
		}
		p, err := exceptionpolicy.Restore(s)
		if err != nil {
			return nil, fmt.Errorf("stored policy %s v%d is invalid: %w", s.SourceType, s.Version, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
