package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// openCaseIndex is the partial unique index behind the one-open-case
// invariant of ADR 0002.
const openCaseIndex = "uq_exception_cases_one_open_per_key"

// CaseRepo is the pgxpool-backed ports.ExceptionCaseRepo. Every statement
// runs on the transaction carried by ctx when one is open (a UnitOfWork) and
// on the pool otherwise.
type CaseRepo struct {
	pool *pgxpool.Pool
}

// NewCaseRepo constructs a CaseRepo over pool.
func NewCaseRepo(pool *pgxpool.Pool) *CaseRepo { return &CaseRepo{pool: pool} }

const caseColumns = `case_id, source_type, source_subject, site_id, status, severity, queue,
	assignee, escalated, occurrence, opened_at, last_seen_at, sla_minutes, source_event_id,
	source_payload, resolution_code, root_cause, note, resolved_at, auto_resolved, auto_rule_id,
	auto_correlated_type, auto_correlated_event_id, version, audit`

// Save implements ports.ExceptionCaseRepo: one version-guarded upsert. The
// aggregate's version is incremented by exactly one per accepted
// transition, so a save is legal only when the stored version is the saved
// version minus one — the guard of the ON CONFLICT update
// (exception_cases.version = excluded.version - 1, RowsAffected 0 =>
// usecases.ErrConcurrentModification). A first save takes the INSERT path;
// a second OPEN case for the same dedup key dies on the partial unique
// index (usecases.ErrOpenCaseExists).
func (r *CaseRepo) Save(ctx context.Context, c *exceptioncase.Case) error {
	s := c.Snapshot()
	audit, err := marshalAudit(s.Audit)
	if err != nil {
		return fmt.Errorf("encode audit of case %s: %w", s.ID, err)
	}
	if !json.Valid(s.SourcePayload) {
		return fmt.Errorf("source payload of case %s is not JSON: the case freezes the consumed event's data object", s.ID)
	}
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO exception_cases (`+caseColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
		ON CONFLICT (case_id) DO UPDATE SET
			status = excluded.status,
			severity = excluded.severity,
			queue = excluded.queue,
			assignee = excluded.assignee,
			escalated = excluded.escalated,
			occurrence = excluded.occurrence,
			last_seen_at = excluded.last_seen_at,
			resolution_code = excluded.resolution_code,
			root_cause = excluded.root_cause,
			note = excluded.note,
			resolved_at = excluded.resolved_at,
			auto_resolved = excluded.auto_resolved,
			auto_rule_id = excluded.auto_rule_id,
			auto_correlated_type = excluded.auto_correlated_type,
			auto_correlated_event_id = excluded.auto_correlated_event_id,
			version = excluded.version,
			audit = excluded.audit,
			updated_at = now()
		WHERE exception_cases.version = excluded.version - 1
	`, caseArgs(s, audit)...)
	if err != nil {
		if isUniqueViolation(err, openCaseIndex) {
			return usecases.ErrOpenCaseExists
		}
		return fmt.Errorf("save exception case %s: %w", s.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return usecases.ErrConcurrentModification
	}
	return nil
}

// FindByID implements ports.ExceptionCaseRepo.
func (r *CaseRepo) FindByID(ctx context.Context, id string) (*exceptioncase.Case, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx,
		`SELECT `+caseColumns+` FROM exception_cases WHERE case_id = $1`, id)
	c, err := scanCase(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	return c, err
}

// FindOpenByDedupKey implements ports.ExceptionCaseRepo: it returns the case
// that OWNS the key — the open case when one exists (at most one can, the
// partial unique index), else the most recent RESOLVED case for the key (a
// later fact touches it and never reopens it).
func (r *CaseRepo) FindOpenByDedupKey(ctx context.Context, key shared.DedupKey) (*exceptioncase.Case, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT `+caseColumns+` FROM exception_cases
		WHERE source_type = $1 AND source_subject = $2 AND site_id = $3
		ORDER BY (status <> 'RESOLVED') DESC, opened_at DESC, case_id
		LIMIT 1
	`, key.SourceType, key.SourceSubject, key.SiteID)
	c, err := scanCase(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ports.ErrNotFound
	}
	return c, err
}

// ListOpen implements ports.ExceptionCaseRepo: every non-resolved case,
// narrowed by the non-blank filters, oldest opened first (then case id) —
// the order the in-memory adapter returns too.
func (r *CaseRepo) ListOpen(ctx context.Context, queue, siteID, sourceType string) ([]*exceptioncase.Case, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT `+caseColumns+` FROM exception_cases
		WHERE status <> 'RESOLVED'
		  AND ($1 = '' OR queue = $1)
		  AND ($2 = '' OR site_id = $2)
		  AND ($3 = '' OR source_type = $3)
		ORDER BY opened_at, case_id
	`, queue, siteID, sourceType)
	if err != nil {
		return nil, fmt.Errorf("list open exception cases: %w", err)
	}
	defer rows.Close()
	out := []*exceptioncase.Case{}
	for rows.Next() {
		c, err := scanCase(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func caseArgs(s exceptioncase.Snapshot, audit []byte) []any {
	return []any{
		s.ID, s.Key.SourceType, s.Key.SourceSubject, s.Key.SiteID,
		string(s.Status), string(s.Severity), s.Queue, s.Assignee, s.Escalated,
		s.Occurrence, s.OpenedAt.UTC(), s.LastSeenAt.UTC(), s.SLAMinutes,
		s.SourceEventID, json.RawMessage(s.SourcePayload),
		s.ResolutionCode, s.RootCause, s.Note, nullTime(s.ResolvedAt),
		s.AutoResolved, s.AutoRuleID, s.AutoType, s.AutoEventID,
		s.Version, json.RawMessage(audit),
	}
}

func scanCase(row pgx.Row) (*exceptioncase.Case, error) {
	var (
		s                  exceptioncase.Snapshot
		status, severity   string
		resolved           *time.Time
		sourcePayload, aud []byte
	)
	if err := row.Scan(
		&s.ID, &s.Key.SourceType, &s.Key.SourceSubject, &s.Key.SiteID,
		&status, &severity, &s.Queue, &s.Assignee, &s.Escalated,
		&s.Occurrence, &s.OpenedAt, &s.LastSeenAt, &s.SLAMinutes, &s.SourceEventID,
		&sourcePayload, &s.ResolutionCode, &s.RootCause, &s.Note, &resolved,
		&s.AutoResolved, &s.AutoRuleID, &s.AutoType, &s.AutoEventID,
		&s.Version, &aud,
	); err != nil {
		return nil, err
	}
	s.Status, s.Severity = exceptioncase.Status(status), shared.Severity(severity)
	s.SourcePayload = sourcePayload
	if resolved != nil {
		s.ResolvedAt = resolved.UTC()
	}
	audit, err := unmarshalAudit(aud)
	if err != nil {
		return nil, fmt.Errorf("stored audit of case %s is invalid: %w", s.ID, err)
	}
	s.Audit = audit
	c, err := exceptioncase.Restore(s)
	if err != nil {
		return nil, fmt.Errorf("stored case %s is invalid: %w", s.ID, err)
	}
	return c, nil
}

// auditWire is the stored form of one audit entry: what happened, who did
// it, when. Times round trip as RFC 3339 with nanoseconds.
type auditWire struct {
	Action    string `json:"action"`
	ActorKind string `json:"actor_kind"`
	ActorName string `json:"actor_name"`
	At        string `json:"at"`
}

func marshalAudit(entries []exceptioncase.AuditEntry) ([]byte, error) {
	out := make([]auditWire, len(entries))
	for i, e := range entries {
		out[i] = auditWire{Action: e.Action, ActorKind: string(e.Actor.Kind), ActorName: e.Actor.Name, At: e.At.UTC().Format(time.RFC3339Nano)}
	}
	return json.Marshal(out)
}

func unmarshalAudit(data []byte) ([]exceptioncase.AuditEntry, error) {
	var wire []auditWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, err
	}
	out := make([]exceptioncase.AuditEntry, len(wire))
	for i, w := range wire {
		at, err := time.Parse(time.RFC3339Nano, w.At)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		out[i] = exceptioncase.AuditEntry{
			Action: w.Action,
			Actor:  shared.Actor{Kind: shared.ActorKind(w.ActorKind), Name: w.ActorName},
			At:     at.UTC(),
		}
	}
	return out, nil
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

// isUniqueViolation reports whether err is a Postgres unique violation of
// the named constraint.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}
