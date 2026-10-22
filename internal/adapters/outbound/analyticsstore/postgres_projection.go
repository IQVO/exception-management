package analyticsstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// Projection is the Postgres WRITER (report.Projection). Apply claims the
// event id and folds the event into the fact tables in ONE transaction: the
// id is recorded if and only if its effect is, so a failure anywhere leaves
// nothing behind and the redelivered event is applied afresh, while a replay
// of an applied id is a no-op.
type Projection struct {
	pool *pgxpool.Pool
}

// NewProjection constructs a Projection over the writer pool.
func NewProjection(pool *pgxpool.Pool) *Projection { return &Projection{pool: pool} }

var _ report.Projection = (*Projection)(nil)

const claimSQL = `
	INSERT INTO analytics_processed_events (event_id, event_type, occurred_at)
	VALUES ($1, $2, $3)
	ON CONFLICT (event_id) DO NOTHING`

// The case upsert keeps the EARLIEST candidate of each milestone (LEAST
// ignores NULLs, so a NULL parameter — the event does not mark that
// milestone — keeps the stored value and the row converges whatever order
// the events arrive in). The descriptive columns keep the first non-NULL
// value; severity, queue and assignee keep the LATEST (a triage or an
// assignment supersedes the opened values — a stale event never reaches
// this upsert, the dedupe claim already stopped it).
const upsertCaseSQL = `
	INSERT INTO exception_case_facts (case_id, source_type, source_subject, site_id, severity, queue,
	                                  opened_at, sla_due_at, triaged_at, assigned_at, assignee,
	                                  escalated_at, resolved_at, auto_resolved_at,
	                                  resolution_code, root_cause, resolution_note, rule_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
	ON CONFLICT (case_id) DO UPDATE SET
		source_type     = COALESCE(exception_case_facts.source_type, EXCLUDED.source_type),
		source_subject  = COALESCE(exception_case_facts.source_subject, EXCLUDED.source_subject),
		site_id         = COALESCE(exception_case_facts.site_id, EXCLUDED.site_id),
		severity        = COALESCE(EXCLUDED.severity, exception_case_facts.severity),
		queue           = COALESCE(EXCLUDED.queue, exception_case_facts.queue),
		assignee        = COALESCE(EXCLUDED.assignee, exception_case_facts.assignee),
		resolution_code = COALESCE(exception_case_facts.resolution_code, EXCLUDED.resolution_code),
		root_cause      = COALESCE(exception_case_facts.root_cause, EXCLUDED.root_cause),
		resolution_note = COALESCE(exception_case_facts.resolution_note, EXCLUDED.resolution_note),
		rule_id         = COALESCE(exception_case_facts.rule_id, EXCLUDED.rule_id),
		opened_at        = LEAST(exception_case_facts.opened_at, EXCLUDED.opened_at),
		sla_due_at       = LEAST(exception_case_facts.sla_due_at, EXCLUDED.sla_due_at),
		triaged_at       = LEAST(exception_case_facts.triaged_at, EXCLUDED.triaged_at),
		assigned_at      = LEAST(exception_case_facts.assigned_at, EXCLUDED.assigned_at),
		escalated_at     = LEAST(exception_case_facts.escalated_at, EXCLUDED.escalated_at),
		resolved_at      = LEAST(exception_case_facts.resolved_at, EXCLUDED.resolved_at),
		auto_resolved_at = LEAST(exception_case_facts.auto_resolved_at, EXCLUDED.auto_resolved_at)`

const upsertPolicySQL = `
	INSERT INTO exception_policy_facts (source_type, version, severity, queue, sla_minutes,
	                                    auto_resolve_on_type, auto_resolve_within_minutes, declared_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFLICT (source_type, version) DO UPDATE SET
		declared_at = LEAST(exception_policy_facts.declared_at, EXCLUDED.declared_at)`

// Apply implements report.Projection.
func (p *Projection) Apply(ctx context.Context, e report.Event) (bool, error) {
	if err := e.Validate(); err != nil {
		// A pure check of the decoded event: deterministic, the same bytes
		// always fail (the consumer dead-letters it).
		return false, fmt.Errorf("%w: %w", report.ErrRejected, err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("analyticsstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	tag, err := tx.Exec(ctx, claimSQL, e.EventID, string(e.Kind), e.At.UTC())
	if err != nil {
		return false, classify(fmt.Errorf("analyticsstore: claim event %s: %w", e.EventID, err))
	}
	if tag.RowsAffected() == 0 {
		return false, nil // already applied: the deferred rollback ends the empty tx
	}
	if err := fold(ctx, tx, e); err != nil {
		return false, classify(fmt.Errorf("analyticsstore: project %s %s: %w", e.Kind, e.EventID, err))
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("analyticsstore: commit: %w", err)
	}
	return true, nil
}

// fold writes the facts of one event inside tx.
func fold(ctx context.Context, tx pgx.Tx, e report.Event) error {
	if e.Kind == report.KindExceptionPolicyDeclared {
		var on, within any
		if e.AutoResolve != nil {
			on, within = e.AutoResolve.OnType, e.AutoResolve.WithinMinutes
		}
		_, err := tx.Exec(ctx, upsertPolicySQL, e.SourceType, e.Version, e.Severity, e.Queue, e.SLAMinutes,
			on, within, e.At.UTC())
		return err
	}
	m := report.MilestonesOf(e)
	_, err := tx.Exec(ctx, upsertCaseSQL, caseArgs(e, m)...)
	return err
}

// caseArgs fills the 18 parameters of upsertCaseSQL from the event and its
// milestones. Milestone candidates are nil when the event marks nothing;
// severity/queue ride only the events that set them (Opened, Triaged).
func caseArgs(e report.Event, m report.Milestones) []any {
	var severity, queue, assignee any
	switch e.Kind {
	case report.KindExceptionOpened, report.KindExceptionTriaged:
		severity, queue = e.Severity, e.Queue
	case report.KindExceptionAssigned:
		assignee = e.Assignee
	case report.KindExceptionResolved:
	case report.KindExceptionAutoResolved:
	case report.KindExceptionEscalated:
	}
	var sourceType, sourceSubject, siteID any
	if e.Kind == report.KindExceptionOpened {
		sourceType, sourceSubject, siteID = e.SourceType, e.SourceSubject, deref(e.SiteID)
	}
	var note, rootCause, code, ruleID any
	if e.Kind == report.KindExceptionResolved {
		code, rootCause, note = e.ResolutionCode, e.RootCause, deref(e.Note)
	}
	if e.Kind == report.KindExceptionAutoResolved {
		ruleID = e.RuleID
	}
	return []any{e.CaseID, sourceType, sourceSubject, siteID, severity, queue,
		m.Opened, m.SLADue, m.Triaged, m.Assigned, assignee,
		m.Escalated, m.Resolved, m.AutoResolved,
		code, rootCause, note, ruleID}
}

func deref(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// classify wraps a Postgres data-exception (SQLSTATE class 22) or
// integrity-violation (class 23) error in report.ErrRejected: the same event
// can never succeed, so the consumer dead-letters it instead of retrying
// forever. Everything else (connection loss, timeouts, locks, failovers) stays
// transient.
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 && (pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23") {
		return fmt.Errorf("%w: %w", report.ErrRejected, err)
	}
	return err
}
