package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// Reader is the Postgres READER (report.Reader). Every window is half-open
// [lo, hi) over the timestamp each report counts on. SQL counts and takes
// averages/percentiles (percentile_cont, the definition report.Percentile
// mirrors); the range rules, the shares and the freshness lag live in
// internal/analytics/report (pure, imports nothing internal).
type Reader struct {
	pool *pgxpool.Pool
}

// NewReader constructs a Reader over the (read-only) reports pool.
func NewReader(pool *pgxpool.Pool) *Reader { return &Reader{pool: pool} }

var _ report.Reader = (*Reader)(nil)

// A case's resolution instant: the manual resolution when there is one, the
// auto resolution otherwise. A case the projection has only auto-resolved
// still counts as resolved (it left the queue) but carries no root cause.
const resolvedInstant = "COALESCE(resolved_at, auto_resolved_at)"

// rootCauseSQL answers the root-cause-frequency report: the MANUAL
// resolutions of the window (a root cause exists only on ExceptionResolved)
// grouped by source type and root cause, with the source type's total
// resolutions (auto-resolved included) as the share denominator. A case whose
// OPENED event has not been seen has no source type and counts in no group.
const rootCauseSQL = `
	WITH totals AS (
		SELECT source_type, count(*) AS total
		FROM exception_case_facts
		WHERE ` + resolvedInstant + ` >= $1 AND ` + resolvedInstant + ` < $2
		GROUP BY source_type
	)
	SELECT f.source_type, f.root_cause, count(*)::bigint, t.total::bigint
	FROM exception_case_facts f
	JOIN totals t ON t.source_type = f.source_type
	WHERE f.resolved_at >= $1 AND f.resolved_at < $2
	  AND f.root_cause IS NOT NULL AND f.root_cause <> ''
	GROUP BY f.source_type, f.root_cause, t.total
	ORDER BY count(*) DESC, f.source_type, f.root_cause`

// RootCauseRows implements report.Reader.
func (r *Reader) RootCauseRows(ctx context.Context, q report.Query) ([]report.RootCauseRow, error) {
	rows, err := r.pool.Query(ctx, rootCauseSQL, q.Range.From.UTC(), q.Range.To.UTC())
	if err != nil {
		return nil, fmt.Errorf("analyticsstore: root cause rows: %w", err)
	}
	defer rows.Close()
	out := []report.RootCauseRow{}
	for rows.Next() {
		var row report.RootCauseRow
		var total int64
		if err := rows.Scan(&row.SourceType, &row.RootCause, &row.Resolved, &total); err != nil {
			return nil, fmt.Errorf("analyticsstore: scan root cause row: %w", err)
		}
		row.Share = report.Share(row.Resolved, total)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyticsstore: root cause rows: %w", err)
	}
	return out, nil
}

// queueTimingSQL answers the mean-time-to-resolve report: the
// opened-to-resolve duration (never negative: events can arrive out of
// order) of the cases RESOLVED in the window, per current queue.
const queueTimingSQL = `
	WITH resolved_cases AS (
		SELECT queue,
		       GREATEST(0, EXTRACT(EPOCH FROM (` + resolvedInstant + ` - opened_at)))::float8 AS secs
		FROM exception_case_facts
		WHERE opened_at IS NOT NULL
		  AND ` + resolvedInstant + ` >= $1 AND ` + resolvedInstant + ` < $2
	)
	SELECT queue, count(*)::bigint,
	       avg(secs),
	       percentile_cont(0.50) WITHIN GROUP (ORDER BY secs),
	       percentile_cont(0.95) WITHIN GROUP (ORDER BY secs)
	FROM resolved_cases
	GROUP BY queue
	ORDER BY queue`

// QueueTimingRows implements report.Reader.
func (r *Reader) QueueTimingRows(ctx context.Context, q report.Query) ([]report.QueueTimingRow, error) {
	rows, err := r.pool.Query(ctx, queueTimingSQL, q.Range.From.UTC(), q.Range.To.UTC())
	if err != nil {
		return nil, fmt.Errorf("analyticsstore: queue timing rows: %w", err)
	}
	defer rows.Close()
	out := []report.QueueTimingRow{}
	for rows.Next() {
		var row report.QueueTimingRow
		if err := rows.Scan(&row.Queue, &row.Resolved, &row.MeanSeconds, &row.P50Seconds, &row.P95Seconds); err != nil {
			return nil, fmt.Errorf("analyticsstore: scan queue timing row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyticsstore: queue timing rows: %w", err)
	}
	return out, nil
}

// slaBreachSQL answers the sla-breach-rate report over the cases OPENED in
// the window, per source type. A case breached its SLA when it was escalated
// or resolved after its due time; escalated_at IS NOT NULL is the sweep's
// verdict, the late-resolved arm catches a resolution that landed after the
// due time without a sweep having run yet.
const slaBreachSQL = `
	SELECT source_type,
	       count(*)::bigint,
	       count(escalated_at)::bigint,
	       count(*) FILTER (
	         WHERE escalated_at IS NOT NULL
	            OR (` + resolvedInstant + ` IS NOT NULL AND ` + resolvedInstant + ` > sla_due_at)
	       )::bigint
	FROM exception_case_facts
	WHERE opened_at >= $1 AND opened_at < $2
	GROUP BY source_type
	ORDER BY source_type`

// SLABreachRows implements report.Reader.
func (r *Reader) SLABreachRows(ctx context.Context, q report.Query) ([]report.SLABreachRow, error) {
	rows, err := r.pool.Query(ctx, slaBreachSQL, q.Range.From.UTC(), q.Range.To.UTC())
	if err != nil {
		return nil, fmt.Errorf("analyticsstore: sla breach rows: %w", err)
	}
	defer rows.Close()
	out := []report.SLABreachRow{}
	for rows.Next() {
		var row report.SLABreachRow
		if err := rows.Scan(&row.SourceType, &row.Opened, &row.Escalated, &row.Breached); err != nil {
			return nil, fmt.Errorf("analyticsstore: scan sla breach row: %w", err)
		}
		row.BreachRate = report.Share(row.Breached, row.Opened)
		row.EscalationRate = report.Share(row.Escalated, row.Opened)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyticsstore: sla breach rows: %w", err)
	}
	return out, nil
}

// LastEventAt implements report.Reader: the newest CloudEvents time among the
// applied events, nil while none was applied.
func (r *Reader) LastEventAt(ctx context.Context) (*time.Time, error) {
	var at *time.Time
	if err := r.pool.QueryRow(ctx, `SELECT max(occurred_at) FROM analytics_processed_events`).Scan(&at); err != nil {
		return nil, fmt.Errorf("analyticsstore: last event: %w", err)
	}
	if at != nil {
		utc := at.UTC()
		at = &utc
	}
	return at, nil
}
