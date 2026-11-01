package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// ListPage implements ports.ExceptionCaseRepo: the cases matching the filter
// (Status "" = every status, resolved cases included — resolve activity is
// part of the record, the GET /exceptions contract), NEWEST opened first
// (opened_at descending, then case id descending), strictly after the keyset
// position, at most limit rows — the order and the page the in-memory
// adapter returns too. The row comparison (opened_at, case_id) < (after)
// is the keyset: rows sharing an opened_at never skip or repeat.
func (r *CaseRepo) ListPage(ctx context.Context, f repository.CaseFilter, after *repository.CaseAfter, limit int) ([]*exceptioncase.Case, error) {
	var (
		afterAt *time.Time
		afterID *string
	)
	if after != nil {
		at := after.OpenedAt.UTC()
		afterAt, afterID = &at, &after.ID
	}
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT `+caseColumns+` FROM exception_cases
		WHERE ($1 = '' OR status = $1)
		  AND ($2 = '' OR queue = $2)
		  AND ($3 = '' OR site_id = $3)
		  AND ($4 = '' OR source_type = $4)
		  AND ($5::timestamptz IS NULL OR (opened_at, case_id) < ($5::timestamptz, $6::text))
		ORDER BY opened_at DESC, case_id DESC
		LIMIT $7
	`, f.Status, f.Queue, f.SiteID, f.SourceType, afterAt, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list exception cases: %w", err)
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
