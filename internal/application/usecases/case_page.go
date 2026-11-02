package usecases

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/application/views"
)

// DefaultListLimit and MaxListLimit bound every paged listing of the
// in-process readers (the MCP tool); the REST page package carries the same
// numbers for the query string.
const (
	DefaultListLimit = 100
	MaxListLimit     = 500
)

// ErrInvalidListQuery marks a limit or cursor a paged listing rejects; the
// MCP adapter maps it to its invalid-query tool error (page.ErrInvalidLimit
// and page.ErrInvalidCursor are the REST adapter's 400 equivalents).
var ErrInvalidListQuery = errors.New("list limit or cursor is invalid")

// CasePage is one page of the operator listing: the cases of this page and
// the opaque cursor of the next one ("" on the last page).
type CasePage struct {
	Items      []views.CaseView
	NextCursor string
}

// Paged serves one page of the operator listing for the in-process readers
// (the MCP tool): at most limit cases (0 means DefaultListLimit; bounds
// 1..MaxListLimit), newest opened first, strictly after the opaque cursor of
// the previous page ("" means the first page). The cursor is the fleet
// keyset token — base64url {"s":<opened-at>,"i":<case id>} — the same form
// the REST adapter serves, so a consumer can carry one between the
// surfaces. A bad limit or cursor fails with ErrInvalidListQuery.
func (uc *ListOpenExceptions) Paged(ctx context.Context, status, queue, siteID, sourceType string, limit int, cursor string) (CasePage, error) {
	n, err := listLimit(limit)
	if err != nil {
		return CasePage{}, err
	}
	after, err := caseAfterCursor(cursor)
	if err != nil {
		return CasePage{}, err
	}
	f := repository.CaseFilter{Status: status, Queue: queue, SiteID: siteID, SourceType: sourceType}
	rows, err := uc.Page(ctx, f, after, n+1)
	if err != nil {
		return CasePage{}, err
	}
	out := CasePage{Items: rows}
	if len(rows) > n {
		out.Items = rows[:n]
		out.NextCursor = encodeCaseCursor(out.Items[n-1])
	}
	return out, nil
}

// listLimit applies the paging bounds: 0 means the default page size.
func listLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultListLimit, nil
	}
	if limit < 1 || limit > MaxListLimit {
		return 0, fmt.Errorf("%w: limit must be 1..%d", ErrInvalidListQuery, MaxListLimit)
	}
	return limit, nil
}

// caseAfterCursor decodes the opaque cursor into the repository keyset; ""
// means the first page, anything malformed is ErrInvalidListQuery.
func caseAfterCursor(cursor string) (*repository.CaseAfter, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor is not a valid page token", ErrInvalidListQuery)
	}
	var p struct {
		Sort string `json:"s"`
		ID   string `json:"i"`
	}
	if err := json.Unmarshal(raw, &p); err != nil || p.ID == "" {
		return nil, fmt.Errorf("%w: cursor is not a valid page token", ErrInvalidListQuery)
	}
	at, err := time.Parse(time.RFC3339Nano, p.Sort)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor is not a valid page token", ErrInvalidListQuery)
	}
	return &repository.CaseAfter{OpenedAt: at.UTC(), ID: p.ID}, nil
}

// encodeCaseCursor builds the opaque cursor pointing after v.
func encodeCaseCursor(v views.CaseView) string {
	b, _ := json.Marshal(struct {
		Sort string `json:"s"`
		ID   string `json:"i"`
	}{Sort: v.OpenedAt.UTC().Format(time.RFC3339Nano), ID: v.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}
