package http

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/inbound/http/page"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/application/views"
)

// errUnknownQueryParameter marks a query parameter the collection does not
// declare (REST guideline 4.2): a consumer still sending a retired paging
// parameter (after, from, offset, nextToken) must fail loudly, not read page
// one forever. The wrapped text names the parameter.
var errUnknownQueryParameter = errors.New("query parameter is not supported")

// pageParams reads limit and cursor of a collection GET and rejects any
// query parameter that is neither one of them nor one of the filters.
func pageParams(r *http.Request, filters ...string) (int, *page.Position, error) {
	if name := page.Unknown(r, filters...); name != "" {
		return 0, nil, fmt.Errorf("%w: %s", errUnknownQueryParameter, logSafe(name))
	}
	limit, err := page.Limit(r)
	if err != nil {
		return 0, nil, err
	}
	after, err := page.After(r)
	if err != nil {
		return 0, nil, err
	}
	return limit, after, nil
}

// casePosition is the cursor position of a case: its opened-at and its id.
func casePosition(v views.CaseView) page.Position {
	return page.Position{Sort: v.OpenedAt.UTC().Format(time.RFC3339Nano), ID: v.ID}
}

// caseAfter decodes a cursor position into the repository keyset; an
// opened-at that does not parse is an invalid cursor.
func caseAfter(p *page.Position) (*repository.CaseAfter, error) {
	if p == nil {
		return nil, nil
	}
	at, err := time.Parse(time.RFC3339Nano, p.Sort)
	if err != nil {
		return nil, page.ErrInvalidCursor
	}
	return &repository.CaseAfter{OpenedAt: at.UTC(), ID: p.ID}, nil
}

// handleListExceptions backs GET /exceptions: 200 with one page of the
// matching cases, {items, nextCursor} (an EMPTY page, never a 404). status,
// queue, site and sourceType narrow the list; no status filter means every
// status, resolved cases included. Ordered by opened-at then case id,
// newest first.
func (s *Server) handleListExceptions(w http.ResponseWriter, r *http.Request) {
	limit, pos, err := pageParams(r, "status", "queue", "site", "sourceType")
	if err != nil {
		writeError(w, r, err)
		return
	}
	q := r.URL.Query()
	status, err := statusFilter(q.Get("status"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	after, err := caseAfter(pos)
	if err != nil {
		writeError(w, r, err)
		return
	}
	f := repository.CaseFilter{Status: status, Queue: q.Get("queue"), SiteID: q.Get("site"), SourceType: q.Get("sourceType")}
	rows, err := s.List.Page(r.Context(), f, after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := page.Of(rows, limit, casePosition)
	items := make([]caseDTO, 0, len(out.Items))
	for _, v := range out.Items {
		items = append(items, toCase(v))
	}
	writeJSON(w, http.StatusOK, page.Page[caseDTO]{Items: items, NextCursor: out.NextCursor})
}

// policyPosition is the cursor position of a policy: its source type, which
// is both the sort key and the id (one row per source type).
func policyPosition(v views.PolicyView) page.Position {
	return page.Position{Sort: v.SourceType, ID: v.SourceType}
}

// handleListPolicies backs GET /exception-policies: one page of the current
// policy version of every declared source type, {items, nextCursor}, ordered
// by source type (an empty page is 200 with {"items": []}).
func (s *Server) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	limit, pos, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	after := ""
	if pos != nil {
		after = pos.ID
	}
	rows, err := s.ListPolicies.Page(r.Context(), after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := page.Of(rows, limit, policyPosition)
	items := make([]policyDTO, 0, len(out.Items))
	for _, v := range out.Items {
		items = append(items, toPolicy(v))
	}
	writeJSON(w, http.StatusOK, page.Page[policyDTO]{Items: items, NextCursor: out.NextCursor})
}
