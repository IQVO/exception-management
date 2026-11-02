package http

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"
)

// walkCases follows nextCursor from the first page of target (which already
// carries its filters and limit) and returns the case ids in the order they
// arrived. Filters are repeated on every page request, as the contract says.
func walkCases(t *testing.T, h http.Handler, target string) []string {
	t.Helper()
	var ids []string
	cursor := ""
	for range 50 {
		u := target
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		}
		var pg casePage
		decodeBody(t, mustCall(t, h, http.MethodGet, u, "", http.StatusOK, nil), &pg)
		for _, c := range pg.Items {
			ids = append(ids, c.CaseID)
		}
		if pg.NextCursor == "" {
			return ids
		}
		cursor = pg.NextCursor
	}
	t.Fatalf("%s: the page walk did not end", target)
	return nil
}

// TestListExceptionsPagesWalkEveryCaseOnce: more cases than limit, several
// sharing an opened-at, are walked page by page with no duplicate and no
// skipped row, newest first.
func TestListExceptionsPagesWalkEveryCaseOnce(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	// Seven cases on three instants: three, two and two cases tie.
	var all []string
	for i, minute := range []int{0, 0, 0, 1, 1, 2, 2} {
		site := "SIM1"
		if i%2 == 1 {
			site = "SIM2"
		}
		all = append(all, f.openCase(t, fmt.Sprintf("ord-%d-line-1", i), site, time.Duration(minute)*time.Minute))
	}
	var want []string
	{
		var pg casePage
		decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?limit=500", "", http.StatusOK, nil), &pg)
		for _, c := range pg.Items {
			want = append(want, c.CaseID)
		}
	}
	if !sameIDs(t, want, all) {
		t.Fatalf("limit=500 answered %v for cases %v", want, all)
	}
	for _, limit := range []int{1, 2, 3, 7, 8} {
		got := walkCases(t, f.router(), fmt.Sprintf("/exceptions?limit=%d", limit))
		if !slices.Equal(got, want) {
			t.Fatalf("limit=%d walked %v, want %v", limit, got, want)
		}
	}
	// A filter narrows every page.
	got := walkCases(t, f.router(), "/exceptions?limit=2&site=SIM2")
	if len(got) != 3 {
		t.Fatalf("site=SIM2 walked %v, want the 3 SIM2 cases", got)
	}
	// The default limit is 100: a short collection has no next page.
	var pg casePage
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions", "", http.StatusOK, nil), &pg)
	if len(pg.Items) != 7 || pg.NextCursor != "" {
		t.Fatalf("default page = %d items, nextCursor %q", len(pg.Items), pg.NextCursor)
	}
}

// sameIDs reports whether got holds exactly the ids of all, in any order.
func sameIDs(t *testing.T, got, all []string) bool {
	t.Helper()
	a, b := slices.Clone(got), slices.Clone(all)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

func TestListPoliciesPagesWalkEveryPolicyOnce(t *testing.T) {
	f := newFixture(t)
	var want []string
	for i := range 5 {
		sourceType := fmt.Sprintf("com.warehouse.wes.fulfillment-execution.task.Paged%d", i)
		f.declarePolicy(t, sourceType, 30)
		want = append(want, sourceType)
	}
	for _, limit := range []int{1, 2, 5, 6} {
		var got []string
		cursor := ""
		for range 20 {
			u := fmt.Sprintf("/exception-policies?limit=%d", limit)
			if cursor != "" {
				u += "&cursor=" + url.QueryEscape(cursor)
			}
			var pg policyPage
			decodeBody(t, mustCall(t, f.router(), http.MethodGet, u, "", http.StatusOK, nil), &pg)
			for _, p := range pg.Items {
				got = append(got, p.SourceType)
			}
			if cursor = pg.NextCursor; cursor == "" {
				break
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit=%d walked %v, want %v", limit, got, want)
		}
	}
}

// TestListsRejectBadPagingParameters: invalid limit, undecodable cursor and
// every parameter a collection does not declare (including the retired
// paging ones) are 400 problems, never a silent first page.
func TestListsRejectBadPagingParameters(t *testing.T) {
	f := newFixture(t)
	h := f.router()
	for _, base := range []string{"/exceptions", "/exception-policies"} {
		for query, slug := range map[string]string{
			"limit=0":                               "invalid-limit",
			"limit=501":                             "invalid-limit",
			"limit=abc":                             "invalid-limit",
			"limit=-1":                              "invalid-limit",
			"cursor=!!!":                            "invalid-cursor",
			"cursor=e30":                            "invalid-cursor", // {} has no id
			"cursor=eyJzIjoieCIsImkiOiJjYXNlLTEifQ": "invalid-cursor", // opened-at "x" / id "case-1" (policies accept it: see below)
			"after=case-1":                          "unknown-query-parameter",
			"offset=10":                             "unknown-query-parameter",
			"nextToken=abc":                         "unknown-query-parameter",
			"from=2026-01-01T00:00:00Z":             "unknown-query-parameter",
		} {
			if base == "/exception-policies" && query == "cursor=eyJzIjoieCIsImkiOiJjYXNlLTEifQ" {
				continue // a policy cursor's id is a source type: any non-empty id decodes
			}
			t.Run(base+"?"+query, func(t *testing.T) {
				wantProblem(t, mustCall(t, h, http.MethodGet, base+"?"+query, "", http.StatusBadRequest, nil),
					http.StatusBadRequest, slug)
			})
		}
	}
	// limit=1 and limit=500 are the inclusive bounds.
	mustCall(t, h, http.MethodGet, "/exceptions?limit=1", "", http.StatusOK, nil)
	mustCall(t, h, http.MethodGet, "/exceptions?limit=500", "", http.StatusOK, nil)
}
