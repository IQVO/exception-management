package repocontract

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// maxPages bounds a page walk so a repository that never ends fails the
// test instead of hanging it.
const maxPages = 100

// listAll returns every case matching f in one page large enough for the
// fixtures of this suite.
func listAll(t *testing.T, r ports.ExceptionCaseRepo, f repository.CaseFilter) []*exceptioncase.Case {
	t.Helper()
	got, err := r.ListPage(bg, f, nil, 1000)
	must(t, err)
	return got
}

// listAllPolicies returns every policy in one page large enough for the
// fixtures of this suite.
func listAllPolicies(t *testing.T, r ports.ExceptionPolicyRepo) []*exceptionpolicy.Policy {
	t.Helper()
	got, err := r.ListPage(bg, "", 1000)
	must(t, err)
	return got
}

// walkCases follows the keyset from the first page with the given page size
// until a short page, and returns the ids in the order they came.
func walkCases(t *testing.T, r ports.ExceptionCaseRepo, f repository.CaseFilter, limit int) []string {
	t.Helper()
	var (
		got   []string
		after *repository.CaseAfter
	)
	for range maxPages {
		rows, err := r.ListPage(bg, f, after, limit)
		must(t, err)
		got = append(got, ids(rows)...)
		if len(rows) < limit {
			return got
		}
		last := rows[len(rows)-1]
		after = &repository.CaseAfter{OpenedAt: last.OpenedAt(), ID: last.ID()}
	}
	t.Fatalf("the page walk did not end in %d pages", maxPages)
	return nil
}

// caseListPages is the paging contract: more cases than the page size, with
// several cases sharing an opened-at, are walked page by page and come back
// exactly once each, in the (opened-at, id) descending order.
func caseListPages(t *testing.T, r ports.ExceptionCaseRepo) {
	type fixture struct {
		id string
		at time.Time
	}
	var all []fixture
	// Seven cases on three instants: three, two and two cases tie.
	for i, offset := range []int{0, 0, 0, 1, 1, 2, 2} {
		id := fmt.Sprintf("case-p%d", i+1)
		at := T0.Add(time.Duration(offset) * time.Minute)
		save(t, r, Opened(t, id, fmt.Sprintf("ord-p%d-line-1", i+1), at))
		all = append(all, fixture{id, at})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].at.Equal(all[j].at) {
			return all[i].id > all[j].id
		}
		return all[i].at.After(all[j].at)
	})
	want := make([]string, len(all))
	for i, f := range all {
		want[i] = f.id
	}
	for _, limit := range []int{1, 2, 3, 7, 8} {
		got := walkCases(t, r, repository.CaseFilter{}, limit)
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d walked %v, want %v (no duplicate, no skipped row)", limit, got, want)
		}
	}
	// A filter is repeated on every page request and narrows every page.
	got := walkCases(t, r, repository.CaseFilter{Status: "OPEN", Queue: "ps-main"}, 2)
	if !slices.Equal(got, want) {
		t.Fatalf("filtered walk = %v, want %v", got, want)
	}
	if got := walkCases(t, r, repository.CaseFilter{Status: "RESOLVED"}, 2); len(got) != 0 {
		t.Fatalf("a filter matching nothing walked %v, want no rows", got)
	}
}

// policyListPages walks the policies page by page: each source type exactly
// once, latest version only, ordered by source type.
func policyListPages(t *testing.T, r ports.ExceptionPolicyRepo) {
	var want []string
	for i := range 5 {
		sourceType := fmt.Sprintf("com.warehouse.wes.fulfillment-execution.task.Paged%d", i)
		must(t, r.Save(bg, declare(t, sourceType, 1, false, T0)))
		want = append(want, sourceType)
	}
	// A second version of one type must not appear as a second row.
	must(t, r.Save(bg, declare(t, want[2], 2, true, T0.Add(time.Minute))))
	for _, limit := range []int{1, 2, 5, 6} {
		var got []string
		after := ""
		for page := 0; ; page++ {
			if page == maxPages {
				t.Fatalf("limit %d: the page walk did not end", limit)
			}
			rows, err := r.ListPage(bg, after, limit)
			must(t, err)
			for _, p := range rows {
				got = append(got, p.SourceType())
			}
			if len(rows) < limit {
				break
			}
			after = rows[len(rows)-1].SourceType()
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d walked %v, want %v", limit, got, want)
		}
	}
}
