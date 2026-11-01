package usecases_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The MCP tools and the A2A skills keep their own unpaged schemas, so the
// listing use cases' Execute must walk every keyset page: more rows than one
// page (ListPageSize) all come back, once each, in order.

func TestListOpenExceptions_ExecuteWalksEveryPage(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, false)
	const total = usecases.ListPageSize*2 + 5
	for i := range total {
		// Every 7th case ties with its neighbour on opened-at.
		at := f.now.Add(time.Duration(i-i%7) * time.Second)
		uc := &usecases.OpenOrTouchException{Cases: f.cases, Policies: f.pol, Events: f.ev, IDs: f.ids, Clock: memory.Clock{T: at}}
		fact := usecases.Fact{SourceType: srcType, SourceSubject: fmt.Sprintf("ord-%d-line-1", i), SiteID: "SIM1",
			SourceEventID: fmt.Sprintf("ev-%d", i), Payload: []byte(`{"a":1}`)}
		if _, err := uc.Execute(context.Background(), fact); err != nil {
			t.Fatal(err)
		}
	}
	list := &usecases.ListOpenExceptions{Cases: f.cases}
	got, err := list.Execute(context.Background(), "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != total {
		t.Fatalf("Execute returned %d cases, want all %d", len(got), total)
	}
	seen := map[string]bool{}
	for i, v := range got {
		if seen[v.ID] {
			t.Fatalf("case %s returned twice", v.ID)
		}
		seen[v.ID] = true
		if i > 0 && (got[i-1].OpenedAt.Before(v.OpenedAt) || (got[i-1].OpenedAt.Equal(v.OpenedAt) && got[i-1].ID < v.ID)) {
			t.Fatalf("order broken at %d: %s then %s", i, got[i-1].ID, v.ID)
		}
	}
	// Page is the single-page primitive REST uses: limit rows, nil position = first page.
	first, err := list.Page(context.Background(), repository.CaseFilter{}, nil, 3)
	if err != nil || len(first) != 3 || first[0].ID != got[0].ID {
		t.Fatalf("Page = %d rows (%v), want the 3 newest", len(first), err)
	}
}

func TestListExceptionPolicies_ExecuteWalksEveryPage(t *testing.T) {
	f := newFixture(t)
	const total = usecases.ListPageSize + 7
	for i := range total {
		in := usecases.PolicyDeclaration{
			SourceType: fmt.Sprintf("com.warehouse.wes.fulfillment-execution.task.T%04d", i),
			Severity:   shared.SeverityHigh, Queue: "ps-main", SLAMinutes: 30, Actor: "op-1",
		}
		uc := &usecases.DeclareExceptionPolicy{Policies: f.pol, Events: f.ev, Clock: memory.Clock{T: f.now}}
		if _, err := uc.Execute(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	}
	list := &usecases.ListExceptionPolicies{Policies: f.pol}
	got, err := list.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != total {
		t.Fatalf("Execute returned %d policies, want all %d", len(got), total)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].SourceType >= got[i].SourceType {
			t.Fatalf("not strictly ordered by source type at %d: %s then %s", i, got[i-1].SourceType, got[i].SourceType)
		}
	}
}
