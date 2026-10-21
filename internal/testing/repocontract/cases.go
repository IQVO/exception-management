package repocontract

import (
	"slices"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// CaseRepo runs the ExceptionCaseRepo contract: Save/Find equivalence, the
// one-open-case-per-dedup-key invariant, the optimistic-concurrency version
// guard and the open-case listing. newRepo must return an EMPTY repository
// on every call.
func CaseRepo(t *testing.T, newRepo func(t *testing.T) ports.ExceptionCaseRepo) {
	t.Helper()
	cases := map[string]func(t *testing.T, r ports.ExceptionCaseRepo){
		"an unknown case is not found":                       caseNotFound,
		"save then find by id round trips an open case":      caseRoundTripOpen,
		"a touched case round trips occurrence and audit":    caseRoundTripTouch,
		"a resolved case round trips its resolution":         caseRoundTripResolved,
		"an auto-resolved case round trips its evidence":     caseRoundTripAutoResolved,
		"a second open case for a dedup key is refused":      caseOneOpenPerKey,
		"the one-open rule is scoped to the whole key":       caseOneOpenScope,
		"a resolved case frees the key for a new case":       caseResolvedFreesKey,
		"an update is guarded by the loaded version":         caseVersionGuard,
		"a re-save of an unchanged case conflicts":           caseResaveConflicts,
		"a restored case has no pending events":              caseNoPendingEvents,
		"list open excludes resolved and narrows by filters": caseListOpen,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newRepo(t)) })
	}
}

func save(t *testing.T, r ports.ExceptionCaseRepo, c *exceptioncase.Case) {
	t.Helper()
	must(t, r.Save(bg, c))
}

func findByID(t *testing.T, r ports.ExceptionCaseRepo, id string) *exceptioncase.Case {
	t.Helper()
	c, err := r.FindByID(bg, id)
	must(t, err)
	return c
}

func owner(t *testing.T, r ports.ExceptionCaseRepo, subject string) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(SrcType, subject, SiteID)
	must(t, err)
	c, err := r.FindOpenByDedupKey(bg, key)
	must(t, err)
	return c
}

func ids(cases []*exceptioncase.Case) []string {
	out := make([]string, len(cases))
	for i, c := range cases {
		out[i] = c.ID()
	}
	return out
}

func equalIDs(got, want []string) bool {
	return slices.Equal(got, want)
}

func caseNotFound(t *testing.T, r ports.ExceptionCaseRepo) {
	_, err := r.FindByID(bg, "case-404")
	wantErr(t, err, ports.ErrNotFound)
	key, kerr := shared.NewDedupKey(SrcType, "ord-none-line-1", SiteID)
	must(t, kerr)
	_, err = r.FindOpenByDedupKey(bg, key)
	wantErr(t, err, ports.ErrNotFound)
}

func caseRoundTripOpen(t *testing.T, r ports.ExceptionCaseRepo) {
	in := Opened(t, "case-1", Subject, T0)
	save(t, r, in)
	if got := findByID(t, r, "case-1").Snapshot(); !sameSnapshot(got, in.Snapshot()) {
		t.Fatalf("got %+v\nwant %+v", got, in.Snapshot())
	}
}

func caseRoundTripTouch(t *testing.T, r ports.ExceptionCaseRepo) {
	in := Opened(t, "case-1", Subject, T0)
	save(t, r, in)
	stored := owner(t, r, Subject)
	must(t, stored.Touch(T0.Add(5*time.Minute)))
	save(t, r, stored)
	got := findByID(t, r, "case-1")
	if got.Occurrence() != 2 || got.Version() != 2 || got.LastSeenAt() != T0.Add(5*time.Minute) {
		t.Fatalf("touch did not round trip: occurrence=%d version=%d lastSeen=%v", got.Occurrence(), got.Version(), got.LastSeenAt())
	}
	audit := got.AuditTrail()
	if len(audit) != 2 || audit[1].Action != exceptioncase.ActionTouched {
		t.Fatalf("audit = %+v, want OPENED then TOUCHED", audit)
	}
}

func caseRoundTripResolved(t *testing.T, r ports.ExceptionCaseRepo) {
	c := Opened(t, "case-1", Subject, T0)
	must(t, c.Triage(shared.SeverityCritical, "ps-urgent", mustActor(t, "op-9"), T0.Add(time.Minute)))
	must(t, c.Assign("op-7", mustActor(t, "op-9"), T0.Add(2*time.Minute)))
	must(t, c.Resolve("RELOCATED", "bin-mismatch", "moved by hand", mustActor(t, "op-7"), T0.Add(3*time.Minute)))
	save(t, r, c)

	got := findByID(t, r, "case-1")
	if got.Status() != exceptioncase.StatusResolved || got.Assignee() != "op-7" || got.Queue() != "ps-urgent" {
		t.Fatalf("resolved state did not round trip: %+v", got.Snapshot())
	}
	if got.ResolutionCode() != "RELOCATED" || got.RootCause() != "bin-mismatch" || got.Note() != "moved by hand" {
		t.Fatalf("resolution did not round trip: %+v", got.Snapshot())
	}
	if got.AutoResolution() != nil {
		t.Fatalf("a manual resolution carries no auto evidence: %+v", got.AutoResolution())
	}
	audit := got.AuditTrail()
	if len(audit) != 4 { // OPENED, TRIAGED, ASSIGNED, RESOLVED
		t.Fatalf("audit = %d entries, want 4: %+v", len(audit), audit)
	}
}

func caseRoundTripAutoResolved(t *testing.T, r ports.ExceptionCaseRepo) {
	c := Opened(t, "case-1", Subject, T0)
	must(t, c.AutoResolve("rule-x", "com.warehouse.wes.order-management.order.OrderRepromised", "evt-9", T0.Add(4*time.Minute)))
	save(t, r, c)

	got := findByID(t, r, "case-1").Snapshot()
	if got.Status != exceptioncase.StatusResolved || !got.AutoResolved {
		t.Fatalf("auto-resolve did not round trip: %+v", got)
	}
	if got.AutoRuleID != "rule-x" || got.AutoType != "com.warehouse.wes.order-management.order.OrderRepromised" || got.AutoEventID != "evt-9" {
		t.Fatalf("auto evidence = %+v", got)
	}
}

func caseOneOpenPerKey(t *testing.T, r ports.ExceptionCaseRepo) {
	save(t, r, Opened(t, "case-1", Subject, T0))
	wantErr(t, r.Save(bg, Opened(t, "case-2", Subject, T0.Add(time.Minute))), usecases.ErrOpenCaseExists)
	if _, err := r.FindByID(bg, "case-2"); err == nil {
		t.Fatal("the refused case must not have been stored")
	}
}

func caseOneOpenScope(t *testing.T, r ports.ExceptionCaseRepo) {
	save(t, r, Opened(t, "case-1", Subject, T0))
	save(t, r, Opened(t, "case-2", "ord-8-line-1", T0))                // another subject
	save(t, r, OpenedFor(t, "case-3", SrcType, Subject, "SIM2", T0))   // another site
	save(t, r, OpenedFor(t, "case-4", OtherType, Subject, SiteID, T0)) // another source type
}

func caseResolvedFreesKey(t *testing.T, r ports.ExceptionCaseRepo) {
	first := Opened(t, "case-1", Subject, T0)
	save(t, r, first)
	must(t, first.Resolve("DONE", "handled", "", mustActor(t, "op-1"), T0.Add(time.Minute)))
	save(t, r, first)

	// The index stops holding the key; the NEW open case becomes the owner.
	save(t, r, Opened(t, "case-2", Subject, T0.Add(2*time.Minute)))
	if got := owner(t, r, Subject); got.ID() != "case-2" {
		t.Fatalf("key owner = %s, want the new open case case-2", got.ID())
	}
	// The resolved case is still there, still resolved.
	if got := findByID(t, r, "case-1"); got.Status() != exceptioncase.StatusResolved {
		t.Fatalf("the resolved case changed: %s", got.Status())
	}
}

func caseVersionGuard(t *testing.T, r ports.ExceptionCaseRepo) {
	save(t, r, Opened(t, "case-1", Subject, T0))
	// Two writers load the same version and both touch: the second save
	// must fail with the typed conflict, and the winner's state stands.
	first, err := r.FindByID(bg, "case-1")
	must(t, err)
	second, err := r.FindByID(bg, "case-1")
	must(t, err)
	must(t, first.Touch(T0.Add(time.Minute)))
	must(t, second.Touch(T0.Add(2*time.Minute)))
	save(t, r, first)
	wantErr(t, r.Save(bg, second), usecases.ErrConcurrentModification)
	if got := findByID(t, r, "case-1"); got.Occurrence() != 2 || !got.LastSeenAt().Equal(T0.Add(time.Minute)) {
		t.Fatalf("the loser overwrote the winner: %+v", got.Snapshot())
	}
	// The loser re-fetches, mutates the CURRENT state and succeeds.
	retry := findByID(t, r, "case-1")
	must(t, retry.Touch(T0.Add(3*time.Minute)))
	save(t, r, retry)
	if got := findByID(t, r, "case-1"); got.Occurrence() != 3 {
		t.Fatalf("retry after re-fetch failed: %+v", got.Snapshot())
	}
}

func caseResaveConflicts(t *testing.T, r ports.ExceptionCaseRepo) {
	c := Opened(t, "case-1", Subject, T0)
	save(t, r, c)
	// Saving the SAME aggregate again (no transition in between) is a
	// version conflict, not a no-op: the caller lost track of state.
	wantErr(t, r.Save(bg, c), usecases.ErrConcurrentModification)
}

func caseNoPendingEvents(t *testing.T, r ports.ExceptionCaseRepo) {
	save(t, r, Opened(t, "case-1", Subject, T0))
	if ev := findByID(t, r, "case-1").DomainEvents(); len(ev) != 0 {
		t.Fatalf("events = %v", ev)
	}
}

func caseListOpen(t *testing.T, r ports.ExceptionCaseRepo) {
	save(t, r, Opened(t, "case-a", Subject, T0))                         // ps-main, SIM1, TaskCPTMissed
	save(t, r, Opened(t, "case-b", "ord-8-line-1", T0.Add(time.Minute))) // ps-main, SIM1, TaskCPTMissed
	c2 := OpenedFor(t, "case-c", OtherType, Subject, "SIM2", T0.Add(2*time.Minute))
	must(t, c2.Triage(shared.SeverityLow, "ps-urgent", mustActor(t, "op-1"), T0.Add(2*time.Minute))) // queue ps-urgent, site SIM2
	save(t, r, c2)
	resolved := Opened(t, "case-x", "ord-9-line-1", T0.Add(3*time.Minute))
	must(t, resolved.Resolve("DONE", "handled", "", mustActor(t, "op-1"), T0.Add(4*time.Minute)))
	save(t, r, resolved)

	got, err := r.ListOpen(bg, "", "", "")
	must(t, err)
	if !equalIDs(ids(got), []string{"case-a", "case-b", "case-c"}) {
		t.Fatalf("all = %v", ids(got))
	}
	got, err = r.ListOpen(bg, "ps-main", "", "")
	must(t, err)
	if !equalIDs(ids(got), []string{"case-a", "case-b"}) {
		t.Fatalf("by queue = %v", ids(got))
	}
	got, err = r.ListOpen(bg, "", "SIM1", "")
	must(t, err)
	if !equalIDs(ids(got), []string{"case-a", "case-b"}) {
		t.Fatalf("by site = %v", ids(got))
	}
	got, err = r.ListOpen(bg, "", "", OtherType)
	must(t, err)
	if !equalIDs(ids(got), []string{"case-c"}) {
		t.Fatalf("by source type = %v", ids(got))
	}
	got, err = r.ListOpen(bg, "ps-urgent", "SIM2", OtherType)
	must(t, err)
	if !equalIDs(ids(got), []string{"case-c"}) {
		t.Fatalf("by all three = %v", ids(got))
	}
}

func mustActor(t *testing.T, name string) shared.Actor {
	t.Helper()
	a, err := shared.Operator(name)
	must(t, err)
	return a
}
