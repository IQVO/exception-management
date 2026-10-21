package repocontract

import (
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// PolicyRepo runs the ExceptionPolicyRepo contract: append-only versions,
// latest-per-source-type reads and the concurrent-declaration guard.
// newRepo must return an EMPTY repository on every call.
func PolicyRepo(t *testing.T, newRepo func(t *testing.T) ports.ExceptionPolicyRepo) {
	t.Helper()
	cases := map[string]func(t *testing.T, r ports.ExceptionPolicyRepo){
		"an unknown source type is not found":        policyNotFound,
		"a declaration round trips version 1":        policyRoundTrip,
		"a declaration round trips its auto rule":    policyRoundTripRule,
		"a redeclaration becomes the latest version": policyLatest,
		"a same-version save is a lost declare race": policySameVersionConflict,
		"list returns every latest version by type":  policyList,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newRepo(t)) })
	}
}

func declare(t *testing.T, sourceType string, version int64, withRule bool, at time.Time) *exceptionpolicy.Policy {
	t.Helper()
	rule, err := exceptionpolicy.NewRule("com.warehouse.wes.order-management.order.OrderRepromised", 10)
	must(t, err)
	in := exceptionpolicy.DeclareInput{
		SourceType: sourceType, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, At: at,
	}
	if withRule {
		in.AutoResolve = &rule
	}
	if version == 1 {
		p, err := exceptionpolicy.Declare(in)
		must(t, err)
		return p
	}
	// Build version N by redeclaring version N-1 (the use case's path).
	prev := declare(t, sourceType, version-1, false, at.Add(-time.Minute))
	in.Severity, in.Queue, in.SLAMinutes = shared.SeverityLow, "ps-urgent", 90
	if withRule {
		in.AutoResolve = &rule
	}
	p, err := prev.Redeclare(in)
	must(t, err)
	return p
}

func get(t *testing.T, r ports.ExceptionPolicyRepo, sourceType string) *exceptionpolicy.Policy {
	t.Helper()
	p, err := r.Get(bg, sourceType)
	must(t, err)
	return p
}

func policyNotFound(t *testing.T, r ports.ExceptionPolicyRepo) {
	_, err := r.Get(bg, "com.warehouse.wes.fulfillment-execution.task.NeverSeen")
	wantErr(t, err, ports.ErrNotFound)
}

func policyRoundTrip(t *testing.T, r ports.ExceptionPolicyRepo) {
	in := declare(t, SrcType, 1, false, T0)
	must(t, r.Save(bg, in))
	got := get(t, r, SrcType)
	if got.Version() != 1 || got.Severity() != shared.SeverityHigh || got.Queue() != "ps-main" || got.SLAMinutes() != 30 {
		t.Fatalf("declaration did not round trip: %+v", got.Snapshot())
	}
	if got.AutoResolve() != nil {
		t.Fatalf("a ruleless declaration grew a rule: %+v", got.AutoResolve())
	}
	if !got.DeclaredAt().Equal(T0) {
		t.Fatalf("declared at = %v, want %v", got.DeclaredAt(), T0)
	}
}

func policyRoundTripRule(t *testing.T, r ports.ExceptionPolicyRepo) {
	in := declare(t, SrcType, 1, true, T0)
	must(t, r.Save(bg, in))
	rule := get(t, r, SrcType).AutoResolve()
	if rule == nil {
		t.Fatal("the auto-resolve rule was lost")
	}
	if rule.OnType() != "com.warehouse.wes.order-management.order.OrderRepromised" || rule.WithinMinutes() != 10 {
		t.Fatalf("rule = %+v", rule)
	}
}

func policyLatest(t *testing.T, r ports.ExceptionPolicyRepo) {
	must(t, r.Save(bg, declare(t, SrcType, 1, false, T0)))
	must(t, r.Save(bg, declare(t, SrcType, 2, true, T0.Add(time.Minute))))
	got := get(t, r, SrcType)
	if got.Version() != 2 || got.Severity() != shared.SeverityLow || got.Queue() != "ps-urgent" || got.SLAMinutes() != 90 {
		t.Fatalf("latest version = %+v, want v2", got.Snapshot())
	}
	if got.AutoResolve() == nil {
		t.Fatal("v2 carries the rule; it was lost")
	}
}

func policySameVersionConflict(t *testing.T, r ports.ExceptionPolicyRepo) {
	must(t, r.Save(bg, declare(t, SrcType, 1, false, T0)))
	// Another writer computed the SAME version and declared it first.
	wantErr(t, r.Save(bg, declare(t, SrcType, 1, false, T0.Add(time.Second))), usecases.ErrConcurrentModification)
}

func policyList(t *testing.T, r ports.ExceptionPolicyRepo) {
	must(t, r.Save(bg, declare(t, SrcType, 1, false, T0)))
	must(t, r.Save(bg, declare(t, SrcType, 2, true, T0.Add(time.Minute))))
	must(t, r.Save(bg, declare(t, OtherType, 1, false, T0.Add(2*time.Minute))))

	got, err := r.List(bg)
	must(t, err)
	if len(got) != 2 {
		t.Fatalf("list = %d policies, want 2 (one per source type)", len(got))
	}
	// Ordered by source type, and only the LATEST version of each.
	if got[0].SourceType() != OtherType || got[0].Version() != 1 {
		t.Fatalf("first = %s v%d, want %s v1 (ordered by source type)", got[0].SourceType(), got[0].Version(), OtherType)
	}
	if got[1].SourceType() != SrcType || got[1].Version() != 2 || got[1].AutoResolve() == nil {
		t.Fatalf("second = %s v%d rule=%v, want %s v2 with its rule", got[1].SourceType(), got[1].Version(), got[1].AutoResolve(), SrcType)
	}
}
