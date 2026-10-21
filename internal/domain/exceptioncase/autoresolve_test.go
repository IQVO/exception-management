package exceptioncase

import (
	"errors"
	"testing"
	"time"
)

// AutoResolve (ADR 0002): a declarative policy rule closes the case,
// recording the rule id and the correlated fact; the audit actor is the
// policy; the case becomes RESOLVED (terminal). It works from any
// unresolved status — including straight from OPEN, which is the CPT-miss
// flow (ADR 0001's OrderRepromised scenario).
func TestAutoResolveClosesTheCaseAndRecordsTheCorrelatedFact(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	at := openedAt.Add(5 * time.Minute)
	if err := c.AutoResolve(
		"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised",
		"com.warehouse.wes.order-management.order.OrderRepromised",
		"1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
		at,
	); err != nil {
		t.Fatal(err)
	}
	if c.Status() != StatusResolved {
		t.Fatal("an auto-resolved case is RESOLVED")
	}
	auto := c.AutoResolution()
	if auto == nil {
		t.Fatal("the auto-resolution evidence must be recorded")
	}
	if auto.correlatedType != "com.warehouse.wes.order-management.order.OrderRepromised" ||
		auto.correlatedEventID != "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d" {
		t.Errorf("evidence = %+v", auto)
	}
	if c.AutoResolution().ruleID == "" {
		t.Error("the rule id must be recorded")
	}
	audit := c.AuditTrail()
	last := audit[len(audit)-1]
	if last.Action != ActionAutoResolved || last.Actor.Kind != "policy" {
		t.Errorf("audit entry = %+v, want AUTO_RESOLVED by the policy", last)
	}
	evts := c.DomainEvents()
	autoEvt, ok := evts[len(evts)-1].(ExceptionAutoResolved)
	if !ok {
		t.Fatalf("last event = %T", evts[len(evts)-1])
	}
	if autoEvt.CorrelatedType == "" || autoEvt.CorrelatedEventID == "" || autoEvt.RuleID == "" {
		t.Errorf("ExceptionAutoResolved = %+v", autoEvt)
	}
	// The evidence is handed out as a copy.
	auto.ruleID = "TAMPERED"
	if c.AutoResolution().ruleID == "TAMPERED" {
		t.Error("AutoResolution must return a copy")
	}
	// Terminal like any resolution.
	if err := c.Triage("LOW", "q", operator(t), at.Add(time.Minute)); !errors.Is(err, ErrCaseClosed) {
		t.Errorf("after auto-resolve: err = %v, want ErrCaseClosed", err)
	}
}

// The failing paths of AutoResolve.
func TestAutoResolveRejectsInvalidEvidence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		call func(c *Case) error
		want error
	}{
		{"blank rule id", func(c *Case) error { return c.AutoResolve(" ", "t", "e", openedAt) }, ErrBlankRuleID},
		{"blank correlated type", func(c *Case) error { return c.AutoResolve("r", " ", "e", openedAt) }, ErrBlankCorrelation},
		{"blank correlated event id", func(c *Case) error { return c.AutoResolve("r", "t", "\x00", openedAt) }, ErrBlankCorrelation},
		{"zero time", func(c *Case) error { return c.AutoResolve("r", "t", "e", time.Time{}) }, ErrNoTimestamp},
		{"already resolved", func(c *Case) error {
			if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", operator(t), openedAt.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			return c.AutoResolve("r", "t", "e", openedAt.Add(2*time.Minute))
		}, ErrCaseClosed},
	}
	for _, tc := range cases {
		c := openedCase(t)
		if err := tc.call(c); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}
