package exceptioncase

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Invariant (ADR 0002): exactly ONE case per dedup key. A second arrival of
// the same source fact TOUCHES the existing case — occurrence + 1, lastSeenAt
// advanced, exactly one TOUCHED audit entry, NO new case and NO new domain
// event — whatever the case's status. Only a key with no case at all opens
// one, and a fact whose key differs is rejected.
func TestASecondFactForTheSameDedupKeyTouchesInsteadOfOpening(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	at := openedAt.Add(10 * time.Minute)
	got, err := OpenOrTouch(c, OpenInput{Key: testKey(), At: at})
	if err != nil {
		t.Fatalf("OpenOrTouch: %v", err)
	}
	if got != c {
		t.Fatal("OpenOrTouch must return the SAME case, never a new one")
	}
	if c.Occurrence() != 2 || c.LastSeenAt() != at {
		t.Errorf("occurrence=%d lastSeenAt=%v, want 2 and %v", c.Occurrence(), c.LastSeenAt(), at)
	}
	if c.Status() != StatusOpen || c.Version() != 2 {
		t.Errorf("a touch changes no status: status=%s version=%d", c.Status(), c.Version())
	}
	if n := len(c.DomainEvents()); n != 1 {
		t.Errorf("a touch raises no new domain event, got %d events", n)
	}
	audit := c.AuditTrail()
	if len(audit) != 2 || audit[1].Action != ActionTouched {
		t.Fatalf("audit = %+v, want one OPENED then one TOUCHED", audit)
	}
	if audit[1].At != at || audit[1].Actor.Kind != shared.ActorSystem {
		t.Errorf("the TOUCHED entry = %+v", audit[1])
	}
}

// A fact arriving AFTER resolution still counts as an occurrence but can
// never reopen the case (ADR 0002).
func TestAFactAfterResolutionCountsButNeverReopens(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	op := operator(t)
	if err := c.Triage(shared.SeverityCritical, "cps-inbound", op, openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.Resolve("WORK_COMPLETED", "OPERATOR_UNAVAILABLE", "", op, openedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	before := c.Occurrence()
	at := openedAt.Add(time.Hour)
	if _, err := OpenOrTouch(c, OpenInput{Key: testKey(), At: at}); err != nil {
		t.Fatalf("OpenOrTouch on a resolved case: %v", err)
	}
	if c.Status() != StatusResolved {
		t.Error("a fact can never reopen a resolved case")
	}
	if c.Occurrence() != before+1 || c.LastSeenAt() != at {
		t.Error("the occurrence still counts")
	}
	if last := c.AuditTrail()[len(c.AuditTrail())-1]; last.Action != ActionTouched {
		t.Errorf("last audit entry = %s, want TOUCHED", last.Action)
	}
	if n := len(c.DomainEvents()); n != 3 { // opened + triaged + resolved, nothing new
		t.Errorf("a touch after resolution raises no event, got %d", n)
	}
}

// A key with no case opens one; a mismatched key is an error.
func TestOpenOrTouchOpensOnlyWhenNoCaseExistsAndRejectsAKeyMismatch(t *testing.T) {
	t.Parallel()
	in := testOpenInput()
	c, err := OpenOrTouch(nil, in)
	if err != nil || c == nil || c.Status() != StatusOpen {
		t.Fatalf("OpenOrTouch(nil) must open: %v", err)
	}
	other := in
	other.Key.SourceSubject = "task-9999"
	if _, err := OpenOrTouch(c, other); !errors.Is(err, ErrKeyMismatch) {
		t.Errorf("key mismatch err = %v, want ErrKeyMismatch", err)
	}
	if c.Occurrence() != 1 {
		t.Error("a rejected fact must not touch the case")
	}
}

// Touch with a zero timestamp is rejected; nothing changes.
func TestTouchRejectsAZeroTimestamp(t *testing.T) {
	t.Parallel()
	c := openedCase(t)
	if err := c.Touch(time.Time{}); !errors.Is(err, ErrNoTimestamp) {
		t.Errorf("err = %v, want ErrNoTimestamp", err)
	}
	if c.Occurrence() != 1 || c.Version() != 1 {
		t.Error("a rejected touch changes nothing")
	}
}

// operator builds a valid operator actor for the command tests.
func operator(t *testing.T) shared.Actor {
	t.Helper()
	a, err := shared.Operator("op-jane")
	if err != nil {
		t.Fatal(err)
	}
	return a
}
