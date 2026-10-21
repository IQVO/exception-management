package exceptioncase

import (
	"bytes"
	"testing"
	"time"
)

// Invariant (ADR 0002): the case holds a FROZEN copy of the source payload.
// Mutating the caller's slice after open, or the slice the case hands out,
// never changes what the case holds; no later transition changes it either.
func TestTheSourcePayloadIsFrozenAfterOpen(t *testing.T) {
	t.Parallel()
	in := testOpenInput()
	mutable := bytes.Clone(payload)
	in.Payload = mutable
	c, err := Open(in)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the caller's slice after Open does not reach the case.
	for i := range mutable {
		mutable[i] = 'X'
	}
	if got := c.SourcePayload(); !bytes.Equal(got, payload) {
		t.Errorf("mutating the input after open changed the case: %q", got)
	}
	// Mutating the returned copy does not reach the case either.
	held := c.SourcePayload()
	held[0] = 'X'
	if got := c.SourcePayload(); !bytes.Equal(got, payload) {
		t.Errorf("mutating the returned copy changed the case: %q", got)
	}
	// Transitions never touch it.
	if err := c.Touch(openedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.Triage("HIGH", "cps-front", operator(t), openedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.Assign("op-marc", operator(t), openedAt.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := c.SourcePayload(); !bytes.Equal(got, payload) {
		t.Error("a transition changed the frozen payload")
	}
	// The event carries its own copy: mutating the case's held copy or the
	// caller's slice never reaches the event.
	evts := c.DomainEvents()
	opened, ok := evts[0].(ExceptionOpened)
	if !ok || !bytes.Equal(opened.SourcePayload, payload) {
		t.Fatal("ExceptionOpened must carry the frozen payload")
	}
	held = c.SourcePayload()
	held[0] = 'Y'
	if !bytes.Equal(opened.SourcePayload, payload) {
		t.Error("the event's payload must be independent of the case's")
	}
	for i := range mutable {
		mutable[i] = 'Z'
	}
	if !bytes.Equal(opened.SourcePayload, payload) {
		t.Error("the event's payload must be independent of the caller's slice")
	}
	// The id of the opening fact is frozen with it.
	if c.SourceEventID() != "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b" {
		t.Errorf("SourceEventID = %q", c.SourceEventID())
	}
}
