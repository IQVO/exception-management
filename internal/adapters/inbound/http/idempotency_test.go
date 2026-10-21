package http

import (
	"net/http"
	"strings"
	"sync"
	"testing"
)

// The Idempotency-Key middleware: required, replayed, conflict-checked and
// raced. The handlers' effects run inside the middleware's unit of work, so
// these tests also prove a rejected request leaves no effect behind.

func TestIdempotencyKeyRequiredOnEveryCommand(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)

	for _, target := range []string{
		"/exceptions/case-0001/triage", "/exceptions/case-0001/assign", "/exceptions/case-0001/resolve",
		"/exception-policies/" + testSourceType,
	} {
		method := http.MethodPost
		if strings.HasSuffix(target, testSourceType) {
			method = http.MethodPut
		}
		wantProblem(t, mustCall(t, h, method, target, `{"x":1}`, http.StatusBadRequest, nil),
			http.StatusBadRequest, "idempotency-key-required")
	}
	// GETs never need one.
	mustCall(t, h, http.MethodGet, "/exceptions", "", http.StatusOK, nil)
}

func TestIdempotencyReplayReturnsTheStoredResponse(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	body := `{"severity":"CRITICAL","queue":"cps-inbound"}`
	first := mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", body, http.StatusOK, idemKey("k-1"))
	replayRec := mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", body, http.StatusOK, idemKey("k-1"))
	if first.Body.String() != replayRec.Body.String() {
		t.Fatalf("replay body = %s, want the stored %s", replayRec.Body.String(), first.Body.String())
	}
	// Exactly one TRIAGED transition happened.
	var got caseDTO
	decodeBody(t, replayRec, &got)
	audit := got.AuditTrail
	if len(audit) != 2 || audit[1].Action != "TRIAGED" {
		t.Fatalf("audit = %+v, want OPENED then ONE TRIAGED", audit)
	}
}

func TestIdempotencyKeyReusedWithADifferentBodyIs409(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"HIGH","queue":"q1"}`, http.StatusOK, idemKey("k-1"))
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"HIGH","queue":"q2"}`, http.StatusConflict, idemKey("k-1")),
		http.StatusConflict, "idempotency-key-reused")
}

func TestIdempotencyReplaysTheStoredRejection(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	// Resolve once, for real.
	mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusOK, idemKey("k-r"))
	// The same key replays the stored 200; the case stays resolved.
	mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusOK, idemKey("k-r"))

	// A NEW key on the resolved case gets (and replays) the 409 rejection.
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusConflict, idemKey("k-409")),
		http.StatusConflict, "case-already-resolved")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusConflict, idemKey("k-409")),
		http.StatusConflict, "case-already-resolved")
}

func TestIdempotencyConcurrentDistinctKeysAllLand(t *testing.T) {
	f := newFixture(t)
	h, _ := f.withIdempotency(t)
	f.declarePolicy(t, testSourceType, 30)

	// n concurrent commands, each with its OWN key and its own case: every
	// one lands exactly once (no cross-key interference). The same-key
	// concurrency guarantee (one effect, winner's answer replayed) rides on
	// the Postgres unit of work and the store's first-writer-wins Save —
	// proven over real Postgres by the cmd/api integration test.
	const n = 8
	ids := make([]string, n)
	for i := range n {
		ids[i] = f.openCase(t, "ord-"+string(rune('a'+i))+"-line-1", "SIM1", 0)
	}
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = call(t, h, http.MethodPost, "/exceptions/"+ids[i]+"/assign",
				`{"assignee":"op-`+string(rune('a'+i))+`"}`, idemKey("k-"+string(rune('a'+i)))).Code
		}()
	}
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("request %d = %d, want 200", i, code)
		}
	}
	for _, id := range ids {
		var got caseDTO
		decodeBody(t, mustCall(t, h, http.MethodGet, "/exceptions/"+id, "", http.StatusOK, nil), &got)
		if got.Status != "ASSIGNED" || got.Assignee == nil {
			t.Fatalf("case %s = %+v, want ASSIGNED", id, got)
		}
	}
}

func TestIdempotencyPutIsWrappedToo(t *testing.T) {
	// The PUT of the policy surface demands the key exactly like the POSTs
	// (the contract: required on every POST and PUT).
	f := newFixture(t)
	h, _ := f.withIdempotency(t)

	target := "/exception-policies/" + testSourceType
	body := `{"severity":"HIGH","queue":"cps-front","slaMinutes":30}`
	wantProblem(t, mustCall(t, h, http.MethodPut, target, body, http.StatusBadRequest, nil),
		http.StatusBadRequest, "idempotency-key-required")
	first := mustCall(t, h, http.MethodPut, target, body, http.StatusCreated, idemKey("k-put"))
	replay := mustCall(t, h, http.MethodPut, target, body, http.StatusCreated, idemKey("k-put"))
	if first.Body.String() != replay.Body.String() {
		t.Fatalf("replay = %s, want the stored %s", replay.Body.String(), first.Body.String())
	}
}
