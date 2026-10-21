package http

import (
	"net/http"
	"testing"
)

// rejectNUL: a NUL anywhere in a request is a client error BEFORE anything
// runs (and before the Idempotency-Key middleware records an answer).

func TestNULInPathIs404(t *testing.T) {
	f := newFixture(t)
	h, store := f.withIdempotency(t)
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/case-a\x00b/triage", `{}`, http.StatusNotFound, idemKey("k1")),
		http.StatusNotFound, "not-found")
	if _, found, _ := store.Lookup(bgCtx, "k1"); found {
		t.Fatal("a NUL-path request must not record an idempotency answer")
	}
}

func TestNULInPathPercentEncodedIs404(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/case-a%00b", "", http.StatusNotFound, nil),
		http.StatusNotFound, "not-found")
}

func TestNULInQueryIs400(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?queue=a%00b", "", http.StatusBadRequest, nil),
		http.StatusBadRequest, "malformed-request-body")
}

func TestNULRawByteInBodyIs400(t *testing.T) {
	f := newFixture(t)
	h, store := f.withIdempotency(t)
	rec := call(t, h, http.MethodPost, "/exceptions/case-0001/triage", "{\"severity\":\"HIGH\",\"queue\":\"a\x00b\"}", idemKey("k2"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("raw NUL = %d %s", rec.Code, rec.Body.String())
	}
	wantProblem(t, rec, http.StatusBadRequest, "malformed-request-body")
	if _, found, _ := store.Lookup(bgCtx, "k2"); found {
		t.Fatal("a NUL-body request must not record an idempotency answer")
	}
}

func TestNULEscapeInBodyIs400(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/case-0001/triage",
		`{"severity":"HIGH","queue":"a`+`\u0000`+`b"}`, http.StatusBadRequest, idemKey("k3")),
		http.StatusBadRequest, "malformed-request-body")
}

func TestEscapedBackslashBeforeU0000IsNotANUL(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)
	// \\u0000 is a literal backslash followed by the text u0000 — valid JSON
	// string content, not a NUL: the request proceeds past the guard.
	rec := call(t, f.router(), http.MethodPost, "/exceptions/"+id+"/triage",
		`{"severity":"HIGH","queue":"a\\u0000b"}`, idemKey("k4"))
	if rec.Code == http.StatusBadRequest {
		t.Fatalf("an escaped backslash must not read as a NUL: %s", rec.Body.String())
	}
}
