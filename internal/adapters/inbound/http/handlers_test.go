package http

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// One test per route (the route table of .claude/rules/rest-api.md), plus
// the problem mapping each handler owns.

func TestListExceptionsEmptyIs200WithEmptyArray(t *testing.T) {
	f := newFixture(t)
	rec := mustCall(t, f.router(), http.MethodGet, "/exceptions", "", http.StatusOK, nil)
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("empty list = %q, want []", rec.Body.String())
	}
}

func TestListExceptionsFiltersAndNewestFirst(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	older := f.openCase(t, "ord-1-line-1", "SIM1", 0)
	newer := f.openCase(t, "ord-2-line-1", "SIM2", time.Minute)
	resolved := f.openCase(t, "ord-3-line-1", "SIM1", 2*time.Minute)
	f.resolveAt(t, resolved, 3*time.Minute)

	var all []caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions", "", http.StatusOK, nil), &all)
	if len(all) != 3 {
		t.Fatalf("all = %d cases, want 3 (resolved activity is part of the record)", len(all))
	}
	if all[0].CaseID != resolved || all[1].CaseID != newer || all[2].CaseID != older {
		t.Fatalf("order = %s,%s,%s, want newest first %s,%s,%s", all[0].CaseID, all[1].CaseID, all[2].CaseID, resolved, newer, older)
	}

	var onlyResolved []caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?status=RESOLVED", "", http.StatusOK, nil), &onlyResolved)
	if len(onlyResolved) != 1 || onlyResolved[0].CaseID != resolved {
		t.Fatalf("status=RESOLVED = %+v", onlyResolved)
	}

	var bySite []caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?site=SIM2&status=OPEN", "", http.StatusOK, nil), &bySite)
	if len(bySite) != 1 || bySite[0].CaseID != newer {
		t.Fatalf("site=SIM2&status=OPEN = %+v", bySite)
	}

	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?queue=ps-main&sourceType="+testSourceType, "", http.StatusOK, nil), &bySite)
	if len(bySite) != 3 {
		t.Fatalf("queue+sourceType = %d, want 3", len(bySite))
	}
}

func TestListExceptionsRejectsUnknownStatus(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions?status=CLOSED", "", http.StatusBadRequest, nil),
		http.StatusBadRequest, "invalid-status")
}

func TestGetException(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "", 0)

	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id, "", http.StatusOK, nil), &got)
	if got.CaseID != id || got.Status != "OPEN" || got.Severity != "HIGH" || got.Queue != testQueue {
		t.Fatalf("case = %+v", got)
	}
	if got.DedupKey.SourceType != testSourceType || got.DedupKey.SourceSubject != "ord-1-line-1" || got.DedupKey.SiteID != nil {
		t.Fatalf("dedup key = %+v, want the site null", got.DedupKey)
	}
	if string(got.SourcePayload) != `{"task_id":"task-8a1f","order_ref":"wu-8a1f"}` {
		t.Fatalf("frozen payload = %s", got.SourcePayload)
	}
	if got.AuditTrail[0].Actor != "system" || got.AuditTrail[0].Action != "OPENED" {
		t.Fatalf("audit = %+v", got.AuditTrail)
	}
	if got.OccurrenceCount != 1 || got.Version != 1 || got.Escalated {
		t.Fatalf("counters = %+v", got)
	}
}

func TestGetExceptionAbsentFieldsStayAbsent(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "", 0)

	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id, "", http.StatusOK, nil), &got)
	if got.Assignee != nil || got.ResolutionCode != nil || got.Note != nil || got.ResolvedAt != nil || got.AutoResolveRuleID != nil {
		t.Fatalf("absent fields leaked: %+v", got)
	}
}

func TestGetExceptionNotFound(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/case-404", "", http.StatusNotFound, nil),
		http.StatusNotFound, "case-not-found")
}

func TestGetExceptionInvalidCaseID(t *testing.T) {
	f := newFixture(t)
	// A '/' in the id cannot reach the handler (chi routes it elsewhere and
	// the 404 not-found answers); everything else the OpenAPI pattern
	// rejects is the 400 invalid-case-id.
	for _, id := range []string{"nope", "case-a b", "case-", "case-0123456789012345678901234567890123456789012345678901234567890123"} {
		wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id, "", http.StatusBadRequest, nil),
			http.StatusBadRequest, "invalid-case-id")
	}
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/case-x/slash", "", http.StatusNotFound, nil),
		http.StatusNotFound, "not-found")
}

func TestTriageException(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/triage",
		`{"severity":"CRITICAL","queue":"cps-inbound"}`, http.StatusOK, idemKey("k-tri")), &got)
	if got.Status != "TRIAGED" || got.Severity != "CRITICAL" || got.Queue != "cps-inbound" {
		t.Fatalf("triaged = %+v", got)
	}
	// The command records its actor: the wire carries no operator identity
	// (no auth layer, no header in the contract), so v1 records system.
	last := got.AuditTrail[len(got.AuditTrail)-1]
	if last.Action != "TRIAGED" || last.Actor != "system" {
		t.Fatalf("audit = %+v, want TRIAGED by system", last)
	}
}

func TestTriageExceptionProblems(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)
	h := f.router()

	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"SEV1","queue":"q"}`, http.StatusBadRequest, idemKey("k1")),
		http.StatusBadRequest, "invalid-severity")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"HIGH","queue":"  "}`, http.StatusBadRequest, idemKey("k2")),
		http.StatusBadRequest, "invalid-queue")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"HIGH"}`, http.StatusBadRequest, idemKey("k3")),
		http.StatusBadRequest, "invalid-queue")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/triage", `{"severity":"HIGH","queue":"q","x":1}`, http.StatusBadRequest, idemKey("k4")),
		http.StatusBadRequest, "malformed-request-body")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/case-404/triage", `{"severity":"HIGH","queue":"q"}`, http.StatusNotFound, idemKey("k5")),
		http.StatusNotFound, "case-not-found")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/nope/triage", `{"severity":"HIGH","queue":"q"}`, http.StatusBadRequest, idemKey("k6")),
		http.StatusBadRequest, "invalid-case-id")
}

func TestAssignException(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/assign",
		`{"assignee":"op-marc"}`, http.StatusOK, idemKey("k-as")), &got)
	if got.Status != "ASSIGNED" || got.Assignee == nil || *got.Assignee != "op-marc" {
		t.Fatalf("assigned = %+v", got)
	}

	// Reassignment is allowed while the case is not resolved.
	decodeBody(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/assign",
		`{"assignee":"op-eva"}`, http.StatusOK, idemKey("k-as2")), &got)
	if *got.Assignee != "op-eva" {
		t.Fatalf("reassigned = %v", *got.Assignee)
	}

	wantProblem(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/assign", `{"assignee":" "}`, http.StatusBadRequest, idemKey("k-as3")),
		http.StatusBadRequest, "invalid-assignee")
}

func TestResolveException(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)

	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusOK, idemKey("k-rs")), &got)
	if got.Status != "RESOLVED" || *got.ResolutionCode != "WORK_COMPLETED" || *got.RootCause != "OPERATOR_UNAVAILABLE" {
		t.Fatalf("resolved = %+v", got)
	}
	// note exists once resolved, null when there is none (an explicit JSON
	// null, not an omitted field).
	raw := mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id, "", http.StatusOK, nil).Body.String()
	if !strings.Contains(raw, `"note":null`) {
		t.Fatalf("note of a noteless resolve must be an explicit null: %s", raw)
	}
	if got.ResolvedAt == nil {
		t.Fatal("resolvedAt is absent on a resolved case")
	}

	// RESOLVED is terminal: any later command is the 409 problem.
	wantProblem(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE"}`, http.StatusConflict, idemKey("k-rs2")),
		http.StatusConflict, "case-already-resolved")
	wantProblem(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id+"/triage",
		`{"severity":"HIGH","queue":"q"}`, http.StatusConflict, idemKey("k-rs3")),
		http.StatusConflict, "case-already-resolved")

	// A note round trips as a string.
	id2 := f.openCase(t, "ord-2-line-1", "SIM1", 0)
	decodeBody(t, mustCall(t, f.router(), http.MethodPost, "/exceptions/"+id2+"/resolve",
		`{"resolutionCode":"WORK_COMPLETED","rootCause":"OPERATOR_UNAVAILABLE","note":"moved by hand"}`, http.StatusOK, idemKey("k-rs4")), &got)
	raw2 := mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id2, "", http.StatusOK, nil).Body.String()
	if !strings.Contains(raw2, `"note":"moved by hand"`) {
		t.Fatalf("note = %s", raw2)
	}
}

func TestResolveExceptionValidation(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)
	h := f.router()

	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve", `{"resolutionCode":"","rootCause":"X"}`, http.StatusBadRequest, idemKey("k1")),
		http.StatusBadRequest, "invalid-resolution")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve", `{"resolutionCode":"WORK_COMPLETED","rootCause":"not a code"}`, http.StatusBadRequest, idemKey("k2")),
		http.StatusBadRequest, "invalid-resolution")
	wantProblem(t, mustCall(t, h, http.MethodPost, "/exceptions/"+id+"/resolve", `{"resolutionCode":"WORK_COMPLETED"}`, http.StatusBadRequest, idemKey("k3")),
		http.StatusBadRequest, "invalid-resolution")
}

func TestListExceptionPolicies(t *testing.T) {
	f := newFixture(t)
	rec := mustCall(t, f.router(), http.MethodGet, "/exception-policies", "", http.StatusOK, nil)
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("empty list = %q, want []", rec.Body.String())
	}

	f.declarePolicy(t, testSourceType, 30)
	f.declarePolicy(t, "com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached", 15)
	var got []policyDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exception-policies", "", http.StatusOK, nil), &got)
	if len(got) != 2 || got[0].SourceType != testSourceType || got[1].SourceType != "com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached" {
		t.Fatalf("policies = %+v, want both, ordered by source type", got)
	}
	if got[0].Version != 1 || got[0].Severity != "HIGH" || got[0].SLAMinutes != 30 || got[0].AutoResolve != nil {
		t.Fatalf("policy = %+v, autoResolve must be null", got[0])
	}
}

func TestDeclareExceptionPolicy(t *testing.T) {
	f := newFixture(t)

	var got policyDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodPut, "/exception-policies/"+testSourceType,
		`{"severity":"HIGH","queue":"cps-front","slaMinutes":30,"autoResolve":{"onType":"com.warehouse.wes.order-management.order.OrderRepromised","withinMinutes":60}}`,
		http.StatusCreated, idemKey("k-pol")), &got)
	if got.SourceType != testSourceType || got.Version != 1 || got.SLAMinutes != 30 {
		t.Fatalf("declared = %+v", got)
	}
	if got.AutoResolve == nil || got.AutoResolve.OnType != "com.warehouse.wes.order-management.order.OrderRepromised" || got.AutoResolve.WithinMinutes != 60 {
		t.Fatalf("rule = %+v", got.AutoResolve)
	}

	// The second declaration is version +1 (append-only).
	decodeBody(t, mustCall(t, f.router(), http.MethodPut, "/exception-policies/"+testSourceType,
		`{"severity":"LOW","queue":"cps-back","slaMinutes":90}`, http.StatusCreated, idemKey("k-pol2")), &got)
	if got.Version != 2 || got.Severity != "LOW" || got.AutoResolve != nil {
		t.Fatalf("redeclared = %+v", got)
	}
}

func TestDeclareExceptionPolicyProblems(t *testing.T) {
	f := newFixture(t)
	h := f.router()

	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/nope", `{"severity":"HIGH","queue":"q","slaMinutes":30}`, http.StatusBadRequest, idemKey("k1")),
		http.StatusBadRequest, "invalid-source-type")
	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/"+testSourceType, `{"severity":"HIGH","queue":"q"}`, http.StatusBadRequest, idemKey("k2")),
		http.StatusBadRequest, "invalid-policy")
	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/"+testSourceType, `{"severity":"HIGH","queue":"q","slaMinutes":0}`, http.StatusBadRequest, idemKey("k3")),
		http.StatusBadRequest, "invalid-policy")
	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/"+testSourceType,
		`{"severity":"HIGH","queue":"q","slaMinutes":30,"autoResolve":{"onType":"not-a-type","withinMinutes":60}}`, http.StatusBadRequest, idemKey("k4")),
		http.StatusBadRequest, "invalid-policy")
	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/"+testSourceType,
		`{"severity":"HIGH","queue":"q","slaMinutes":30,"autoResolve":{"onType":"com.warehouse.wes.order-management.order.OrderRepromised","withinMinutes":0}}`, http.StatusBadRequest, idemKey("k5")),
		http.StatusBadRequest, "invalid-policy")
	wantProblem(t, mustCall(t, h, http.MethodPut, "/exception-policies/"+testSourceType, `{"severity":"SEV1","queue":"q","slaMinutes":30}`, http.StatusBadRequest, idemKey("k6")),
		http.StatusBadRequest, "invalid-severity")
}

func TestPostExceptionsIsA405ExplainingItself(t *testing.T) {
	f := newFixture(t)
	// No Idempotency-Key needed: a 405 is answered before idempotency runs.
	rec := mustCall(t, f.router(), http.MethodPost, "/exceptions", `{}`, http.StatusMethodNotAllowed, nil)
	wantProblem(t, rec, http.StatusMethodNotAllowed, "method-not-allowed")
	if !strings.Contains(rec.Body.String(), "no POST /exceptions") {
		t.Fatalf("the 405 must say why: %s", rec.Body.String())
	}
	if allow := rec.Header().Get("Allow"); allow != "GET" {
		t.Fatalf("Allow = %q, want GET", allow)
	}
}

func TestUnknownMethodAndPath(t *testing.T) {
	f := newFixture(t)
	wantProblem(t, mustCall(t, f.router(), http.MethodDelete, "/exceptions/case-1", "", http.StatusMethodNotAllowed, nil),
		http.StatusMethodNotAllowed, "method-not-allowed")
	wantProblem(t, mustCall(t, f.router(), http.MethodGet, "/nope", "", http.StatusNotFound, nil),
		http.StatusNotFound, "not-found")
}

func TestAutoResolvedCaseCarriesItsRuleID(t *testing.T) {
	f := newFixture(t)
	f.declarePolicy(t, testSourceType, 30)
	id := f.openCase(t, "ord-1-line-1", "SIM1", 0)
	if err := f.cases.Save(bgCtx, mustAutoResolve(t, f, id)); err != nil {
		t.Fatal(err)
	}
	var got caseDTO
	decodeBody(t, mustCall(t, f.router(), http.MethodGet, "/exceptions/"+id, "", http.StatusOK, nil), &got)
	if got.AutoResolveRuleID == nil || *got.AutoResolveRuleID == "" {
		t.Fatalf("autoResolveRuleId = %v", got.AutoResolveRuleID)
	}
}
