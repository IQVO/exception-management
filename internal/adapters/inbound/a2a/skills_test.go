package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
)

// now is the fixture clock the seeded cases are stamped against.
var now = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

const (
	testSourceType = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	testQueue      = "ps-main"
)

// harness builds Deps over the in-memory repositories, exactly as cmd/a2a
// builds them over Postgres. The write use cases exist ONLY here, to seed data
// the way the REST adapter would: the A2A surface itself holds none.
type harness struct {
	deps    Deps
	cases   *memory.Cases
	pol     *memory.Policies
	ids     *memory.IDGen
	declare *usecases.DeclareExceptionPolicy
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	cases, pol := memory.NewCases(), memory.NewPolicies()
	h := &harness{
		cases:   cases,
		pol:     pol,
		ids:     memory.NewIDGen(),
		declare: &usecases.DeclareExceptionPolicy{Policies: pol, Events: &memory.Events{}, Clock: memory.Clock{T: now}},
	}
	h.deps = depsOver(cases, pol)
	return h
}

func depsOver(cases ports.ExceptionCaseRepo, pol ports.ExceptionPolicyRepo) Deps {
	return Deps{
		List:      &usecases.ListOpenExceptions{Cases: cases},
		Get:       &usecases.GetException{Cases: cases},
		Policies:  &usecases.ListExceptionPolicies{Policies: pol},
		SLAReport: &usecases.GetSLAReport{Cases: cases, Clock: memory.Clock{T: now}},
	}
}

func (h *harness) declarePolicy(t *testing.T, sourceType string, sla int64, autoOn string, within int64) {
	t.Helper()
	_, err := h.declare.Execute(context.Background(), usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: "HIGH", Queue: testQueue, SLAMinutes: sla,
		AutoResolveOn: autoOn, WithinMinutes: within, Actor: "op-1",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// openCase seeds one case from a consumed fact (opened at now + offset).
func (h *harness) openCase(t *testing.T, subject, site string, offset time.Duration) string {
	t.Helper()
	uc := &usecases.OpenOrTouchException{
		Cases: h.cases, Policies: h.pol, Events: &memory.Events{}, IDs: h.ids, Clock: memory.Clock{T: now.Add(offset)},
	}
	c, err := uc.Execute(context.Background(), usecases.Fact{
		SourceType: testSourceType, SourceSubject: subject, SiteID: site,
		SourceEventID: "evt-" + subject, Payload: []byte(`{"task_id":"task-8a1f","order_ref":"wu-8a1f"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return c.ID()
}

// seed builds the three-case fixture: c1 (SITE-1, 2h old) and c2 (SITE-2, 1h
// old) are past their 30 minute SLA; c3 (SITE-1, fresh) is within it. c2 is
// then resolved and c3 triaged into queue triage-q (severity LOW).
func (h *harness) seed(t *testing.T) (c1, c2, c3 string) {
	t.Helper()
	h.declarePolicy(t, testSourceType, 30, "", 0)
	c1 = h.openCase(t, "task-1", "SITE-1", -2*time.Hour)
	c2 = h.openCase(t, "task-2", "SITE-2", -1*time.Hour)
	c3 = h.openCase(t, "task-3", "SITE-1", 0)
	resolve := &usecases.ResolveException{Cases: h.cases, Events: &memory.Events{}, Clock: memory.Clock{T: now.Add(time.Minute)}}
	if _, err := resolve.Execute(context.Background(), c2, "FIXED", "ROOT_CAUSE", "done", "op-1"); err != nil {
		t.Fatal(err)
	}
	triage := &usecases.TriageException{Cases: h.cases, Events: &memory.Events{}, Clock: memory.Clock{T: now.Add(time.Minute)}}
	if _, err := triage.Execute(context.Background(), c3, "LOW", "triage-q", "op-1"); err != nil {
		t.Fatal(err)
	}
	return c1, c2, c3
}

// ---- the wire: JSON-RPC over HTTP, driven by the SDK's own client ----------

// wireCase is a TEST-LOCAL copy of the case answer contract. If the adapter's
// envelope drifts, this suite fails instead of silently following it.
type wireCase struct {
	CaseID          string         `json:"caseId"`
	SourceType      string         `json:"sourceType"`
	SourceSubject   string         `json:"sourceSubject"`
	SiteID          string         `json:"siteId"`
	Status          string         `json:"status"`
	Severity        string         `json:"severity"`
	Queue           string         `json:"queue"`
	OccurrenceCount int64          `json:"occurrenceCount"`
	Assignee        string         `json:"assignee"`
	ResolutionCode  string         `json:"resolutionCode"`
	RootCause       string         `json:"rootCause"`
	SLAMinutes      int64          `json:"slaMinutes"`
	SLADueAt        string         `json:"slaDueAt"`
	SourceEventID   string         `json:"sourceEventId"`
	SourcePayload   map[string]any `json:"sourcePayload"`
	Audit           []struct {
		Action string `json:"action"`
		Actor  string `json:"actor"`
	} `json:"audit"`
}

type wireEnvelope struct {
	Skill string `json:"skill"`
	Data  struct {
		SLAStatus *struct {
			GeneratedAt string         `json:"generatedAt"`
			OpenCases   int            `json:"openCases"`
			Breached    int            `json:"breached"`
			Escalated   int            `json:"escalated"`
			BySeverity  map[string]int `json:"bySeverity"`
			ByQueue     map[string]int `json:"byQueue"`
			Policies    []struct {
				SourceType  string `json:"sourceType"`
				Version     int64  `json:"version"`
				Severity    string `json:"severity"`
				Queue       string `json:"queue"`
				SLAMinutes  int64  `json:"slaMinutes"`
				AutoResolve *struct {
					OnType        string `json:"onType"`
					WithinMinutes int64  `json:"withinMinutes"`
				} `json:"autoResolve"`
			} `json:"policies"`
		} `json:"slaStatus"`
		Exceptions *struct {
			Count int        `json:"count"`
			Cases []wireCase `json:"cases"`
		} `json:"exceptions"`
	} `json:"data"`
	Problem *struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	} `json:"problem"`
}

type wire struct {
	client *a2aclient.Client
}

func newWire(t *testing.T, deps Deps) *wire {
	t.Helper()
	srv := httptest.NewServer(Handler(NewExecutor(deps)))
	t.Cleanup(srv.Close)
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return &wire{client: client}
}

// answered is a decoded answer: the envelope, its text summary and the raw JSON of
// every part (for "no part mentions X" assertions).
type answered struct {
	env  wireEnvelope
	text string
	raw  string
}

// send sends one message and returns the decoded reply.
func (w *wire) send(t *testing.T, msg *a2a.Message) (answered, *a2a.Message) {
	t.Helper()
	resp, err := w.client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("SendMessage: %v (a skill failure must be answered in-band, never a JSON-RPC error)", err)
	}
	out, ok := resp.(*a2a.Message)
	if !ok || len(out.Parts) < 2 {
		t.Fatalf("reply = %T, want a message with a data and a text part", resp)
	}
	data, ok := out.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("first reply part is %T, want data", out.Parts[0].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("reply is not the skill envelope: %v (%s)", err, raw)
	}
	text, _ := out.Parts[1].Content.(a2a.Text)
	return answered{env: env, text: string(text), raw: string(raw) + " " + string(text)}, out
}

func (w *wire) call(t *testing.T, part *a2a.Part) answered {
	t.Helper()
	r, _ := w.send(t, a2a.NewMessage(a2a.MessageRoleUser, part))
	return r
}

func (w *wire) skill(t *testing.T, skill string, args map[string]any) answered {
	t.Helper()
	return w.call(t, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
}

func (r answered) wantProblem(t *testing.T, status int, slug string) {
	t.Helper()
	if r.env.Problem == nil {
		t.Fatalf("expected a %d problem, got data: %+v", status, r.env.Data)
	}
	if r.env.Problem.Status != status || r.env.Problem.Type != problemTypeBase+slug {
		t.Fatalf("problem = %+v, want %d %s", r.env.Problem, status, slug)
	}
	if !strings.Contains(r.text, r.env.Problem.Detail) {
		t.Errorf("text summary %q does not carry the problem detail %q", r.text, r.env.Problem.Detail)
	}
}

func caseIDs(cs []wireCase) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.CaseID)
	}
	return out
}

func TestWireSLAStatusReportsTheCaseloadAgainstItsClocks(t *testing.T) {
	h := newHarness(t)
	h.seed(t)
	w := newWire(t, h.deps)

	r := w.skill(t, skillSLAStatus, nil)
	if r.env.Problem != nil || r.env.Skill != skillSLAStatus || r.env.Data.SLAStatus == nil {
		t.Fatalf("envelope = %+v, want sla_status data", r.env)
	}
	got := r.env.Data.SLAStatus
	// c1 and c3 are open (c2 is resolved); only c1 is past its 30 minute clock.
	if got.OpenCases != 2 || got.Breached != 1 || got.Escalated != 0 {
		t.Errorf("report = open %d breached %d escalated %d, want 2/1/0", got.OpenCases, got.Breached, got.Escalated)
	}
	if got.BySeverity["HIGH"] != 1 || got.BySeverity["LOW"] != 1 || got.ByQueue[testQueue] != 1 || got.ByQueue["triage-q"] != 1 {
		t.Errorf("breakdown = %v / %v", got.BySeverity, got.ByQueue)
	}
	if got.GeneratedAt != now.Format(time.RFC3339Nano) {
		t.Errorf("generatedAt = %q, want the fixture clock %q", got.GeneratedAt, now.Format(time.RFC3339Nano))
	}
	if got.Policies != nil {
		t.Errorf("policies = %v, want them omitted unless includePolicies is true", got.Policies)
	}
	if r.text != "2 open case(s): 1 breached, 0 escalated" {
		t.Errorf("summary = %q", r.text)
	}
}

func TestWireSLAStatusIncludesPoliciesOnRequest(t *testing.T) {
	h := newHarness(t)
	h.seed(t)
	h.declarePolicy(t, "com.warehouse.wes.inventory-storage.stock.StockMismatch", 60, "com.warehouse.wes.inventory-storage.stock.StockReconciled", 15)
	w := newWire(t, h.deps)

	r := w.skill(t, skillSLAStatus, map[string]any{"includePolicies": true})
	if r.env.Problem != nil || r.env.Data.SLAStatus == nil {
		t.Fatalf("envelope = %+v", r.env)
	}
	pols := r.env.Data.SLAStatus.Policies
	if len(pols) != 2 {
		t.Fatalf("policies = %+v, want the two declared source types", pols)
	}
	if pols[0].SourceType > pols[1].SourceType {
		t.Errorf("policies are not ordered by source type: %q, %q", pols[0].SourceType, pols[1].SourceType)
	}
	byType := map[string]int{}
	for i, p := range pols {
		byType[p.SourceType] = i
	}
	cpt := pols[byType[testSourceType]]
	if cpt.Severity != "HIGH" || cpt.Queue != testQueue || cpt.SLAMinutes != 30 || cpt.AutoResolve != nil {
		t.Errorf("CPT policy = %+v", cpt)
	}
	inv := pols[byType["com.warehouse.wes.inventory-storage.stock.StockMismatch"]]
	if inv.AutoResolve == nil || inv.AutoResolve.WithinMinutes != 15 || !strings.HasSuffix(inv.AutoResolve.OnType, "StockReconciled") {
		t.Errorf("auto-resolve rule = %+v", inv.AutoResolve)
	}
	if !strings.Contains(r.text, "2 source-type polic") {
		t.Errorf("summary = %q, want the policy count", r.text)
	}
}

func TestWireSLAStatusOnAnEmptyCaseloadIsAnEmptyReportNotAProblem(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	r := w.skill(t, skillSLAStatus, map[string]any{})
	if r.env.Problem != nil || r.env.Data.SLAStatus == nil || r.env.Data.SLAStatus.OpenCases != 0 {
		t.Fatalf("envelope = %+v, want an empty report", r.env)
	}
	if !strings.Contains(r.raw, `"bySeverity":{}`) || !strings.Contains(r.raw, `"byQueue":{}`) {
		t.Errorf("empty breakdowns must be {} objects, got %s", r.raw)
	}
}

func TestWireFindExceptionsListsNewestFirstAndFilters(t *testing.T) {
	h := newHarness(t)
	c1, c2, c3 := h.seed(t)
	w := newWire(t, h.deps)

	filters := []struct {
		name string
		args map[string]any
		want []string
	}{
		{"no argument lists every case, open and resolved alike", nil, []string{c3, c2, c1}},
		{"status OPEN", map[string]any{"status": "OPEN"}, []string{c1}},
		{"status TRIAGED", map[string]any{"status": "TRIAGED"}, []string{c3}},
		{"status RESOLVED", map[string]any{"status": "RESOLVED"}, []string{c2}},
		{"site", map[string]any{"siteId": "SITE-1"}, []string{c3, c1}},
		{"queue", map[string]any{"queue": "triage-q"}, []string{c3}},
		{"source type", map[string]any{"sourceType": testSourceType}, []string{c3, c2, c1}},
		{"a source type nothing holds", map[string]any{"sourceType": "com.warehouse.wes.other.Type"}, nil},
		{"filters combine", map[string]any{"siteId": "SITE-1", "status": "OPEN"}, []string{c1}},
	}
	for _, tc := range filters {
		t.Run(tc.name, func(t *testing.T) {
			r := w.skill(t, skillFindExceptions, tc.args)
			if r.env.Problem != nil || r.env.Data.Exceptions == nil {
				t.Fatalf("envelope = %+v", r.env)
			}
			got := caseIDs(r.env.Data.Exceptions.Cases)
			if len(got) != len(tc.want) || r.env.Data.Exceptions.Count != len(tc.want) {
				t.Fatalf("cases = %v (count %d), want %v", got, r.env.Data.Exceptions.Count, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("cases = %v, want %v newest first", got, tc.want)
				}
			}
			if len(tc.want) == 0 && !strings.Contains(r.raw, `"cases":[]`) {
				t.Errorf("an empty result must be an empty list, never null: %s", r.raw)
			}
		})
	}
}

func TestWireFindExceptionsReadsOneCaseInFull(t *testing.T) {
	h := newHarness(t)
	_, c2, _ := h.seed(t)
	w := newWire(t, h.deps)

	r := w.skill(t, skillFindExceptions, map[string]any{"caseId": c2})
	if r.env.Problem != nil || r.env.Data.Exceptions == nil || r.env.Data.Exceptions.Count != 1 {
		t.Fatalf("envelope = %+v", r.env)
	}
	c := r.env.Data.Exceptions.Cases[0]
	want := wireCase{
		CaseID: c2, SourceType: testSourceType, SourceSubject: "task-2", SiteID: "SITE-2",
		Status: "RESOLVED", Severity: "HIGH", Queue: testQueue, OccurrenceCount: 1,
		SLAMinutes: 30, SourceEventID: "evt-task-2", ResolutionCode: "FIXED", RootCause: "ROOT_CAUSE",
	}
	assertCaseIdentity(t, c, want)
	assertCaseDetail(t, c)
	if !strings.HasPrefix(r.text, c2+": RESOLVED") {
		t.Errorf("summary = %q", r.text)
	}
}

// assertCaseIdentity compares the scalar fields of a case answer.
func assertCaseIdentity(t *testing.T, got, want wireCase) {
	t.Helper()
	pairs := []struct {
		name      string
		got, want any
	}{
		{"caseId", got.CaseID, want.CaseID}, {"sourceType", got.SourceType, want.SourceType},
		{"sourceSubject", got.SourceSubject, want.SourceSubject}, {"siteId", got.SiteID, want.SiteID},
		{"status", got.Status, want.Status}, {"severity", got.Severity, want.Severity},
		{"queue", got.Queue, want.Queue}, {"occurrenceCount", got.OccurrenceCount, want.OccurrenceCount},
		{"slaMinutes", got.SLAMinutes, want.SLAMinutes}, {"sourceEventId", got.SourceEventID, want.SourceEventID},
		{"resolutionCode", got.ResolutionCode, want.ResolutionCode}, {"rootCause", got.RootCause, want.RootCause},
	}
	for _, p := range pairs {
		if p.got != p.want {
			t.Errorf("%s = %v, want %v", p.name, p.got, p.want)
		}
	}
}

// assertCaseDetail checks the structured parts of a case answer: the SLA
// clock, the frozen source payload and the audit trail.
func assertCaseDetail(t *testing.T, c wireCase) {
	t.Helper()
	if c.SLADueAt == "" || c.Assignee != "" {
		t.Errorf("slaDueAt = %q assignee = %q", c.SLADueAt, c.Assignee)
	}
	if c.SourcePayload["task_id"] != "task-8a1f" {
		t.Errorf("sourcePayload = %#v, want the frozen fact object", c.SourcePayload)
	}
	if len(c.Audit) == 0 || c.Audit[len(c.Audit)-1].Action == "" || c.Audit[len(c.Audit)-1].Actor == "" {
		t.Errorf("audit = %+v, want the accepted transitions", c.Audit)
	}
}

// Domain rejections are ANSWERED in-band with the REST adapter's status and
// slug, never raised as JSON-RPC errors.
func TestWireFindExceptionsUnknownCaseIsAnsweredAs404(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	r := w.skill(t, skillFindExceptions, map[string]any{"caseId": "case-doesnotexist"})
	r.wantProblem(t, 404, "case-not-found")
	if r.env.Problem.Detail != "exception case not found" || strings.Contains(r.raw, "ports:") {
		t.Errorf("detail = %q: want the adapter-owned text, not the package-prefixed sentinel", r.env.Problem.Detail)
	}
}

func TestWireArgumentValidation(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	longID := "case-" + strings.Repeat("x", 70)
	cases := []struct {
		name   string
		skill  string
		args   map[string]any
		slug   string
		detail string
	}{
		{"unknown status", skillFindExceptions, map[string]any{"status": "CLOSED"}, "invalid-status", "OPEN, TRIAGED, ASSIGNED, RESOLVED"},
		{"lowercase status is not a status", skillFindExceptions, map[string]any{"status": "open"}, "invalid-status", ""},
		{"blank case id", skillFindExceptions, map[string]any{"caseId": ""}, "invalid-case-id", "start with case-"},
		{"case id without the prefix", skillFindExceptions, map[string]any{"caseId": "abc-123456"}, "invalid-case-id", ""},
		{"case id too short", skillFindExceptions, map[string]any{"caseId": "case-"}, "invalid-case-id", ""},
		{"case id too long", skillFindExceptions, map[string]any{"caseId": longID}, "invalid-case-id", ""},
		{"case id with a slash", skillFindExceptions, map[string]any{"caseId": "case-a/b/c"}, "invalid-case-id", ""},
		{"case id with whitespace", skillFindExceptions, map[string]any{"caseId": "case-a b c"}, "invalid-case-id", ""},
		{"case id with a control character", skillFindExceptions, map[string]any{"caseId": "case-abc\u0007def"}, "invalid-case-id", ""},
		{"case id with a filter", skillFindExceptions, map[string]any{"caseId": "case-abcdef", "status": "OPEN"}, "invalid-argument", "cannot be combined"},
		{"NUL in a case id", skillFindExceptions, map[string]any{"caseId": "case-abc\u0000def"}, "malformed-request-body", "NUL"},
		{"NUL in the queue", skillFindExceptions, map[string]any{"queue": "ps\u0000main"}, "malformed-request-body", "NUL"},
		{"NUL in the site", skillFindExceptions, map[string]any{"siteId": "\u0000"}, "malformed-request-body", "NUL"},
		{"NUL in the source type", skillFindExceptions, map[string]any{"sourceType": "a\u0000b"}, "malformed-request-body", "NUL"},
		{"NUL in the status", skillFindExceptions, map[string]any{"status": "OPEN\u0000"}, "malformed-request-body", "NUL"},
		{"a misspelled filter is refused, not ignored", skillFindExceptions, map[string]any{"statu": "OPEN"}, "invalid-argument", "unknown field"},
		{"a REST-style site key is not an argument", skillFindExceptions, map[string]any{"site": "SITE-1"}, "invalid-argument", "unknown field"},
		{"a snake_case MCP key is not an argument", skillFindExceptions, map[string]any{"case_id": "case-abcdef"}, "invalid-argument", "unknown field"},
		{"a filter of the wrong type", skillFindExceptions, map[string]any{"queue": 7}, "invalid-argument", ""},
		{"a write-shaped argument is unknown", skillFindExceptions, map[string]any{"assignee": "op-1"}, "invalid-argument", "unknown field"},
		{"misspelled includePolicies", skillSLAStatus, map[string]any{"includePolicy": true}, "invalid-argument", "unknown field"},
		{"includePolicies of the wrong type", skillSLAStatus, map[string]any{"includePolicies": "yes"}, "invalid-argument", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The Go escapes above are real control characters; the client
			// encodes them as JSON \u00XX escapes the executor decodes again.
			r := w.skill(t, tc.skill, tc.args)
			r.wantProblem(t, 400, tc.slug)
			if tc.detail != "" && !strings.Contains(r.env.Problem.Detail, tc.detail) {
				t.Errorf("detail = %q, want it to mention %q", r.env.Problem.Detail, tc.detail)
			}
		})
	}
}

func TestWireRequestShapeProblems(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	text := a2a.NewTextPart
	cases := []struct {
		name string
		part *a2a.Part
		slug string
	}{
		{"unknown skill", a2a.NewDataPart(map[string]any{"skill": "triage_exception", "arguments": map[string]any{}}), "unknown-skill"},
		{"no skill field", a2a.NewDataPart(map[string]any{"arguments": map[string]any{}}), "malformed-skill-request"},
		{"unknown top-level field", a2a.NewDataPart(map[string]any{"skill": skillSLAStatus, "arguments": map[string]any{}, "extra": 1}), "malformed-skill-request"},
		{"text that is not JSON", text("how many cases are breached?"), "malformed-skill-request"},
		{"text with trailing data", text(`{"skill":"sla_status","arguments":{}} {"skill":"sla_status"}`), "malformed-skill-request"},
		{"text holding a JSON array", text(`[1,2]`), "malformed-skill-request"},
		{"arguments that are not an object", a2a.NewDataPart(map[string]any{"skill": skillSLAStatus, "arguments": []any{1}}), "invalid-argument"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w.call(t, tc.part).wantProblem(t, 400, tc.slug)
		})
	}

	t.Run("a text part holding the skill JSON works like a data part", func(t *testing.T) {
		r := w.call(t, text(`{"skill":"sla_status","arguments":{}}`))
		if r.env.Problem != nil || r.env.Data.SLAStatus == nil {
			t.Fatalf("envelope = %+v", r.env)
		}
	})
	t.Run("absent arguments mean the empty object", func(t *testing.T) {
		r := w.call(t, a2a.NewDataPart(map[string]any{"skill": skillFindExceptions}))
		if r.env.Problem != nil || r.env.Data.Exceptions == nil {
			t.Fatalf("envelope = %+v", r.env)
		}
	})
}

func TestWireReplyIsBoundToTheCallersContext(t *testing.T) {
	w := newWire(t, newHarness(t).deps)
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skillSLAStatus, "arguments": map[string]any{}}))
	msg.ContextID = "ctx-saga-1"
	_, out := w.send(t, msg)
	if out.ContextID != "ctx-saga-1" || out.Role != a2a.MessageRoleAgent {
		t.Errorf("reply context = %q role = %q, want the caller's context and the agent role", out.ContextID, out.Role)
	}
}

// failingCases fails every read with an infrastructure-looking error.
type failingCases struct{ ports.ExceptionCaseRepo }

var errInfra = errors.New(`pq: password authentication failed for user "exc" (dsn=postgres://exc:s3cr3t@db:5432/exc)`)

func (failingCases) FindByID(context.Context, string) (*exceptioncase.Case, error) {
	return nil, errInfra
}

func (failingCases) ListPage(context.Context, repository.CaseFilter, *repository.CaseAfter, int) ([]*exceptioncase.Case, error) {
	return nil, errInfra
}

func (failingCases) ListOpen(context.Context, string, string, string) ([]*exceptioncase.Case, error) {
	return nil, errInfra
}

func TestWireInfrastructureFailureIsAnswered500WithoutLeakingInternals(t *testing.T) {
	deps := depsOver(failingCases{ExceptionCaseRepo: memory.NewCases()}, memory.NewPolicies())
	w := newWire(t, deps)
	for _, tc := range []struct {
		skill string
		args  map[string]any
	}{
		{skillFindExceptions, map[string]any{"caseId": "case-abcdef"}},
		{skillFindExceptions, nil},
		{skillSLAStatus, nil},
	} {
		r := w.skill(t, tc.skill, tc.args)
		r.wantProblem(t, 500, "internal-error")
		if strings.Contains(r.raw, "s3cr3t") || strings.Contains(r.raw, "postgres://") || strings.Contains(r.raw, "pq:") {
			t.Errorf("%s leaked the infrastructure error: %s", tc.skill, r.raw)
		}
	}
}

// The read-only charter (ADR 0007, mirrored from the MCP adapter): Deps holds
// the four READ use cases and nothing else, so no skill can ever reach a write.
func TestDepsHoldNoWriteUseCase(t *testing.T) {
	allowed := map[string]bool{"ListOpenExceptions": true, "GetException": true, "ListExceptionPolicies": true, "GetSLAReport": true}
	typ := reflect.TypeOf(Deps{})
	if typ.NumField() != len(allowed) {
		t.Errorf("Deps has %d fields, want exactly the %d read use cases", typ.NumField(), len(allowed))
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		name := f.Type.Elem().Name()
		if f.Type.Kind() != reflect.Pointer || f.Type.Elem().PkgPath() != reflect.TypeOf(usecases.GetException{}).PkgPath() || !allowed[name] {
			t.Errorf("Deps.%s is %s: only the read use cases %v may be wired", f.Name, f.Type, allowed)
		}
	}
}

// Every answered skill is an OTel span "a2a.skill <id>" with the a2a.skill
// attribute, like the MCP adapter's mcp.tool spans; a problem marks it Error.
func TestSkillRunsInASpanNamedAfterTheSkill(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	w := newWire(t, newHarness(t).deps)
	w.skill(t, skillSLAStatus, nil)
	w.skill(t, skillFindExceptions, map[string]any{"status": "CLOSED"})

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}
	for i, want := range []string{skillSLAStatus, skillFindExceptions} {
		if spans[i].Name() != "a2a.skill "+want {
			t.Errorf("span %d = %q, want %q", i, spans[i].Name(), "a2a.skill "+want)
		}
		if !hasAttr(spans[i].Attributes(), "a2a.skill", want) {
			t.Errorf("span %q lacks the a2a.skill=%s attribute: %v", spans[i].Name(), want, spans[i].Attributes())
		}
	}
	if spans[0].Status().Code != 0 || spans[1].Status().Description == "" {
		t.Errorf("span status = %+v / %+v, want Unset for the answer and an Error for the problem", spans[0].Status(), spans[1].Status())
	}
}

func hasAttr(attrs []attribute.KeyValue, key, value string) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == value {
			return true
		}
	}
	return false
}

// TestCardIsServedAndDialable proves the card advertises the URL the server
// actually answers on: a client built from it reaches the agent.
func TestCardIsServedAndDialable(t *testing.T) {
	srv := httptest.NewServer(Handler(NewExecutor(newHarness(t).deps)))
	defer srv.Close()
	card := Card(Config{AgentURL: srv.URL + "/", Version: "test"})
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skillSLAStatus})),
	})
	if err != nil || resp == nil {
		t.Fatalf("SendMessage via the card = %v, %v", resp, err)
	}
}
