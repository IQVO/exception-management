package mcp

import (
	"context"
	"encoding/json"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/application/views"
)

// Deps is everything the MCP tools need, injected by the composition root.
// It carries the SAME read use cases the REST adapter uses (GET /exceptions,
// GET /exceptions/{caseId}, GET /exception-policies and the SLA report read
// the application layer serves); the adapter never constructs an outbound
// adapter itself, and it holds no write use case at all. Triage, assign,
// resolve and declare stay on REST.
type Deps struct {
	List      *usecases.ListOpenExceptions
	Get       *usecases.GetException
	Policies  *usecases.ListExceptionPolicies
	SLAReport *usecases.GetSLAReport
}

// --- inputs -------------------------------------------------------------------

type listOpenExceptionsInput struct {
	Status     string `json:"status,omitempty" jsonschema:"keep only cases in this status: OPEN, TRIAGED, ASSIGNED or RESOLVED; omitted means every status, open and resolved alike"`
	Queue      string `json:"queue,omitempty" jsonschema:"keep only cases of this queue; omitted means every queue"`
	SiteID     string `json:"site_id,omitempty" jsonschema:"keep only cases of this site; omitted means every site"`
	SourceType string `json:"source_type,omitempty" jsonschema:"keep only cases opened by this CloudEvents source type; omitted means every source type"`
}

type getExceptionInput struct {
	CaseID string `json:"case_id" jsonschema:"the exception case id, starting with case-, e.g. case-1b4e28ba"`
}

// --- outputs (the REST bodies with snake_case names) --------------------------

type caseView struct {
	CaseID            string         `json:"case_id"`
	SourceType        string         `json:"source_type"`
	SourceSubject     string         `json:"source_subject"`
	SiteID            string         `json:"site_id,omitempty"`
	Status            string         `json:"status"`
	Severity          string         `json:"severity"`
	Queue             string         `json:"queue"`
	OccurrenceCount   int64          `json:"occurrence_count"`
	Escalated         bool           `json:"escalated"`
	Assignee          string         `json:"assignee,omitempty"`
	ResolutionCode    string         `json:"resolution_code,omitempty"`
	RootCause         string         `json:"root_cause,omitempty"`
	Note              string         `json:"note,omitempty"`
	ResolvedAt        string         `json:"resolved_at,omitempty"`
	AutoResolveRuleID string         `json:"auto_resolve_rule_id,omitempty"`
	CorrelatedType    string         `json:"correlated_type,omitempty"`
	CorrelatedEventID string         `json:"correlated_event_id,omitempty"`
	OpenedAt          string         `json:"opened_at"`
	LastSeenAt        string         `json:"last_seen_at"`
	SLAMinutes        int64          `json:"sla_minutes"`
	SLADueAt          string         `json:"sla_due_at"`
	SourceEventID     string         `json:"source_event_id"`
	SourcePayload     map[string]any `json:"source_payload"`
	Audit             []auditView    `json:"audit"`
	Version           int64          `json:"version"`
}

type auditView struct {
	Action string `json:"action"`
	Actor  string `json:"actor"`
	At     string `json:"at"`
}

type policyView struct {
	SourceType  string           `json:"source_type"`
	Version     int64            `json:"version"`
	Severity    string           `json:"severity"`
	Queue       string           `json:"queue"`
	SLAMinutes  int64            `json:"sla_minutes"`
	AutoResolve *autoResolveView `json:"auto_resolve"`
	DeclaredAt  string           `json:"declared_at"`
}

type autoResolveView struct {
	OnType        string `json:"on_type"`
	WithinMinutes int64  `json:"within_minutes"`
}

type slaReportView struct {
	GeneratedAt string         `json:"generated_at"`
	OpenCases   int            `json:"open_cases"`
	Breached    int            `json:"breached"`
	Escalated   int            `json:"escalated"`
	BySeverity  map[string]int `json:"by_severity"`
	ByQueue     map[string]int `json:"by_queue"`
}

type exceptionListView struct {
	Items []caseView `json:"items"`
}

type policyListView struct {
	Items []policyView `json:"items"`
}

// --- mapping ------------------------------------------------------------------

// instant formats an instant as RFC 3339 UTC; the zero time is "" (omitted).
func instant(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func toCase(v views.CaseView) caseView {
	out := caseView{
		CaseID: v.ID, SourceType: v.SourceType, SourceSubject: v.SourceSubject, SiteID: v.SiteID,
		Status: v.Status, Severity: v.Severity, Queue: v.Queue,
		OccurrenceCount: v.Occurrence, Escalated: v.Escalated, Assignee: v.Assignee,
		ResolutionCode: v.ResolutionCode, RootCause: v.RootCause, Note: v.Note,
		ResolvedAt:        instant(v.ResolvedAt),
		AutoResolveRuleID: v.RuleID, CorrelatedType: v.CorrelatedType, CorrelatedEventID: v.CorrelatedEventID,
		OpenedAt: instant(v.OpenedAt), LastSeenAt: instant(v.LastSeenAt),
		SLAMinutes: v.SLAMinutes, SLADueAt: instant(v.SLADueAt),
		SourceEventID: v.SourceEventID,
		Audit:         make([]auditView, 0, len(v.Audit)),
		Version:       v.Version,
	}
	// The payload is the frozen JSON object of the opening fact; the REST
	// adapter serves the same bytes as sourcePayload.
	if len(v.SourcePayload) > 0 {
		_ = json.Unmarshal(v.SourcePayload, &out.SourcePayload)
	}
	for _, a := range v.Audit {
		out.Audit = append(out.Audit, auditView{Action: a.Action, Actor: a.Actor, At: instant(a.At)})
	}
	return out
}

func toPolicy(v views.PolicyView) policyView {
	out := policyView{
		SourceType: v.SourceType, Version: v.Version, Severity: v.Severity,
		Queue: v.Queue, SLAMinutes: v.SLAMinutes, DeclaredAt: instant(v.DeclaredAt),
	}
	if v.AutoResolve != nil {
		out.AutoResolve = &autoResolveView{OnType: v.AutoResolve.OnType, WithinMinutes: v.AutoResolve.WithinMinutes}
	}
	return out
}

func toSLAReport(v views.SLAReport) slaReportView {
	return slaReportView{
		GeneratedAt: instant(v.GeneratedAt), OpenCases: v.OpenCases,
		Breached: v.Breached, Escalated: v.Escalated,
		BySeverity: v.BySeverity, ByQueue: v.ByQueue,
	}
}

// --- handlers -----------------------------------------------------------------

func (d Deps) listOpenExceptions(ctx context.Context, in listOpenExceptionsInput) (exceptionListView, error) {
	items, err := d.List.Execute(ctx, in.Status, in.Queue, in.SiteID, in.SourceType)
	if err != nil {
		return exceptionListView{}, mapError(err)
	}
	out := exceptionListView{Items: make([]caseView, 0, len(items))}
	for _, v := range items {
		out.Items = append(out.Items, toCase(v))
	}
	return out, nil
}

func (d Deps) getException(ctx context.Context, in getExceptionInput) (caseView, error) {
	if in.CaseID == "" {
		return caseView{}, toolError("invalid-case-id", "case_id must name the case to read, starting with case-")
	}
	v, err := d.Get.Execute(ctx, in.CaseID)
	if err != nil {
		return caseView{}, mapError(err)
	}
	return toCase(v), nil
}

func (d Deps) getExceptionPolicies(ctx context.Context, _ struct{}) (policyListView, error) {
	items, err := d.Policies.Execute(ctx)
	if err != nil {
		return policyListView{}, mapError(err)
	}
	out := policyListView{Items: make([]policyView, 0, len(items))}
	for _, v := range items {
		out.Items = append(out.Items, toPolicy(v))
	}
	return out, nil
}

func (d Deps) getSLAReport(ctx context.Context, _ struct{}) (slaReportView, error) {
	v, err := d.SLAReport.Execute(ctx)
	if err != nil {
		return slaReportView{}, mapError(err)
	}
	return toSLAReport(v), nil
}

// --- registry -----------------------------------------------------------------

// registerTools adds the four read-only tools. Every tool is annotated
// read-only, idempotent and closed-world; TestToolSurface pins the set and
// fails the build on any write-verb name.
func (d Deps) registerTools(server *mcp.Server) {
	closedWorld := false
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: &closedWorld}

	addTool(server, &mcp.Tool{
		Name: "list_open_exceptions",
		Description: "List the exception cases of the warehouse, newest opened first, optionally narrowed by status " +
			"(OPEN, TRIAGED, ASSIGNED or RESOLVED; omitted means every status, open and resolved alike — resolve " +
			"activity is part of the record), queue, site and/or the CloudEvents source type that opened them. Each " +
			"case carries its dedup identity (source_type, source_subject, site_id), severity, queue, occurrence_count " +
			"(how often the same exception happened), escalated flag, assignee when one exists, the SLA clock " +
			"(sla_minutes, sla_due_at), the frozen source_payload of the opening fact and the audit trail of accepted " +
			"transitions. Read-only.",
		Annotations: readOnly,
	}, d.listOpenExceptions)

	addTool(server, &mcp.Tool{
		Name: "get_exception",
		Description: "Read one exception case by id: the dedup identity, status, severity, queue, occurrence_count, " +
			"escalated flag, assignee, the SLA clock (sla_minutes, sla_due_at), the frozen source_payload, the " +
			"resolution (resolution_code, root_cause, note, resolved_at) once resolved — manually or auto-resolved, " +
			"which also carries auto_resolve_rule_id and the correlated event — and the audit trail. Read-only; fails " +
			"with case-not-found for an unknown id and invalid-case-id for a blank one.",
		Annotations: readOnly,
	}, d.getException)

	addTool(server, &mcp.Tool{
		Name: "get_exception_policies",
		Description: "List the exception policy declared for every source type, ordered by source type: the latest " +
			"version, the severity and queue a new case of that type opens with, sla_minutes, and auto_resolve — " +
			"null when the type has no rule, else {on_type, within_minutes}, the correlated CloudEvents type and " +
			"window within which it auto-resolves the case. Read-only.",
		Annotations: readOnly,
	}, d.getExceptionPolicies)

	addTool(server, &mcp.Tool{
		Name: "get_sla_report",
		Description: "Read how the open caseload sits against its SLA clocks at generated_at: open_cases, breached " +
			"(open cases past their sla_due_at), escalated (cases the SLA sweeper flagged) and the open counts " +
			"by_severity and by_queue. Read-only.",
		Annotations: readOnly,
	}, d.getSLAReport)
}

// addTool registers one tool. A handler error is returned to the SDK as the
// handler's error, which the SDK turns into an isError tool result (never a
// transport/protocol failure), carrying the error text.
func addTool[In, Out any](
	server *mcp.Server,
	tool *mcp.Tool,
	handle func(context.Context, In) (Out, error),
) {
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
		out, err := handle(ctx, in)
		if err != nil {
			var zero Out
			return nil, zero, err
		}
		return nil, out, nil
	})
}
