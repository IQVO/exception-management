package a2a

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/claudioed/exception-management/internal/application/views"
)

// problemTypeBase namespaces this context's problem types. It is the SAME
// base URI the REST adapter uses (internal/adapters/inbound/http/errors.go), so
// a slug means one thing over both surfaces.
const problemTypeBase = "https://errors.exception-management.warehouse-systems.dev/"

// problem is the RFC 7807-shaped rejection an agent client receives instead
// of a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func newProblem(status int, slug, title, detail string) *problem {
	return &problem{Type: problemTypeBase + slug, Title: title, Detail: detail, Status: status}
}

func problemMalformedRequest(detail string) *problem {
	return newProblem(400, "malformed-skill-request", "Malformed skill request", detail)
}

func problemUnknownSkill(skill string) *problem {
	return newProblem(400, "unknown-skill", "Unknown skill",
		fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill))
}

func problemInvalidArgument(detail string) *problem {
	return newProblem(400, "invalid-argument", "Invalid skill argument", detail)
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field, keeping the envelope
// flat and versioned as one contract. The one-line summary is not part of the
// wire shape (it travels as the reply's text part).
type skillDTO struct {
	SLAStatus  *slaStatus      `json:"slaStatus,omitempty"`
	Exceptions *exceptionsList `json:"exceptions,omitempty"`
	summary    string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// slaStatus is the sla_status DTO: the SLA report read model and, only when
// asked for, the declared policies. Domain types never cross the boundary.
type slaStatus struct {
	GeneratedAt string         `json:"generatedAt"`
	OpenCases   int            `json:"openCases"`
	Breached    int            `json:"breached"`
	Escalated   int            `json:"escalated"`
	BySeverity  map[string]int `json:"bySeverity"`
	ByQueue     map[string]int `json:"byQueue"`
	Policies    []policyDTO    `json:"policies,omitempty"`
}

// policyDTO mirrors the MCP and REST policy read shape (camelCase).
type policyDTO struct {
	SourceType  string          `json:"sourceType"`
	Version     int64           `json:"version"`
	Severity    string          `json:"severity"`
	Queue       string          `json:"queue"`
	SLAMinutes  int64           `json:"slaMinutes"`
	AutoResolve *autoResolveDTO `json:"autoResolve"`
	DeclaredAt  string          `json:"declaredAt"`
}

type autoResolveDTO struct {
	OnType        string `json:"onType"`
	WithinMinutes int64  `json:"withinMinutes"`
}

// exceptionsList is the find_exceptions DTO: the matching cases, newest
// opened first (exactly one for a caseId lookup).
type exceptionsList struct {
	Count int       `json:"count"`
	Cases []caseDTO `json:"cases"`
}

// caseDTO mirrors the MCP case read shape (camelCase, flat).
type caseDTO struct {
	CaseID            string         `json:"caseId"`
	SourceType        string         `json:"sourceType"`
	SourceSubject     string         `json:"sourceSubject"`
	SiteID            string         `json:"siteId,omitempty"`
	Status            string         `json:"status"`
	Severity          string         `json:"severity"`
	Queue             string         `json:"queue"`
	OccurrenceCount   int64          `json:"occurrenceCount"`
	Escalated         bool           `json:"escalated"`
	Assignee          string         `json:"assignee,omitempty"`
	ResolutionCode    string         `json:"resolutionCode,omitempty"`
	RootCause         string         `json:"rootCause,omitempty"`
	Note              string         `json:"note,omitempty"`
	ResolvedAt        string         `json:"resolvedAt,omitempty"`
	AutoResolveRuleID string         `json:"autoResolveRuleId,omitempty"`
	CorrelatedType    string         `json:"correlatedType,omitempty"`
	CorrelatedEventID string         `json:"correlatedEventId,omitempty"`
	OpenedAt          string         `json:"openedAt"`
	LastSeenAt        string         `json:"lastSeenAt"`
	SLAMinutes        int64          `json:"slaMinutes"`
	SLADueAt          string         `json:"slaDueAt"`
	SourceEventID     string         `json:"sourceEventId"`
	SourcePayload     map[string]any `json:"sourcePayload"`
	Audit             []auditDTO     `json:"audit"`
	Version           int64          `json:"version"`
}

type auditDTO struct {
	Action string `json:"action"`
	Actor  string `json:"actor"`
	At     string `json:"at"`
}

// instant formats an instant as RFC 3339 UTC; the zero time is "" (omitted).
func instant(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func toCase(v views.CaseView) caseDTO {
	out := caseDTO{
		CaseID: v.ID, SourceType: v.SourceType, SourceSubject: v.SourceSubject, SiteID: v.SiteID,
		Status: v.Status, Severity: v.Severity, Queue: v.Queue,
		OccurrenceCount: v.Occurrence, Escalated: v.Escalated, Assignee: v.Assignee,
		ResolutionCode: v.ResolutionCode, RootCause: v.RootCause, Note: v.Note,
		ResolvedAt:        instant(v.ResolvedAt),
		AutoResolveRuleID: v.RuleID, CorrelatedType: v.CorrelatedType, CorrelatedEventID: v.CorrelatedEventID,
		OpenedAt: instant(v.OpenedAt), LastSeenAt: instant(v.LastSeenAt),
		SLAMinutes: v.SLAMinutes, SLADueAt: instant(v.SLADueAt),
		SourceEventID: v.SourceEventID,
		Audit:         make([]auditDTO, 0, len(v.Audit)),
		Version:       v.Version,
	}
	// The payload is the frozen JSON object of the opening fact; the REST
	// adapter serves the same bytes as sourcePayload.
	if len(v.SourcePayload) > 0 {
		_ = json.Unmarshal(v.SourcePayload, &out.SourcePayload)
	}
	for _, a := range v.Audit {
		out.Audit = append(out.Audit, auditDTO{Action: a.Action, Actor: a.Actor, At: instant(a.At)})
	}
	return out
}

func toPolicy(v views.PolicyView) policyDTO {
	out := policyDTO{
		SourceType: v.SourceType, Version: v.Version, Severity: v.Severity,
		Queue: v.Queue, SLAMinutes: v.SLAMinutes, DeclaredAt: instant(v.DeclaredAt),
	}
	if v.AutoResolve != nil {
		out.AutoResolve = &autoResolveDTO{OnType: v.AutoResolve.OnType, WithinMinutes: v.AutoResolve.WithinMinutes}
	}
	return out
}

func toSLAStatus(v views.SLAReport, policies []views.PolicyView, withPolicies bool) slaStatus {
	out := slaStatus{
		GeneratedAt: instant(v.GeneratedAt), OpenCases: v.OpenCases,
		Breached: v.Breached, Escalated: v.Escalated,
		BySeverity: v.BySeverity, ByQueue: v.ByQueue,
	}
	if out.BySeverity == nil {
		out.BySeverity = map[string]int{}
	}
	if out.ByQueue == nil {
		out.ByQueue = map[string]int{}
	}
	if withPolicies {
		out.Policies = make([]policyDTO, 0, len(policies))
		for _, p := range policies {
			out.Policies = append(out.Policies, toPolicy(p))
		}
	}
	return out
}

// summary is the one-line text of an sla_status answer.
func (s slaStatus) summary() string {
	line := fmt.Sprintf("%d open case(s): %d breached, %d escalated", s.OpenCases, s.Breached, s.Escalated)
	if s.Policies != nil {
		line += fmt.Sprintf("; %d source-type polic(ies)", len(s.Policies))
	}
	return line
}

// summary is the one-line text of a find_exceptions answer.
func (l exceptionsList) summary() string {
	if l.Count == 1 {
		c := l.Cases[0]
		return fmt.Sprintf("%s: %s, %s severity, queue %s, seen %d time(s)", c.CaseID, c.Status, c.Severity, c.Queue, c.OccurrenceCount)
	}
	return fmt.Sprintf("%d exception case(s)", l.Count)
}
