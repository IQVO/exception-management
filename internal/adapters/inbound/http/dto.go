package http

import (
	"encoding/json"
	"time"

	"github.com/claudioed/exception-management/internal/application/views"
)

// The wire DTOs of apis/openapi.yaml. Nullable-and-required fields use
// pointers (absent = omitted; present null = nil pointer); a field that is
// "null when empty" uses nullOrString so the JSON carries an explicit null.

// dedupKeyDTO is the case's dedup key: the identity of the stream of source
// facts. siteId is null when the source fact carried no site.
type dedupKeyDTO struct {
	SourceType    string  `json:"sourceType"`
	SourceSubject string  `json:"sourceSubject"`
	SiteID        *string `json:"siteId"`
}

// auditEntryDTO is one accepted transition.
type auditEntryDTO struct {
	Action string    `json:"action"`
	Actor  string    `json:"actor"`
	At     time.Time `json:"at"`
}

// caseDTO is the ExceptionCase schema: everything the operator sees of one
// case. assignee, resolutionCode, rootCause and resolvedAt are absent until
// they exist; note is absent until RESOLVED and null when resolved without
// one; autoResolveRuleId is present only on an auto-resolved case.
type caseDTO struct {
	CaseID            string          `json:"caseId"`
	DedupKey          dedupKeyDTO     `json:"dedupKey"`
	Status            string          `json:"status"`
	Severity          string          `json:"severity"`
	Queue             string          `json:"queue"`
	OccurrenceCount   int64           `json:"occurrenceCount"`
	Escalated         bool            `json:"escalated"`
	Assignee          *string         `json:"assignee,omitempty"`
	ResolutionCode    *string         `json:"resolutionCode,omitempty"`
	RootCause         *string         `json:"rootCause,omitempty"`
	Note              *nullOrString   `json:"note,omitempty"`
	ResolvedAt        *time.Time      `json:"resolvedAt,omitempty"`
	AutoResolveRuleID *string         `json:"autoResolveRuleId,omitempty"`
	OpenedAt          time.Time       `json:"openedAt"`
	LastSeenAt        time.Time       `json:"lastSeenAt"`
	SLADueAt          time.Time       `json:"slaDueAt"`
	SourceEventID     string          `json:"sourceEventId"`
	SourcePayload     json.RawMessage `json:"sourcePayload"`
	AuditTrail        []auditEntryDTO `json:"auditTrail"`
	Version           int64           `json:"version"`
}

// nullOrString marshals as JSON null when empty, else as the string — the
// contract's "null when resolved without a note".
type nullOrString string

// MarshalJSON implements json.Marshaler.
func (n nullOrString) MarshalJSON() ([]byte, error) {
	if n == "" {
		return []byte("null"), nil
	}
	return json.Marshal(string(n))
}

// toCase renders a read view as the wire shape.
func toCase(v views.CaseView) caseDTO {
	dto := caseDTO{
		CaseID: v.ID,
		DedupKey: dedupKeyDTO{
			SourceType:    v.SourceType,
			SourceSubject: v.SourceSubject,
		},
		Status:          v.Status,
		Severity:        v.Severity,
		Queue:           v.Queue,
		OccurrenceCount: v.Occurrence,
		Escalated:       v.Escalated,
		OpenedAt:        v.OpenedAt,
		LastSeenAt:      v.LastSeenAt,
		SLADueAt:        v.SLADueAt,
		SourceEventID:   v.SourceEventID,
		SourcePayload:   json.RawMessage(v.SourcePayload),
		AuditTrail:      make([]auditEntryDTO, 0, len(v.Audit)),
		Version:         v.Version,
	}
	if v.SiteID != "" {
		site := v.SiteID
		dto.DedupKey.SiteID = &site
	}
	if v.Assignee != "" {
		assignee := v.Assignee
		dto.Assignee = &assignee
	}
	if v.ResolutionCode != "" {
		code := v.ResolutionCode
		dto.ResolutionCode = &code
	}
	if v.RootCause != "" {
		cause := v.RootCause
		dto.RootCause = &cause
	}
	if !v.ResolvedAt.IsZero() {
		resolvedAt := v.ResolvedAt
		dto.ResolvedAt = &resolvedAt
		// note exists once the case is resolved (null when there is none).
		note := nullOrString(v.Note)
		dto.Note = &note
	}
	if v.AutoResolved && v.RuleID != "" {
		rule := v.RuleID
		dto.AutoResolveRuleID = &rule
	}
	for _, a := range v.Audit {
		dto.AuditTrail = append(dto.AuditTrail, auditEntryDTO{Action: a.Action, Actor: a.Actor, At: a.At})
	}
	return dto
}

// autoResolveDTO is the policy's auto-resolve rule.
type autoResolveDTO struct {
	OnType        string `json:"onType"`
	WithinMinutes int64  `json:"withinMinutes"`
}

// policyDTO is the ExceptionPolicy schema; autoResolve is always present and
// null when the source type has no rule.
type policyDTO struct {
	SourceType  string          `json:"sourceType"`
	Version     int64           `json:"version"`
	Severity    string          `json:"severity"`
	Queue       string          `json:"queue"`
	SLAMinutes  int64           `json:"slaMinutes"`
	AutoResolve *autoResolveDTO `json:"autoResolve"`
	DeclaredAt  time.Time       `json:"declaredAt"`
}

// toPolicy renders a policy read view as the wire shape.
func toPolicy(v views.PolicyView) policyDTO {
	dto := policyDTO{
		SourceType: v.SourceType, Version: v.Version, Severity: v.Severity,
		Queue: v.Queue, SLAMinutes: v.SLAMinutes, DeclaredAt: v.DeclaredAt,
	}
	if v.AutoResolve != nil {
		dto.AutoResolve = &autoResolveDTO{OnType: v.AutoResolve.OnType, WithinMinutes: v.AutoResolve.WithinMinutes}
	}
	return dto
}

// The request bodies (strict decode: unknown fields are a 400).

type triageBody struct {
	Severity string `json:"severity"`
	Queue    string `json:"queue"`
}

type assignBody struct {
	Assignee string `json:"assignee"`
}

type resolveBody struct {
	ResolutionCode string  `json:"resolutionCode"`
	RootCause      string  `json:"rootCause"`
	Note           *string `json:"note"`
}

type autoResolveBody struct {
	OnType        string `json:"onType"`
	WithinMinutes int64  `json:"withinMinutes"`
}

type declarePolicyBody struct {
	Severity    string           `json:"severity"`
	Queue       string           `json:"queue"`
	SLAMinutes  *int64           `json:"slaMinutes"`
	AutoResolve *autoResolveBody `json:"autoResolve"`
}
