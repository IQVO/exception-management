package usecases

import (
	"context"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/views"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// ListOpenExceptions serves the operator listing: every case matching the
// filters, whatever its status unless one is named ("" = open AND resolved
// cases — resolve activity is part of the record, the contract of
// apis/openapi.yaml — so the name is the guide's, the semantics are the
// contract's).
type ListOpenExceptions struct {
	Cases ports.ExceptionCaseRepo
}

// Execute returns the matching cases as read views (never aggregate
// pointers), newest opened first.
func (uc *ListOpenExceptions) Execute(ctx context.Context, status, queue, siteID, sourceType string) ([]views.CaseView, error) {
	matching, err := uc.Cases.List(ctx, status, queue, siteID, sourceType)
	if err != nil {
		return nil, err
	}
	out := make([]views.CaseView, 0, len(matching))
	for _, c := range matching {
		out = append(out, CaseView(c))
	}
	return out, nil
}

// GetException serves one case by id (ports.ErrNotFound -> HTTP 404).
type GetException struct {
	Cases ports.ExceptionCaseRepo
}

// Execute returns the case view.
func (uc *GetException) Execute(ctx context.Context, caseID string) (views.CaseView, error) {
	c, err := uc.Cases.FindByID(ctx, caseID)
	if err != nil {
		return views.CaseView{}, err
	}
	return CaseView(c), nil
}

// GetSLAReport reports how the open caseload sits against its SLA clocks.
type GetSLAReport struct {
	Cases ports.ExceptionCaseRepo
	Clock ports.Clock
}

// Execute builds the report at the current instant.
func (uc *GetSLAReport) Execute(ctx context.Context) (views.SLAReport, error) {
	open, err := uc.Cases.ListOpen(ctx, "", "", "")
	if err != nil {
		return views.SLAReport{}, err
	}
	now := uc.Clock.Now()
	rep := views.SLAReport{
		GeneratedAt: now,
		OpenCases:   len(open),
		BySeverity:  map[string]int{},
		ByQueue:     map[string]int{},
	}
	for _, c := range open {
		if c.BreachesAt(now) {
			rep.Breached++
		}
		if c.Escalated() {
			rep.Escalated++
		}
		rep.BySeverity[string(c.Severity())]++
		rep.ByQueue[c.Queue()]++
	}
	return rep, nil
}

// CaseView projects a case aggregate into its read DTO via the snapshot —
// the one place the wire shape is derived from the persisted form.
func CaseView(c *exceptioncase.Case) views.CaseView {
	s := c.Snapshot()
	v := views.CaseView{
		ID: s.ID, SourceType: s.Key.SourceType, SourceSubject: s.Key.SourceSubject,
		SiteID: s.Key.SiteID, Status: string(s.Status), Severity: string(s.Severity),
		Queue: s.Queue, Assignee: s.Assignee, Escalated: s.Escalated,
		Occurrence: s.Occurrence, OpenedAt: s.OpenedAt, LastSeenAt: s.LastSeenAt,
		SLAMinutes: s.SLAMinutes, SLADueAt: c.SLADueAt(),
		SourceEventID: s.SourceEventID, SourcePayload: s.SourcePayload,
		ResolutionCode: s.ResolutionCode,
		RootCause:      s.RootCause, Note: s.Note, ResolvedAt: s.ResolvedAt,
		AutoResolved: s.AutoResolved, RuleID: s.AutoRuleID,
		CorrelatedType: s.AutoType, CorrelatedEventID: s.AutoEventID,
		Version: s.Version,
	}
	for _, a := range s.Audit {
		v.Audit = append(v.Audit, views.AuditView{Action: a.Action, Actor: a.Actor.String(), At: a.At})
	}
	return v
}

// PolicyView projects a policy version into its read DTO via the snapshot.
func PolicyView(p *exceptionpolicy.Policy) views.PolicyView {
	s := p.Snapshot()
	v := views.PolicyView{
		SourceType: s.SourceType, Version: s.Version, Severity: string(s.Severity),
		Queue: s.Queue, SLAMinutes: s.SLAMinutes, DeclaredAt: s.DeclaredAt,
	}
	if s.AutoResolve != nil {
		v.AutoResolve = &views.AutoResolveRuleView{
			OnType:        s.AutoResolve.OnType(),
			WithinMinutes: s.AutoResolve.WithinMinutes(),
		}
	}
	return v
}

// ListExceptionPolicies serves the policy listing: the current version of
// every declared source type, ordered by source type.
type ListExceptionPolicies struct {
	Policies ports.ExceptionPolicyRepo
}

// Execute returns the policy views (never aggregate pointers).
func (uc *ListExceptionPolicies) Execute(ctx context.Context) ([]views.PolicyView, error) {
	declared, err := uc.Policies.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]views.PolicyView, 0, len(declared))
	for _, p := range declared {
		out = append(out, PolicyView(p))
	}
	return out, nil
}
