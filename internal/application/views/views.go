package views

import "time"

// Package views holds the application's read DTOs: wire-friendly shapes the
// inbound adapters consume. The ports package stays interfaces-only (arch
// rule); the aggregates never leak past this package.

// CaseView is the read DTO an inbound adapter (HTTP handler, ops agent)
// receives for one case: the aggregate's Snapshot rendered as wire-friendly
// scalars. The application layer builds it; nothing outside the application
// imports the domain aggregate for reads.
type CaseView struct {
	ID                string
	SourceType        string
	SourceSubject     string
	SiteID            string
	Status            string
	Severity          string
	Queue             string
	Assignee          string
	Escalated         bool
	Occurrence        int64
	OpenedAt          time.Time
	LastSeenAt        time.Time
	SLAMinutes        int64
	SLADueAt          time.Time
	SourceEventID     string
	ResolutionCode    string
	RootCause         string
	Note              string
	ResolvedAt        time.Time
	AutoResolved      bool
	RuleID            string
	CorrelatedType    string
	CorrelatedEventID string
	Version           int64
	Audit             []AuditView
}

// AuditView is one accepted transition, wire-form.
type AuditView struct {
	Action string
	Actor  string
	At     time.Time
}

// SLAReport is the operations read: how the open caseload sits against its
// SLA clocks at GeneratedAt. Breached counts open cases past their deadline
// (with or without the escalation flag); Escalated counts flagged ones.
type SLAReport struct {
	GeneratedAt time.Time
	OpenCases   int
	Breached    int
	Escalated   int
	BySeverity  map[string]int
	ByQueue     map[string]int
}
