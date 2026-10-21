package exceptioncase

import (
	"time"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// Snapshot is the persisted form of a case. Restore validates it, so a
// corrupt row can never become a live aggregate.
type Snapshot struct {
	ID             string
	Key            shared.DedupKey
	Status         Status
	Severity       shared.Severity
	Queue          string
	Assignee       string
	Escalated      bool
	Occurrence     int64
	OpenedAt       time.Time
	LastSeenAt     time.Time
	SLAMinutes     int64
	SourceEventID  string
	SourcePayload  []byte
	ResolutionCode string
	RootCause      string
	Note           string
	ResolvedAt     time.Time
	AutoResolved   bool
	AutoRuleID     string
	AutoType       string
	AutoEventID    string
	Version        int64
	Audit          []AuditEntry
}

// Snapshot returns the persisted form of the case.
func (c *Case) Snapshot() Snapshot {
	s := Snapshot{
		ID:             string(c.id),
		Key:            c.key,
		Status:         c.status,
		Severity:       c.severity,
		Queue:          c.queue,
		Assignee:       c.assignee,
		Escalated:      c.escalated,
		Occurrence:     c.occurrence,
		OpenedAt:       c.openedAt,
		LastSeenAt:     c.lastSeenAt,
		SLAMinutes:     c.slaMinutes,
		SourceEventID:  c.sourceEventID,
		SourcePayload:  bytesClone(c.sourcePayload),
		ResolutionCode: c.resolution.code,
		RootCause:      c.resolution.rootCause,
		Note:           c.resolution.note,
		ResolvedAt:     c.resolution.at,
		Version:        c.version,
		Audit:          c.AuditTrail(),
	}
	if a := c.resolution.auto; a != nil {
		s.AutoResolved = true
		s.AutoRuleID = a.ruleID
		s.AutoType = a.correlatedType
		s.AutoEventID = a.correlatedEventID
	}
	return s
}

// Restore rebuilds a persisted case, validating every invariant the snapshot
// can show. The restored case has no pending events.
func Restore(s Snapshot) (*Case, error) {
	if err := validateSnapshotIdentity(s); err != nil {
		return nil, err
	}
	if err := validateSnapshotState(s); err != nil {
		return nil, err
	}
	if err := validateSnapshotAudit(s); err != nil {
		return nil, err
	}
	c := &Case{
		id:            shared.ID(s.ID),
		key:           s.Key,
		status:        s.Status,
		severity:      s.Severity,
		queue:         s.Queue,
		assignee:      s.Assignee,
		escalated:     s.Escalated,
		occurrence:    s.Occurrence,
		openedAt:      utcOrZero(s.OpenedAt),
		lastSeenAt:    utcOrZero(s.LastSeenAt),
		slaMinutes:    s.SLAMinutes,
		sourceEventID: s.SourceEventID,
		sourcePayload: bytesClone(s.SourcePayload),
		resolution: resolution{
			code:      s.ResolutionCode,
			rootCause: s.RootCause,
			note:      s.Note,
			at:        utcOrZero(s.ResolvedAt),
		},
		version: s.Version,
		audit:   append([]AuditEntry(nil), s.Audit...),
	}
	if s.AutoResolved {
		c.resolution.auto = &autoResolution{
			ruleID:            s.AutoRuleID,
			correlatedType:    s.AutoType,
			correlatedEventID: s.AutoEventID,
		}
	}
	return c, nil
}

// validateSnapshotIdentity checks the fields every state shares.
func validateSnapshotIdentity(s Snapshot) error {
	if _, err := shared.NewID(s.ID); err != nil {
		return ErrInvalidSnapshot
	}
	if _, err := shared.NewDedupKey(s.Key.SourceType, s.Key.SourceSubject, s.Key.SiteID); err != nil {
		return ErrInvalidSnapshot
	}
	if !s.Status.Valid() || !s.Severity.Valid() || shared.IsBlank(s.Queue) {
		return ErrInvalidSnapshot
	}
	if s.SLAMinutes < 1 || s.Occurrence < 1 || s.Version < 1 {
		return ErrInvalidSnapshot
	}
	if !shared.ValidText(s.SourceEventID) || len(s.SourcePayload) == 0 {
		return ErrInvalidSnapshot
	}
	if s.OpenedAt.IsZero() || s.LastSeenAt.IsZero() {
		return ErrInvalidSnapshot
	}
	if s.LastSeenAt.Before(s.OpenedAt) {
		return ErrInvalidSnapshot
	}
	return nil
}

// validateSnapshotState checks the status-dependent invariants.
func validateSnapshotState(s Snapshot) error {
	if s.Status == StatusOpen || s.Status == StatusTriaged {
		if s.Assignee != "" {
			return ErrInvalidSnapshot
		}
	}
	if s.Status == StatusAssigned && shared.IsBlank(s.Assignee) {
		return ErrInvalidSnapshot
	}
	if s.Status != StatusResolved {
		return validateUnresolvedTail(s)
	}
	return validateResolvedTail(s)
}

// validateUnresolvedTail: an unresolved case carries no resolution at all.
func validateUnresolvedTail(s Snapshot) error {
	if s.ResolutionCode != "" || s.RootCause != "" || !s.ResolvedAt.IsZero() || s.AutoResolved {
		return ErrInvalidSnapshot
	}
	return nil
}

// validateResolvedTail: RESOLVED needs a code, a root cause and an instant.
func validateResolvedTail(s Snapshot) error {
	if !shared.ValidText(s.ResolutionCode) || !shared.ValidText(s.RootCause) || s.ResolvedAt.IsZero() {
		return ErrInvalidSnapshot
	}
	if s.ResolvedAt.Before(s.OpenedAt) {
		return ErrInvalidSnapshot
	}
	if s.AutoResolved && (!shared.ValidText(s.AutoRuleID) || !shared.ValidText(s.AutoType) || !shared.ValidText(s.AutoEventID)) {
		return ErrInvalidSnapshot
	}
	return nil
}

// validateSnapshotAudit: the trail exists, starts with OPENED, and every
// entry carries a valid actor and instant at or after the open.
func validateSnapshotAudit(s Snapshot) error {
	if len(s.Audit) == 0 || s.Audit[0].Action != ActionOpened {
		return ErrInvalidSnapshot
	}
	for _, e := range s.Audit {
		if shared.IsBlank(e.Action) || !e.Actor.Valid() || e.At.IsZero() || e.At.Before(s.OpenedAt) {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

func bytesClone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func utcOrZero(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}
