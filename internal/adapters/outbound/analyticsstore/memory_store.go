package analyticsstore

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// memoryPolicy mirrors one exception_policy_facts row.
type memoryPolicy struct {
	sourceType, severity, queue string
	version, slaMinutes         int64
	autoResolve                 *report.AutoResolveRule
	declaredAt                  time.Time
}

// Memory is the in-memory twin of Projection+Reader: the same fold semantics
// (earliest milestone wins, latest severity/queue/assignee wins, first
// non-NULL descriptive value) and the same report definitions, computed with
// the pure helpers of internal/analytics/report. One contract test runs
// against Memory and Postgres so the two can never drift (ADR 0006).
type Memory struct {
	mu        sync.Mutex
	processed map[string]time.Time
	cases     map[string]*memoryCase
	policies  map[string]memoryPolicy
}

// memoryCase mirrors one exception_case_facts row.
type memoryCase struct {
	sourceType, sourceSubject, siteID       *string
	severity, queue, assignee               *string
	openedAt, slaDueAt                      *time.Time
	triagedAt, assignedAt, escalatedAt      *time.Time
	resolvedAt, autoResolvedAt              *time.Time
	resolutionCode, rootCause, note, ruleID *string
}

// NewMemory constructs an empty Memory store.
func NewMemory() *Memory {
	return &Memory{
		processed: map[string]time.Time{},
		cases:     map[string]*memoryCase{},
		policies:  map[string]memoryPolicy{},
	}
}

var _ report.Projection = (*Memory)(nil)
var _ report.Reader = (*Memory)(nil)

// Apply implements report.Projection with the Postgres fold semantics.
func (m *Memory) Apply(_ context.Context, e report.Event) (bool, error) {
	if err := e.Validate(); err != nil {
		return false, fmt.Errorf("%w: %w", report.ErrRejected, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, seen := m.processed[e.EventID]; seen {
		return false, nil
	}
	m.processed[e.EventID] = e.At.UTC()
	if e.Kind == report.KindExceptionPolicyDeclared {
		m.foldPolicy(e)
		return true, nil
	}
	m.foldCase(e)
	return true, nil
}

func (m *Memory) foldPolicy(e report.Event) {
	key := e.SourceType + "\x00" + strconv.FormatInt(e.Version, 10)
	row, ok := m.policies[key]
	if !ok || e.At.UTC().Before(row.declaredAt) {
		m.policies[key] = memoryPolicy{
			sourceType: e.SourceType, version: e.Version, severity: e.Severity, queue: e.Queue,
			slaMinutes: e.SLAMinutes, autoResolve: e.AutoResolve, declaredAt: e.At.UTC(),
		}
	}
}

func (m *Memory) foldCase(e report.Event) {
	row := m.cases[e.CaseID]
	if row == nil {
		row = &memoryCase{}
		m.cases[e.CaseID] = row
	}
	milestones := report.MilestonesOf(e)
	keepEarliest(&row.openedAt, milestones.Opened)
	keepEarliest(&row.slaDueAt, milestones.SLADue)
	keepEarliest(&row.triagedAt, milestones.Triaged)
	keepEarliest(&row.assignedAt, milestones.Assigned)
	keepEarliest(&row.escalatedAt, milestones.Escalated)
	keepEarliest(&row.resolvedAt, milestones.Resolved)
	keepEarliest(&row.autoResolvedAt, milestones.AutoResolved)
	keepFirst(&row.sourceType, e.SourceType)
	keepFirst(&row.sourceSubject, e.SourceSubject)
	keepFirst(&row.resolutionCode, e.ResolutionCode)
	keepFirst(&row.rootCause, e.RootCause)
	keepFirst(&row.ruleID, e.RuleID)
	if e.SiteID != nil {
		keepFirst(&row.siteID, *e.SiteID)
	}
	if e.Note != nil {
		keepFirst(&row.note, *e.Note)
	}
	keepLatest(&row.severity, e.Severity)
	keepLatest(&row.queue, e.Queue)
	keepLatest(&row.assignee, e.Assignee)
}

// keepEarliest sets dst to the earliest non-nil candidate (dst must never be
// nil itself: foldCase calls it with the row's field address).
func keepEarliest(dst **time.Time, candidate *time.Time) {
	if candidate == nil {
		return
	}
	if *dst == nil || candidate.Before(**dst) {
		*dst = candidate
	}
}

func keepFirst(dst **string, candidate string) {
	if candidate == "" || *dst != nil {
		return
	}
	v := candidate
	*dst = &v
}

func keepLatest(dst **string, candidate string) {
	if candidate == "" {
		return
	}
	v := candidate
	*dst = &v
}

// resolution returns the case's resolution instant: the manual resolution
// when there is one, the auto resolution otherwise.
func (c *memoryCase) resolution() *time.Time {
	if c.resolvedAt != nil {
		return c.resolvedAt
	}
	return c.autoResolvedAt
}

func inWindow(t, from, to time.Time) bool { return t.Compare(from) >= 0 && t.Compare(to) < 0 }

// RootCauseRows implements report.Reader.
func (m *Memory) RootCauseRows(_ context.Context, q report.Query) ([]report.RootCauseRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	totals := map[string]int64{}
	for _, c := range m.cases {
		if c.sourceType != nil && c.resolution() != nil && inWindow(*c.resolution(), q.Range.From, q.Range.To) {
			totals[*c.sourceType]++
		}
	}
	counts := map[string]int64{}
	for _, c := range m.cases {
		if c.sourceType == nil || c.rootCause == nil || c.resolvedAt == nil || !inWindow(*c.resolvedAt, q.Range.From, q.Range.To) {
			continue
		}
		counts[*c.sourceType+"\x00"+*c.rootCause]++
	}
	out := []report.RootCauseRow{}
	for key, n := range counts {
		sourceType, rootCause, _ := strings.Cut(key, "\x00")
		out = append(out, report.RootCauseRow{
			SourceType: sourceType, RootCause: rootCause, Resolved: n,
			Share: report.Share(n, totals[sourceType]),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Resolved != out[j].Resolved {
			return out[i].Resolved > out[j].Resolved
		}
		if out[i].SourceType != out[j].SourceType {
			return out[i].SourceType < out[j].SourceType
		}
		return out[i].RootCause < out[j].RootCause
	})
	return out, nil
}

// QueueTimingRows implements report.Reader.
func (m *Memory) QueueTimingRows(_ context.Context, q report.Query) ([]report.QueueTimingRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byQueue := map[string][]float64{}
	for _, c := range m.cases {
		if c.queue == nil || c.openedAt == nil || c.resolution() == nil || !inWindow(*c.resolution(), q.Range.From, q.Range.To) {
			continue
		}
		secs := report.Seconds(c.resolution().Sub(*c.openedAt))
		if secs < 0 {
			secs = 0
		}
		byQueue[*c.queue] = append(byQueue[*c.queue], secs)
	}
	queues := make([]string, 0, len(byQueue))
	for queue := range byQueue {
		queues = append(queues, queue)
	}
	sort.Strings(queues)
	out := make([]report.QueueTimingRow, 0, len(queues))
	for _, queue := range queues {
		secs := byQueue[queue]
		mean, p50, p95 := report.TimingOf(secs)
		out = append(out, report.QueueTimingRow{Queue: queue, Resolved: int64(len(secs)), MeanSeconds: mean, P50Seconds: p50, P95Seconds: p95})
	}
	return out, nil
}

// SLABreachRows implements report.Reader.
func (m *Memory) SLABreachRows(_ context.Context, q report.Query) ([]report.SLABreachRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type agg struct{ opened, escalated, breached int64 }
	bySource := map[string]*agg{}
	for _, c := range m.cases {
		if c.sourceType == nil || c.openedAt == nil || !inWindow(*c.openedAt, q.Range.From, q.Range.To) {
			continue
		}
		a := bySource[*c.sourceType]
		if a == nil {
			a = &agg{}
			bySource[*c.sourceType] = a
		}
		a.opened++
		breached := c.escalatedAt != nil ||
			(c.resolution() != nil && c.slaDueAt != nil && c.resolution().After(*c.slaDueAt))
		if c.escalatedAt != nil {
			a.escalated++
		}
		if breached {
			a.breached++
		}
	}
	sources := make([]string, 0, len(bySource))
	for sourceType := range bySource {
		sources = append(sources, sourceType)
	}
	sort.Strings(sources)
	out := make([]report.SLABreachRow, 0, len(sources))
	for _, sourceType := range sources {
		a := bySource[sourceType]
		out = append(out, report.SLABreachRow{
			SourceType: sourceType, Opened: a.opened, Escalated: a.escalated, Breached: a.breached,
			BreachRate: report.Share(a.breached, a.opened), EscalationRate: report.Share(a.escalated, a.opened),
		})
	}
	return out, nil
}

// LastEventAt implements report.Reader.
func (m *Memory) LastEventAt(_ context.Context) (*time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var newest *time.Time
	for _, at := range m.processed {
		utc := at.UTC()
		if newest == nil || utc.After(*newest) {
			newest = &utc
		}
	}
	return newest, nil
}
