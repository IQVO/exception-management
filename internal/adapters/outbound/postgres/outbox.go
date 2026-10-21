package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// Aggregate type names of the outbox rows (aggreg_type column).
const (
	aggregExceptionCase   = "exceptioncase"
	aggregExceptionPolicy = "exceptionpolicy"
)

// OutboxPublisher is the pgxpool-backed ports.EventPublisher: the write side
// of the transactional outbox. Called in the SAME UnitOfWork as the
// aggregate's Save, every Publish inserts one row per domain fact; the relay
// (a later step) drains unpublished rows to Kafka as CloudEvents 1.0. The
// CloudEvents id (event_id, UUID v4) is minted ONCE here and persisted, so a
// relay retry republishes the same id.
type OutboxPublisher struct {
	pool *pgxpool.Pool
}

// NewOutboxPublisher constructs an OutboxPublisher over pool.
func NewOutboxPublisher(pool *pgxpool.Pool) *OutboxPublisher {
	return &OutboxPublisher{pool: pool}
}

// outboxEnvelope is the JSON payload of one outbox row: which fact happened,
// to which aggregate, when, and the fact's data already in the wire shape of
// ADR 0004 (the relay builds the CloudEvent from these fields; it never
// re-derives domain state).
type outboxEnvelope struct {
	Event       string    `json:"event"`
	AggregateID string    `json:"aggregate_id"`
	OccurredAt  time.Time `json:"occurred_at"`
	Data        any       `json:"data"`
}

// PublishCaseEvents implements ports.EventPublisher.
func (p *OutboxPublisher) PublishCaseEvents(ctx context.Context, events ...exceptioncase.Event) error {
	for _, e := range events {
		data, err := caseFactData(e)
		if err != nil {
			return err
		}
		env := outboxEnvelope{Event: e.EventName(), AggregateID: e.CaseID(), OccurredAt: e.OccurredAt().UTC(), Data: data}
		if err := p.enqueue(ctx, aggregExceptionCase, e.CaseID(), env); err != nil {
			return err
		}
	}
	return nil
}

// PublishPolicyDeclared implements ports.EventPublisher. The declaration
// fact carries only the identity of the new version, so the row's data is
// read back from exception_policies ON THE CALLER'S TRANSACTION — the
// declaring unit of work inserted that version in this same transaction, so
// the outbox row is self-contained and wire-complete without trusting the
// caller's in-memory value.
func (p *OutboxPublisher) PublishPolicyDeclared(ctx context.Context, d exceptionpolicy.PolicyDeclared) error {
	data, err := p.declaredFactData(ctx, d)
	if err != nil {
		return err
	}
	env := outboxEnvelope{Event: d.EventName(), AggregateID: d.SourceType(), OccurredAt: d.OccurredAt().UTC(), Data: data}
	return p.enqueue(ctx, aggregExceptionPolicy, d.SourceType(), env)
}

func (p *OutboxPublisher) enqueue(ctx context.Context, aggregType, aggregID string, env outboxEnvelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("encode outbox row for %s %s: %w", aggregType, aggregID, err)
	}
	_, err = queryFor(ctx, p.pool).Exec(ctx, `
		INSERT INTO outbox (event_id, aggreg_type, aggreg_id, payload, occurred_at)
		VALUES ($1, $2, $3, $4, $5)
	`, uuid.NewString(), aggregType, aggregID, json.RawMessage(payload), env.OccurredAt)
	if err != nil {
		return fmt.Errorf("enqueue %s fact for %s %s: %w", env.Event, aggregType, aggregID, err)
	}
	return nil
}

// caseFactData renders one case fact's data in the pinned wire shape of ADR
// 0004 (snake_case keys; the frozen source payload embedded as the JSON
// object it is). An unknown fact type is a programming error, not a
// runtime condition: the domain raises exactly the six facts below.
func caseFactData(e exceptioncase.Event) (any, error) {
	switch f := e.(type) {
	case exceptioncase.ExceptionOpened:
		return openedData{
			CaseID: f.CaseID(), SourceType: f.Key.SourceType, SourceSubject: f.Key.SourceSubject,
			SiteID: f.Key.SiteID, Severity: string(f.Severity), Queue: f.Queue,
			OpenedAt: f.OccurredAt().UTC(), SLADueAt: f.SLADueAt.UTC(),
			SourceEventID: f.SourceEventID, SourcePayload: json.RawMessage(f.SourcePayload),
		}, nil
	case exceptioncase.ExceptionTriaged:
		return triagedData{
			CaseID: f.CaseID(), Severity: string(f.Severity), Queue: f.Queue,
			Actor: f.Actor.String(), At: f.OccurredAt().UTC(),
		}, nil
	case exceptioncase.ExceptionAssigned:
		return assignedData{
			CaseID: f.CaseID(), Assignee: f.Assignee, Actor: f.Actor.String(), At: f.OccurredAt().UTC(),
		}, nil
	case exceptioncase.ExceptionEscalated:
		return escalatedData{
			CaseID: f.CaseID(), SLADueAt: f.SLADueAt.UTC(), At: f.OccurredAt().UTC(),
		}, nil
	case exceptioncase.ExceptionResolved:
		return resolvedData{
			CaseID: f.CaseID(), ResolutionCode: f.ResolutionCode, RootCause: f.RootCause,
			Note: f.Note, Actor: f.Actor.String(), At: f.OccurredAt().UTC(),
		}, nil
	case exceptioncase.ExceptionAutoResolved:
		return autoResolvedData{
			CaseID: f.CaseID(), RuleID: f.RuleID, CorrelatedType: f.CorrelatedType,
			CorrelatedEventID: f.CorrelatedEventID, At: f.OccurredAt().UTC(),
		}, nil
	default:
		return nil, fmt.Errorf("unknown exception case fact %T: no wire shape is declared for it", e)
	}
}

type openedData struct {
	CaseID        string          `json:"case_id"`
	SourceType    string          `json:"source_type"`
	SourceSubject string          `json:"source_subject"`
	SiteID        string          `json:"site_id"`
	Severity      string          `json:"severity"`
	Queue         string          `json:"queue"`
	OpenedAt      time.Time       `json:"opened_at"`
	SLADueAt      time.Time       `json:"sla_due_at"`
	SourceEventID string          `json:"source_event_id"`
	SourcePayload json.RawMessage `json:"source_payload"`
}

type triagedData struct {
	CaseID   string    `json:"case_id"`
	Severity string    `json:"severity"`
	Queue    string    `json:"queue"`
	Actor    string    `json:"actor"`
	At       time.Time `json:"at"`
}

type assignedData struct {
	CaseID   string    `json:"case_id"`
	Assignee string    `json:"assignee"`
	Actor    string    `json:"actor"`
	At       time.Time `json:"at"`
}

type escalatedData struct {
	CaseID   string    `json:"case_id"`
	SLADueAt time.Time `json:"sla_due_at"`
	At       time.Time `json:"at"`
}

type resolvedData struct {
	CaseID         string    `json:"case_id"`
	ResolutionCode string    `json:"resolution_code"`
	RootCause      string    `json:"root_cause"`
	Note           string    `json:"note"`
	Actor          string    `json:"actor"`
	At             time.Time `json:"at"`
}

type autoResolvedData struct {
	CaseID            string    `json:"case_id"`
	RuleID            string    `json:"rule_id"`
	CorrelatedType    string    `json:"correlated_type"`
	CorrelatedEventID string    `json:"correlated_event_id"`
	At                time.Time `json:"at"`
}

// declaredData is the wire shape of the analytics-only declaration fact
// (ADR 0004): the full policy version, auto_resolve null when the source
// type has no rule.
type declaredData struct {
	SourceType  string           `json:"source_type"`
	Version     int64            `json:"version"`
	Severity    string           `json:"severity"`
	Queue       string           `json:"queue"`
	SLAMinutes  int64            `json:"sla_minutes"`
	AutoResolve *autoResolveData `json:"auto_resolve"`
}

type autoResolveData struct {
	OnType        string `json:"on_type"`
	WithinMinutes int64  `json:"within_minutes"`
}

func (p *OutboxPublisher) declaredFactData(ctx context.Context, d exceptionpolicy.PolicyDeclared) (any, error) {
	var (
		severity, queue string
		sla             int64
		on              *string
		within          *int64
	)
	err := queryFor(ctx, p.pool).QueryRow(ctx, `
		SELECT severity, queue, sla_minutes, auto_resolve_on_type, auto_resolve_within_minutes
		FROM exception_policies WHERE source_type = $1 AND version = $2
	`, d.SourceType(), d.Version()).Scan(&severity, &queue, &sla, &on, &within)
	if err != nil {
		return nil, fmt.Errorf("read declared policy %s v%d for its outbox row: %w", d.SourceType(), d.Version(), err)
	}
	data := declaredData{SourceType: d.SourceType(), Version: d.Version(), Severity: severity, Queue: queue, SLAMinutes: sla}
	if on != nil && within != nil {
		data.AutoResolve = &autoResolveData{OnType: *on, WithinMinutes: *within}
	}
	return data, nil
}
