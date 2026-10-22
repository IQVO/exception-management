package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/exception-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/exception-management/internal/analytics/report"
)

// The `<entity>` segments of the types this service publishes
// (com.warehouse.wes.exception-management.<entity>.<EventName>). They mirror
// the outbound encoder without importing an outbound package (inbound
// adapters never depend on outbound ones, arch test).
const (
	analyticsEntityCase   = "exceptioncase"
	analyticsEntityPolicy = "exceptionpolicy"
)

// analyticsKinds maps the FULL CloudEvents type of the seven published events
// to their report.Kind (ADR 0006's catalogue). Every other type on the topic
// is acknowledged and ignored.
var analyticsKinds = map[string]report.Kind{
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionOpened)):           report.KindExceptionOpened,
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionTriaged)):          report.KindExceptionTriaged,
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionAssigned)):         report.KindExceptionAssigned,
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionEscalated)):        report.KindExceptionEscalated,
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionResolved)):         report.KindExceptionResolved,
	cloudevents.Type(analyticsEntityCase, string(report.KindExceptionAutoResolved)):     report.KindExceptionAutoResolved,
	cloudevents.Type(analyticsEntityPolicy, string(report.KindExceptionPolicyDeclared)): report.KindExceptionPolicyDeclared,
}

// analyticsAutoResolve mirrors the declaration payload's auto_resolve object.
type analyticsAutoResolve struct {
	OnType        string `json:"on_type"`
	WithinMinutes int64  `json:"within_minutes"`
}

// analyticsData is the union of the analytics payload fields the projection
// reads (hand-mirrored from apis/asyncapi.yaml, never imported from the
// domain): every published type's data, snake_case, timestamps as the RFC
// 3339 strings the publisher writes.
type analyticsData struct {
	CaseID            string                `json:"case_id"`
	SourceType        string                `json:"source_type"`
	SourceSubject     string                `json:"source_subject"`
	SiteID            *string               `json:"site_id"`
	Severity          string                `json:"severity"`
	Queue             string                `json:"queue"`
	SLADueAt          string                `json:"sla_due_at"`
	Assignee          string                `json:"assignee"`
	ResolutionCode    string                `json:"resolution_code"`
	RootCause         string                `json:"root_cause"`
	Note              *string               `json:"note"`
	RuleID            string                `json:"rule_id"`
	CorrelatedType    string                `json:"correlated_type"`
	CorrelatedEventID string                `json:"correlated_event_id"`
	Version           int64                 `json:"version"`
	SLAMinutes        int64                 `json:"sla_minutes"`
	AutoResolve       *analyticsAutoResolve `json:"auto_resolve"`
}

// NewAnalyticsConsumer builds the projector's consumer (ADR 0006): it reads
// the analytics topic under a FIXED, env-supplied consumer group and folds
// each event into the analytical model through report.Projection.
//
// A brand-new group starts at the EARLIEST offset (kafka-go's default for a
// group without committed offsets is the latest): the model is a projection
// that can be rebuilt from retained history, so a fresh projector replays
// the whole topic instead of skipping to the tail.
//
// Outcomes per message (the reference delivery policy of the fleet's
// analytics read sides): not a CloudEvents 1.0 message: WARN and commit
// past; a valid CloudEvent of another type: ignored; a known type with an
// unusable payload or a store rejection (report.ErrRejected):
// dead-lettered AT ONCE to "<topic>.dlq", then committed past; a transient
// failure: retried on the SAME message with capped backoff, never
// dead-lettered.
func NewAnalyticsConsumer(brokers []string, topic, groupID string, projection report.Projection, logger *slog.Logger) *Consumer {
	handlers := make(map[string]HandlerFunc, len(analyticsKinds))
	for fullType, kind := range analyticsKinds {
		handlers[fullType] = func(ctx context.Context, e ce.Event) error { return project(ctx, projection, kind, e) }
	}
	cfg := readerConfig(brokers, topic, groupID)
	// kafka-go reads a group's start offset only from the config, and its
	// default for a group without committed offsets is the LATEST; the
	// projector's model is rebuildable, so it starts at the earliest.
	cfg.StartOffset = kafkago.FirstOffset
	cfg.ErrorLogger = readerErrorLogger(defaultLogger(logger), "analytics consumer", topic)
	return &Consumer{
		Name:     "analytics consumer",
		Topic:    topic,
		Reader:   kafkago.NewReader(cfg),
		Handlers: handlers,
		Logger:   defaultLogger(logger),
		DLQ:      newDLQWriter(brokers, topic),
	}
}

// project maps one decoded CloudEvent of a known kind onto a report.Event
// and applies it. Every deterministic refusal is an errDeadLetter marker (the
// run loop dead-letters and commits past); anything else is transient.
func project(ctx context.Context, projection report.Projection, kind report.Kind, e ce.Event) error {
	pe, err := toReportEvent(kind, e)
	if err != nil {
		return &errDeadLetter{cause: err}
	}
	if _, err := projection.Apply(ctx, pe); err != nil {
		if errors.Is(err, report.ErrRejected) {
			//dlq:deterministic ErrRejected is minted only by the pure event validation or a Postgres SQLSTATE 22/23 rejection (analyticsstore.classify) -- never by a cancelled context
			return &errDeadLetter{cause: err}
		}
		return fmt.Errorf("analytics: project %s %s: %w", kind, e.ID(), err)
	}
	return nil
}

// toReportEvent validates a decoded CloudEvent of a known kind and maps it to
// a report.Event. Every error is deterministic: the same bytes can never
// pass.
func toReportEvent(kind report.Kind, evt ce.Event) (report.Event, error) {
	var d analyticsData
	if err := evt.DataAs(&d); err != nil {
		return report.Event{}, fmt.Errorf("decode %s data: %w", kind, err)
	}
	e := report.Event{Kind: kind, EventID: evt.ID(), At: evt.Time().UTC(),
		CaseID: d.CaseID, SourceType: d.SourceType, SourceSubject: d.SourceSubject, SiteID: d.SiteID,
		Severity: d.Severity, Queue: d.Queue, Assignee: d.Assignee,
		ResolutionCode: d.ResolutionCode, RootCause: d.RootCause, Note: d.Note,
		RuleID: d.RuleID, CorrelatedType: d.CorrelatedType,
		Version: d.Version, SLAMinutes: d.SLAMinutes}
	if d.SLADueAt != "" {
		due, err := time.Parse(time.RFC3339, d.SLADueAt)
		if err != nil {
			return report.Event{}, fmt.Errorf("%s %s: sla_due_at %q is not an RFC 3339 timestamp", kind, evt.ID(), d.SLADueAt)
		}
		e.SLADueAt = due.UTC()
	}
	if d.AutoResolve != nil {
		e.AutoResolve = &report.AutoResolveRule{OnType: d.AutoResolve.OnType, WithinMinutes: d.AutoResolve.WithinMinutes}
	}
	if want := subjectOf(e); want != evt.Subject() {
		return report.Event{}, fmt.Errorf("%s %s: payload identifies %q but the subject is %q", kind, evt.ID(), want, evt.Subject())
	}
	if err := e.Validate(); err != nil {
		return report.Event{}, err
	}
	return e, nil
}

// subjectOf is the CloudEvents subject an event must carry: the case id for
// the case facts, the governed source type for the policy declaration.
func subjectOf(e report.Event) string {
	if e.Kind.IsCase() {
		return e.CaseID
	}
	return e.SourceType
}
