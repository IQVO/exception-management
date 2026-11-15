package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/exception-management/internal/application/ports"
)

// meterName scopes this service's own instruments, keeping them distinct from
// the ones otelchi and the runtime collector register.
const meterName = "github.com/claudioed/exception-management"

// casesOpenedName is the Tier 2 business metric of the context (ADR 0014, the
// fleet's standard metrics convention): every consumed fact the open-or-touch
// decision handles, by outcome.
const casesOpenedName = "exception.cases.opened"

const (
	outcomeKey  = attribute.Key("outcome")
	reasonKey   = attribute.Key("reason")
	severityKey = attribute.Key("exception.severity")

	outcomeOpened   = "opened"
	outcomeTouched  = "touched"
	outcomeReplayed = "replayed"
	outcomeRejected = "rejected"
)

// CaseMetrics implements ports.CaseMetrics against the global MeterProvider.
// Until Setup installs a real provider the global one is a no-op, so recording
// is cheap and safe in tests and local runs.
type CaseMetrics struct {
	counter metric.Int64Counter
}

var _ ports.CaseMetrics = (*CaseMetrics)(nil)

// NewCaseMetrics registers the counter. It only fails if the instrument name
// is invalid, which is a programming error, not a runtime condition. Call it
// after Setup so the instrument binds to the real MeterProvider.
func NewCaseMetrics() (*CaseMetrics, error) {
	counter, err := otel.Meter(meterName).Int64Counter(
		casesOpenedName,
		metric.WithDescription("Consumed facts handled by the open-or-touch decision, by outcome: opened (a new case), "+
			"touched (an existing case's occurrence went up), replayed (the opening event again, a no-op) or "+
			"rejected (reason is the problem slug: policy-undeclared, invalid-fact, concurrent-modification, "+
			"open-case-exists, internal-error). A rising touched share means one condition keeps recurring on "+
			"the same subject; a rising policy-undeclared means a producer emits a source type nobody declared "+
			"a policy for (a configuration gap); a rising internal-error or concurrent-modification means the "+
			"service itself is failing to take facts in."),
		metric.WithUnit("{case}"),
	)
	if err != nil {
		return nil, err
	}
	return &CaseMetrics{counter: counter}, nil
}

// CaseOpened counts a fact that opened a case of the given severity.
func (m *CaseMetrics) CaseOpened(ctx context.Context, severity string) {
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcomeOpened), severityKey.String(severity)))
}

// CaseTouched counts a fact that touched an existing case.
func (m *CaseMetrics) CaseTouched(ctx context.Context, severity string) {
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcomeTouched), severityKey.String(severity)))
}

// CaseReplayed counts the replay of a case's opening event.
func (m *CaseMetrics) CaseReplayed(ctx context.Context, severity string) {
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcomeReplayed), severityKey.String(severity)))
}

// CaseRejected counts a fact that opened or touched nothing, with the reason.
func (m *CaseMetrics) CaseRejected(ctx context.Context, reason string) {
	m.counter.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(outcomeRejected), reasonKey.String(reason)))
}
