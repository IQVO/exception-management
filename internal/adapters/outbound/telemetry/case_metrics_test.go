package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/exception-management/internal/adapters/outbound/telemetry"
)

// useManualReader installs a manual-reader MeterProvider globally for the
// test and returns the reader. NewCaseMetrics binds the global provider when
// it is built, so call this first.
func useManualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

// casePoints reads exception.cases.opened as "outcome|reason|severity" -> value
// (an absent attribute is the empty string), checking the instrument's shape.
func casePoints(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "exception.cases.opened" {
				addCasePoints(t, m, got)
			}
		}
	}
	return got
}

func addCasePoints(t *testing.T, m metricdata.Metrics, got map[string]int64) {
	t.Helper()
	if m.Unit != "{case}" || m.Description == "" {
		t.Errorf("unit = %q, description empty = %v; the convention wants a UCUM unit and a description", m.Unit, m.Description == "")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok || !sum.IsMonotonic {
		t.Fatalf("exception.cases.opened is a %T (monotonic=%v), want a monotonic int64 Sum (a counter)", m.Data, ok && sum.IsMonotonic)
	}
	for _, point := range sum.DataPoints {
		outcome, _ := point.Attributes.Value("outcome")
		reason, _ := point.Attributes.Value("reason")
		severity, _ := point.Attributes.Value("exception.severity")
		got[outcome.AsString()+"|"+reason.AsString()+"|"+severity.AsString()] += point.Value
	}
}

func TestCaseMetricsCountsByOutcomeReasonAndSeverity(t *testing.T) {
	reader := useManualReader(t)
	metrics, err := telemetry.NewCaseMetrics()
	if err != nil {
		t.Fatalf("NewCaseMetrics: %v", err)
	}
	ctx := context.Background()
	metrics.CaseOpened(ctx, "HIGH")
	metrics.CaseOpened(ctx, "HIGH")
	metrics.CaseOpened(ctx, "LOW")
	metrics.CaseTouched(ctx, "HIGH")
	metrics.CaseReplayed(ctx, "LOW")
	metrics.CaseRejected(ctx, "policy-undeclared")
	metrics.CaseRejected(ctx, "policy-undeclared")
	metrics.CaseRejected(ctx, "invalid-fact")

	got := casePoints(t, reader)
	want := map[string]int64{
		"opened||HIGH":                2,
		"opened||LOW":                 1,
		"touched||HIGH":               1,
		"replayed||LOW":               1,
		"rejected|policy-undeclared|": 2,
		"rejected|invalid-fact|":      1,
	}
	if len(got) != len(want) {
		t.Fatalf("data points = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
}

// Whatever the global provider is, recording is fire-and-forget.
func TestCaseMetricsRecordingNeverPanics(t *testing.T) {
	metrics, err := telemetry.NewCaseMetrics()
	if err != nil {
		t.Fatal(err)
	}
	metrics.CaseOpened(context.Background(), "HIGH")
	metrics.CaseRejected(context.Background(), "internal-error")
}
