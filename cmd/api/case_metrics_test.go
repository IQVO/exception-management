package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// The Tier 2 counter is wired into the one use case the consumers drive: a
// fact it opens is counted on exception.cases.opened through the global
// MeterProvider that startTelemetry installed.
func TestOpenOrTouchIsWiredWithTheCaseCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	cases, policies, events := memory.NewCases(), memory.NewPolicies(), &memory.Events{}
	uc := newOpenOrTouch(cases, policies, events, nil, memory.Clock{}, quietLogger())
	if uc.Metrics == nil {
		t.Fatal("OpenOrTouchException is wired without its CaseMetrics")
	}

	// No policy declared: the fact is refused, and the refusal is counted.
	_, err := uc.Execute(context.Background(), usecases.Fact{
		SourceType: "com.warehouse.wes.x.y.Z", SourceSubject: "s-1", SourceEventID: "ev-1", Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("a fact without a declared policy was accepted")
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			found = found || m.Name == "exception.cases.opened"
		}
	}
	if !found {
		t.Fatalf("exception.cases.opened was not emitted: %+v", collected.ScopeMetrics)
	}
}
