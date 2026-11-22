package http

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// durationPoints installs a manual-reader MeterProvider globally for the
// duration of the test and returns a func that collects the data points of
// http.server.request.duration as "route" -> sample count. It must be
// called BEFORE the router under test is built: otelchimetric binds to
// otel.GetMeterProvider() at construction time.
func durationPoints(t *testing.T) func() map[string]uint64 {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	return func() map[string]uint64 {
		t.Helper()
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("collect metrics: %v", err)
		}
		got := map[string]uint64{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "http.server.request.duration" {
					addRouteCounts(t, m, got)
				}
			}
		}
		return got
	}
}

// addRouteCounts adds the sample count of each data point of the
// request-duration histogram m to got, keyed by its http.route attribute.
func addRouteCounts(t *testing.T, m metricdata.Metrics, got map[string]uint64) {
	t.Helper()
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("http.server.request.duration is a %T, want a float64 Histogram", m.Data)
	}
	if m.Unit != "s" {
		t.Errorf("unit = %q, want s", m.Unit)
	}
	for _, dp := range hist.DataPoints {
		route, _ := dp.Attributes.Value("http.route")
		got[route.AsString()] += dp.Count
	}
}

// ADR-0016 Tier 1: every HTTP server of the fleet emits
// http.server.request.duration, labelled with the chi route PATTERN (never
// the raw path, which would mint one series per case id).
func TestRouter_RecordsTheRequestDurationByRoutePattern(t *testing.T) {
	collect := durationPoints(t)
	f := newFixture(t)
	router := f.router()

	mustCall(t, router, http.MethodGet, "/exceptions/case-cardinality-1", "", http.StatusNotFound, nil)
	mustCall(t, router, http.MethodGet, "/exceptions/case-cardinality-2", "", http.StatusNotFound, nil)

	got := collect()
	if got["/exceptions/{caseId}"] != 2 {
		t.Fatalf("durations by route = %v, want 2 samples under the pattern /exceptions/{caseId}", got)
	}
	for route := range got {
		if route == "/exceptions/case-cardinality-1" || route == "/exceptions/case-cardinality-2" {
			t.Errorf("route label %q is a raw path: it would mint one series per case", route)
		}
	}
}

func TestReportsRouter_RecordsTheRequestDuration(t *testing.T) {
	collect := durationPoints(t)
	router := NewReportsRouter(&ReportsServer{})

	mustCall(t, router, http.MethodGet, "/healthz", "", http.StatusOK, nil)

	if got := collect(); got["/healthz"] != 1 {
		t.Fatalf("durations by route = %v, want one sample for /healthz on the reports router", got)
	}
}

// spanCapturingHandler stands in for the trace-correlating slog handler the
// composition root installs. That handler lives in the outbound telemetry
// adapter, which this inbound adapter must not import, so what is asserted
// here is the part this layer is responsible for: that RequestLogger hands
// its records a context carrying the request's span.
type spanCapturingHandler struct {
	slog.Handler
	seen trace.SpanContext
}

func (h *spanCapturingHandler) Handle(ctx context.Context, record slog.Record) error {
	h.seen = trace.SpanContextFromContext(ctx)
	return h.Handler.Handle(ctx, record)
}

// tracedRouter builds the REST router with a recording TracerProvider
// installed globally and a logger whose handler records the span context of
// the record it is handed.
func tracedRouter(t *testing.T) (http.Handler, *tracetest.SpanRecorder, *spanCapturingHandler) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	handler := &spanCapturingHandler{Handler: slog.NewJSONHandler(&bytes.Buffer{}, nil)}
	f := newFixture(t)
	f.srv.Logger = slog.New(handler)
	f.srv.ServiceName = "exception-management"
	return f.router(), recorder, handler
}

func TestRouter_NamesTheServerSpanAfterTheRoutePattern(t *testing.T) {
	router, recorder, _ := tracedRouter(t)

	mustCall(t, router, http.MethodGet, "/exceptions/case-cardinality", "", http.StatusNotFound, nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if spans[0].SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want server", spans[0].SpanKind())
	}
	if name := spans[0].Name(); name != "/exceptions/{caseId}" {
		t.Errorf("span name = %q, want the route pattern /exceptions/{caseId}", name)
	}
}

// The request line must be written under the request span: otelchi has to
// run BEFORE RequestLogger and the logger must use the *Context variants.
func TestRouter_RequestLogsAreWrittenUnderTheRequestSpan(t *testing.T) {
	router, recorder, logHandler := tracedRouter(t)

	mustCall(t, router, http.MethodGet, "/exceptions/case-1", "", http.StatusNotFound, nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	want := spans[0].SpanContext()
	if !logHandler.seen.IsValid() {
		t.Fatal("the request log was written with a context carrying no span: middleware order is wrong")
	}
	if logHandler.seen.TraceID() != want.TraceID() || logHandler.seen.SpanID() != want.SpanID() {
		t.Errorf("log ids = %s/%s, want the server span's %s/%s",
			logHandler.seen.TraceID(), logHandler.seen.SpanID(), want.TraceID(), want.SpanID())
	}
}

// A handler panic is still answered with the RFC 7807 500 and the request
// is still logged and measured (Recoverer sits INSIDE the logger).
func TestRouter_APanicIsAProblemAndStillLogged(t *testing.T) {
	collect := durationPoints(t)
	f := newFixture(t)
	f.srv.Get = nil // the handler dereferences it: a panic
	var buf bytes.Buffer
	f.srv.Logger = slog.New(slog.NewJSONHandler(&buf, nil))
	router := f.router()

	rec := call(t, router, http.MethodGet, "/exceptions/case-1", "", nil)

	wantProblem(t, rec, http.StatusInternalServerError, "internal-error")
	if !bytes.Contains(buf.Bytes(), []byte(`"msg":"http request"`)) || !bytes.Contains(buf.Bytes(), []byte(`"status":500`)) {
		t.Errorf("the panicked request was not logged at 500: %s", buf.String())
	}
	if collect()["/exceptions/{caseId}"] != 1 {
		t.Error("the panicked request was not measured")
	}
}
