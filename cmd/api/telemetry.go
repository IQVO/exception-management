package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/claudioed/exception-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/exception-management/internal/application/ports"
)

// defaultServiceName is the OTEL_SERVICE_NAME default: the name every span,
// metric and log `app` label of this binary carries.
const defaultServiceName = "exception-management"

// telemetryFlushTimeout bounds the final export attempt. Without a deadline the
// exporter would retry against an unreachable Collector and stretch a shutdown
// out well past what an orchestrator will wait for.
const telemetryFlushTimeout = 5 * time.Second

// telemetryEnv is the observability part of the environment (ADR 0014).
//
//	OTEL_SERVICE_NAME            service.name of spans and metrics (default exception-management)
//	OTEL_EXPORTER_OTLP_ENDPOINT  Collector OTLP/gRPC endpoint, host:port or URL (default telemetry.DefaultEndpoint)
//	SERVICE_VERSION              service.version (default telemetry.DefaultServiceVersion)
//	ENVIRONMENT                  deployment.environment.name, read by telemetry.Setup
type telemetryEnv struct {
	serviceName    string
	serviceVersion string
	otlpEndpoint   string
}

func loadTelemetryEnv(lookup func(string) string) telemetryEnv {
	or := func(key, fallback string) string {
		if v := lookup(key); v != "" {
			return v
		}
		return fallback
	}
	return telemetryEnv{
		serviceName:    or("OTEL_SERVICE_NAME", defaultServiceName),
		serviceVersion: or("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		otlpEndpoint:   or("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
	}
}

// newCaseMetrics builds the Tier 2 counter (exception.cases.opened) on the
// MeterProvider startTelemetry installed. It fails only on an invalid
// instrument name, a programming error: the service then runs without the
// counter, never without consuming facts, so the error is logged and a nil
// port (the use case's no-op) is returned. It must run after startTelemetry.
func newCaseMetrics(logger *slog.Logger) ports.CaseMetrics {
	m, err := telemetry.NewCaseMetrics()
	if err != nil {
		logger.Warn("case metrics unavailable; the service will run without the exception.cases.opened counter", "error", err)
		return nil
	}
	return m
}

// bootTelemetry reads the observability environment and brings OpenTelemetry
// up; it returns the config (the service name the router labels its spans
// with) and the flush func to defer.
func bootTelemetry(logger *slog.Logger) (telemetryEnv, func(), error) {
	cfg := loadTelemetryEnv(os.Getenv)
	flush, err := startTelemetry(logger, cfg)
	return cfg, flush, err
}

// startTelemetry brings OpenTelemetry up and returns the flush func to defer.
// Export is non-blocking: an unreachable Collector costs telemetry, never
// availability. A failed flush is logged, never returned: turning that into a
// non-zero exit would make every clean shutdown look like a crash.
//
// It must run before the HTTP router is built: otelchimetric binds the global
// MeterProvider when the router is constructed.
func startTelemetry(logger *slog.Logger, cfg telemetryEnv) (func(), error) {
	shutdown, err := telemetry.Setup(context.Background(), cfg.serviceName, cfg.serviceVersion, cfg.otlpEndpoint)
	if err != nil {
		return nil, err
	}
	logger.Info("telemetry configured", "service_name", cfg.serviceName, "otlp_endpoint", cfg.otlpEndpoint)
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlushTimeout)
		defer cancel()
		if err := shutdown(flushCtx); err != nil {
			logger.Warn("telemetry flush failed on shutdown", "error", err)
		}
	}, nil
}
