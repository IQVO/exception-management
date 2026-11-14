// Command exceptions-reports is the read-only HTTP reader of the
// exception-management analytics read side (ADR 0013):
// GET /reports/root-cause-frequency, GET /reports/mean-time-to-resolve,
// GET /reports/sla-breach-rate, GET /reports/freshness and /healthz on
// :8092, answered from the ANALYTICAL database only (a read-only pool; the
// database role should be read-only too). It writes nothing and never opens
// the OLTP database or Kafka. No auth layer: access control is the
// in-cluster boundary (fleet rule).
//
// Environment:
//
//	ANALYTICS_DATABASE_URL  REQUIRED. Analytical Postgres DSN, ideally a read-only role
//	HTTP_ADDR               listen address (default :8092)
//	LOG_LEVEL               debug | info | warn (or warning) | error (default info)
//	OTEL_EXPORTER_OTLP_ENDPOINT  Collector OTLP/gRPC endpoint (default localhost:4317; export never blocks)
//	OTEL_SERVICE_NAME       service.name of spans, metrics and logs (default exception-management-reports)
//	SERVICE_VERSION         service.version (default dev)
//	ENVIRONMENT             deployment.environment.name (default local)
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/exception-management/internal/adapters/inbound/http"
	"github.com/claudioed/exception-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/exception-management/internal/adapters/outbound/telemetry"
)

const shutdownTimeout = 10 * time.Second

var errMissingAnalyticsURL = errors.New("ANALYTICS_DATABASE_URL is required: the reports are served from the analytical database")

func main() {
	if err := run(); err != nil {
		slog.Error("exceptions-reports exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	url := os.Getenv("ANALYTICS_DATABASE_URL")
	if url == "" {
		return errMissingAnalyticsURL
	}

	ctx := context.Background()
	pool, err := analyticsstore.NewReadOnlyPool(ctx, url)
	if err != nil {
		return fmt.Errorf("open analytics pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping analytics database: %w", err)
	}

	telemetryCfg := loadTelemetryEnv(os.Getenv)
	flushTelemetry, err := startTelemetry(logger, telemetryCfg)
	if err != nil {
		return fmt.Errorf("start telemetry: %w", err)
	}
	defer flushTelemetry()

	httpAddr := getenv("HTTP_ADDR", ":8092")
	srv := &http.Server{
		Addr: httpAddr,
		Handler: inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{
			Reader: analyticsstore.NewReader(pool), Logger: logger, ServiceName: telemetryCfg.serviceName,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("reports server listening", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	sig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		return fmt.Errorf("reports server failed: %w", err)
	case <-sig.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func newLogger(level string) *slog.Logger { return newLoggerTo(os.Stdout, level) }

// newLoggerTo builds the JSON logger writing to w; the handler is wrapped so a
// record written with a span-carrying context also carries trace_id and span_id.
func newLoggerTo(w io.Writer, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(telemetry.WithTraceContext(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
