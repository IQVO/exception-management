// Command mcp is the composition root for the exception-management MCP
// server: it wires env config to the Postgres repositories, the repositories
// to the SAME read use cases the REST adapter uses (ListOpenExceptions,
// GetException, ListExceptionPolicies, GetSLAReport), and those to the
// inbound MCP adapter, then serves MCP over Streamable HTTP (and nothing
// else: no stdio, no SSE). It is a second, independent deployable alongside
// cmd/api — the ops-agent read surface (no manual case creation, auto-resolve
// is a policy rule, the agent only reads).
//
// The surface is READ-ONLY: no write use case is wired here at all, so this
// binary can never insert into the outbox (it builds no event sink, no unit
// of work and no outbox repository). It never starts the outbox relay, never
// runs the six fact consumers or the SLA sweeper and never dials Kafka (no
// KAFKA_BROKERS, no consumer groups, no *_MODE): those run in cmd/api only.
// Like cmd/api it has no in-memory mode: without DATABASE_URL it refuses to
// boot.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	MCP_ADDR                 listen address (default :8090)
//	LOG_LEVEL                debug | info | warn | error (default info)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/exception-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// shutdownTimeout bounds the HTTP server's graceful drain.
const shutdownTimeout = 10 * time.Second

// errNoDatabase is the boot error of a missing DATABASE_URL.
var errNoDatabase = errors.New("DATABASE_URL is required: this context runs on real Postgres")

func main() {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	if err := run(context.Background(), logger); err != nil {
		logger.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("mcp server stopped cleanly")
}

// run boots the server and serves until SIGINT/SIGTERM or until ctx is
// cancelled, then drains gracefully. It reads the whole configuration from
// the environment.
func run(ctx context.Context, logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	deps, closeAdapters, err := buildDeps(ctx, logger, databaseURL)
	if err != nil {
		return err
	}
	defer closeAdapters()

	srv := &http.Server{
		Addr:              getenv("MCP_ADDR", ":8090"),
		Handler:           newRouter(inboundmcp.Handler(inboundmcp.NewServer(deps))),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveMCP(logger, srv)
}

// buildDeps migrates and connects Postgres and wires the READ use cases over
// it. The returned close func releases the pool. An empty databaseURL is a
// boot error: there is no fallback runtime.
//
// Like every sibling's cmd/mcp this binary runs the idempotent embedded
// migrations on start, so it can boot against a fresh database before cmd/api
// has; golang-migrate's advisory lock makes concurrent starts safe.
// MIGRATIONS_DATABASE_URL is used ONLY for that step (a DIRECT Postgres DSN
// where DATABASE_URL points at PgBouncer, which cannot honour the
// session-scoped advisory lock); the runtime pool always uses DATABASE_URL.
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL string) (inboundmcp.Deps, func(), error) {
	noop := func() {}
	if databaseURL == "" {
		return inboundmcp.Deps{}, noop, errNoDatabase
	}
	if err := postgres.RunMigrations(postgres.MigrationsURL(os.Getenv)); err != nil {
		return inboundmcp.Deps{}, noop, fmt.Errorf("run migrations: %w", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inboundmcp.Deps{}, noop, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return inboundmcp.Deps{}, noop, fmt.Errorf("ping postgres: %w", err)
	}
	logger.Info("postgres repositories configured")

	// The read use cases only; the system clock stamps the SLA report.
	cases := postgres.NewCaseRepo(pool)
	policies := postgres.NewPolicyRepo(pool)
	return inboundmcp.Deps{
		List:      &usecases.ListOpenExceptions{Cases: cases},
		Get:       &usecases.GetException{Cases: cases},
		Policies:  &usecases.ListExceptionPolicies{Policies: policies},
		SLAReport: &usecases.GetSLAReport{Cases: cases, Clock: system.Clock{}},
	}, pool.Close, nil
}

// serveMCP runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveMCP(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// newLogger builds the process-wide JSON logger. LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive; "warning" is accepted
// like "warn" fleet-wide), defaulting to Info.
func newLogger(level string) *slog.Logger {
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
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
