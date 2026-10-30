// Command a2a is the composition root for the exception-management A2A
// (Agent2Agent) agent: it wires env config to the Postgres repositories, the
// repositories to the SAME read use cases the REST and MCP adapters use
// (ListOpenExceptions, GetException, ListExceptionPolicies, GetSLAReport), and
// those to the inbound A2A adapter, then serves Agent2Agent JSON-RPC over HTTP
// (ADR 0007). It is a third, independent deployable alongside cmd/api and
// cmd/mcp: A2A carries DELEGATION of a goal (sla_status, find_exceptions),
// never data sync.
//
// The surface is READ-ONLY, mirroring the MCP charter: no write use case is
// wired here at all, so this binary can never insert into the outbox (it
// builds no event sink, no unit of work and no outbox repository). It never
// starts the outbox relay, never runs the six fact consumers or the SLA
// sweeper and never dials Kafka (no KAFKA_BROKERS, no consumer groups, no
// EVENT_PUBLISHER): those run in cmd/api only. Like cmd/api it has no
// in-memory mode: without DATABASE_URL it refuses to boot.
//
// There is NO auth of any kind (fleet-wide revert 2026-09-11): no keys, no
// bearer checks. Access control is the in-cluster ClusterIP boundary.
//
// Environment:
//
//	DATABASE_URL             REQUIRED. Postgres DSN
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	                         (in-cluster: http://<release>-a2a.<ns>.svc.cluster.local:8093/)
//	A2A_ADDR                 listen address (default :8093)
//	LOG_LEVEL                debug | info | warn | error (default info)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inbounda2a "github.com/claudioed/exception-management/internal/adapters/inbound/a2a"
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
		logger.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("a2a agent stopped cleanly")
}

// errNoAgentURL is the boot error of a missing A2A_AGENT_URL: the Agent Card
// must advertise the URL clients dial.
var errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR 0007), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract, so a relative or schemeless value would be a card
// no client can dial; it is refused at boot rather than served.
func checkAgentURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR 0007)", raw)
	}
	return nil
}

// run boots the agent and serves until SIGINT/SIGTERM or until ctx is
// cancelled, then drains gracefully. It reads the whole configuration from
// the environment and validates it BEFORE any connection or listener.
func run(ctx context.Context, logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errNoDatabase
	}
	agentURL := strings.TrimSpace(os.Getenv("A2A_AGENT_URL"))
	if err := checkAgentURL(agentURL); err != nil {
		return err
	}
	deps, closeAdapters, err := buildDeps(ctx, logger, databaseURL)
	if err != nil {
		return err
	}
	defer closeAdapters()

	card := inbounda2a.Card(inbounda2a.Config{AgentURL: agentURL, Version: serviceVersion})
	srv := &http.Server{
		Addr:              getenv("A2A_ADDR", ":8093"),
		Handler:           newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serveA2A(logger, srv)
}

// serviceVersion is the Agent Card version.
const serviceVersion = "1.0.0"

// buildDeps migrates and connects Postgres and wires the READ use cases over
// it. The returned close func releases the pool. An empty databaseURL is a
// boot error: there is no fallback runtime.
//
// Like cmd/mcp this binary runs the idempotent embedded
// migrations on start, so it can boot against a fresh database before cmd/api
// has; golang-migrate's advisory lock makes concurrent starts safe.
// MIGRATIONS_DATABASE_URL is used ONLY for that step (a DIRECT Postgres DSN
// where DATABASE_URL points at PgBouncer, which cannot honour the
// session-scoped advisory lock); the runtime pool always uses DATABASE_URL.
func buildDeps(ctx context.Context, logger *slog.Logger, databaseURL string) (inbounda2a.Deps, func(), error) {
	noop := func() {}
	if databaseURL == "" {
		return inbounda2a.Deps{}, noop, errNoDatabase
	}
	if err := postgres.RunMigrations(postgres.MigrationsURL(os.Getenv)); err != nil {
		return inbounda2a.Deps{}, noop, fmt.Errorf("run migrations: %w", err)
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return inbounda2a.Deps{}, noop, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return inbounda2a.Deps{}, noop, fmt.Errorf("ping postgres: %w", err)
	}
	logger.Info("postgres repositories configured")

	// The read use cases only; the system clock stamps the SLA report.
	cases := postgres.NewCaseRepo(pool)
	policies := postgres.NewPolicyRepo(pool)
	return inbounda2a.Deps{
		List:      &usecases.ListOpenExceptions{Cases: cases},
		Get:       &usecases.GetException{Cases: cases},
		Policies:  &usecases.ListExceptionPolicies{Policies: policies},
		SLAReport: &usecases.GetSLAReport{Cases: cases, Clock: system.Clock{}},
	}, pool.Close, nil
}

// serveA2A runs srv until it fails or SIGINT/SIGTERM arrives, then drains it
// gracefully.
func serveA2A(logger *slog.Logger, srv *http.Server) error {
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC over HTTP)", "addr", srv.Addr)
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
