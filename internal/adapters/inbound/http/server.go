package http

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/exception-management/internal/adapters/inbound/requestlog"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// DefaultServiceName labels the REST router's spans and metrics when the
// caller names none. It matches the api's OTEL_SERVICE_NAME default.
const DefaultServiceName = "exception-management"

// DefaultReportsServiceName is the same for the reports router
// (cmd/exceptions-reports).
const DefaultReportsServiceName = "exception-management-reports"

// serviceNameOr is name, or fallback when name is empty.
func serviceNameOr(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

// systemActor is the actor a command records when the request names no
// operator: one of the ADR 0002 actor kinds, and the pre-header behavior
// (the assignee field carries ownership). Policy declaration always
// records it.
const systemActor = "system"

// actorFor is the actor a case command records: the operator identity of
// the request's optional X-Actor header, or `system` when the header is
// absent or blank. It is a label for the audit trail, not authentication
// (there is no auth layer, fleet decision 2026-09-11); the use cases
// validate the name, so a header with a NUL byte is the 400 invalid-actor
// problem.
func actorFor(r *http.Request) string {
	if v := r.Header.Get("X-Actor"); v != "" {
		return v
	}
	return systemActor
}

// Server holds the use cases the handlers call.
type Server struct {
	List         *usecases.ListOpenExceptions
	Get          *usecases.GetException
	Triage       *usecases.TriageException
	Assign       *usecases.AssignException
	Resolve      *usecases.ResolveException
	Declare      *usecases.DeclareExceptionPolicy
	ListPolicies *usecases.ListExceptionPolicies

	// Idempotency wraps every POST and the PUT (the fleet Idempotency-Key
	// middleware, Idempotency.Middleware, built by cmd/ over the Postgres
	// adapters); nil leaves the routes unwrapped (handler unit tests only:
	// cmd/ always wires it).
	Idempotency func(http.Handler) http.Handler
	// Readiness backs GET /readyz; nil means always ready.
	Readiness *Readiness

	// Logger writes the one access-log line per request (slog.Default when
	// nil). cmd/api passes the process logger, whose handler stamps
	// trace_id/span_id on a record written under the request span.
	Logger *slog.Logger
	// ServiceName labels the request span and the request-duration
	// histogram (DefaultServiceName when empty); it is the process's
	// OTEL_SERVICE_NAME.
	ServiceName string
}

// NewRouter builds the chi router for every endpoint of apis/openapi.yaml
// (.claude/rules/rest-api.md). Every POST sits in the Idempotency group
// (REST guideline R3), POST /exceptions included: it exists only to answer
// 405 with the reason (there is deliberately no case-creation endpoint:
// every case opens from a consumed fact, ADR 0001), and like every POST it
// demands a key first and stores and replays that deterministic answer.
//
// The middleware order is the fleet observability standard (ADR 0014):
// RequestID, otelchi (the server span, named after the route pattern), the
// request-duration histogram, RequestLogger (runs under the span, so its
// line carries trace_id/span_id), then the panic recoverer. telemetry.Setup
// must have run before this call: otelchimetric binds the global
// MeterProvider when the router is constructed.
func NewRouter(s *Server) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(otelchi.Middleware(serviceNameOr(s.ServiceName, DefaultServiceName), otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceNameOr(s.ServiceName, DefaultServiceName))))
	r.Use(requestlog.RequestLogger(s.Logger))
	r.Use(recoverPanic)
	r.Use(rejectNUL)

	r.Get("/healthz", handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/metrics", handleMetrics)

	r.Get("/exceptions", s.handleListExceptions)
	r.Get("/exceptions/{caseId}", s.handleGetException)

	r.Get("/exception-policies", s.handleListPolicies)

	r.Group(func(r chi.Router) {
		if s.Idempotency != nil {
			r.Use(s.Idempotency)
		}
		r.Post("/exceptions", s.handleNoPostExceptions)
		r.Post("/exceptions/{caseId}/triage-decisions", s.handleTriageException)
		r.Post("/exceptions/{caseId}/assignments", s.handleAssignException)
		r.Post("/exceptions/{caseId}/resolutions", s.handleResolveException)
		r.Put("/exception-policies/{sourceType}", s.handleDeclarePolicy)
	})

	r.NotFound(s.handleNotFound)
	r.MethodNotAllowed(s.handleMethodNotAllowed)
	return r
}

// handleNoPostExceptions backs POST /exceptions, which the contract
// documents as a permanent 405: every case opens from a consumed fact
// (ADR 0001), so there is no create endpoint to call.
func (s *Server) handleNoPostExceptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "GET")
	writeProblem(w, r, methodNotAllowed, "there is deliberately no POST /exceptions: every case opens from a consumed fact (ADR 0001)")
}

// handleMethodNotAllowed answers any other method/path mismatch with the
// RFC 7807 405 (chi names the allowed methods in Allow).
func (s *Server) handleMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, r, methodNotAllowed, "the method is not allowed on this resource")
}

// handleNotFound answers an unknown path (and the NUL-path case of
// rejectNUL) with the contract's 404 problem.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, r, notFoundProblem, "no such path")
}
