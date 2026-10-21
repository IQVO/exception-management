package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// wireActor is the actor every REST command records. The contract carries
// no operator identity (there is no auth layer, fleet decision 2026-09-11,
// and no actor header in apis/openapi.yaml), so v1 records `system` — one of
// the ADR 0002 actor kinds — and the assignee field carries ownership.
const wireActor = "system"

// Server holds the use cases the handlers call.
type Server struct {
	List         *usecases.ListOpenExceptions
	Get          *usecases.GetException
	Triage       *usecases.TriageException
	Assign       *usecases.AssignException
	Resolve      *usecases.ResolveException
	Declare      *usecases.DeclareExceptionPolicy
	ListPolicies *usecases.ListExceptionPolicies

	// Idempotency wraps the POSTs and the PUT (the fleet Idempotency-Key
	// middleware, Idempotency.Middleware, built by cmd/ over the Postgres
	// adapters); nil leaves the routes unwrapped (handler unit tests only:
	// cmd/ always wires it).
	Idempotency func(http.Handler) http.Handler
	// Readiness backs GET /readyz; nil means always ready.
	Readiness *Readiness
}

// NewRouter builds the chi router for every endpoint of apis/openapi.yaml
// (.claude/rules/rest-api.md). The POST /exceptions route exists only to
// answer 405 with the reason (there is deliberately no case-creation
// endpoint: every case opens from a consumed fact, ADR 0001), so it sits
// OUTSIDE the Idempotency group: a 405 must not demand a key first.
func NewRouter(s *Server) http.Handler {
	r := chi.NewRouter()

	r.Use(recoverPanic)
	r.Use(rejectNUL)

	r.Get("/healthz", handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Get("/metrics", handleMetrics)

	r.Get("/exceptions", s.handleListExceptions)
	r.Post("/exceptions", s.handleNoPostExceptions)
	r.Get("/exceptions/{caseId}", s.handleGetException)

	r.Get("/exception-policies", s.handleListPolicies)

	r.Group(func(r chi.Router) {
		if s.Idempotency != nil {
			r.Use(s.Idempotency)
		}
		r.Post("/exceptions/{caseId}/triage", s.handleTriageException)
		r.Post("/exceptions/{caseId}/assign", s.handleAssignException)
		r.Post("/exceptions/{caseId}/resolve", s.handleResolveException)
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
