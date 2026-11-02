package http

import (
	"net/http"

	"github.com/claudioed/exception-management/internal/application/usecases"
)

// handleGetException backs GET /exceptions/{caseId}.
func (s *Server) handleGetException(w http.ResponseWriter, r *http.Request) {
	id, err := caseIDParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	v, err := s.Get.Execute(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCase(v))
}

// handleTriageException backs POST /exceptions/{caseId}/triage-decisions: sets or
// changes the severity and queue, and moves an OPEN case to TRIAGED.
func (s *Server) handleTriageException(w http.ResponseWriter, r *http.Request) {
	id, err := caseIDParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var body triageBody
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, r, err)
		return
	}
	severity, err := severityOf(body.Severity)
	if err != nil {
		writeError(w, r, err)
		return
	}
	queue, err := queueOf(body.Queue)
	if err != nil {
		writeError(w, r, err)
		return
	}
	c, err := s.Triage.Execute(r.Context(), id, severity, queue, wireActor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCase(usecases.CaseView(c)))
}

// handleAssignException backs POST /exceptions/{caseId}/assignments: gives the
// case an owner and moves it to ASSIGNED (reassignment allowed).
func (s *Server) handleAssignException(w http.ResponseWriter, r *http.Request) {
	id, err := caseIDParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var body assignBody
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, r, err)
		return
	}
	assignee := body.Assignee
	if utf8Len(assignee) < 1 || utf8Len(assignee) > maxQueueLen || isBlank(assignee) {
		writeError(w, r, errInvalidAssignee)
		return
	}
	c, err := s.Assign.Execute(r.Context(), id, assignee, wireActor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCase(usecases.CaseView(c)))
}

// handleResolveException backs POST /exceptions/{caseId}/resolutions: records
// the resolution code and root cause and moves the case to its terminal
// state. note is optional.
func (s *Server) handleResolveException(w http.ResponseWriter, r *http.Request) {
	id, err := caseIDParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var body resolveBody
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, r, err)
		return
	}
	code, err := codeOf(body.ResolutionCode)
	if err != nil {
		writeError(w, r, err)
		return
	}
	rootCause, err := codeOf(body.RootCause)
	if err != nil {
		writeError(w, r, err)
		return
	}
	note := ""
	if body.Note != nil {
		if note, err = noteOf(*body.Note); err != nil {
			writeError(w, r, err)
			return
		}
	}
	c, err := s.Resolve.Execute(r.Context(), id, code, rootCause, note, wireActor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCase(usecases.CaseView(c)))
}

// handleDeclarePolicy backs PUT /exception-policies/{sourceType}: declares
// the next version of the source type's handling policy (201).
func (s *Server) handleDeclarePolicy(w http.ResponseWriter, r *http.Request) {
	sourceType, err := sourceTypeParam(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var body declarePolicyBody
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, r, err)
		return
	}
	severity, err := severityOf(body.Severity)
	if err != nil {
		writeError(w, r, err)
		return
	}
	queue, err := queueOf(body.Queue)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if body.SLAMinutes == nil || *body.SLAMinutes < 1 {
		writeError(w, r, errInvalidPolicy)
		return
	}
	in := usecases.PolicyDeclaration{
		SourceType: sourceType, Severity: severity, Queue: queue,
		SLAMinutes: *body.SLAMinutes, Actor: wireActor,
	}
	if body.AutoResolve != nil {
		// The contract bundles a broken autoResolve rule under
		// invalid-policy (its onType is not the path's {sourceType}), so the
		// rule is validated here, not left to the domain's own (differently
		// slugged) rule validation.
		if !sourceTypePattern.MatchString(body.AutoResolve.OnType) || utf8Len(body.AutoResolve.OnType) > maxSourceTypeLen ||
			body.AutoResolve.WithinMinutes < 1 {
			writeError(w, r, errInvalidPolicy)
			return
		}
		in.AutoResolveOn = body.AutoResolve.OnType
		in.WithinMinutes = body.AutoResolve.WithinMinutes
	}
	p, err := s.Declare.Execute(r.Context(), in)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toPolicy(usecases.PolicyView(p)))
}
