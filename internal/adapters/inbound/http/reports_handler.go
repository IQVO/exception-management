package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/exception-management/internal/analytics/report"
)

// ReportsServer is the inbound HTTP adapter of cmd/exceptions-reports: the
// read-only analytics reports of ADR 0006. It depends only on the
// report.Reader port over the analytical database; it never touches the OLTP
// use cases, and it writes nothing.
type ReportsServer struct {
	Reader report.Reader
	// Now is the clock the default range ends at and the freshness lag is
	// measured against (time.Now when nil).
	Now func() time.Time
}

// The wire shapes (snake_case like the other fleet reports); the report
// structs never leak onto the wire.
type timingDTO struct {
	Resolved    int      `json:"resolved"`
	MeanSeconds *float64 `json:"mean_seconds"`
	P50Seconds  *float64 `json:"p50_seconds"`
	P95Seconds  *float64 `json:"p95_seconds"`
}

type rootCauseReportDTO struct {
	From       time.Time         `json:"from"`
	To         time.Time         `json:"to"`
	RootCauses []rootCauseRowDTO `json:"root_causes"`
}

type rootCauseRowDTO struct {
	SourceType string  `json:"source_type"`
	RootCause  string  `json:"root_cause"`
	Resolved   int64   `json:"resolved"`
	Share      float64 `json:"share"`
}

type mttrReportDTO struct {
	From   time.Time     `json:"from"`
	To     time.Time     `json:"to"`
	Queues []queueRowDTO `json:"queues"`
}

type queueRowDTO struct {
	Queue  string    `json:"queue"`
	Timing timingDTO `json:"timing"`
}

type slaBreachReportDTO struct {
	From        time.Time          `json:"from"`
	To          time.Time          `json:"to"`
	SourceTypes []sourceTypeRowDTO `json:"source_types"`
}

type sourceTypeRowDTO struct {
	SourceType     string  `json:"source_type"`
	Opened         int64   `json:"opened"`
	Escalated      int64   `json:"escalated"`
	Breached       int64   `json:"breached"`
	BreachRate     float64 `json:"breach_rate"`
	EscalationRate float64 `json:"escalation_rate"`
}

// Problems of the reports binary.
var (
	invalidReportRange = problem{http.StatusBadRequest, "invalid-report-range",
		"from and to must form a valid RFC 3339 range of at most 366 days", ""}
	reportStoreError = problem{http.StatusInternalServerError, "report-store-error",
		"The report could not be served", "the analytical database query failed"}
)

// NewReportsRouter builds the router of cmd/exceptions-reports: /healthz, GET
// /reports/root-cause-frequency, GET /reports/mean-time-to-resolve, GET
// /reports/sla-breach-rate and GET /reports/freshness. No auth middleware
// (fleet rule), no CORS: the service is cluster-internal.
func NewReportsRouter(s *ReportsServer) http.Handler {
	r := chi.NewRouter()
	r.Use(recoverPanic)
	r.Get("/healthz", handleHealthz)
	r.Get("/reports/root-cause-frequency", s.handleRootCauseFrequency)
	r.Get("/reports/mean-time-to-resolve", s.handleMeanTimeToResolve)
	r.Get("/reports/sla-breach-rate", s.handleSLABreachRate)
	r.Get("/reports/freshness", s.handleFreshness)
	return r
}

func (s *ReportsServer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// parseReportRange reads and validates the shared ?from/&to window; on a
// bad one it writes the 400 itself and reports false.
func (s *ReportsServer) parseReportRange(w http.ResponseWriter, r *http.Request) (report.Range, bool) {
	q := r.URL.Query()
	rg, err := report.ParseRange(q.Get("from"), q.Get("to"), s.now())
	if err != nil {
		writeProblem(w, r, invalidReportRange, err.Error())
		return report.Range{}, false
	}
	return rg, true
}

// handleRootCauseFrequency serves the manual resolutions of the window
// grouped by source type and root cause, most resolved first.
func (s *ReportsServer) handleRootCauseFrequency(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseReportRange(w, r)
	if !ok {
		return
	}
	rows, err := s.Reader.RootCauseRows(r.Context(), report.Query{Range: rg})
	if err != nil {
		writeReportStoreError(w, r, err)
		return
	}
	out := rootCauseReportDTO{From: rg.From, To: rg.To, RootCauses: make([]rootCauseRowDTO, 0, len(rows))}
	for _, row := range rows {
		out.RootCauses = append(out.RootCauses, rootCauseRowDTO{
			SourceType: row.SourceType, RootCause: row.RootCause, Resolved: row.Resolved, Share: row.Share,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleMeanTimeToResolve serves the opened-to-resolve timing of the window
// per queue.
func (s *ReportsServer) handleMeanTimeToResolve(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseReportRange(w, r)
	if !ok {
		return
	}
	rows, err := s.Reader.QueueTimingRows(r.Context(), report.Query{Range: rg})
	if err != nil {
		writeReportStoreError(w, r, err)
		return
	}
	out := mttrReportDTO{From: rg.From, To: rg.To, Queues: make([]queueRowDTO, 0, len(rows))}
	for _, row := range rows {
		out.Queues = append(out.Queues, queueRowDTO{Queue: row.Queue, Timing: timingDTO{
			Resolved: int(row.Resolved), MeanSeconds: row.MeanSeconds, P50Seconds: row.P50Seconds, P95Seconds: row.P95Seconds,
		}})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSLABreachRate serves the breach and escalation shares of the window
// per source type, over the cases opened in it.
func (s *ReportsServer) handleSLABreachRate(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseReportRange(w, r)
	if !ok {
		return
	}
	rows, err := s.Reader.SLABreachRows(r.Context(), report.Query{Range: rg})
	if err != nil {
		writeReportStoreError(w, r, err)
		return
	}
	out := slaBreachReportDTO{From: rg.From, To: rg.To, SourceTypes: make([]sourceTypeRowDTO, 0, len(rows))}
	for _, row := range rows {
		out.SourceTypes = append(out.SourceTypes, sourceTypeRowDTO{
			SourceType: row.SourceType, Opened: row.Opened, Escalated: row.Escalated, Breached: row.Breached,
			BreachRate: row.BreachRate, EscalationRate: row.EscalationRate,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleFreshness serves how far the projection is behind (now - the newest
// applied event time). Both fields are null until the first event is applied.
func (s *ReportsServer) handleFreshness(w http.ResponseWriter, r *http.Request) {
	asOf, err := s.Reader.LastEventAt(r.Context())
	if err != nil {
		writeReportStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, report.ComputeFreshness(asOf, s.now()))
}

// writeReportStoreError is the 500 for a failed analytical query. The cause is
// logged, never echoed: it can carry SQL and connection details.
func writeReportStoreError(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "report query failed", "error", err, "path", logSafe(r.URL.Path))
	writeProblem(w, r, reportStoreError, reportStoreError.detail)
}
