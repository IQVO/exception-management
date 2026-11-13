package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// YardManagementTopic is yard-management's integration topic (ADR 0010).
// Only the four exception-class types below are acted on; every other type
// on it (yard-management is analytics-heavy) is ignored.
const YardManagementTopic = "warehouse.yard-management.events"

// The yard-management types this service acts on, byte-identical to the
// producer's AsyncAPI.
const (
	TypeVisitRejected        = usecases.TypeVisitRejected
	TypeSealExceptionRaised  = usecases.TypeSealExceptionRaised
	TypeTrailerDwellExceeded = usecases.TypeTrailerDwellExceeded
	TypeYardCheckClosed      = usecases.TypeYardCheckClosed
)

// visitRejectedData mirrors the pinned VisitRejected v1 payload. The dedup
// subject is `visit_id`; a visit is rejected at most once.
type visitRejectedData struct {
	VisitID     string `json:"visit_id"`
	SiteID      string `json:"site_id"`
	CarrierSCAC string `json:"carrier_scac"`
	Reason      string `json:"reason"`
	RejectedAt  string `json:"rejected_at"`
}

// sealExceptionRaisedData mirrors the pinned SealExceptionRaised v1
// payload. The dedup subject is `trailer_id/raised_at`: a trailer can
// mismatch again on a later visit, and a resolved case never reopens, so
// the raise instant is part of the key.
type sealExceptionRaisedData struct {
	TrailerID          string `json:"trailer_id"`
	SiteID             string `json:"site_id"`
	AppointmentID      string `json:"appointment_id"`
	ExpectedSealNumber string `json:"expected_seal_number"`
	FoundSealNumber    string `json:"found_seal_number"`
	RaisedAt           string `json:"raised_at"`
}

// trailerDwellExceededData mirrors the pinned TrailerDwellExceeded v1
// payload. It carries no instant of its own: the dedup subject is
// `trailer_id/<CloudEvents time>` (the producer raises once per trailer
// and threshold crossing; a sweep that re-publishes the same crossing
// carries the same time and touches the case).
type trailerDwellExceededData struct {
	TrailerID        string `json:"trailer_id"`
	SiteID           string `json:"site_id"`
	Kind             string `json:"kind"`
	DwellMinutes     int64  `json:"dwell_minutes"`
	ThresholdMinutes int64  `json:"threshold_minutes"`
	LocationCode     string `json:"location_code"`
}

// yardCheckClosedData mirrors the pinned YardCheckClosed v1 payload.
type yardCheckClosedData struct {
	YardCheckID   string                          `json:"yard_check_id"`
	SiteID        string                          `json:"site_id"`
	ClosedAt      string                          `json:"closed_at"`
	Discrepancies []usecases.YardCheckDiscrepancy `json:"discrepancies"`
}

// occurredAt is the instant that tells two raises of the same trailer
// apart: the payload's own instant when present, else the CloudEvents time.
func occurredAt(payloadInstant string, e ce.Event) string {
	if s := strings.TrimSpace(payloadInstant); s != "" {
		return s
	}
	if t := e.Time(); !t.IsZero() {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return "-"
}

// YardExceptionsHandlers maps yard-management's four consumed types to the
// open-or-touch use cases. VisitRejected, SealExceptionRaised and
// TrailerDwellExceeded open one case each; YardCheckClosed opens one per
// discrepancy (a clean check opens none).
func YardExceptionsHandlers(consume *usecases.ConsumeFact, checks *usecases.OpenYardCheckDiscrepancies, logger *slog.Logger) map[string]HandlerFunc {
	logger = defaultLogger(logger)
	return map[string]HandlerFunc{
		TypeVisitRejected:        visitRejectedHandler(consume, logger),
		TypeSealExceptionRaised:  sealExceptionHandler(consume, logger),
		TypeTrailerDwellExceeded: trailerDwellHandler(consume, logger),
		TypeYardCheckClosed:      yardCheckClosedHandler(checks, logger),
	}
}

func visitRejectedHandler(consume *usecases.ConsumeFact, logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[visitRejectedData](e)
		if err != nil {
			return err
		}
		f, err := fact(e, d.VisitID, d.SiteID)
		if err != nil {
			return err
		}
		return apply(ctx, logger, e, func() (usecases.Outcome, error) { return consume.Execute(ctx, f) })
	}
}

func sealExceptionHandler(consume *usecases.ConsumeFact, logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[sealExceptionRaisedData](e)
		if err != nil {
			return err
		}
		if err := requireTrailer(e, d.TrailerID); err != nil {
			return err
		}
		f, err := fact(e, usecases.TrailerOccurrenceSubject(d.TrailerID, occurredAt(d.RaisedAt, e)), d.SiteID)
		if err != nil {
			return err
		}
		return apply(ctx, logger, e, func() (usecases.Outcome, error) { return consume.Execute(ctx, f) })
	}
}

func trailerDwellHandler(consume *usecases.ConsumeFact, logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[trailerDwellExceededData](e)
		if err != nil {
			return err
		}
		if err := requireTrailer(e, d.TrailerID); err != nil {
			return err
		}
		f, err := fact(e, usecases.TrailerOccurrenceSubject(d.TrailerID, occurredAt("", e)), d.SiteID)
		if err != nil {
			return err
		}
		return apply(ctx, logger, e, func() (usecases.Outcome, error) { return consume.Execute(ctx, f) })
	}
}

// requireTrailer refuses (as poison) a payload without a usable trailer id:
// the occurrence instant alone would otherwise make a valid-looking key.
func requireTrailer(e ce.Event, trailerID string) error {
	if shared.ValidText(trailerID) {
		return nil
	}
	return &deadLetterError{cause: skipf("payload of %s carries no mappable id: %v", e.Type(), shared.ErrBlankSourceSubject)}
}

func yardCheckClosedHandler(checks *usecases.OpenYardCheckDiscrepancies, logger *slog.Logger) HandlerFunc {
	return func(ctx context.Context, e ce.Event) error {
		d, err := decodeData[yardCheckClosedData](e)
		if err != nil {
			return err
		}
		opened, outcome, err := checks.Execute(ctx, usecases.YardCheckClosure{
			EventID: e.ID(), YardCheckID: d.YardCheckID, SiteID: d.SiteID, ClosedAt: d.ClosedAt, Discrepancies: d.Discrepancies,
		})
		if err != nil {
			return yardCheckError(e, err)
		}
		logger.InfoContext(ctx, "fact consumed", "type", e.Type(), "event_id", e.ID(), "subject", e.Subject(),
			"discrepancies", opened, "outcome", string(outcome))
		return nil
	}
}

// yardCheckError classifies a closure refusal: a payload that can never be
// applied is poison, an undeclared policy is the commit-past skip, anything
// else is transient and retried.
func yardCheckError(e ce.Event, err error) error {
	switch {
	case errors.Is(err, shared.ErrBlankSourceSubject), errors.Is(err, shared.ErrInvalidSite):
		return &deadLetterError{cause: skipf("payload of %s carries no mappable id: %v", e.Type(), err)}
	case errors.Is(err, usecases.ErrPolicyUndeclared), errors.Is(err, usecases.ErrNoEventID):
		return skipf("%s: %v", e.Type(), err)
	}
	return err
}

// NewYardExceptionsConsumer reads yard-management's topic under groupID
// (YARD_MANAGEMENT_EXCEPTIONS_CONSUMER_GROUP, never a literal).
func NewYardExceptionsConsumer(brokers []string, groupID string, consume *usecases.ConsumeFact, checks *usecases.OpenYardCheckDiscrepancies, logger *slog.Logger) *Consumer {
	return newConsumer("yard exceptions consumer", brokers, YardManagementTopic, groupID,
		YardExceptionsHandlers(consume, checks, logger), logger)
}

// NewYardExceptionsConsumerForTopic is NewYardExceptionsConsumer on an
// explicit topic (the integration tests' unique topics).
func NewYardExceptionsConsumerForTopic(brokers []string, topic, groupID string, consume *usecases.ConsumeFact, checks *usecases.OpenYardCheckDiscrepancies, logger *slog.Logger) *Consumer {
	return newConsumer("yard exceptions consumer", brokers, topic, groupID,
		YardExceptionsHandlers(consume, checks, logger), logger)
}
