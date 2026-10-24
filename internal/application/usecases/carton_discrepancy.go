package usecases

import (
	"context"
	"fmt"
	"strings"

	"github.com/claudioed/exception-management/internal/domain/shared"
)

// TypeCartonDiscrepancyRaised is inbound-receiving's carton-discrepancy
// integration event, byte-identical to its AsyncAPI (contract R12).
const TypeCartonDiscrepancyRaised = "com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised"

// The two routes of a carton discrepancy. Each is a policy source type
// (seeded by migration 0003, redeclarable through the policy API): the
// queue, severity and SLA of the case come from it, so routing stays
// data, not code.
const (
	// RouteProblemSolve: a discrepancy a problem-solver must resolve.
	RouteProblemSolve = TypeCartonDiscrepancyRaised + "#problem-solve"
	// RouteReceivingLead: informational, to the receiving lead, no block.
	RouteReceivingLead = TypeCartonDiscrepancyRaised + "#receiving-lead"
)

// cartonRoutes is the routing table (contract R12). A kind absent from it
// (inbound-receiving may add kinds) routes to problem-solve: an unknown
// discrepancy is never dropped and never silently demoted.
var cartonRoutes = map[string]string{
	"UnexpectedCarton":            RouteProblemSolve,
	"MissingCarton":               RouteProblemSolve,
	"DuplicateSscc":               RouteProblemSolve,
	"UnknownGtin":                 RouteProblemSolve,
	"CaseQtyMismatch":             RouteProblemSolve,
	"MixedCaseDeclaredAsCasePack": RouteProblemSolve,
	"MixedCartonContentMismatch":  RouteProblemSolve,
	"UnlabelledCarton":            RouteReceivingLead,
	"CartonOverLimit":             RouteReceivingLead,
}

// RouteForCartonKind returns the policy source type a kind routes to.
func RouteForCartonKind(kind string) string {
	if r, ok := cartonRoutes[kind]; ok {
		return r
	}
	return RouteProblemSolve
}

// CartonDiscrepancy is one consumed CartonDiscrepancyRaised fact.
type CartonDiscrepancy struct {
	EventID   string
	ReceiptID string
	SSCC      string // empty when the discrepancy is not about one carton
	Kind      string
	Payload   []byte
}

// OpenCartonDiscrepancy opens (or touches) the routed case of one carton
// discrepancy: one case per (receipt, sscc|"-", kind), idempotent by the
// CloudEvents id AND that natural key (ConsumeFact claims the id; the dedup
// key makes a re-raised discrepancy touch the open case).
type OpenCartonDiscrepancy struct {
	Consume *ConsumeFact
}

// CartonSubject is the dedup subject: receipt, SSCC (or "-") and kind.
func CartonSubject(receiptID, sscc, kind string) string {
	if strings.TrimSpace(sscc) == "" {
		sscc = "-"
	}
	return receiptID + "/" + sscc + "/" + kind
}

// Execute applies the discrepancy.
func (uc *OpenCartonDiscrepancy) Execute(ctx context.Context, d CartonDiscrepancy) (Outcome, error) {
	if !shared.ValidText(d.ReceiptID) {
		return "", fmt.Errorf("carton discrepancy: %w", shared.ErrBlankSourceSubject)
	}
	if !shared.ValidText(d.Kind) {
		return "", fmt.Errorf("carton discrepancy kind: %w", shared.ErrBlankSourceSubject)
	}
	return uc.Consume.Execute(ctx, Fact{
		SourceType:    TypeCartonDiscrepancyRaised,
		SourceSubject: CartonSubject(d.ReceiptID, d.SSCC, d.Kind),
		SourceEventID: d.EventID,
		Payload:       d.Payload,
		PolicyType:    RouteForCartonKind(d.Kind),
	})
}
