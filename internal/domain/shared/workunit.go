package shared

import (
	"strconv"
	"strings"
)

// The work-unit formula of WES fulfillment (order-management ADR 0018):
// every per-line work unit id is `{order_id}-line-{line_no}`. TaskCPTMissed
// carries it as `order_ref`; the OrderRepromised correlation must REVERSE it
// to compare with order-management's own `order_id` (ADR 0002).

const lineMarker = "-line-"

// WorkUnitRef is a reversed work-unit reference.
type WorkUnitRef struct {
	OrderID string
	LineNo  int
}

// FormatWorkUnitRef builds `{order_id}-line-{line_no}` (the frozen formula).
func FormatWorkUnitRef(orderID string, lineNo int) string {
	return orderID + lineMarker + strconv.Itoa(lineNo)
}

// ParseWorkUnitRef reverses the frozen formula. It fails soft: any input
// that is not exactly `<order id>-line-<positive integer>` returns
// ok=false, never an error — a no-match correlation must leave the case
// open, not fault the consumer (ADR 0002).
//
// The marker is matched at its LAST occurrence and the numeric suffix must
// be a plain positive integer (no sign, no leading zeros), so an order id
// that itself contains "-line-" still round-trips.
func ParseWorkUnitRef(ref string) (WorkUnitRef, bool) {
	i := strings.LastIndex(ref, lineMarker)
	if i <= 0 {
		return WorkUnitRef{}, false
	}
	orderID := ref[:i]
	suffix := ref[i+len(lineMarker):]
	lineNo, ok := parsePositiveInt(suffix)
	if !ok {
		return WorkUnitRef{}, false
	}
	return WorkUnitRef{OrderID: orderID, LineNo: lineNo}, true
}

// parsePositiveInt reads a plain positive decimal integer (no sign, no
// leading zeros, at most nine digits), reporting ok=false on anything else.
func parsePositiveInt(s string) (int, bool) {
	if s == "" || len(s) > 9 {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	if s[0] == '0' {
		return 0, false
	}
	return n, true
}
