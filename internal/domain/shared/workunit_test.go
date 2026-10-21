package shared

import "testing"

// The parser must reverse the frozen work-unit formula `{order_id}-line-{line_no}`
// and FAIL SOFT on anything else: a no-match correlation leaves the case open
// instead of faulting the consumer (ADR 0002).
func TestParseWorkUnitRefReversesTheFrozenFormula(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ref     string
		orderID string
		lineNo  int
		wantOK  bool
	}{
		{"ord-7-line-3", "ord-7", 3, true},
		{"ord-7c9e6679-7d5a-4b37-b2f1-93b0c4a1d8f2-line-12", "ord-7c9e6679-7d5a-4b37-b2f1-93b0c4a1d8f2", 12, true},
		{"o-line-1", "o", 1, true},
		{"a-line-999999999", "a", 999999999, true},
		// An order id that itself contains the marker still round-trips: the
		// LAST occurrence is the separator.
		{"ord-line-2-line-5", "ord-line-2", 5, true},
		// Failures: soft, never an error.
		{"", "", 0, false},
		{"ord-7-line-", "", 0, false},
		{"-line-3", "", 0, false},
		{"ord-7", "", 0, false},
		{"ord-7-line-0", "", 0, false},
		{"ord-7-line-01", "", 0, false},
		{"ord-7-line-+3", "", 0, false},
		{"ord-7-line-3x", "", 0, false},
		{"ord-7-line-x3", "", 0, false},
		{"ord-7-line-3.0", "", 0, false},
		{"ord-7-line- 3", "", 0, false},
		{"ord-7-line-1000000000", "", 0, false}, // ten digits: out of the guarded range
	}
	for _, tc := range cases {
		ref, ok := ParseWorkUnitRef(tc.ref)
		if ok != tc.wantOK {
			t.Errorf("ParseWorkUnitRef(%q) ok = %v, want %v", tc.ref, ok, tc.wantOK)
			continue
		}
		if !tc.wantOK {
			if ref != (WorkUnitRef{}) {
				t.Errorf("ParseWorkUnitRef(%q) = %+v on failure, want the zero ref", tc.ref, ref)
			}
			continue
		}
		if ref.OrderID != tc.orderID || ref.LineNo != tc.lineNo {
			t.Errorf("ParseWorkUnitRef(%q) = %+v, want {%s %d}", tc.ref, ref, tc.orderID, tc.lineNo)
		}
	}
}

func TestFormatWorkUnitRefRoundTrips(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		orderID string
		lineNo  int
	}{
		{"ord-7", 3},
		{"ord-line-2", 5}, // marker inside the order id round-trips via the last occurrence
		{"o", 1},
	} {
		ref := FormatWorkUnitRef(tc.orderID, tc.lineNo)
		if ref != tc.orderID+"-line-"+itoaOf(tc.lineNo) {
			t.Fatalf("FormatWorkUnitRef(%q, %d) = %q", tc.orderID, tc.lineNo, ref)
		}
		got, ok := ParseWorkUnitRef(ref)
		if !ok || got.OrderID != tc.orderID || got.LineNo != tc.lineNo {
			t.Errorf("round trip of (%q, %d): %+v ok=%v", tc.orderID, tc.lineNo, got, ok)
		}
	}
}

// itoaOf mirrors strconv.Itoa so the test asserts the exact formula, not
// itself.
func itoaOf(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}
