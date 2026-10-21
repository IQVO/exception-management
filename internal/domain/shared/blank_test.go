package shared

import "testing"

// The blank rule must match the contract's `pattern: '\S'` exactly, which is
// the ECMA-262 `\s`, NOT unicode.IsSpace (U+0085 and U+FEFF disagree).
func TestIsBlankMatchesTheECMA262BlankRule(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"empty is blank", "", true},
		{"space is blank", " ", true},
		{"tab is blank", "\t", true},
		{"newline is blank", "\n", true},
		{"carriage return is blank", "\r", true},
		{"vertical tab is blank", "\v", true},
		{"form feed is blank", "\f", true},
		{"BOM is blank (ECMA-262)", string(rune(0xFEFF)), true},
		{"line separator is blank", string(rune(0x2028)), true},
		{"paragraph separator is blank", string(rune(0x2029)), true},
		{"ideographic space is blank", string(rune(0x3000)), true},
		{"NEL is NOT an ECMA-262 space", string(rune(0x85)), false},
		{"letters are not blank", "abc", false},
		{"letter with spaces is not blank", " a ", false},
		{"NUL is not blank (it is content)", "\x00", false},
	}
	for _, tc := range cases {
		if got := IsBlank(tc.in); got != tc.want {
			t.Errorf("%s: IsBlank(%q) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestHasNUL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"clean", false},
		{"a\x00b", true},
		{"\x00", true},
	}
	for _, tc := range cases {
		if got := HasNUL(tc.in); got != tc.want {
			t.Errorf("HasNUL(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"case-1", true},
		{"", false},
		{" ", false},
		{"\x00id", false},
		{" \x00 ", false},
	}
	for _, tc := range cases {
		if got := ValidText(tc.in); got != tc.want {
			t.Errorf("ValidText(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
