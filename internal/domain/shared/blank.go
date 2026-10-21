// Package shared holds the tiny value rules every domain package of this
// context agrees on: the ECMA-262 blank-string rule, the identifier value
// object, the dedup key, the actor and the work-unit parser (ADR 0002).
// It imports nothing outside the standard library.
package shared

import (
	"strings"
	"unicode"
)

// IsSpace reports whether r is white space or a line terminator in the
// ECMA-262 sense, which is what the `\s` of the contract's `pattern: '\S'`
// means. It is NOT unicode.IsSpace: U+0085 (NEL) is not an ECMA-262 space,
// U+FEFF (the byte order mark) is.
func IsSpace(r rune) bool {
	if r == '\t' || r == '\n' || r == '\v' || r == '\f' || r == '\r' || r == 0xFEFF {
		return true
	}
	if r == 0x2028 || r == 0x2029 {
		return true
	}
	return unicode.Is(unicode.Zs, r)
}

// IsBlank reports whether s is empty or holds only ECMA-262 white space.
func IsBlank(s string) bool {
	for _, r := range s {
		if !IsSpace(r) {
			return false
		}
	}
	return true
}

// HasNUL reports whether s holds a NUL byte, which Postgres rejects: the
// inbound adapters must refuse it before the database ever sees it.
func HasNUL(s string) bool {
	return strings.ContainsRune(s, 0)
}

// ValidText reports whether s is acceptable as a free-form identifier-ish
// domain string: non-blank in the ECMA-262 sense and free of NUL bytes.
func ValidText(s string) bool {
	return !IsBlank(s) && !HasNUL(s)
}
