package shared

import "testing"

func TestNewIDRejectsBlankAndNUL(t *testing.T) {
	t.Parallel()
	bad := []string{"", " ", "	", "\x00", "a\x00", string(rune(0xFEFF))}
	for _, in := range bad {
		if _, err := NewID(in); err != ErrBlankID {
			t.Errorf("NewID(%q) err = %v, want ErrBlankID", in, err)
		}
	}
	id, err := NewID("case-1")
	if err != nil || id != "case-1" || id.IsZero() {
		t.Fatalf("NewID(case-1) = %q, %v", id, err)
	}
	var zero ID
	if !zero.IsZero() {
		t.Error("the zero ID must report IsZero")
	}
	if zero.String() != "" {
		t.Error("the zero ID must render as the empty string")
	}
}

func TestNewDedupKeyRejectsBlankPartsAndKeepsAnEmptySite(t *testing.T) {
	t.Parallel()
	if _, err := NewDedupKey("  ", "task-8a1f", "site-1"); err != ErrBlankSourceType {
		t.Errorf("blank source type: err = %v, want ErrBlankSourceType", err)
	}
	if _, err := NewDedupKey("type", "\x00", "site-1"); err != ErrBlankSourceSubject {
		t.Errorf("NUL source subject: err = %v, want ErrBlankSourceSubject", err)
	}
	if _, err := NewDedupKey("type", "task-8a1f", "  "); err != ErrInvalidSite {
		t.Errorf("blank-ish site: err = %v, want ErrInvalidSite", err)
	}
	k, err := NewDedupKey("type", "task-8a1f", "")
	if err != nil {
		t.Fatalf("an empty site is allowed (the source carries no site): %v", err)
	}
	if k.SiteID != "" {
		t.Error("the empty site must stay empty")
	}
}

func TestDedupKeyStringIsInjectiveAndEqualityIsStructural(t *testing.T) {
	t.Parallel()
	a, _ := NewDedupKey("t", "s", "site")
	b, _ := NewDedupKey("t", "s", "site")
	c, _ := NewDedupKey("t\x1fs", "", "")
	if !a.Equal(b) {
		t.Error("two equal keys must be Equal")
	}
	if a.Equal(c) {
		t.Error("a part boundary must not be forgeable by injecting the separator")
	}
	if a.String() != "t\x1fs\x1fsite" {
		t.Errorf("String() = %q, want the three parts joined by U+001F", a.String())
	}
}

func TestSeverityValid(t *testing.T) {
	t.Parallel()
	for _, s := range []Severity{SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical} {
		if !s.Valid() {
			t.Errorf("%q must be valid", s)
		}
	}
	for _, s := range []Severity{"", "low", "URGENT", "SEV1"} {
		if s.Valid() {
			t.Errorf("%q must be invalid", s)
		}
	}
}
