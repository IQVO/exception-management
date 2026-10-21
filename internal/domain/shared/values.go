package shared

import "errors"

// Errors of the shared value objects. Every constructor rejects what the
// persistence layer could not store honestly (blank, NUL).
var (
	// ErrBlankID is a blank or NUL-bearing identifier.
	ErrBlankID = errors.New("shared: id must not be blank or contain NUL")
	// ErrBlankSourceType is a blank source type.
	ErrBlankSourceType = errors.New("shared: source type must not be blank")
	// ErrBlankSourceSubject is a blank source subject.
	ErrBlankSourceSubject = errors.New("shared: source subject must not be blank")
	// ErrInvalidSite is a site id that is blank-ish but not empty.
	ErrInvalidSite = errors.New("shared: site id must be empty or non-blank")
	// ErrBlankActor is a blank actor name.
	ErrBlankActor = errors.New("shared: actor name must not be blank")
)

// ID is a domain identifier value object: non-blank in the ECMA-262 sense
// and free of NUL bytes. The zero ID is absent, never a valid value.
type ID string

// NewID validates and wraps an identifier string.
func NewID(s string) (ID, error) {
	if !ValidText(s) {
		return "", ErrBlankID
	}
	return ID(s), nil
}

// String returns the raw identifier.
func (i ID) String() string { return string(i) }

// IsZero reports whether the ID is absent.
func (i ID) IsZero() bool { return i == "" }

// DedupKey is the identity of a stream of source facts: (source type, source
// subject, site). Exactly one open case exists per key (ADR 0002). SiteID is
// empty when the source fact carries no site (the key still dedupes).
type DedupKey struct {
	SourceType    string
	SourceSubject string
	SiteID        string
}

// keySep separates the key parts; U+001F (unit separator) cannot occur in a
// validated part because it is an ECMA-262 space, so the join is injective.
const keySep = "\x1f"

// NewDedupKey validates and wraps the three key parts. SourceType and
// SourceSubject must be non-blank; SiteID must be empty or non-blank.
func NewDedupKey(sourceType, sourceSubject, siteID string) (DedupKey, error) {
	if !ValidText(sourceType) {
		return DedupKey{}, ErrBlankSourceType
	}
	if !ValidText(sourceSubject) {
		return DedupKey{}, ErrBlankSourceSubject
	}
	if siteID != "" && IsBlank(siteID) {
		return DedupKey{}, ErrInvalidSite
	}
	return DedupKey{SourceType: sourceType, SourceSubject: sourceSubject, SiteID: siteID}, nil
}

// String renders the key as its unique-index form: the three parts joined by
// U+001F, which no validated part can contain.
func (k DedupKey) String() string {
	return k.SourceType + keySep + k.SourceSubject + keySep + k.SiteID
}

// Equal reports whether two keys are the same key.
func (k DedupKey) Equal(other DedupKey) bool {
	return k == other
}
