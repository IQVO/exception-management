package repository

import "time"

// CaseFilter narrows the case listing; a blank field narrows nothing (Status
// "" means every status, open and resolved alike).
type CaseFilter struct {
	Status     string
	Queue      string
	SiteID     string
	SourceType string
}

// CaseAfter is the keyset position a case page starts after: the opened-at
// and the id of the last row of the previous page. The listing is ordered by
// (opened-at, id) descending, so rows sharing an opened-at never skip or
// repeat.
type CaseAfter struct {
	OpenedAt time.Time
	ID       string
}
