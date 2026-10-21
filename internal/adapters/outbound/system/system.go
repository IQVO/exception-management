// Package system provides the real environment adapters of the shipped
// binary (the in-memory doubles live in internal/adapters/outbound/memory
// and never reach cmd/): the system clock.
package system

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Clock is the real clock: the process's notion of now, in UTC.
type Clock struct{}

// Now implements ports.Clock.
func (Clock) Now() time.Time { return time.Now().UTC() }

// CaseIDs is the real ports.IDGen: case ids are prefixed UUID v4 strings
// (prefix "case-", then a fresh uuid — the same shape the outbox mints for
// CloudEvents ids). A collision needs a UUID collision; no counter is kept.
type CaseIDs struct{}

// NewCaseID implements ports.IDGen.
func (CaseIDs) NewCaseID() string { return fmt.Sprintf("case-%s", uuid.NewString()) }
