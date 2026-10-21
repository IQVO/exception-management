// Package system provides the real environment adapters of the shipped
// binary (the in-memory doubles live in internal/adapters/outbound/memory
// and never reach cmd/): the system clock.
package system

import "time"

// Clock is the real clock: the process's notion of now, in UTC.
type Clock struct{}

// Now implements ports.Clock.
func (Clock) Now() time.Time { return time.Now().UTC() }
