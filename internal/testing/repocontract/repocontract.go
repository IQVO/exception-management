// Package repocontract holds the behavioural contract of the outbound
// repositories. The in-memory doubles and the Postgres adapters run the
// SAME cases, so what the BDD suite (over memory) shows is what production
// (over Postgres) does.
package repocontract

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// T0 is the fixed instant the cases build on. It has no sub-microsecond
// part, so a Postgres round trip returns it unchanged.
var T0 = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

// The dedup key the contract cases share: the fleet's re-fired CPT-missed
// fact of one work unit.
const (
	SrcType   = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	Subject   = "ord-7-line-1"
	SiteID    = "SIM1"
	OtherType = "com.warehouse.wes.fulfillment-execution.package.WeightDiscrepancyDetected"
)

var bg = context.Background()

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantErr(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

// Opened builds an OPEN case at version 1 under the default key.
func Opened(t *testing.T, id, subject string, at time.Time) *exceptioncase.Case {
	t.Helper()
	return OpenedFor(t, id, SrcType, subject, SiteID, at)
}

// OpenedFor builds an OPEN case at version 1 for any key parts.
func OpenedFor(t *testing.T, id, sourceType, subject, site string, at time.Time) *exceptioncase.Case {
	t.Helper()
	key, err := shared.NewDedupKey(sourceType, subject, site)
	must(t, err)
	c, err := exceptioncase.Open(exceptioncase.OpenInput{
		CaseID: id, Key: key, Severity: shared.SeverityHigh, Queue: "ps-main",
		SLAMinutes: 30, SourceEventID: "evt-open-" + id,
		Payload: []byte(`{"task_id":"task-8a1f","order_ref":"ord-7-line-1","cpt":"2026-10-21T09:00:00Z"}`),
		At:      at,
	})
	must(t, err)
	return c
}

// sameSnapshot compares two snapshots: instants by Equal (not by location),
// payloads as parsed JSON (a jsonb column canonicalizes spacing/key order),
// audit entry by entry.
func sameSnapshot(a, b exceptioncase.Snapshot) bool {
	times := [][2]time.Time{{a.OpenedAt, b.OpenedAt}, {a.LastSeenAt, b.LastSeenAt}, {a.ResolvedAt, b.ResolvedAt}}
	for _, p := range times {
		if !p[0].Equal(p[1]) {
			return false
		}
	}
	if !sameJSON(a.SourcePayload, b.SourcePayload) || !sameAudit(a.Audit, b.Audit) {
		return false
	}
	a.OpenedAt, a.LastSeenAt, a.ResolvedAt = time.Time{}, time.Time{}, time.Time{}
	b.OpenedAt, b.LastSeenAt, b.ResolvedAt = time.Time{}, time.Time{}, time.Time{}
	a.SourcePayload, b.SourcePayload = nil, nil
	a.Audit, b.Audit = nil, nil
	return reflect.DeepEqual(a, b)
}

func sameJSON(a, b []byte) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

func sameAudit(a, b []exceptioncase.AuditEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Action != b[i].Action || a[i].Actor != b[i].Actor || !a[i].At.Equal(b[i].At) {
			return false
		}
	}
	return true
}
