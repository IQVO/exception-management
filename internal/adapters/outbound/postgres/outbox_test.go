package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The wire payloads the outbox publisher persists, in their nullable
// particulars (ADR 0004 / apis/asyncapi.yaml): site_id and note are PRESENT
// and null when unset — never "" — and the opened payload freezes the source
// payload verbatim. The full envelope of every type is pinned by the kafka
// encoder's golden tests and the Kafka integration proof.

func openedFact(t *testing.T, site string) exceptioncase.ExceptionOpened {
	t.Helper()
	key, err := shared.NewDedupKey("com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed", "task-8a1f", site)
	if err != nil {
		t.Fatal(err)
	}
	return exceptioncase.ExceptionOpened{
		Header:        exceptioncase.Header{Case: "case-1", Version: 1, At: time.Date(2026, 10, 21, 9, 30, 0, 0, time.UTC)},
		Key:           key,
		Severity:      shared.SeverityHigh,
		Queue:         "ps-main",
		SLADueAt:      time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC),
		SourceEventID: "src-1",
		SourcePayload: []byte(`{"task_id":"task-8a1f","cpt":"2026-09-30T14:55:00Z"}`),
	}
}

func asJSON(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestOpenedWireShapeSiteIDNullWhenUnset(t *testing.T) {
	data, err := caseFactData(openedFact(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	m := asJSON(t, data)
	if v, ok := m["site_id"]; !ok || v != nil {
		t.Fatalf("site_id = %v (present=%v), want present and null", v, ok)
	}
	with, err := caseFactData(openedFact(t, "site-1"))
	if err != nil {
		t.Fatal(err)
	}
	if v := asJSON(t, with)["site_id"]; v != "site-1" {
		t.Fatalf("site_id = %v, want site-1", v)
	}
}

func TestOpenedWireShapeFreezesSourcePayload(t *testing.T) {
	data, err := caseFactData(openedFact(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	sp := asJSON(t, data)["source_payload"].(map[string]any)
	if sp["task_id"] != "task-8a1f" || sp["cpt"] != "2026-09-30T14:55:00Z" {
		t.Fatalf("source_payload = %v", sp)
	}
}

func TestResolvedWireShapeNoteNullWhenUnset(t *testing.T) {
	f := exceptioncase.ExceptionResolved{
		Header:         exceptioncase.Header{Case: "case-1", Version: 2, At: time.Date(2026, 10, 21, 10, 15, 0, 0, time.UTC)},
		ResolutionCode: "STOCK_FOUND", RootCause: "COUNT_ERROR",
		Actor: shared.Actor{Kind: shared.ActorOperator, Name: "jlee"},
	}
	data, err := caseFactData(f)
	if err != nil {
		t.Fatal(err)
	}
	m := asJSON(t, data)
	if v, ok := m["note"]; !ok || v != nil {
		t.Fatalf("note = %v (present=%v), want present and null", v, ok)
	}
	f.Note = "found in bin A-12-3"
	if v := asJSON(t, mustData(t, f))["note"]; v != "found in bin A-12-3" {
		t.Fatalf("note = %v, want the operator remark", v)
	}
}

func mustData(t *testing.T, e exceptioncase.Event) any {
	t.Helper()
	data, err := caseFactData(e)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
