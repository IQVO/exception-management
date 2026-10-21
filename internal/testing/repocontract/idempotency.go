package repocontract

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// IdempotencyStore runs the ports.IdempotencyStore contract: first-writer-wins
// saves, lookup misses and round trips. newStore must return an EMPTY store
// on every call.
func IdempotencyStore(t *testing.T, newStore func(t *testing.T) ports.IdempotencyStore) {
	t.Helper()
	cases := map[string]func(t *testing.T, s ports.IdempotencyStore){
		"an unknown key has no answer":              idemNotFound,
		"save then lookup round trips the answer":   idemRoundTrip,
		"the first writer of a key wins":            idemFirstWriterWins,
		"keys are independent":                      idemKeysIndependent,
		"the stored body may be empty but not null": idemEmptyBody,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}

var idemAt = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

func idemRecord(key, fp string, status int, body []byte) repository.IdempotencyRecord {
	return repository.IdempotencyRecord{Key: key, Fingerprint: fp, Status: status, ContentType: "application/json", Body: body, CreatedAt: idemAt}
}

func idemNotFound(t *testing.T, s ports.IdempotencyStore) {
	rec, found, err := s.Lookup(context.Background(), "nope")
	must(t, err)
	if found || rec.Key != "" {
		t.Fatalf("lookup of an unknown key = (%+v, %v), want the zero record and not-found", rec, found)
	}
}

func idemRoundTrip(t *testing.T, s ports.IdempotencyStore) {
	rec := idemRecord("k1", "fp1", 200, []byte(`{"body":"ok"}`))
	must(t, s.Save(context.Background(), rec))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found {
		t.Fatal("the saved key was not found")
	}
	if got.Fingerprint != "fp1" || got.Status != 200 || got.ContentType != "application/json" ||
		string(got.Body) != `{"body":"ok"}` || !got.CreatedAt.Equal(idemAt) {
		t.Fatalf("round trip = %+v, want %+v", got, rec)
	}
}

func idemFirstWriterWins(t *testing.T, s ports.IdempotencyStore) {
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 200, []byte("first"))))
	must(t, s.Save(context.Background(), idemRecord("k1", "fp2", 409, []byte("second"))))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found || got.Fingerprint != "fp1" || got.Status != 200 || string(got.Body) != "first" {
		t.Fatalf("after two saves = (%+v, %v), want the first writer's answer", got, found)
	}
}

func idemKeysIndependent(t *testing.T, s ports.IdempotencyStore) {
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 200, []byte("one"))))
	must(t, s.Save(context.Background(), idemRecord("k2", "fp2", 201, []byte("two"))))
	for key, want := range map[string]int{"k1": 200, "k2": 201} {
		got, found, err := s.Lookup(context.Background(), key)
		must(t, err)
		if !found || got.Status != want {
			t.Fatalf("lookup %s = (%d, %v), want (%d, true)", key, got.Status, found, want)
		}
	}
}

func idemEmptyBody(t *testing.T, s ports.IdempotencyStore) {
	// A 204-class answer has no body; the stored row must still round trip
	// (as empty bytes, never as an error).
	must(t, s.Save(context.Background(), idemRecord("k1", "fp1", 204, nil)))
	got, found, err := s.Lookup(context.Background(), "k1")
	must(t, err)
	if !found || got.Status != 204 || len(got.Body) != 0 {
		t.Fatalf("empty-body round trip = (%+v, %v)", got, found)
	}
}
