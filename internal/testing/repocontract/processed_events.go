package repocontract

import (
	"testing"

	"github.com/claudioed/exception-management/internal/application/ports"
)

// ProcessedEvents runs the ports.ProcessedEvents contract: the consumers'
// dedupe guard. newStore must return an EMPTY store on every call.
func ProcessedEvents(t *testing.T, newStore func(t *testing.T) ports.ProcessedEvents) {
	t.Helper()
	cases := map[string]func(t *testing.T, s ports.ProcessedEvents){
		"the first claim of an id wins":        claimFirstWins,
		"ids are independent":                  claimIdsIndependent,
		"the claim survives a later reconnect": claimPersists,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}

func claimFirstWins(t *testing.T, s ports.ProcessedEvents) {
	claimed, err := s.Claim(bg, "evt-1")
	must(t, err)
	if !claimed {
		t.Fatal("the first claim of evt-1 was refused")
	}
	claimed, err = s.Claim(bg, "evt-1")
	must(t, err)
	if claimed {
		t.Fatal("the second claim of evt-1 won: a redelivered id must be a duplicate")
	}
}

func claimIdsIndependent(t *testing.T, s ports.ProcessedEvents) {
	for _, id := range []string{"evt-1", "evt-2"} {
		claimed, err := s.Claim(bg, id)
		must(t, err)
		if !claimed {
			t.Fatalf("the first claim of %s was refused", id)
		}
	}
}

func claimPersists(t *testing.T, s ports.ProcessedEvents) {
	// A claim read back through the same store (the reconnect shape: the
	// consumers hold one long-lived store per process) stays a duplicate.
	if _, err := s.Claim(bg, "evt-1"); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(bg, "evt-1")
	must(t, err)
	if claimed {
		t.Fatal("the claimed id was claimable again")
	}
}
