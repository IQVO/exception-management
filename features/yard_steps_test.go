package features_test

import (
	"context"
	"errors"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// yardSteps drives the real ConsumeFact and OpenYardCheckDiscrepancies use
// cases (event-id claim included) over the same memory doubles as the other
// facts. The dedup subjects come from the same usecases helpers the Kafka
// handlers call.
type yardSteps struct {
	w       *world
	consume *usecases.ConsumeFact
	checks  *usecases.OpenYardCheckDiscrepancies
}

// registerYardSteps registers the yard-exception steps (ADR 0010).
func registerYardSteps(sc *godog.ScenarioContext, w *world) {
	y := &yardSteps{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		y.consume, y.checks = nil, nil
		return ctx, nil
	})
	y.registerFactSteps(sc)
	y.registerYardCheckSteps(sc)
	sc.Step(`^(\d+) cases are opened$`, func(want int) error {
		return wantInt("ExceptionOpened events", countEvents(w, "ExceptionOpened"), want)
	})
}

// wire builds the use cases of the scenario on first use (they share one
// processed-events set, like the production wiring).
func (y *yardSteps) wire() {
	if y.consume != nil {
		return
	}
	processed := memory.NewProcessedEvents()
	y.consume = &usecases.ConsumeFact{Processed: processed, OpenOrTouch: y.w.uc.open}
	y.checks = &usecases.OpenYardCheckDiscrepancies{Processed: processed, OpenOrTouch: y.w.uc.open}
}

// remember points the scenario at the case owning (sourceType, subject, site).
func (y *yardSteps) remember(sourceType, subject, site string) error {
	key, err := shared.NewDedupKey(sourceType, subject, site)
	if err != nil {
		return err
	}
	c, err := y.w.cases.FindOpenByDedupKey(y.w.ctx, key)
	if err != nil {
		return errors.New("no case under the yard key: " + err.Error())
	}
	y.w.caseID = c.ID()
	return nil
}

// fact runs one single-case fact and remembers the case it owns.
func (y *yardSteps) fact(sourceType, subject, site, eventID, payload string) error {
	y.wire()
	y.w.run(func() error {
		if _, err := y.consume.Execute(y.w.ctx, usecases.Fact{
			SourceType: sourceType, SourceSubject: subject, SiteID: site,
			SourceEventID: eventID, Payload: []byte(payload),
		}); err != nil {
			return err
		}
		return y.remember(sourceType, subject, site)
	})
	return nil
}

func (y *yardSteps) registerFactSteps(sc *godog.ScenarioContext) {
	sc.Step(`^a visit rejection for visit (\S+) at site (\S+) with event id (\S+) arrives$`,
		func(visit, site, eventID string) error {
			return y.fact(usecases.TypeVisitRejected, visit, site, eventID,
				`{"visit_id":"`+visit+`","site_id":"`+site+`","reason":"SEAL_MISMATCH"}`)
		})
	sc.Step(`^a seal exception for trailer (\S+) raised at (\S+) at site (\S+) with event id (\S+) arrives$`,
		func(trailer, raisedAt, site, eventID string) error {
			return y.fact(usecases.TypeSealExceptionRaised, usecases.TrailerOccurrenceSubject(trailer, raisedAt), site, eventID,
				`{"trailer_id":"`+trailer+`","site_id":"`+site+`","raised_at":"`+raisedAt+`"}`)
		})
	sc.Step(`^a dwell exceedance for trailer (\S+) at (\S+) at site (\S+) with event id (\S+) arrives$`,
		func(trailer, at, site, eventID string) error {
			return y.fact(usecases.TypeTrailerDwellExceeded, usecases.TrailerOccurrenceSubject(trailer, at), site, eventID,
				`{"trailer_id":"`+trailer+`","site_id":"`+site+`","dwell_minutes":3000,"threshold_minutes":2880}`)
		})
	sc.Step(`^a no-show for appointment (\S+) at site (\S+) with event id (\S+) arrives$`,
		func(appointment, site, eventID string) error {
			return y.fact(usecases.TypeDockAppointmentNoShow, appointment, site, eventID,
				`{"appointment_id":"`+appointment+`","site_id":"`+site+`","direction":"INBOUND"}`)
		})
}

// parseDiscrepancies reads "KIND:LOCATION:TRAILER, ..." ("-" = no trailer).
func parseDiscrepancies(list string) ([]usecases.YardCheckDiscrepancy, error) {
	var ds []usecases.YardCheckDiscrepancy
	for _, item := range strings.Split(list, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) != 3 {
			return nil, errors.New("discrepancy " + item + " is not KIND:LOCATION:TRAILER")
		}
		d := usecases.YardCheckDiscrepancy{Kind: parts[0], LocationCode: parts[1]}
		if parts[2] != "-" {
			id := parts[2]
			d.TrailerID = &id
		}
		ds = append(ds, d)
	}
	return ds, nil
}

// closure closes a yard check; the "the case ..." assertions that follow read
// the case of its first discrepancy.
func (y *yardSteps) closure(check, site, eventID, list string) error {
	y.wire()
	ds, err := parseDiscrepancies(list)
	if err != nil {
		return err
	}
	y.w.run(func() error {
		_, _, err := y.checks.Execute(y.w.ctx, usecases.YardCheckClosure{
			EventID: eventID, YardCheckID: check, SiteID: site, ClosedAt: "2026-11-05T09:00:00Z", Discrepancies: ds,
		})
		if err != nil || len(ds) == 0 {
			return err
		}
		return y.remember(usecases.TypeYardCheckClosed, usecases.YardCheckSubject(check, ds[0]), site)
	})
	return nil
}

func (y *yardSteps) registerYardCheckSteps(sc *godog.ScenarioContext) {
	sc.Step(`^yard check (\S+) at site (\S+) closes with event id (\S+) and discrepancies (.+)$`, y.closure)
	sc.Step(`^yard check (\S+) at site (\S+) closes with event id (\S+) and no discrepancies$`,
		func(check, site, eventID string) error { return y.closure(check, site, eventID, "") })
}
