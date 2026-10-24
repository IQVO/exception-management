package features_test

import (
	"context"
	"errors"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// registerCartonSteps registers the CartonDiscrepancyRaised steps: the real
// OpenCartonDiscrepancy use case (event-id claim included) over the same
// memory doubles as the other facts.
func registerCartonSteps(sc *godog.ScenarioContext, w *world) {
	var open *usecases.OpenCartonDiscrepancy
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		open = nil
		return ctx, nil
	})
	run := func(kind, receipt, sscc, eventID string) error {
		if open == nil {
			open = &usecases.OpenCartonDiscrepancy{Consume: &usecases.ConsumeFact{
				Processed: memory.NewProcessedEvents(), OpenOrTouch: w.uc.open,
			}}
		}
		w.run(func() error {
			_, err := open.Execute(w.ctx, usecases.CartonDiscrepancy{
				EventID: eventID, ReceiptID: receipt, SSCC: sscc, Kind: kind,
				Payload: []byte(`{"receipt_id":"` + receipt + `","kind":"` + kind + `"}`),
			})
			if err != nil {
				return err
			}
			key, err := shared.NewDedupKey(usecases.TypeCartonDiscrepancyRaised, usecases.CartonSubject(receipt, sscc, kind), "")
			if err != nil {
				return err
			}
			c, err := w.cases.FindOpenByDedupKey(w.ctx, key)
			if err != nil {
				return errors.New("no case under the carton key: " + err.Error())
			}
			w.caseID = c.ID()
			return nil
		})
		return nil
	}
	sc.Step(`^a carton discrepancy of kind (\S+) for receipt (\S+) and sscc (\d+) with event id (\S+) arrives$`, run)
	sc.Step(`^a carton discrepancy of kind (\S+) for receipt (\S+) and no sscc with event id (\S+) arrives$`,
		func(kind, receipt, eventID string) error { return run(kind, receipt, "", eventID) })
}
