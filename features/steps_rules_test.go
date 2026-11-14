package features_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/adapters/outbound/memory"
	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// steps_rules_test.go — the vocabulary of the documentation-derived rule
// features (intake, lifecycle, sla, correlation, policy and read rules):
// switching between the cases of one scenario, a fact without a site, the
// order-correlation consumer (claim + auto-resolve in one unit of work), the
// SLA report read, the policy version assertions, the fields of the last
// published event, and following a page cursor.

// ruleSteps holds the per-scenario state of the rule steps.
type ruleSteps struct {
	w    *world
	corr *usecases.ConsumeCorrelation
}

// registerRuleSteps registers the steps of the documentation-derived features.
func registerRuleSteps(sc *godog.ScenarioContext, w *world) {
	r := &ruleSteps{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		r.corr = nil
		return ctx, nil
	})
	r.registerCaseSteps(sc)
	r.registerCorrelationSteps(sc)
	r.registerReportSteps(sc)
	r.registerEventSteps(sc)
	r.registerPolicySteps(sc)
	r.registerPageSteps(sc)
	r.registerReadinessSteps(sc)
}

// correlation builds the order-correlation consumer of the scenario on first
// use: its own processed-events set over the same auto-resolve use case.
func (r *ruleSteps) correlation() *usecases.ConsumeCorrelation {
	if r.corr == nil {
		r.corr = &usecases.ConsumeCorrelation{Processed: memory.NewProcessedEvents(), AutoResolve: r.w.uc.auto}
	}
	return r.corr
}

func (r *ruleSteps) registerCaseSteps(sc *godog.ScenarioContext) {
	w := r.w
	sc.Step(`^I look at case (\S+)$`, func(id string) error {
		if _, err := w.cases.FindByID(w.ctx, id); err != nil {
			return errors.New("no such case " + id + ": " + err.Error())
		}
		w.caseID = id
		return nil
	})

	sc.Step(`^a fact of type (\S+) for work unit (\S+) without a site with event id (\S+) and payload (.+) arrives$`,
		func(sourceType, subject, eventID, payload string) error {
			w.run(func() error {
				c, err := w.uc.open.Execute(w.ctx, usecases.Fact{
					SourceType: sourceType, SourceSubject: subject,
					SourceEventID: eventID, Payload: []byte(payload),
				})
				if err == nil {
					w.caseID = c.ID()
				}
				return err
			})
			return nil
		})

	sc.Step(`^the case has been brought to status (OPEN|TRIAGED|ASSIGNED)$`, func(status string) error {
		switch status {
		case "TRIAGED":
			w.run(func() error {
				_, err := w.uc.triage.Execute(w.ctx, w.caseID, shared.Severity("MEDIUM"), "ps-triage", "op-1")
				return err
			})
		case "ASSIGNED":
			w.run(func() error {
				_, err := w.uc.assign.Execute(w.ctx, w.caseID, "ana", "op-1")
				return err
			})
		}
		return nil
	})

	sc.Step(`^the case version is (\d+)$`, func(want int) error {
		return wantInt("version", int(w.current().Version()), want)
	})

	// A raw 0x00 byte cannot be written in a feature file: the step builds it.
	sc.Step(`^a PUT request is sent to "([^"]+)" with key "([^"]+)" and a body holding a raw NUL byte$`, func(path, key string) error {
		return w.do(http.MethodPut, path, key, "", "{\"severity\":\"HIGH\",\"queue\":\"ps\x00main\",\"slaMinutes\":30}")
	})
}

func (r *ruleSteps) registerCorrelationSteps(sc *godog.ScenarioContext) {
	w := r.w
	sc.Step(`^an OrderRepromised fact for order (\S+) with event id ?(\S*) is consumed$`, func(orderID, eventID string) error {
		var (
			closed int
			out    usecases.Outcome
		)
		w.run(func() error {
			var err error
			closed, out, err = r.correlation().Execute(w.ctx, usecases.CorrelatedFact{
				CorrelatedType: repromised, CorrelatedOrderID: orderID, CorrelatedEventID: eventID,
			})
			return err
		})
		w.closed, w.consumeOut = closed, out
		return nil
	})
}

func (r *ruleSteps) registerReportSteps(sc *godog.ScenarioContext) {
	w := r.w
	report := func() (open, breached, escalated int, bySeverity, byQueue map[string]int, err error) {
		rep, err := (&usecases.GetSLAReport{Cases: w.cases, Clock: w.clock}).Execute(w.ctx)
		return rep.OpenCases, rep.Breached, rep.Escalated, rep.BySeverity, rep.ByQueue, err
	}
	sc.Step(`^the SLA report counts (\d+) open, (\d+) breached and (\d+) escalated cases?$`, func(open, breached, escalated int) error {
		o, b, e, _, _, err := report()
		if err != nil {
			return err
		}
		if o != open || b != breached || e != escalated {
			return fmt.Errorf("SLA report = %d open, %d breached, %d escalated; want %d, %d, %d", o, b, e, open, breached, escalated)
		}
		return nil
	})
	sc.Step(`^the SLA report counts (\d+) open cases? with severity (\S+)$`, func(want int, severity string) error {
		_, _, _, bySeverity, _, err := report()
		if err != nil {
			return err
		}
		return wantInt("open cases with severity "+severity, bySeverity[severity], want)
	})
	sc.Step(`^the SLA report counts (\d+) open cases? in queue (\S+)$`, func(want int, queue string) error {
		_, _, _, _, byQueue, err := report()
		if err != nil {
			return err
		}
		return wantInt("open cases in queue "+queue, byQueue[queue], want)
	})
}

func (r *ruleSteps) registerEventSteps(sc *godog.ScenarioContext) {
	w := r.w
	// The fields of the NEWEST published event of that name (dotted paths
	// reach into embedded and nested structs: Version, Key.SourceSubject).
	sc.Step(`^the last (Exception\w+) event has (\S+) (.+)$`, func(name, path, want string) error {
		for i := len(w.ev.Case) - 1; i >= 0; i-- {
			if w.ev.Case[i].EventName() != name {
				continue
			}
			v := reflect.ValueOf(w.ev.Case[i])
			for _, seg := range strings.Split(path, ".") {
				v = v.FieldByName(seg)
				if !v.IsValid() {
					return fmt.Errorf("%s has no field %q", name, path)
				}
			}
			got := renderField(v.Interface())
			if strings.HasPrefix(want, "{") {
				return sameJSONError([]byte(got), []byte(want))
			}
			return wantStr(name+" "+path, got, want)
		}
		return errors.New("no " + name + " event was published")
	})
}

// renderField prints an event field the way the feature files write it.
func renderField(v any) string {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func (r *ruleSteps) registerPolicySteps(sc *godog.ScenarioContext) {
	w := r.w
	sc.Step(`^the policy for (\S+) is at version (\d+)$`, func(sourceType string, want int) error {
		p, err := w.pol.Get(w.ctx, sourceType)
		if err != nil {
			return err
		}
		return wantInt("policy version", int(p.Version()), want)
	})
	// The policy aggregate's own errors (the use case driven directly, behind
	// the REST adapter's validation).
	sc.Step(`^the policy declaration fails with (\S+)$`, func(name string) error {
		w.errChecked = true
		want, ok := map[string]error{
			"ErrInvalidSLA":        exceptionpolicy.ErrInvalidSLA,
			"ErrInvalidSourceType": exceptionpolicy.ErrInvalidSourceType,
		}[name]
		if !ok {
			return errors.New("unknown policy error " + name + " in the scenario")
		}
		if w.lastErr == nil {
			return errors.New("the declaration succeeded, want " + name)
		}
		if !errors.Is(w.lastErr, want) {
			return errors.New("declaration error = " + w.lastErr.Error() + ", want " + name)
		}
		return nil
	})
	sc.Step(`^the policy declarations published for (\S+) are versions (.+)$`, func(sourceType, raw string) error {
		var got []string
		for _, d := range w.ev.Policy {
			if d.SourceType() == sourceType {
				got = append(got, itoa(int(d.Version())))
			}
		}
		return wantStr("published policy versions", strings.Join(got, ","), strings.Join(splitList(raw), ","))
	})
}

func (r *ruleSteps) registerPageSteps(sc *godog.ScenarioContext) {
	w := r.w
	sc.Step(`^a GET request is sent to "([^"]+)" with the cursor of the previous page$`, func(path string) error {
		v, ok := w.field("nextCursor")
		if !ok {
			return fmt.Errorf("the previous page carries no nextCursor: %s", w.lastBody)
		}
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		return w.do(http.MethodGet, path+sep+"cursor="+scalar(v), "", "", "")
	})
}

func (r *ruleSteps) registerReadinessSteps(sc *godog.ScenarioContext) {
	w := r.w
	sc.Step(`^Postgres is unreachable$`, func() error { w.postgresDown.Store(true); return nil })
	sc.Step(`^Postgres is reachable again$`, func() error { w.postgresDown.Store(false); return nil })
	sc.Step(`^no Kafka broker answers$`, func() error { w.kafkaDown.Store(true); return nil })
	sc.Step(`^the service starts draining for shutdown$`, func() error { w.ready.SetNotReady(); return nil })
}
