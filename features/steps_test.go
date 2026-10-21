package features_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// The full CloudEvents types the scenarios consume (the producers'
// AsyncAPI contracts, byte-identical).
const (
	taskCPTMissed = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	repromised    = "com.warehouse.wes.order-management.order.OrderRepromised"
)

// domainErrors maps the error names the feature files use to the domain
// sentinels.
var domainErrors = map[string]error{
	"ErrCaseClosed":          exceptioncase.ErrCaseClosed,
	"ErrBlankResolutionCode": exceptioncase.ErrBlankResolutionCode,
	"ErrBlankRootCause":      exceptioncase.ErrBlankRootCause,
	"ErrBlankAssignee":       exceptioncase.ErrBlankAssignee,
	"ErrBlankQueue":          exceptioncase.ErrBlankQueue,
	"ErrPolicyUndeclared":    usecases.ErrPolicyUndeclared,
}

// registerSetupSteps registers the clock and policy-declaration steps.
func registerSetupSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the clock is (.+)$`, func(raw string) error {
		at, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return err
		}
		w.clock.now = at.UTC()
		return nil
	})

	sc.Step(`^time advances (\d+) minutes$`, func(minutes int) error {
		w.clock.now = w.clock.now.Add(time.Duration(minutes) * time.Minute)
		return nil
	})

	sc.Step(`^time advances (\d+) hours$`, func(hours int) error {
		w.clock.now = w.clock.now.Add(time.Duration(hours) * time.Hour)
		return nil
	})

	sc.Step(`^a policy is declared for source type (\S+) with severity (\S+) queue (\S+) and sla (\d+) minutes$`,
		func(sourceType, severity, queue string, sla int) error {
			return declarePolicy(w, sourceType, severity, queue, sla, "", 0)
		})

	sc.Step(`^a policy is declared for source type (\S+) with severity (\S+) queue (\S+) and sla (\d+) minutes and auto-resolve on (\S+) within (\d+) minutes$`,
		func(sourceType, severity, queue string, sla int, onType string, within int) error {
			return declarePolicy(w, sourceType, severity, queue, sla, onType, within)
		})
}

func declarePolicy(w *world, sourceType, severity, queue string, sla int, onType string, within int) error {
	w.run(func() error {
		_, err := w.uc.declare.Execute(w.ctx, usecases.PolicyDeclaration{
			SourceType: sourceType, Severity: shared.Severity(severity), Queue: queue,
			SLAMinutes: int64(sla), AutoResolveOn: onType, WithinMinutes: int64(within), Actor: "op-1",
		})
		return err
	})
	return nil
}

// registerFactSteps registers the consumed-fact steps: the open-or-touch
// entry point, the correlated repromise of the auto-resolve rule, and the
// SLA sweeper.
func registerFactSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^a fact of type (\S+) for work unit (\S+) at site (\S+) with event id (\S+) and payload (.+) arrives$`,
		func(sourceType, subject, site, eventID, payload string) error {
			w.run(func() error {
				c, err := w.uc.open.Execute(w.ctx, usecases.Fact{
					SourceType: sourceType, SourceSubject: subject, SiteID: site,
					SourceEventID: eventID, Payload: []byte(payload),
				})
				if err == nil {
					w.caseID = c.ID()
				}
				return err
			})
			return nil
		})

	sc.Step(`^an OrderRepromised fact for order (\S+) with event id (\S+) arrives$`,
		func(orderID, eventID string) error {
			w.run(func() error {
				n, err := w.uc.auto.Execute(w.ctx, usecases.CorrelatedFact{
					CorrelatedType: repromised, CorrelatedOrderID: orderID, CorrelatedEventID: eventID,
				})
				w.closed = n
				return err
			})
			return nil
		})

	sc.Step(`^the SLA sweeper runs$`, func() error {
		w.run(func() error {
			_, err := w.uc.sweep.Execute(w.ctx)
			return err
		})
		return nil
	})
}

// registerCommandSteps registers the operator command steps.
func registerCommandSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^operator (\S+) triages the case to severity (\S+) queue (\S+)$`,
		func(actor, severity, queue string) error {
			w.run(func() error {
				_, err := w.uc.triage.Execute(w.ctx, w.caseID, shared.Severity(severity), queue, actor)
				return err
			})
			return nil
		})

	sc.Step(`^operator (\S+) assigns the case to (\S+)$`,
		func(actor, assignee string) error {
			w.run(func() error {
				_, err := w.uc.assign.Execute(w.ctx, w.caseID, assignee, actor)
				return err
			})
			return nil
		})

	sc.Step(`^operator (\S+) resolves the case with code (.+?) and root cause (.+)$`,
		func(actor, code, rootCause string) error {
			w.run(func() error {
				_, err := w.uc.resolve.Execute(w.ctx, w.caseID, unquote(code), unquote(rootCause), "", actor)
				return err
			})
			return nil
		})
}

// unquote turns the literal "" of a feature file into the empty string.
func unquote(s string) string {
	if s == `""` {
		return ""
	}
	return s
}

// wantStr builds the "got X, want Y" failure of a scalar assertion.
func wantStr(what, got, want string) error {
	if got != want {
		return errors.New(what + " = " + got + ", want " + want)
	}
	return nil
}

// wantInt is wantStr for integers.
func wantInt(what string, got, want int) error {
	return wantStr(what, itoa(got), itoa(want))
}

// wantTime is wantStr for instants (compared by instant, not clock).
func wantTime(what string, got, want time.Time) error {
	if !got.Equal(want) {
		return errors.New(what + " = " + got.Format(time.RFC3339) + ", want " + want.Format(time.RFC3339))
	}
	return nil
}

// registerCaseStateSteps registers the case-state assertions.
func registerCaseStateSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^a new case is opened$`, func() error {
		if opened := countEvents(w, "ExceptionOpened"); opened != 1 {
			return errors.New("ExceptionOpened published " + itoa(opened) + " times, want 1")
		}
		if w.caseID == "" {
			return errors.New("no case was returned")
		}
		return nil
	})

	sc.Step(`^no new case is opened$`, func() error {
		if opened := countEvents(w, "ExceptionOpened"); opened != 1 {
			return errors.New("ExceptionOpened published " + itoa(opened) + " times, want exactly 1 (touch, not open)")
		}
		return nil
	})

	sc.Step(`^the case occurrence is (\d+)$`, func(want int) error {
		return wantInt("occurrence", int(w.current().Occurrence()), want)
	})

	sc.Step(`^the case status is (\S+)$`, func(want string) error {
		return wantStr("status", string(w.current().Status()), want)
	})

	sc.Step(`^the case severity is (\S+)$`, func(want string) error {
		return wantStr("severity", string(w.current().Severity()), want)
	})

	sc.Step(`^the case queue is (\S+)$`, func(want string) error {
		return wantStr("queue", w.current().Queue(), want)
	})

	sc.Step(`^the case assignee is (\S+)$`, func(want string) error {
		return wantStr("assignee", w.current().Assignee(), want)
	})

	sc.Step(`^the case sla is (\d+) minutes$`, func(want int) error {
		return wantInt("sla minutes", int(w.current().SLAMinutes()), want)
	})

	sc.Step(`^the case sla is due at (.+)$`, func(raw string) error {
		want, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return err
		}
		return wantTime("sla due at", w.current().SLADueAt(), want)
	})

	sc.Step(`^the case was last seen at (.+)$`, func(raw string) error {
		want, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return err
		}
		return wantTime("last seen at", w.current().LastSeenAt(), want)
	})

	sc.Step(`^the frozen payload of the case is (.+)$`, func(raw string) error {
		return sameJSONError(w.current().SourcePayload(), []byte(raw))
	})
}

// registerOutcomeSteps registers the escalation, resolution and
// correlation-outcome assertions.
func registerOutcomeSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the case is escalated$`, func() error {
		return wantStr("escalated", itoa(btoi(w.current().Escalated())), "1")
	})

	sc.Step(`^the case is not escalated$`, func() error {
		return wantStr("escalated", itoa(btoi(w.current().Escalated())), "0")
	})

	sc.Step(`^the case resolution code is (\S+)$`, func(want string) error {
		return wantStr("resolution code", w.current().ResolutionCode(), want)
	})

	sc.Step(`^the case root cause is (\S+)$`, func(want string) error {
		return wantStr("root cause", w.current().RootCause(), want)
	})

	sc.Step(`^the case was auto-resolved by rule (\S+) with correlated event (\S+)$`, func(rule, eventID string) error {
		s := w.current().Snapshot()
		if !s.AutoResolved {
			return errors.New("the case was not auto-resolved")
		}
		if err := wantStr("rule", s.AutoRuleID, rule); err != nil {
			return err
		}
		return wantStr("correlated event", s.AutoEventID, eventID)
	})

	sc.Step(`^the correlation closes (\d+) cases?$`, func(want int) error {
		return wantInt("cases closed by the correlation", w.closed, want)
	})

	sc.Step(`^the open caseload has (\d+) cases?$`, func(want int) error {
		all, err := w.uc.list.Execute(w.ctx, "", "", "", "")
		if err != nil {
			return err
		}
		// The open caseload counts every case that is not RESOLVED yet
		// (resolve activity is part of the record, so the listing keeps it).
		open := 0
		for _, c := range all {
			if c.Status != "RESOLVED" {
				open++
			}
		}
		return wantInt("open caseload", open, want)
	})
}

// registerEventAuditSteps registers the published-events, audit-trail and
// command-error assertions.
func registerEventAuditSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the published events are (.+)$`, func(raw string) error {
		return wantStr("events", strings.Join(w.ev.CaseEventNames(), ","), strings.Join(splitList(raw), ","))
	})

	sc.Step(`^no escalation event is published$`, func() error {
		return wantInt("ExceptionEscalated count", countEvents(w, "ExceptionEscalated"), 0)
	})

	sc.Step(`^no auto-resolution event is published$`, func() error {
		return wantInt("ExceptionAutoResolved count", countEvents(w, "ExceptionAutoResolved"), 0)
	})

	sc.Step(`^the audit trail is (.+)$`, func(raw string) error {
		trail := w.current().AuditTrail()
		got := make([]string, len(trail))
		for i, e := range trail {
			got[i] = e.Action
		}
		return wantStr("audit", strings.Join(got, ","), strings.Join(splitList(raw), ","))
	})

	sc.Step(`^the audit entry (\S+) records actor (\S+)$`, func(action, actor string) error {
		for _, e := range w.current().AuditTrail() {
			if e.Action == action {
				return wantStr("audit "+action+" actor", e.Actor.String(), actor)
			}
		}
		return errors.New("no " + action + " audit entry")
	})

	sc.Step(`^the command fails with (\S+)$`, func(name string) error {
		w.errChecked = true
		if w.lastErr == nil {
			return errors.New("the last command succeeded, want " + name)
		}
		want, ok := domainErrors[name]
		if !ok {
			return errors.New("unknown error " + name + " in the scenario")
		}
		if !errors.Is(w.lastErr, want) {
			return errors.New("command error = " + w.lastErr.Error() + ", want " + name)
		}
		return nil
	})
}

func countEvents(w *world, name string) int {
	n := 0
	for _, got := range w.ev.CaseEventNames() {
		if got == name {
			n++
		}
	}
	return n
}

func splitList(raw string) []string {
	out := []string{}
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func itoa(n int) string { return strconv.Itoa(n) }

// btoi renders a bool as 0/1 for the wantStr-based assertions.
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// sameJSONError compares two JSON documents by value (key order and
// spacing are not semantic).
func sameJSONError(a, b []byte) error {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return errors.New("stored payload is not JSON: " + err.Error())
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return errors.New("scenario payload is not JSON: " + err.Error())
	}
	if !reflect.DeepEqual(va, vb) {
		return errors.New("payloads differ: " + string(a) + " vs " + string(b))
	}
	return nil
}
