package features_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/exception-management/internal/application/usecases"
	"github.com/claudioed/exception-management/internal/domain/shared"
)

// features_wave2_test.go — the wave-2 step vocabulary: the REST edge through
// the real chi router (every operation of apis/openapi.yaml, the RFC 7807
// problem catalogue and the Idempotency-Key middleware) and the ConsumeFact
// entry point the inbound Kafka consumers call. The JSON steps are GENERIC
// (raw body sent verbatim, dotted response paths), so no step per field is
// needed.

// passthroughUoW is the memory UnitOfWork: fn runs unwrapped (the Postgres
// adapter's Do is the real transaction).
type passthroughUoW struct{}

func (passthroughUoW) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// do sends one request to the scenario's API server and records the
// response. A 2xx case body is remembered, so the domain-worded assertion
// steps keep working after REST commands.
func (w *world) do(method, path, key, actor, body string) error {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(w.ctx, method, w.httpSrv.URL+path, rd)
	if err != nil {
		return err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if actor != "" {
		req.Header.Set("X-Actor", actor)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.lastStatus, w.lastHeader, w.lastBody = resp.StatusCode, resp.Header, raw
	if resp.StatusCode < 300 {
		var m map[string]any
		if json.Unmarshal(raw, &m) == nil {
			if id, ok := m["caseId"].(string); ok && id != "" {
				w.caseID = id
			}
		}
	}
	return nil
}

// field reads a dotted path ("a.b.0.c") of the last response body.
func (w *world) field(path string) (any, bool) {
	var v any
	if json.Unmarshal(w.lastBody, &v) != nil {
		return nil, false
	}
	cur := v
	for _, seg := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// scalar renders a decoded JSON value the way the feature files write it.
func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return "null"
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func registerWave2Steps(sc *godog.ScenarioContext, w *world) {
	registerRequestSteps(sc, w)
	registerResponseSteps(sc, w)
	registerConsumeSteps(sc, w)
}

// registerRequestSteps registers the raw request steps (the body goes out
// verbatim, so malformed JSON is expressible).
func registerRequestSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^a (POST|PUT) request is sent to "([^"]+)" with the JSON body (.*)$`,
		func(method, path, body string) error { return w.do(method, path, "", "", body) })
	sc.Step(`^a (POST|PUT) request is sent to "([^"]+)" with key "([^"]+)" and the JSON body (.*)$`,
		func(method, path, key, body string) error { return w.do(method, path, key, "", body) })
	sc.Step(`^a (POST|PUT) request is sent to "([^"]+)" with key "([^"]+)" as (\S+) and the JSON body (.*)$`,
		func(method, path, key, actor, body string) error { return w.do(method, path, key, actor, body) })
	sc.Step(`^a GET request is sent to "([^"]+)"$`,
		func(path string) error { return w.do(http.MethodGet, path, "", "", "") })
}

// registerResponseSteps registers the assertions on the last response.
func registerResponseSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the response status is (\d+)$`, func(want int) error {
		if w.lastStatus != want {
			return fmt.Errorf("status = %d, want %d (body: %s)", w.lastStatus, want, w.lastBody)
		}
		return nil
	})
	sc.Step(`^the response problem is "([^"]+)"$`, func(slug string) error {
		v, ok := w.field("type")
		if !ok {
			return fmt.Errorf("no problem type in body: %s", w.lastBody)
		}
		if got, _ := v.(string); !strings.HasSuffix(got, "/"+slug) {
			return fmt.Errorf("problem type = %q, want slug %q", got, slug)
		}
		return nil
	})
	sc.Step(`^the response header "([^"]+)" is "([^"]+)"$`, func(name, want string) error {
		if got := w.lastHeader.Get(name); got != want {
			return fmt.Errorf("header %s = %q, want %q", name, got, want)
		}
		return nil
	})
	sc.Step(`^the response field "([^"]+)" is (.+)$`, func(path, want string) error {
		v, ok := w.field(path)
		if !ok {
			return fmt.Errorf("no field %q in body: %s", path, w.lastBody)
		}
		if got := scalar(v); got != want {
			return fmt.Errorf("field %q = %q, want %q (body: %s)", path, got, want, w.lastBody)
		}
		return nil
	})
	sc.Step(`^the response list "([^"]*)" has (\d+) items$`, func(path string, want int) error {
		v, ok := w.field(path)
		if !ok {
			return fmt.Errorf("no list %q in body: %s", path, w.lastBody)
		}
		list, isList := v.([]any)
		if !isList {
			return fmt.Errorf("field %q is not a list: %s", path, w.lastBody)
		}
		if len(list) != want {
			return fmt.Errorf("list %q has %d items, want %d", path, len(list), want)
		}
		return nil
	})
	sc.Step(`^the response has no field "([^"]+)"$`, func(path string) error {
		if _, ok := w.field(path); ok {
			return fmt.Errorf("field %q present in body: %s", path, w.lastBody)
		}
		return nil
	})
}

// registerConsumeSteps registers the ConsumeFact entry point the inbound
// Kafka consumers call (claim + open-or-touch in one unit of work).
func registerConsumeSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^a fact of type (\S+) for work unit (\S+) at site (\S+) with event id ?(\S*) and payload (.+) is consumed$`,
		func(sourceType, subject, site, eventID, payload string) error {
			var out usecases.Outcome
			w.run(func() error {
				var err error
				out, err = w.uc.consume.Execute(w.ctx, usecases.Fact{
					SourceType:    sourceType,
					SourceSubject: subject,
					SiteID:        site,
					SourceEventID: eventID,
					Payload:       []byte(payload),
				})
				return err
			})
			w.consumeOut = out
			if w.lastErr == nil {
				// Track the case the fact opened or touched, so the shared
				// assertion steps keep working.
				if c, err := w.cases.FindOpenByDedupKey(w.ctx, shared.DedupKey{
					SourceType: sourceType, SourceSubject: subject, SiteID: site,
				}); err == nil {
					w.caseID = c.ID()
				}
			}
			return nil
		})
	sc.Step(`^the consume outcome is (\S+)$`, func(want string) error {
		if string(w.consumeOut) != want {
			return fmt.Errorf("consume outcome = %q, want %q", w.consumeOut, want)
		}
		return nil
	})
}
