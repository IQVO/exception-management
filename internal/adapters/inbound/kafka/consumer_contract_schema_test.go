package kafka_test

// Schema-walking helpers for the pinned AsyncAPI specs: collecting the
// CloudEvents envelope attributes a message payload pins (const or
// single-value enum, through $ref and allOf overlays) and the attribute
// set the envelope schema requires. Split from
// consumer_contract_spec_test.go to stay under the file-length gate.

import (
	"testing"
)

func (d *pinnedAsyncAPI) payloadConsts(t *testing.T, payload any) map[string]string {
	t.Helper()
	consts := map[string]string{}
	d.collectPayloadConsts(t, payload, consts, map[string]bool{})
	return consts
}

func (d *pinnedAsyncAPI) collectPayloadConsts(t *testing.T, node any, consts map[string]string, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectPayloadConsts(t, schema, consts, resolving)
		return
	}
	for attr, c := range pinnedPropertyValues(v) {
		if _, seen := consts[attr]; !seen {
			consts[attr] = c
		}
	}
	for _, item := range pinnedAllOfItems(v) {
		d.collectPayloadConsts(t, item, consts, resolving)
	}
}

// pinnedPropertyValues returns the pinned string each directly-declared
// property of node carries — a const, or a single-value enum (the two
// overlay spellings producers use).
func pinnedPropertyValues(node map[string]any) map[string]string {
	values := map[string]string{}
	props, ok := node["properties"].(map[string]any)
	if !ok {
		return values
	}
	for attr, p := range props {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		if c, ok := pm["const"].(string); ok {
			values[attr] = c
			continue
		}
		if enum, ok := pm["enum"].([]any); ok && len(enum) == 1 {
			if s, ok := enum[0].(string); ok {
				values[attr] = s
			}
		}
	}
	return values
}

func pinnedAllOfItems(node map[string]any) []any {
	items, _ := node["allOf"].([]any)
	return items
}

// resolveSchemaRef resolves node's top-level $ref into components/schemas
// (cycle-guarded). ok is false when node carries no $ref, or the ref is
// already being resolved.
func (d *pinnedAsyncAPI) resolveSchemaRef(t *testing.T, node map[string]any, resolving map[string]bool) (any, bool) {
	ref, ok := node["$ref"].(string)
	if !ok {
		return nil, false
	}
	name := refName(t, ref)
	if resolving[name] {
		return nil, false
	}
	resolving[name] = true
	schema, ok := d.Components.Schemas[name]
	if !ok {
		t.Fatalf("pinned spec: unresolved schema $ref %q", ref)
	}
	return schema, true
}

// envelopeRequired returns the required attribute names declared by a
// message payload's envelope schema (resolving top-level $refs into
// components/schemas). The subset that names CloudEvents attributes is
// what E3 asserts on.
func (d *pinnedAsyncAPI) envelopeRequired(t *testing.T, payload any) map[string]bool {
	t.Helper()
	required := map[string]bool{}
	d.collectRequired(t, payload, required, map[string]bool{})
	return required
}

func (d *pinnedAsyncAPI) collectRequired(t *testing.T, node any, required map[string]bool, resolving map[string]bool) {
	v, ok := node.(map[string]any)
	if !ok {
		return
	}
	for _, r := range pinnedRequiredList(v) {
		required[r] = true
	}
	if schema, ok := d.resolveSchemaRef(t, v, resolving); ok {
		d.collectRequired(t, schema, required, resolving)
		return
	}
	for _, item := range pinnedAllOfItems(v) {
		d.collectRequired(t, item, required, resolving)
	}
}

func pinnedRequiredList(node map[string]any) []string {
	raw, ok := node["required"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
