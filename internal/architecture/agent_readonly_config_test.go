// Rendered from warehouse-harness-template develop ea62687
// templates/architecture/agent_readonly_config_test.go.tmpl — the repo's half of the read-only agent
// sensor (agent_readonly_fitness_test.go and agent_readonly_methods_fitness_test.go are verbatim
// copies of the template). This context's MCP server and A2A agent are READ-ONLY by charter (ADR
// 0007, ADR 0009: four L1/L2 tools and skills, no write operation at all; triage, assign, resolve
// and declare stay on REST, the fact consumers on Kafka).
//
// Adopt it only for a surface whose charter is READ-ONLY (its ADR says so, every tool is L1/L2). A
// surface with write tools (L3/L4) is governed by mcp_authority_fitness_test.go instead.
//
// The defaults are strict on purpose: with empty allowlists every guarded identifier the surface
// references is reported, which is the list to review. Allowlist a READ identifier only (a reader
// constructor, a view type, a not-found error, the read-only pool); never a write use case, the unit
// of work, the outbox, a lock or the idempotency store. Worked examples (fleet, 2026-11):
// material-flow-control and amr-fleet-control internal/architecture/agent_readonly_fitness_test.go.
//
// The package's modulePath constant (architecture_test.go) is the module path both halves build
// import paths from; it must be the module line of go.mod.
package architecture

// agentSurfaces is every read-only agent surface of the service: the MCP server (cmd/mcp) and the
// A2A agent (cmd/a2a). A listed surface whose adapter or cmd/ directory is missing fails the sensor.
var agentSurfaces = []agentSurface{
	{name: "mcp", adapterDir: "../adapters/inbound/mcp", cmdDir: "../../cmd/mcp", reads: agentReads, forbidSibling: "/internal/adapters/inbound/a2a"},
	{name: "a2a", adapterDir: "../adapters/inbound/a2a", cmdDir: "../../cmd/a2a", reads: agentReads, forbidSibling: "/internal/adapters/inbound/mcp"},
}

// agentReads is the exact method set of the adapter's `Reads` interface, checked wherever a file
// declares `type Reads interface`. Empty: neither adapter declares a Reads seam; both are built over
// the four read use cases (mcp.UseCases, a2a.UseCases), which the usecases allowlist pins.
var agentReads = []string{}

// agentGuardedPackages maps the import-path SUFFIX of each guarded package to its allowlist key. A
// guarded package is referenced only through its own name (no alias, no dot import) and only for
// the identifiers agentAllowedSelectors lists. Add every package that holds a write seam (a second
// use-case package, a store adapter).
var agentGuardedPackages = map[string]string{
	"/internal/application/usecases":       "usecases",
	"/internal/application/ports":          "ports",
	"/internal/adapters/outbound/postgres": "postgres",
}

// agentAllowedSelectors maps an allowlist key to the identifiers agent sources may reference through
// it. Anything else is a violation. The four read use cases (the MCP charter, ADR 0009) and the
// list-query error; the not-found error; the pool, migrations and the two case/policy repos the read
// use cases are built over. The postgres allowlist keeps NewPool: this repo has no read-only pool
// constructor yet (a separate change); the repos' write methods are refused by the verb half.
var agentAllowedSelectors = map[string][]string{
	"usecases": {"ListOpenExceptions", "GetException", "ListExceptionPolicies", "GetSLAReport", "ErrInvalidListQuery"},
	"ports":    {"ErrNotFound"},
	"postgres": {"MigrationsURL", "RunMigrations", "NewPool", "NewCaseRepo", "NewPolicyRepo"},
}

// agentForbiddenImports are import-path fragments no agent source may use, and why. A fragment
// starting with "/" is relative to modulePath. Keep the fleet entries; add the service's own
// write-side and device-side adapters (an outbound REST client of another context, MQTT, ...).
var agentForbiddenImports = map[string]string{
	"a2a-go/v2/a2aclient":                "an outbound A2A client (the agent only answers)",
	"github.com/segmentio/kafka-go":      "Kafka: a read-only surface never dials Kafka",
	"/internal/adapters/outbound/kafka":  "Kafka: a read-only surface publishes no fact",
	"/internal/adapters/inbound/kafka":   "the fact consumers: only cmd/api consumes",
	"/internal/adapters/outbound/outbox": "the outbox relay: a read-only surface never publishes",
	"/internal/adapters/outbound/memory": "the in-memory test doubles (real Postgres only at runtime)",
	"/internal/adapters/inbound/http":    "the REST adapter (the inbound adapters are siblings)",
	// This service's own write side: the analytics read model the projector writes.
	"/internal/adapters/outbound/analyticsstore": "the analytics store the projector writes (cmd/exceptions-projector, cmd/exceptions-reports)",
}

// agentRepoWriteVerbs are this service's write METHODS on values an agent source can reach, refused
// on any value like the fleet verbs: the outbox publisher (ports.EventPublisher), the consumed-fact
// claim (ports.ProcessedEvents) and the unit of work (ports.UnitOfWork.Do). The repos' Save is a
// fleet verb already. The write use cases are types (usecases.TriageException, ...) the usecases
// allowlist refuses; their Execute is shared with the read use cases, so it is not a verb.
var agentRepoWriteVerbs = []string{"PublishCaseEvents", "PublishPolicyDeclared", "Claim", "Do"}

// agentWriteVerbAllowances maps an exact selector expression (types.ExprString) to why it is not a
// write. Empty unless a real read needs a verb name; say why in the value.
var agentWriteVerbAllowances = map[string]string{}
