package architecture

import (
	"slices"
	"testing"
)

// Hardened agent read-only sensor (fleet audit 2026-11-21): this repo's own
// sensor was TestA2AAdapterReferencesNoWriteUseCase — a `usecases.<Name>`
// regex over the A2A adapter only, plus the a2aclient import regex — and the
// MCP server had no code-level read-only check at all (only the tool-name
// verb sensor). Every fixture below passed it:
//
//	G1 a qualifier was matched by its LOCAL name, so an alias laundered a write seam;
//	G2 a dot or aliased import of a guarded package was not refused;
//	G3 a write METHOD on a value an allowlisted constructor returned got through;
//	G4 the pgx pool's raw-SQL methods (Query, QueryRow, Exec, Begin, Acquire ...) got through;
//	G5 a raw driver import (pgx, database/sql) was not refused;
//
// plus the cmd/ composition roots, the postgres write adapters, Kafka, the
// outbox and the analytics store were not guarded at all. The fixtures use
// only agentSourceViolations (the template's entry point) and pin the verb
// list from the test side.

// agentWriteVerbsWant pins agentWriteVerbs from the test side: the fleet
// repo/pool write methods and raw-SQL entry points, and this module's own
// (the outbox publisher, the consumed-fact claim, the unit of work).
var agentWriteVerbsWant = []string{"Save", "Insert", "Update", "Delete", "Lock", "Purge", "Append", "Enqueue",
	"Exec", "Begin", "BeginTx", "CopyFrom", "SendBatch",
	"Query", "QueryRow", "Acquire", "AcquireFunc", "AcquireAllIdle", "Conn", "PgConn", "ExecParams", "Hijack",
	"PublishCaseEvents", "PublishPolicyDeclared", "Claim", "Do"}

const (
	ucImport = "\"" + modulePath + "/internal/application/usecases\""
	pgImport = "\"" + modulePath + "/internal/adapters/outbound/postgres\""
	ptImport = "\"" + modulePath + "/internal/application/ports\""
)

// agentHardeningBad is one source per gap, in this repo's names; each must be
// flagged on BOTH surfaces.
func agentHardeningBad() map[string]string {
	pg := "import " + pgImport + "\n"
	bad := map[string]string{
		// G1: the allowlist resolves a qualifier through the file's imports.
		"an aliased triage use case":    "package x\nimport uc " + ucImport + "\nvar _ = &uc.TriageException{}\n",
		"an aliased assign use case":    "package x\nimport uc " + ucImport + "\nvar _ = &uc.AssignException{}\n",
		"an aliased resolve use case":   "package x\nimport uc " + ucImport + "\nvar _ = &uc.ResolveException{}\n",
		"an aliased declare use case":   "package x\nimport uc " + ucImport + "\nvar _ = &uc.DeclareExceptionPolicy{}\n",
		"an aliased unit of work":       "package x\nimport p " + pgImport + "\nvar _ = p.NewUnitOfWork\n",
		"an aliased outbox publisher":   "package x\nimport p " + pgImport + "\nvar _ = p.NewOutboxPublisher\n",
		"an aliased idempotency store":  "package x\nimport p " + pgImport + "\nvar _ = p.NewIdempotencyStore\n",
		"an aliased unit-of-work port":  "package x\nimport pt " + ptImport + "\nvar _ pt.UnitOfWork\n",
		"an aliased event publisher":    "package x\nimport pt " + ptImport + "\nvar _ pt.EventPublisher\n",
		"a write use case on mcp/a2a":   "package x\nimport " + ucImport + "\nvar _ = &usecases.TriageException{}\n",
		"a write use case in cmd/":      "package x\nimport " + ucImport + "\nvar _ = &usecases.ResolveException{}\n",
		"the postgres unit of work":     "package x\n" + pg + "var _ = postgres.NewUnitOfWork\n",
		"the postgres outbox publisher": "package x\n" + pg + "var _ = postgres.NewOutboxPublisher\n",
		"the idempotency store":         "package x\n" + pg + "var _ = postgres.NewIdempotencyStore\n",
		"the processed-events claim":    "package x\n" + pg + "var _ = postgres.NewProcessedEvents\n",
		// G2: a dot or aliased import of a guarded package is itself refused.
		"an aliased guarded import":  "package x\nimport uc " + ucImport + "\nvar _ = uc.GetException{}\n",
		"a dot import of usecases":   "package x\nimport . " + ucImport + "\nvar _ = TriageException{}\n",
		"a dot import of postgres":   "package x\nimport . " + pgImport + "\nvar _ = NewUnitOfWork\n",
		"a dot import of ports":      "package x\nimport . " + ptImport + "\nvar _ UnitOfWork\n",
		"an aliased postgres import": "package x\nimport store " + pgImport + "\nvar _ = store.NewCaseRepo\n",
		// G3: a write method on a value an allowlisted constructor returned.
		"Save on a constructed case repo":   "package x\n" + pg + "func f() { _ = postgres.NewCaseRepo(nil).Save(ctx, c) }\n",
		"Save on a policy repo variable":    "package x\n" + pg + "func f() { r := postgres.NewPolicyRepo(nil); _ = r.Save(ctx, p) }\n",
		"Save through a read use case":      "package x\nimport " + ucImport + "\nfunc f(g *usecases.GetException) { _ = g.Cases.Save(ctx, c) }\n",
		"a method value of Save":            "package x\n" + pg + "func f() { save := postgres.NewCaseRepo(nil).Save; _ = save }\n",
		"a method expression of Save":       "package x\n" + pg + "var _ = (*postgres.CaseRepo).Save\n",
		"a unit of work through a field":    "package x\nfunc (d deps) f() { _ = d.uow.Do(ctx, fn) }\n",
		"an event published through a dep":  "package x\nfunc (d deps) f() { _ = d.events.PublishCaseEvents(ctx) }\n",
		"a policy fact through a dep":       "package x\nfunc (d deps) f() { _ = d.events.PublishPolicyDeclared(ctx, decl) }\n",
		"a consumed-fact claim through dep": "package x\nfunc (d deps) f() { _, _ = d.seen.Claim(ctx, id) }\n",
		// G4: raw SQL through the pool.
		"Exec on the pool":              "package x\n" + pg + "func f() { p, _ := postgres.NewPool(ctx, dsn); _, _ = p.Exec(ctx, \"DELETE FROM exception_cases\") }\n",
		"raw SQL through pool.Query":    "package x\n" + pg + "func f() { p, _ := postgres.NewPool(ctx, dsn); _, _ = p.Query(ctx, \"DELETE FROM exception_cases\") }\n",
		"raw SQL through pool.QueryRow": "package x\n" + pg + "func f() { p, _ := postgres.NewPool(ctx, dsn); _ = p.QueryRow(ctx, \"UPDATE exception_cases SET status = 'RESOLVED' RETURNING 1\") }\n",
		"a transaction on the pool":     "package x\n" + pg + "func f() { p, _ := postgres.NewPool(ctx, dsn); _, _ = p.Begin(ctx) }\n",
		"a pooled connection":           "package x\n" + pg + "func f() { p, _ := postgres.NewPool(ctx, dsn); c, _ := p.Acquire(ctx); _ = c.Conn().PgConn().ExecParams(ctx, \"DELETE FROM outbox\", nil, nil, nil, nil) }\n",
		// G5: a raw driver import.
		"a pgxpool import":      "package x\nimport _ \"github.com/jackc/pgx/v5/pgxpool\"\n",
		"a pgx root import":     "package x\nimport _ \"github.com/jackc/pgx/v5\"\n",
		"a database/sql import": "package x\nimport _ \"database/sql\"\n",
		// The rest of the write side, never guarded before.
		"the Kafka driver":         "package x\nimport _ \"github.com/segmentio/kafka-go\"\n",
		"the Kafka publisher":      "package x\nimport _ \"" + modulePath + "/internal/adapters/outbound/kafka\"\n",
		"the fact consumers":       "package x\nimport _ \"" + modulePath + "/internal/adapters/inbound/kafka\"\n",
		"the outbox relay":         "package x\nimport _ \"" + modulePath + "/internal/adapters/outbound/outbox\"\n",
		"the analytics store":      "package x\nimport _ \"" + modulePath + "/internal/adapters/outbound/analyticsstore\"\n",
		"the memory doubles":       "package x\nimport _ \"" + modulePath + "/internal/adapters/outbound/memory\"\n",
		"the REST adapter":         "package x\nimport _ \"" + modulePath + "/internal/adapters/inbound/http\"\n",
		"an outbound A2A client":   "package x\nimport _ \"github.com/a2aproject/a2a-go/v2/a2aclient\"\n",
		"an unlisted use case too": "package x\nimport " + ucImport + "\nvar _ = usecases.ConsumeFact\n",
	}
	for _, verb := range agentWriteVerbsWant {
		bad["the write verb "+verb] = "package x\nfunc f(v repo) { _ = v." + verb + " }\n"
	}
	return bad
}

func TestAgentReadOnlySensorClosesTheLocalSensorsGaps(t *testing.T) {
	for _, s := range agentSurfaces {
		for name, src := range agentHardeningBad() {
			if got, err := agentSourceViolations(s, "fixture.go", src); err != nil || len(got) == 0 {
				t.Errorf("%s surface: the sensor does not flag %s (violations=%v, err=%v)", s.name, name, got, err)
			}
		}
	}
}

// The real wiring shapes of cmd/mcp and cmd/a2a stay allowed: the aliased
// sibling-free adapter import, the pool's Close/Ping, the case/policy repos
// handed to the four read use cases and their Execute, and a guarded local
// NAME bound to another import path (the old regex flagged it).
func TestAgentReadOnlySensorAcceptsThisRepositorysReadWiring(t *testing.T) {
	wiring := "package x\nimport (\n\t\"slices\"\n\tinboundmcp \"" + modulePath + "/internal/adapters/inbound/mcp\"\n" +
		"\t" + pgImport + "\n\t\"" + modulePath + "/internal/adapters/outbound/system\"\n\t" + ucImport + "\n\t" + ptImport + "\n)\n" +
		"func f() { pool, _ := postgres.NewPool(ctx, dsn); defer pool.Close(); _ = pool.Ping(ctx)\n" +
		"_ = postgres.RunMigrations(postgres.MigrationsURL(getenv))\n" +
		"cases, policies := postgres.NewCaseRepo(pool), postgres.NewPolicyRepo(pool)\n" +
		"_ = inboundmcp.UseCases{List: &usecases.ListOpenExceptions{Cases: cases}, Get: &usecases.GetException{Cases: cases},\n" +
		"Policies: &usecases.ListExceptionPolicies{Policies: policies}, SLAReport: &usecases.GetSLAReport{Cases: cases, Clock: system.Clock{}}}\n" +
		"_, _ = (&usecases.GetException{Cases: cases}).Execute(ctx, id); _ = errors.Is(err, ports.ErrNotFound); _ = usecases.ErrInvalidListQuery\n" +
		"s := slices.Insert(xs, 0, 1); _ = slices.Delete(s, 0, 1) }\n"
	elsewhere := "package x\nimport usecases \"example.com/elsewhere/usecases\"\nvar _ = usecases.TriageException{}\n"
	mcp, a2a := agentSurfaces[0], agentSurfaces[1]
	for name, c := range map[string]struct {
		s   agentSurface
		src string
	}{"cmd/mcp's read wiring": {mcp, wiring}, "a usecases name bound to another path": {a2a, elsewhere}} {
		if got, err := agentSourceViolations(c.s, "fixture.go", c.src); err != nil || len(got) != 0 {
			t.Errorf("%s surface: false positive on %s: %v (err=%v)", c.s.name, name, got, err)
		}
	}
}

func TestAgentWriteVerbsArePinned(t *testing.T) {
	got := agentWriteVerbs()
	for _, verb := range agentWriteVerbsWant {
		if !slices.Contains(got, verb) {
			t.Errorf("agentWriteVerbs() lacks %s", verb)
		}
	}
}
