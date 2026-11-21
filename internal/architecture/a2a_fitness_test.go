package architecture

// The A2A inbound adapter's fitness rules (ADR 0007). It is the third driving
// adapter over the SAME application layer as REST and MCP, and it is a READ-ONLY
// delegation surface: it may serve, never dial, and never write.

import (
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// a2aAdapterDir is where the fleet keeps the A2A inbound adapter (relative to
// this package).
const a2aAdapterDir = "../adapters/inbound/a2a"

// TestA2AAdapterDependencyRule: the A2A adapter depends only on the
// application and domain layers (never on an outbound adapter, another
// inbound adapter or cmd), and nothing else in the codebase depends on it, so
// it stays an additive entrypoint cmd/a2a wires up and nothing else touches.
func TestA2AAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{{
				Package: "**.internal.adapters.inbound.a2a.**",
				ShouldOnlyDependsOn: &configuration.Dependencies{
					Internal: []string{
						"**.internal.adapters.inbound.a2a.**",
						"**.internal.application.**",
						"**.internal.domain.**",
					},
				},
			}},
		})
		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{{
				Package: "**.internal.**",
				ShouldNotDependsOn: &configuration.Dependencies{
					Internal: []string{"**.internal.adapters.inbound.a2a.**"},
				},
			}},
		})
		assertArchGoPasses(t, result)
	})
}

// The A2A adapter's code-level read-only rules (server only: no a2aclient
// import; no write use case, through any alias or dot import; no write method,
// raw SQL or driver; no sibling, Kafka, outbox or REST import) live in the
// fleet sensor agent_readonly_fitness_test.go (+ _methods_ and _config_),
// which replaced this file's local-name usecases.<Name> regex
// (TestA2AAdapterReferencesNoWriteUseCase) and a2aclient regex
// (TestA2AAdapterIsServerOnly): fleet audit 2026-11-21.
