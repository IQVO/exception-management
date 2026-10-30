package architecture

// The A2A inbound adapter's fitness rules (ADR 0007). It is the third driving
// adapter over the SAME application layer as REST and MCP, and it is a READ-ONLY
// delegation surface: it may serve, never dial, and never write.

import (
	"os"
	"regexp"
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

// a2aClientImportRE matches an import of the A2A SDK's CLIENT package.
var a2aClientImportRE = regexp.MustCompile(`"github\.com/a2aproject/a2a-go/v2/a2aclient"`)

// TestA2AAdapterIsServerOnly: the adapter is inbound; the SDK client (an
// outbound A2A dial) may be imported by tests, never by the adapter's own
// sources. This context's first A2A role is to be asked.
func TestA2AAdapterIsServerOnly(t *testing.T) {
	for _, path := range goFilesUnder(t, a2aAdapterDir, false) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if a2aClientImportRE.Match(src) {
			t.Errorf("%s imports the A2A SDK client: the A2A adapter is a server, any outbound A2A call belongs in a separate outbound adapter under its own ADR", path)
		}
	}
}

// a2aUseCaseRefRE captures every usecases.<Name> reference.
var a2aUseCaseRefRE = regexp.MustCompile(`\busecases\.([A-Z][A-Za-z0-9]*)`)

// a2aReadUseCases are the only use cases the A2A adapter may name: the READ
// use cases the MCP adapter's charter allows (a read-only context surface is
// never widened by a new transport).
var a2aReadUseCases = map[string]bool{
	"ListOpenExceptions":    true,
	"GetException":          true,
	"ListExceptionPolicies": true,
	"GetSLAReport":          true,
}

// TestA2AAdapterReferencesNoWriteUseCase: the write gate, enforced on the
// sources. Triage, assign, resolve, declare and the fact consumers stay on
// REST and Kafka.
func TestA2AAdapterReferencesNoWriteUseCase(t *testing.T) {
	for _, path := range goFilesUnder(t, a2aAdapterDir, false) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, m := range a2aUseCaseRefRE.FindAllStringSubmatch(string(src), -1) {
			if !a2aReadUseCases[m[1]] {
				t.Errorf("%s references usecases.%s: the A2A surface is read-only (allowed: ListOpenExceptions, GetException, ListExceptionPolicies, GetSLAReport)", path, m[1])
			}
		}
	}
}
