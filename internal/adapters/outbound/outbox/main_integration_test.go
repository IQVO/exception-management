//go:build integration

package outbox_test

import (
	"os"
	"testing"

	// Every Postgres-touching integration test of this package boots its
	// database through internal/testing/pgtest, which drives the
	// testcontainers postgres module imported here — the package's shared
	// anchor for the TestPostgresIntegrationTestsUseTestcontainers sensor.
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/exception-management/internal/testing/pgtest"
)

// The infrastructure comes from testcontainers, never from the environment:
// internal/testing/pgtest boots ONE Postgres per test binary (lazily, on
// first use); TestMain only tears it down at the end. The Kafka-side
// integration proof of this package lives in relay_integration_test.go and
// boots its broker the same way.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}
