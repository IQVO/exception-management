package postgres

import (
	"github.com/claudioed/exception-management/internal/application/ports"
)

// Compile-time proof the pgx adapters implement the outbound ports.
var (
	_ ports.ExceptionCaseRepo   = (*CaseRepo)(nil)
	_ ports.ExceptionPolicyRepo = (*PolicyRepo)(nil)
	_ ports.EventPublisher      = (*OutboxPublisher)(nil)
	_ ports.UnitOfWork          = (*UnitOfWork)(nil)
)
