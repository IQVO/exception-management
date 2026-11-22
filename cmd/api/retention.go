package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/repository"
)

// The idempotency-key retention (ADR 0015, addendum 2026-11-22): a stored
// answer lives for repository.IdempotencyTTL, then the purge deletes its row.
// The worker runs one pass at start and then every idempotencyPurgeInterval,
// in statements of at most idempotencyPurgeBatch rows so a pass never holds a
// long lock, however far behind it is (order-sourcing #84's design; the port
// shape is last-mile-delivery #86's PurgeExpired). The two values are fixed
// on purpose (like the sibling services): the age is
// repository.IdempotencyTTL, the rest is housekeeping, not configuration —
// expiry is enforced at read, so the interval only bounds how long a dead row
// stays on disk.
const (
	idempotencyPurgeInterval = time.Hour
	idempotencyPurgeBatch    = 500
)

// purgeIdempotencyKeys runs batches until one comes back short (nothing
// expired is left), a pass fails or ctx ends, and returns how many rows it
// deleted. A failed pass is retried by the next tick: the purge is
// idempotent, and a row that outlives its TTL is never replayed, so a
// failure costs only disk.
func purgeIdempotencyKeys(ctx context.Context, store ports.IdempotencyStore, batch int, logger *slog.Logger) int {
	total := 0
	for ctx.Err() == nil {
		n, err := store.PurgeExpired(ctx, batch)
		if err != nil {
			if ctx.Err() == nil {
				logger.ErrorContext(ctx, "idempotency purge failed; the next tick retries", "error", sanitizeErr(err), "purged", total)
			}
			return total
		}
		total += n
		if n < batch {
			return total
		}
	}
	return total
}

// startIdempotencyPurge runs the retention purge: one pass at once, then one
// every interval, until the returned channel's context (run's ctx) is
// cancelled; the channel closes when the loop has returned. Like the SLA
// sweep the purge only ever logs — a failed pass is not a death.
func startIdempotencyPurge(ctx context.Context, store ports.IdempotencyStore, logger *slog.Logger) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		purgeLoop(ctx, store, logger, idempotencyPurgeInterval, idempotencyPurgeBatch)
	}()
	return done
}

// purgeLoop purges at once and then every interval until ctx ends.
func purgeLoop(ctx context.Context, store ports.IdempotencyStore, logger *slog.Logger, interval time.Duration, batch int) {
	logger.Info("idempotency purge running", "interval", interval.String(), "ttl", repository.IdempotencyTTL.String(), "batch", batch)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if n := purgeIdempotencyKeys(ctx, store, batch, logger); n > 0 {
			logger.InfoContext(ctx, "idempotency purge done", "purged", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// awaitPurge waits (bounded) for the purge loop to return after its context
// is cancelled; a cancelled batch aborts mid-statement, so the wait is short.
func awaitPurge(logger *slog.Logger, done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(purgeDrainTimeout):
		logger.Warn("the idempotency purge did not stop before the shutdown drain deadline")
	}
}

// purgeDrainTimeout bounds how long shutdown waits for the purge loop.
const purgeDrainTimeout = 10 * time.Second
