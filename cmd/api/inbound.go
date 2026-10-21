package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	kafkain "github.com/claudioed/exception-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/exception-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/exception-management/internal/adapters/outbound/system"
	"github.com/claudioed/exception-management/internal/application/ports"
	"github.com/claudioed/exception-management/internal/application/usecases"
)

// The inbound side of the composition root: the six fact consumers of
// .claude/rules/integration-events.md and the SLA sweep ticker. All of it
// runs on the real Postgres adapters — no memory doubles reach cmd/.

// inboundDrainTimeout bounds how long shutdown waits for the consumers and
// the sweeper to return after the context is cancelled.
const inboundDrainTimeout = 15 * time.Second

// inboundCloseTimeout bounds one consumer's Close (a reader Close can block
// on an in-flight fetch; shutdown must stay bounded).
const inboundCloseTimeout = 5 * time.Second

// inboundRuntime is the booted inbound side: the consumers, their done
// channels and the sweeper's stop channel.
type inboundRuntime struct {
	consumers []*kafkain.Consumer
	dones     []chan struct{}
	sweepDone chan struct{}
}

// buildConsumers wires the use cases over the Postgres adapters and the six
// consumers over the pinned group ids (each id from its own environment
// variable, never a literal — .claude/rules/integration-events.md).
func buildConsumers(cfg bootConfig, pool *pgxpool.Pool, uow ports.UnitOfWork, clock ports.Clock, logger *slog.Logger) []*kafkain.Consumer {
	cases := postgres.NewCaseRepo(pool)
	policies := postgres.NewPolicyRepo(pool)
	events := postgres.NewOutboxPublisher(pool)
	processed := postgres.NewProcessedEventRepo(pool)
	consume := &usecases.ConsumeFact{
		Processed: processed,
		OpenOrTouch: &usecases.OpenOrTouchException{
			Cases: cases, Policies: policies, Events: events,
			IDs: system.CaseIDs{}, Clock: clock, UnitOfWork: uow,
		},
		UnitOfWork: uow,
	}
	correlate := &usecases.ConsumeCorrelation{
		Processed: processed,
		AutoResolve: &usecases.AutoResolveOnCorrelation{
			Cases: cases, Policies: policies, Events: events, Clock: clock, UnitOfWork: uow,
		},
		UnitOfWork: uow,
	}
	g := cfg.consumerGroups
	return []*kafkain.Consumer{
		kafkain.NewFulfillmentExceptionsConsumer(cfg.brokers, g["FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP"], consume, logger),
		kafkain.NewWorkPlanningExceptionsConsumer(cfg.brokers, g["WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP"], consume, logger),
		kafkain.NewNetworkFulfillmentExceptionsConsumer(cfg.brokers, g["NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP"], consume, logger),
		kafkain.NewInventoryExceptionsConsumer(cfg.brokers, g["INVENTORY_EXCEPTIONS_CONSUMER_GROUP"], consume, logger),
		kafkain.NewReturnsExceptionsConsumer(cfg.brokers, g["RETURNS_EXCEPTIONS_CONSUMER_GROUP"], consume, logger),
		kafkain.NewOrderCorrelationConsumer(cfg.brokers, g["ORDER_CORRELATION_CONSUMER_GROUP"], correlate, logger),
	}
}

// startInbound boots the six consumers and the SLA sweep ticker. A consumer
// that returns anything other than ctx.Cancellation is a DEATH: it lands on
// errCh, run returns the error and the process exits non-zero — the pod
// restarts (the fleet rule; a silently dead consumer is a silently missed
// exception). The sweeper only ever logs: a failed sweep is retried by the
// next tick.
func startInbound(ctx context.Context, cfg bootConfig, pool *pgxpool.Pool, uow ports.UnitOfWork, clock ports.Clock, logger *slog.Logger, errCh chan<- error) *inboundRuntime {
	consumers := buildConsumers(cfg, pool, uow, clock, logger)
	rt := &inboundRuntime{consumers: consumers, sweepDone: make(chan struct{})}
	for _, c := range consumers {
		done := make(chan struct{})
		rt.dones = append(rt.dones, done)
		go func(c *kafkain.Consumer, done chan struct{}) {
			defer close(done)
			logger.Info("consumer started", "consumer", c.Name, "topic", c.Topic)
			// Run's loop only ever ENDS through an error (a returned nil
			// would mean it stopped fetching without one): any return that
			// is not the shutdown cancellation is a death.
			err := c.Run(ctx)
			if !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("the %s died: %w", c.Name, err)
				return
			}
			logger.Info("consumer stopped", "consumer", c.Name)
		}(c, done)
	}
	sweep := &usecases.SweepSLABreaches{
		Cases: postgres.NewCaseRepo(pool), Events: postgres.NewOutboxPublisher(pool),
		Clock: clock, UnitOfWork: uow,
	}
	interval := slaSweepInterval(os.Getenv("SLA_SWEEP_INTERVAL"), logger)
	go func() {
		defer close(rt.sweepDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		logger.Info("sla sweeper started", "interval", interval.String())
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				flagged, err := sweep.Execute(ctx)
				if err != nil {
					logger.Error("sla sweep failed; the next tick retries", "error", err)
					continue
				}
				if len(flagged) > 0 {
					logger.Warn("sla breaches escalated", "count", len(flagged))
				}
			}
		}
	}()
	return rt
}

// await waits (bounded) for every consumer and the sweeper to return.
func (rt *inboundRuntime) await(logger *slog.Logger) {
	deadline := time.After(inboundDrainTimeout)
	dones := append(append([]chan struct{}{}, rt.dones...), rt.sweepDone)
	for _, done := range dones {
		select {
		case <-done:
		case <-deadline:
			logger.Warn("the inbound side did not stop before the shutdown drain deadline")
			return
		}
	}
}

// close closes every consumer's reader and dead-letter writer, each bounded.
func (rt *inboundRuntime) close(logger *slog.Logger) {
	var wg sync.WaitGroup
	for _, c := range rt.consumers {
		wg.Add(1)
		go func(c *kafkain.Consumer) {
			defer wg.Done()
			done := make(chan error, 1)
			go func() { done <- c.Close() }()
			select {
			case err := <-done:
				if err != nil {
					logger.Error("consumer close failed", "consumer", c.Name, "error", err)
				}
			case <-time.After(inboundCloseTimeout):
				logger.Warn("consumer close timed out; reader left to GC", "consumer", c.Name, "timeout", inboundCloseTimeout.String())
			}
		}(c)
	}
	wg.Wait()
}
