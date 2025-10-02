package controller

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"s3load/internal/config"
	"s3load/internal/keyspace"
	"s3load/internal/rate"
	"s3load/internal/retry"
	"s3load/internal/stats"
	"s3load/internal/worker"
	"s3load/internal/workload"
)

// Controller manages job scheduling and worker coordination.
type Controller struct {
	cfg       *config.Config
	logger    *slog.Logger
	generator *keyspace.Generator
	runner    *worker.Runner
	metrics   *stats.Collector
	workload  *workload.Selector
	limiter   rate.Limiter
	activeOps atomic.Int64
	concurrencyLimit atomic.Int64
	maxConcurrency   int
	baseConcurrency  int
}

// New constructs a Controller.
func New(cfg *config.Config, logger *slog.Logger, generator *keyspace.Generator, runner *worker.Runner, metrics *stats.Collector, limiter rate.Limiter, wl *workload.Selector) *Controller {
	rand.Seed(time.Now().UnixNano())
	ctrl := &Controller{
		cfg:       cfg,
		logger:    logger,
		generator: generator,
		runner:    runner,
		metrics:   metrics,
		limiter:   limiter,
		workload:  wl,
	}
	ctrl.baseConcurrency = cfg.Concurrency
	ctrl.maxConcurrency = cfg.MaxConcurrency
	ctrl.concurrencyLimit.Store(int64(cfg.Concurrency))
	return ctrl
}

// Run executes the workload until duration elapses.
func (c *Controller) Run(ctx context.Context) stats.Snapshot {
	var opsWG sync.WaitGroup
	start := time.Now()
	deadline := start.Add(c.cfg.Duration)
	tick := time.NewTicker(100 * time.Millisecond)
	secondTicker := time.NewTicker(time.Second)
	defer tick.Stop()
	defer secondTicker.Stop()

	lastTotal := c.metrics.Total()
	running := true
	totalScheduled := 0

	for running {
		select {
		case <-ctx.Done():
			running = false
	case <-tick.C:
		now := time.Now()
		if c.cfg.Requests > 0 && totalScheduled >= c.cfg.Requests {
			running = false
			continue
		}
		if c.cfg.Duration > 0 && now.After(deadline) {
			running = false
			continue
		}
		allowed := c.limiter.Take(now)
		pending := int(c.activeOps.Load())
		if allowed <= 0 {
			c.metrics.SetBacklog(pending)
			c.metrics.SetBackpressure(c.shouldBackpressure(pending))
			continue
		}
		if c.cfg.Requests > 0 {
			remaining := c.cfg.Requests - totalScheduled
			if remaining <= 0 {
				running = false
				continue
			}
			if allowed > remaining {
				allowed = remaining
			}
		}
		allowed = c.adjustAllowed(allowed, pending)
		if allowed <= 0 {
			c.metrics.SetBacklog(pending)
			c.metrics.SetBackpressure(c.shouldBackpressure(pending))
			continue
		}
		for i := 0; i < allowed; i++ {
			key := c.generator.Next()
			selection := workload.WeightedOperation{}
			if c.workload != nil {
				selection = c.workload.Pick()
				}
				op := selection.Operation
				if op == "" {
					op = c.cfg.Operation
				}
				var aux map[string]string
				if len(selection.Options) > 0 {
					aux = cloneMap(selection.Options)
				}
				item := worker.WorkItem{Operation: op, Key: key, Aux: aux}
				c.launchOperation(ctx, item, &opsWG)
			}
		totalScheduled += allowed
		backlog := int(c.activeOps.Load())
		c.metrics.SetBacklog(backlog)
		c.metrics.SetBackpressure(c.shouldBackpressure(backlog))
		if c.cfg.Requests > 0 && totalScheduled >= c.cfg.Requests {
			running = false
		}
	case <-secondTicker.C:
			total := c.metrics.Total()
			delta := total - lastTotal
			lastTotal = total
			actual := float64(delta)
			c.limiter.Observe(actual)
			c.metrics.SetRateStats(c.limiter.CurrentTarget(), actual)
			c.metrics.SetConnections(c.runner.OpenConnections())
		backlog := int(c.activeOps.Load())
		c.metrics.SetBacklog(backlog)
		c.metrics.SetBackpressure(c.shouldBackpressure(backlog))
	}
}

	done := make(chan struct{})
	go func() {
		opsWG.Wait()
		close(done)
	}()
	if c.cfg.ShutdownGrace > 0 {
		select {
		case <-done:
		case <-time.After(c.cfg.ShutdownGrace):
			if c.logger != nil {
				c.logger.Warn("shutdown grace period elapsed; continuing shutdown")
			}
		}
	} else {
		<-done
	}

	c.metrics.SetBackpressure(false)
	c.metrics.SetBacklog(int(c.activeOps.Load()))
	c.metrics.SetConnections(c.runner.OpenConnections())
	c.metrics.SetRateStats(c.limiter.CurrentTarget(), 0)
	return c.metrics.Snapshot()
}

func (c *Controller) launchOperation(ctx context.Context, item worker.WorkItem, wg *sync.WaitGroup) {
	maxAttempts := c.cfg.Retries + 1
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	c.activeOps.Add(1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer c.activeOps.Add(-1)
		attempt := 0
		backoff := c.cfg.RetryBackoff
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			attempt++
			start := time.Now()
			c.metrics.MarkStart(start)
			c.metrics.InflightAdd(1)
			result, err := c.runner.Process(ctx, item)
			c.metrics.InflightAdd(-1)
			finish := time.Now()
			c.metrics.MarkFinish(finish)
			c.metrics.Observe(finish.Sub(start), result.BytesSent, result.BytesReceived, err == nil)
			if err == nil {
				return
			}
			retryable := retry.IsRetryable(err)
			if !retryable || attempt >= maxAttempts {
				if c.logger != nil {
					msg := "non-retryable failure"
					if retryable {
						msg = "retry limit reached"
					}
					c.logger.Error(msg, slog.String("key", item.Key), slog.String("operation", item.Operation), slog.String("error", err.Error()), slog.Int("attempt", attempt))
				}
				return
			}
			if c.logger != nil {
				c.logger.Warn("operation retry", slog.String("key", item.Key), slog.String("operation", item.Operation), slog.Int("attempt", attempt), slog.String("error", err.Error()), slog.Duration("backoff", backoff))
			}
			sleepFor := jitter(backoff, c.cfg.RetryJitterPct)
			select {
			case <-time.After(sleepFor):
			case <-ctx.Done():
				return
			}
			backoff *= 2
		}
	}()
}

func (c *Controller) adjustAllowed(allowed, pending int) int {
	maxLimit := c.maxConcurrency
	if maxLimit > 0 && pending >= maxLimit {
		return 0
	}

	current := int(c.concurrencyLimit.Load())
	if current <= 0 {
		current = c.baseConcurrency
	}
	if maxLimit > 0 && current > maxLimit {
		current = maxLimit
	}

	desired := pending + allowed
	if desired < c.baseConcurrency {
		desired = c.baseConcurrency
	}
	if maxLimit > 0 && desired > maxLimit {
		desired = maxLimit
	}

	if desired > current {
		c.updateConcurrencyLimit(desired)
		current = desired
	}

	available := current - pending
	if maxLimit > 0 {
		remaining := maxLimit - pending
		if remaining < available {
			available = remaining
		}
	}
	if available < allowed {
		allowed = available
	}
	if allowed < 0 {
		return 0
	}
	return allowed
}

func (c *Controller) updateConcurrencyLimit(limit int) {
	if limit <= 0 {
		limit = c.baseConcurrency
	}
	if limit <= 0 {
		return
	}
	for {
		current := int(c.concurrencyLimit.Load())
		if current >= limit {
			return
		}
		if c.concurrencyLimit.CompareAndSwap(int64(current), int64(limit)) {
			if c.runner != nil {
				c.runner.SetConcurrency(limit)
			}
			return
		}
	}
}

func (c *Controller) shouldBackpressure(backlog int) bool {
	if backlog <= 0 {
		return false
	}
	maxLimit := c.maxConcurrency
	if maxLimit > 0 && backlog > maxLimit {
		return true
	}
	limit := int(c.concurrencyLimit.Load())
	if limit <= 0 {
		limit = c.baseConcurrency
	}
	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}
	if limit <= 0 {
		return false
	}
	return backlog > limit
}

func jitter(base time.Duration, pct float64) time.Duration {
	if base <= 0 || pct <= 0 {
		return base
	}
	delta := base.Seconds() * pct
	min := base.Seconds() - delta
	max := base.Seconds() + delta
	if min < 0 {
		min = 0
	}
	span := max - min
	add := rand.Float64() * span
	seconds := min + add
	return time.Duration(seconds * float64(time.Second))
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
