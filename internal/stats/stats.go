package stats

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Constants describing default latency buckets (in microseconds).
var defaultLatencyBuckets = []time.Duration{
	100 * time.Microsecond,
	500 * time.Microsecond,
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

// Collector keeps runtime metrics that can be exported.
type Collector struct {
	latencyBuckets []time.Duration
	bucketCounts   []atomic.Uint64

	total      atomic.Uint64
	success    atomic.Uint64
	failures   atomic.Uint64
	inflight   atomic.Uint64
	bytesSent  atomic.Uint64
	bytesRecv  atomic.Uint64
	firstStart atomic.Int64
	lastFinish atomic.Int64

	latencySum   atomic.Int64
	latencyCount atomic.Uint64
	latencyMin   atomic.Int64
	latencyMax   atomic.Int64

	mu sync.RWMutex
	// For percentile calculation we keep sample of latencies (bounded).
	latencySamples []int64
	sampleLimit    int

	backlog      atomic.Int64
	backpressure atomic.Uint32
	targetTPS    atomic.Uint64
	actualTPS    atomic.Uint64
	connections  atomic.Int64
}

// NewCollector initialises a Collector with default settings.
func NewCollector() *Collector {
	buckets := append([]time.Duration(nil), defaultLatencyBuckets...)
	counts := make([]atomic.Uint64, len(buckets)+1) // +1 for overflow
	c := &Collector{
		latencyBuckets: buckets,
		bucketCounts:   counts,
		latencyMin:     atomic.Int64{},
		latencyMax:     atomic.Int64{},
		sampleLimit:    2048,
	}
	c.latencyMin.Store(math.MaxInt64)
	c.firstStart.Store(0)
	c.lastFinish.Store(0)
	return c
}

// InflightAdd increments the in-flight counter.
func (c *Collector) InflightAdd(delta int) {
	if delta > 0 {
		c.inflight.Add(uint64(delta))
	} else if delta < 0 {
		c.inflight.Add(^uint64(-delta - 1))
	}
}

// Total returns the total number of observed requests.
func (c *Collector) Total() uint64 {
	return c.total.Load()
}

// Observe records the outcome of a request.
func (c *Collector) Observe(duration time.Duration, bytesSent, bytesRecv int64, success bool) {
	if success {
		c.success.Add(1)
	} else {
		c.failures.Add(1)
	}
	c.total.Add(1)
	if bytesSent > 0 {
		c.bytesSent.Add(uint64(bytesSent))
	}
	if bytesRecv > 0 {
		c.bytesRecv.Add(uint64(bytesRecv))
	}

	// Latency stats
	ns := duration.Nanoseconds()
	c.latencySum.Add(ns)
	c.latencyCount.Add(1)

	for idx, upper := range c.latencyBuckets {
		if duration <= upper {
			c.bucketCounts[idx].Add(1)
			goto minmax
		}
	}
	// overflow bucket
	c.bucketCounts[len(c.bucketCounts)-1].Add(1)

minmax:
	for {
		current := c.latencyMin.Load()
		if ns >= current {
			break
		}
		if c.latencyMin.CompareAndSwap(current, ns) {
			break
		}
	}
	for {
		current := c.latencyMax.Load()
		if ns <= current {
			break
		}
		if c.latencyMax.CompareAndSwap(current, ns) {
			break
		}
	}

	// Keep a bounded reservoir of samples.
	if c.sampleLimit == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.latencySamples) < c.sampleLimit {
		c.latencySamples = append(c.latencySamples, ns)
		return
	}
	// Reservoir sampling.
	idx := time.Now().UnixNano() % int64(c.sampleLimit)
	c.latencySamples[idx] = ns
}

// MarkStart records the start of a request.
func (c *Collector) MarkStart(ts time.Time) {
	unix := ts.UnixNano()
	for {
		current := c.firstStart.Load()
		if current != 0 && current <= unix {
			return
		}
		if c.firstStart.CompareAndSwap(current, unix) {
			return
		}
	}
}

// MarkFinish records the finish time of a request.
func (c *Collector) MarkFinish(ts time.Time) {
	unix := ts.UnixNano()
	for {
		current := c.lastFinish.Load()
		if current != 0 && current >= unix {
			return
		}
		if c.lastFinish.CompareAndSwap(current, unix) {
			return
		}
	}
}

// Snapshot is a read-only view of the counters.
type Snapshot struct {
	TotalRequests   uint64                   `json:"total_requests"`
	SuccessRequests uint64                   `json:"success_requests"`
	FailedRequests  uint64                   `json:"failed_requests"`
	Inflight        uint64                   `json:"inflight"`
	BytesSent       uint64                   `json:"bytes_sent"`
	BytesReceived   uint64                   `json:"bytes_received"`
	AvgLatency      time.Duration            `json:"avg_latency"`
	MinLatency      time.Duration            `json:"min_latency"`
	MaxLatency      time.Duration            `json:"max_latency"`
	Percentiles     map[string]time.Duration `json:"percentiles"`
	BucketCounts    map[string]uint64        `json:"latency_buckets"`
	FirstRequestAt  time.Time                `json:"first_request_at"`
	LastRequestAt   time.Time                `json:"last_request_at"`
	BacklogDepth    int                      `json:"backlog_depth"`
	Backpressure    bool                     `json:"backpressure"`
	TargetTPS       float64                  `json:"target_tps"`
	ActualTPS       float64                  `json:"actual_tps"`
	OpenConnections int                      `json:"open_connections"`
}

// Snapshot gathers a copy of current metrics.
func (c *Collector) Snapshot() Snapshot {
	snap := Snapshot{
		TotalRequests:   c.total.Load(),
		SuccessRequests: c.success.Load(),
		FailedRequests:  c.failures.Load(),
		Inflight:        c.inflight.Load(),
		BytesSent:       c.bytesSent.Load(),
		BytesReceived:   c.bytesRecv.Load(),
		Percentiles:     make(map[string]time.Duration),
		BucketCounts:    make(map[string]uint64),
		BacklogDepth:    int(c.backlog.Load()),
		Backpressure:    c.backpressure.Load() == 1,
		TargetTPS:       math.Float64frombits(c.targetTPS.Load()),
		ActualTPS:       math.Float64frombits(c.actualTPS.Load()),
		OpenConnections: int(c.connections.Load()),
	}
	count := c.latencyCount.Load()
	if count > 0 {
		sum := c.latencySum.Load()
		snap.AvgLatency = time.Duration(sum / int64(count))
		snap.MinLatency = time.Duration(c.latencyMin.Load())
		snap.MaxLatency = time.Duration(c.latencyMax.Load())
	}

	for idx, upper := range c.latencyBuckets {
		key := upper.String()
		snap.BucketCounts[key] = c.bucketCounts[idx].Load()
	}
	snap.BucketCounts["+Inf"] = c.bucketCounts[len(c.bucketCounts)-1].Load()

	samples := c.cloneSamples()
	if len(samples) > 0 {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		percentiles := []struct {
			label string
			pct   float64
		}{
			{"p50", 0.50},
			{"p90", 0.90},
			{"p95", 0.95},
			{"p99", 0.99},
		}
		for _, p := range percentiles {
			idx := int(math.Max(0, math.Min(float64(len(samples)-1), p.pct*float64(len(samples)-1))))
			snap.Percentiles[p.label] = time.Duration(samples[idx])
		}
	}

	if ts := c.firstStart.Load(); ts != 0 {
		snap.FirstRequestAt = time.Unix(0, ts)
	}
	if ts := c.lastFinish.Load(); ts != 0 {
		snap.LastRequestAt = time.Unix(0, ts)
	}

	return snap
}

func (c *Collector) cloneSamples() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]int64, len(c.latencySamples))
	copy(out, c.latencySamples)
	return out
}

// JSONHandler responds with a JSON snapshot of the metrics.
func (c *Collector) JSONHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snap := c.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode(snap)
	})
}

// SetBacklog records the current backlog depth.
func (c *Collector) SetBacklog(value int) {
	c.backlog.Store(int64(value))
}

// SetBackpressure marks whether the scheduler is experiencing backpressure.
func (c *Collector) SetBackpressure(flag bool) {
	if flag {
		c.backpressure.Store(1)
	} else {
		c.backpressure.Store(0)
	}
}

// SetRateStats records target and actual TPS measurements for the last interval.
func (c *Collector) SetRateStats(target, actual float64) {
	c.targetTPS.Store(math.Float64bits(target))
	c.actualTPS.Store(math.Float64bits(actual))
}

// SetConnections records the number of open HTTP connections.
func (c *Collector) SetConnections(value int) {
	c.connections.Store(int64(value))
}
