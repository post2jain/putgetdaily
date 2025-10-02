package rate

import (
	"sync"
	"time"
)

// Limiter defines the behaviour of a rate controller.
type Limiter interface {
	Take(now time.Time) int
	Observe(actual float64)
	CurrentTarget() float64
}

// TokenBucket implements a basic token bucket limiter.
type TokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	last     time.Time
}

// NewTokenBucket constructs a limiter with the provided rate (tokens per second).
func NewTokenBucket(rate float64) *TokenBucket {
	if rate <= 0 {
		rate = 1
	}
	return &TokenBucket{
		rate:     rate,
		capacity: rate * 2,
		tokens:   rate,
		last:     time.Now(),
	}
}

// Take returns the number of whole tokens available at this instant.
func (t *TokenBucket) Take(now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(now)
	if t.tokens < 1 {
		return 0
	}
	allowed := int(t.tokens)
	t.tokens -= float64(allowed)
	return allowed
}

// Observe is a no-op for token bucket.
func (t *TokenBucket) Observe(actual float64) {}

// CurrentTarget returns the current rate.
func (t *TokenBucket) CurrentTarget() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rate
}

// SetRate adjusts the bucket rate (tokens per second).
func (t *TokenBucket) SetRate(rate float64) {
	if rate <= 0 {
		rate = 0.1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refill(time.Now())
	t.rate = rate
	t.capacity = rate * 2
	if t.tokens > t.capacity {
		t.tokens = t.capacity
	}
}

func (t *TokenBucket) refill(now time.Time) {
	if now.Before(t.last) {
		t.last = now
		return
	}
	elapsed := now.Sub(t.last).Seconds()
	if elapsed <= 0 {
		return
	}
	t.tokens += elapsed * t.rate
	if t.tokens > t.capacity {
		t.tokens = t.capacity
	}
	t.last = now
}

// PIDLimiter wraps a token bucket with PID control to adjust the target rate based on feedback.
type PIDLimiter struct {
	bucket   *TokenBucket
	baseRate float64
	kp       float64
	ki       float64
	kd       float64

	mu        sync.Mutex
	integral  float64
	prevError float64
	maxRate   float64
	minRate   float64
}

// NewPIDLimiter constructs a PID-controlled limiter.
func NewPIDLimiter(baseRate, kp, ki, kd float64) *PIDLimiter {
	if baseRate <= 0 {
		baseRate = 1
	}
	bucket := NewTokenBucket(baseRate)
	return &PIDLimiter{
		bucket:   bucket,
		baseRate: baseRate,
		kp:       kp,
		ki:       ki,
		kd:       kd,
		maxRate:  baseRate * 4,
		minRate:  0.1,
	}
}

// Take delegates to the underlying token bucket.
func (p *PIDLimiter) Take(now time.Time) int {
	return p.bucket.Take(now)
}

// Observe adjusts the rate based on the observed actual throughput.
func (p *PIDLimiter) Observe(actual float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	error := p.baseRate - actual
	p.integral += error
	derivative := error - p.prevError
	adjustment := p.kp*error + p.ki*p.integral + p.kd*derivative

	newRate := p.bucket.CurrentTarget() + adjustment
	if newRate > p.maxRate {
		newRate = p.maxRate
	}
	if newRate < p.minRate {
		newRate = p.minRate
	}
	p.bucket.SetRate(newRate)
	p.prevError = error
}

// CurrentTarget returns the bucket's current rate.
func (p *PIDLimiter) CurrentTarget() float64 {
	return p.bucket.CurrentTarget()
}
