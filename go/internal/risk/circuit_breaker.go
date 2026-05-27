// Package risk implements a lock-free circuit breaker.
package risk

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// State of the circuit breaker.
type CBState int32

const (
	CBClosed   CBState = iota // normal operation
	CBOpen                    // halted
	CBHalfOpen                // testing recovery
)

func (s CBState) String() string {
	switch s {
	case CBClosed:
		return "CLOSED"
	case CBOpen:
		return "OPEN"
	case CBHalfOpen:
		return "HALF_OPEN"
	default:
		return "?"
	}
}

// CircuitBreaker monitors risk metrics and blocks trading when tripped.
type CircuitBreaker struct {
	state              atomic.Int32
	triggeredAtNs      atomic.Int64
	consecutiveLosses  atomic.Int32
	failedTxs          atomic.Int32
	lastFeedLatencyMs  atomic.Int64 // stored as int64 micros for precision

	maxConsecLosses    int32
	maxFailedTxs       int32
	maxFeedLatencyMs   int64
	cooldownNs         int64

	logger *zap.Logger
}

// NewCircuitBreaker creates a circuit breaker with the given limits.
func NewCircuitBreaker(
	maxLosses, maxFailedTxs, maxLatencyMs, cooldownSec int,
	logger *zap.Logger,
) *CircuitBreaker {
	cb := &CircuitBreaker{
		maxConsecLosses:  int32(maxLosses),
		maxFailedTxs:     int32(maxFailedTxs),
		maxFeedLatencyMs: int64(maxLatencyMs),
		cooldownNs:       int64(cooldownSec) * int64(time.Second),
		logger:           logger,
	}
	cb.state.Store(int32(CBClosed))
	return cb
}

// IsTradingAllowed returns true if the circuit is closed or half-open.
func (cb *CircuitBreaker) IsTradingAllowed() bool {
	return CBState(cb.state.Load()) != CBOpen
}

// Trip opens the circuit breaker.
func (cb *CircuitBreaker) Trip(reason string) {
	cb.state.Store(int32(CBOpen))
	cb.triggeredAtNs.Store(time.Now().UnixNano())
	cb.logger.Warn("⚠️ CIRCUIT BREAKER TRIPPED", zap.String("reason", reason))
}

// TryReset checks cooldown and conditions, transitioning to HALF_OPEN if safe.
func (cb *CircuitBreaker) TryReset() bool {
	if CBState(cb.state.Load()) != CBOpen {
		return true
	}
	elapsed := time.Now().UnixNano() - cb.triggeredAtNs.Load()
	if elapsed < cb.cooldownNs {
		return false
	}
	if cb.consecutiveLosses.Load() < cb.maxConsecLosses &&
		cb.failedTxs.Load() < cb.maxFailedTxs {
		cb.state.Store(int32(CBHalfOpen))
		cb.logger.Info("circuit breaker → HALF_OPEN")
		return true
	}
	return false
}

// RecordLoss increments the consecutive loss counter and trips if needed.
func (cb *CircuitBreaker) RecordLoss() {
	n := cb.consecutiveLosses.Add(1)
	if n >= cb.maxConsecLosses {
		cb.Trip("max consecutive losses exceeded")
	}
}

// RecordWin resets the consecutive loss counter.
func (cb *CircuitBreaker) RecordWin() {
	cb.consecutiveLosses.Store(0)
	if CBState(cb.state.Load()) == CBHalfOpen {
		cb.state.Store(int32(CBClosed))
		cb.logger.Info("circuit breaker → CLOSED (recovered)")
	}
}

// RecordFailedTx increments the failed transaction counter.
func (cb *CircuitBreaker) RecordFailedTx() {
	n := cb.failedTxs.Add(1)
	if n >= cb.maxFailedTxs {
		cb.Trip("max failed transactions exceeded")
	}
}

// RecordFeedLatency checks feed latency and trips if too high.
func (cb *CircuitBreaker) RecordFeedLatency(latencyMs float64) {
	cb.lastFeedLatencyMs.Store(int64(latencyMs * 1000)) // store as micros
	if int64(latencyMs) > cb.maxFeedLatencyMs {
		if CBState(cb.state.Load()) == CBClosed {
			cb.Trip("feed latency exceeded")
		}
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CBState {
	return CBState(cb.state.Load())
}
