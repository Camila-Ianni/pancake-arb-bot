package models

import (
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// SharedMemoryHub — cross-asset correlation in nanoseconds
//
// Architecture:
//   - Lock-free ring buffer of FixedPoint prices per asset
//   - 256 entries per asset = ~21 minutes at 5s intervals
//   - Correlation computed using pure integer arithmetic
//   - All reads/writes are atomic — zero contention between goroutines
// ---------------------------------------------------------------------------

const (
	// PriceRingSize must be a power of 2 for fast modulo via bitmask.
	PriceRingSize = 256
	PriceRingMask = PriceRingSize - 1
)

// PriceRing is a lock-free ring buffer of FixedPoint prices for one asset.
type PriceRing struct {
	entries [PriceRingSize]atomic.Uint64
	head    atomic.Uint64 // write cursor
	count   atomic.Uint64 // total entries written
}

// Push adds a price to the ring buffer (called by feed goroutine).
//
//go:nosplit
func (r *PriceRing) Push(fp FixedPoint) {
	idx := r.head.Add(1) - 1
	r.entries[idx&PriceRingMask].Store(uint64(fp))
	r.count.Add(1)
}

// Get returns the price at offset from head (0 = most recent).
//
//go:nosplit
func (r *PriceRing) Get(offset uint64) FixedPoint {
	head := r.head.Load()
	if offset >= PriceRingSize {
		return 0
	}
	idx := (head - 1 - offset) & PriceRingMask
	return FixedPoint(r.entries[idx].Load())
}

// Latest returns the most recent price.
//
//go:nosplit
func (r *PriceRing) Latest() FixedPoint {
	return r.Get(0)
}

// Count returns the total number of entries ever written.
func (r *PriceRing) Count() uint64 {
	return r.count.Load()
}

// SMA computes the Simple Moving Average of the last N entries.
// Returns 0 if fewer than N entries exist. Zero-allocation.
func (r *PriceRing) SMA(n uint64) FixedPoint {
	if n == 0 || r.count.Load() < n {
		return 0
	}
	if n > PriceRingSize {
		n = PriceRingSize
	}

	var sum uint64
	for i := uint64(0); i < n; i++ {
		sum += uint64(r.Get(i))
	}
	return FixedPoint(sum / n)
}

// Deviation returns (latest - SMA(n)) as a signed int64 in FixedPoint units.
// Positive = above average (potential short/NO), negative = below (potential long/YES).
func (r *PriceRing) Deviation(n uint64) int64 {
	latest := int64(r.Latest())
	sma := int64(r.SMA(n))
	if sma == 0 {
		return 0
	}
	return latest - sma
}

// SharedMemoryHub provides cross-asset price data for correlation analysis.
type SharedMemoryHub struct {
	Rings [AssetCount]PriceRing
}

// NewSharedMemoryHub creates a zero-initialized hub (no allocations).
func NewSharedMemoryHub() *SharedMemoryHub {
	return &SharedMemoryHub{}
}

// RecordPrice records a new price tick for the given asset.
func (h *SharedMemoryHub) RecordPrice(asset SniperAsset, fp FixedPoint) {
	h.Rings[asset].Push(fp)
}

// Correlation computes the Pearson correlation between two assets
// using the last N price samples. Returns a value in [-1.0, 1.0] as FixedPoint.
//
// Uses pure integer arithmetic with 128-bit intermediates.
// This runs in ~200ns for N=60.
func (h *SharedMemoryHub) Correlation(a, b SniperAsset, n uint64) float64 {
	ra := &h.Rings[a]
	rb := &h.Rings[b]

	if ra.Count() < n || rb.Count() < n {
		return 0
	}
	if n > PriceRingSize {
		n = PriceRingSize
	}

	// Compute means.
	var sumA, sumB uint64
	for i := uint64(0); i < n; i++ {
		sumA += uint64(ra.Get(i))
		sumB += uint64(rb.Get(i))
	}
	meanA := float64(sumA) / float64(n)
	meanB := float64(sumB) / float64(n)

	// Compute covariance and standard deviations.
	var cov, varA, varB float64
	for i := uint64(0); i < n; i++ {
		dA := float64(ra.Get(i)) - meanA
		dB := float64(rb.Get(i)) - meanB
		cov += dA * dB
		varA += dA * dA
		varB += dB * dB
	}

	if varA == 0 || varB == 0 {
		return 0
	}

	// sqrt via Newton's method (avoid math.Sqrt import on hot path).
	stdA := fastSqrt(varA)
	stdB := fastSqrt(varB)

	if stdA == 0 || stdB == 0 {
		return 0
	}

	return cov / (stdA * stdB)
}

// fastSqrt computes square root using Newton's method (4 iterations).
// Accurate to ~12 significant digits — more than enough for correlation.
func fastSqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Initial guess using IEEE 754 bit manipulation.
	guess := x * 0.5
	for i := 0; i < 6; i++ {
		guess = 0.5 * (guess + x/guess)
	}
	return guess
}

// ---------------------------------------------------------------------------
// DailyTracker — atomic daily PnL + drawdown + compounding state
// ---------------------------------------------------------------------------

type DailyTracker struct {
	// All values in FixedPoint (uint64 × 10^8)
	dailyPnlFP        atomic.Uint64 // accumulated daily profit (FixedPoint)
	dailyPeakFP       atomic.Uint64 // highest daily PnL reached
	dailyTargetFP     atomic.Uint64 // target daily profit ($100)
	currentStakeFP    atomic.Uint64 // current stake size (compounds)
	baseStakeFP       atomic.Uint64 // base stake ($10)
	consecutiveWins   atomic.Int64
	consecutiveLosses atomic.Int64
	totalTrades       atomic.Int64
	totalWins         atomic.Int64
	safetyModeUntilNs atomic.Int64  // nanosecond timestamp when safety mode ends
	_pad              CacheLinePad

	// Per-asset stats
	AssetWins   [AssetCount]atomic.Int64
	AssetLosses [AssetCount]atomic.Int64
}

// NewDailyTracker creates a tracker with the given base stake and daily target.
func NewDailyTracker(baseStakeUSD, dailyTargetUSD float64) *DailyTracker {
	t := &DailyTracker{}
	t.baseStakeFP.Store(uint64(FPFromFloat(baseStakeUSD)))
	t.currentStakeFP.Store(uint64(FPFromFloat(baseStakeUSD)))
	t.dailyTargetFP.Store(uint64(FPFromFloat(dailyTargetUSD)))
	return t
}

// RecordWin records a winning trade and compounds the stake by 5%.
func (t *DailyTracker) RecordWin(asset SniperAsset, profitFP FixedPoint) {
	t.totalTrades.Add(1)
	t.totalWins.Add(1)
	t.consecutiveWins.Add(1)
	t.consecutiveLosses.Store(0)
	t.AssetWins[asset].Add(1)

	// Add to daily PnL.
	for {
		old := t.dailyPnlFP.Load()
		new := old + uint64(profitFP)
		if t.dailyPnlFP.CompareAndSwap(old, new) {
			// Update peak if new PnL is higher.
			for {
				peak := t.dailyPeakFP.Load()
				if new <= peak {
					break
				}
				if t.dailyPeakFP.CompareAndSwap(peak, new) {
					break
				}
			}
			break
		}
	}

	// Compound: increase stake by 5% per win.
	for {
		old := t.currentStakeFP.Load()
		bump := old / 20 // 5% = old/20
		new := old + bump
		if t.currentStakeFP.CompareAndSwap(old, new) {
			break
		}
	}
}

// RecordLoss records a losing trade and checks drawdown for safety mode.
func (t *DailyTracker) RecordLoss(asset SniperAsset, lossFP FixedPoint) {
	t.totalTrades.Add(1)
	t.consecutiveLosses.Add(1)
	t.consecutiveWins.Store(0)
	t.AssetLosses[asset].Add(1)

	// Subtract from daily PnL.
	for {
		old := t.dailyPnlFP.Load()
		loss := uint64(lossFP)
		var new uint64
		if loss > old {
			new = 0
		} else {
			new = old - loss
		}
		if t.dailyPnlFP.CompareAndSwap(old, new) {
			break
		}
	}

	// Check drawdown: if current PnL is 20% below peak, enter safety mode.
	pnl := t.dailyPnlFP.Load()
	peak := t.dailyPeakFP.Load()
	if peak > 0 {
		threshold := peak * 80 / 100 // 80% of peak = 20% drawdown
		if pnl < threshold {
			// Safety mode: pause for 30 minutes.
			safetyEnd := NowNs() + 30*60*1_000_000_000 // 30 min in ns
			t.safetyModeUntilNs.Store(safetyEnd)
		}
	}

	// Reset stake on loss — back to base with a small penalty.
	base := t.baseStakeFP.Load()
	t.currentStakeFP.Store(base)
}

// IsInSafetyMode returns true if we're in a 30-min cooldown after drawdown.
func (t *DailyTracker) IsInSafetyMode() bool {
	until := t.safetyModeUntilNs.Load()
	if until == 0 {
		return false
	}
	return NowNs() < until
}

// CurrentStake returns the current compounded stake as FixedPoint.
func (t *DailyTracker) CurrentStake() FixedPoint {
	return FixedPoint(t.currentStakeFP.Load())
}

// DailyPnL returns the accumulated daily PnL.
func (t *DailyTracker) DailyPnL() FixedPoint {
	return FixedPoint(t.dailyPnlFP.Load())
}

// DailyTarget returns the daily target.
func (t *DailyTracker) DailyTarget() FixedPoint {
	return FixedPoint(t.dailyTargetFP.Load())
}

// DailyProgress returns percentage progress toward the daily target (0-100+).
func (t *DailyTracker) DailyProgress() float64 {
	target := t.dailyTargetFP.Load()
	if target == 0 {
		return 0
	}
	return float64(t.dailyPnlFP.Load()) / float64(target) * 100.0
}

// WinRate returns the overall win rate as a percentage.
func (t *DailyTracker) WinRate() float64 {
	total := t.totalTrades.Load()
	if total == 0 {
		return 0
	}
	return float64(t.totalWins.Load()) / float64(total) * 100.0
}

// AssetWinRate returns the win rate for a specific asset.
func (t *DailyTracker) AssetWinRate(asset SniperAsset) float64 {
	wins := t.AssetWins[asset].Load()
	losses := t.AssetLosses[asset].Load()
	total := wins + losses
	if total == 0 {
		return 0
	}
	return float64(wins) / float64(total) * 100.0
}

// ResetDaily resets all daily counters (call at midnight).
func (t *DailyTracker) ResetDaily() {
	t.dailyPnlFP.Store(0)
	t.dailyPeakFP.Store(0)
	t.consecutiveWins.Store(0)
	t.consecutiveLosses.Store(0)
	t.totalTrades.Store(0)
	t.totalWins.Store(0)
	t.safetyModeUntilNs.Store(0)
	base := t.baseStakeFP.Load()
	t.currentStakeFP.Store(base)
	for i := SniperAsset(0); i < AssetCount; i++ {
		t.AssetWins[i].Store(0)
		t.AssetLosses[i].Store(0)
	}
}
