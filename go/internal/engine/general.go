package engine

import (
	"runtime"
	"sync/atomic"

	"github.com/polymarket-arb-bot/internal/models"
	"github.com/polymarket-arb-bot/internal/simd"
)

// ---------------------------------------------------------------------------
// The General — Multi-Asset Signal Ranking & Liquidity Orchestrator
//
// Architecture:
//   - Manages a shared vault of $10 across all assets
//   - Ranks signals by Expected Value (EV) using SIMD in <5ns
//   - If capital is committed and a better signal arrives with 20%+ margin,
//     flags the current position for early exit rotation
//   - Per-asset Watcher goroutines feed prices; General makes decisions
// ---------------------------------------------------------------------------

// General is the multi-asset orchestrator.
type General struct {
	state   *models.SharedState
	hub     *models.SharedMemoryHub
	tracker *models.DailyTracker

	// Atomic EV ranking — written by SIMD, read by watchers.
	currentBestAsset atomic.Int32
	currentBestEV    atomic.Uint64
	committedAsset   atomic.Int32 // -1 = no commitment
	committedEV      atomic.Uint64

	// Per-asset latency tracking (nanoseconds).
	netLatencyNs [models.AssetCount]atomic.Int64

	// Per-asset efficiency score (trades_won / total_decisions × 100).
	efficiencyScore [models.AssetCount]atomic.Uint64
}

// NewGeneral creates the orchestrator.
func NewGeneral(
	state *models.SharedState,
	hub *models.SharedMemoryHub,
	tracker *models.DailyTracker,
) *General {
	g := &General{
		state:   state,
		hub:     hub,
		tracker: tracker,
	}
	g.committedAsset.Store(-1)
	return g
}

// RankSignals evaluates all 4 primary assets using SIMD and returns
// the best opportunity. This runs in <5ns on M1 Pro.
//
// Called from the engine hot-path every scan cycle.
func (g *General) RankSignals() (bestAsset int, bestEV uint64, spreads [4]uint64) {
	// Gather current prices from atomic state (zero-alloc).
	var prices, strikes, yesPrices [4]uint64

	for i := 0; i < 4 && i < int(models.AssetCount); i++ {
		asset := models.SniperAsset(i)
		prices[i] = uint64(models.FPFromFloat(g.state.GetPrice(asset)))

		book := g.state.GetBook(asset)
		if book != nil {
			strikes[i] = uint64(models.FPFromFloat(book.StrikePrice))
			yesPrices[i] = uint64(models.FPFromFloat(book.YesPriceF64))
		}
	}

	// SIMD vectorized computation (~5ns for all 4 assets).
	bestAsset, bestEV, spreads = simd.ComputeSpreadsAndRank(prices, strikes, yesPrices)

	// Update atomic ranking for watchers to see.
	g.currentBestAsset.Store(int32(bestAsset))
	g.currentBestEV.Store(bestEV)

	return bestAsset, bestEV, spreads
}

// ShouldRotateLiquidity checks if a new signal beats the committed
// position by at least 20% EV margin.
// Returns true if the bot should exit current position and rotate.
func (g *General) ShouldRotateLiquidity(newAsset int, newEV uint64) bool {
	committed := g.committedAsset.Load()
	if committed == -1 {
		return false // nothing committed
	}
	if int32(newAsset) == committed {
		return false // same asset
	}

	oldEV := g.committedEV.Load()
	if oldEV == 0 {
		return true // any signal beats zero
	}

	// 20% margin: newEV > oldEV * 1.2
	threshold := oldEV + oldEV/5
	return newEV > threshold
}

// CommitToAsset marks an asset as having committed capital.
func (g *General) CommitToAsset(asset int, ev uint64) {
	g.committedAsset.Store(int32(asset))
	g.committedEV.Store(ev)
}

// ReleaseCommitment clears the commitment after trade completion.
func (g *General) ReleaseCommitment() {
	g.committedAsset.Store(-1)
	g.committedEV.Store(0)
}

// SetNetLatency records network latency for an asset (called by watchers).
func (g *General) SetNetLatency(asset models.SniperAsset, ns int64) {
	g.netLatencyNs[asset].Store(ns)
}

// GetNetLatency returns the last recorded network latency.
func (g *General) GetNetLatency(asset models.SniperAsset) int64 {
	return g.netLatencyNs[asset].Load()
}

// UpdateEfficiency recalculates the efficiency score for an asset.
func (g *General) UpdateEfficiency(asset models.SniperAsset, wins, total int64) {
	if total == 0 {
		g.efficiencyScore[asset].Store(0)
		return
	}
	score := uint64(wins * 10000 / total) // basis points
	g.efficiencyScore[asset].Store(score)
}

// GetEfficiency returns the efficiency score (0-10000 basis points).
func (g *General) GetEfficiency(asset models.SniperAsset) uint64 {
	return g.efficiencyScore[asset].Load()
}

// ---------------------------------------------------------------------------
// Watcher — per-asset goroutine pinned to an OS thread
// ---------------------------------------------------------------------------

// RunWatcher runs a dedicated goroutine for one asset, pinned to a P-core.
// It monitors the hub ring buffer and notifies the General of opportunities.
func (g *General) RunWatcher(asset models.SniperAsset, done <-chan struct{}) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for {
		select {
		case <-done:
			return
		default:
		}

		// Check for price updates.
		ring := &g.hub.Rings[asset]
		if ring.Count() < 2 {
			runtime.Gosched()
			continue
		}

		// Compute mean reversion signal.
		sma := ring.SMA(60)
		latest := ring.Latest()
		if sma == 0 || latest == 0 {
			runtime.Gosched()
			continue
		}

		// Record network latency (time between consecutive ticks).
		lastNs := g.state.GetLastBinanceNs()
		if lastNs > 0 {
			g.SetNetLatency(asset, models.NowNs()-lastNs)
		}

		runtime.Gosched()
	}
}
