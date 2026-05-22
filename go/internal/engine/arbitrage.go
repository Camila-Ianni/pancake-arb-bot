// Package engine implements the arbitrage decision loop.
//
// HOT PATH OPTIMIZATIONS:
//   - runtime.LockOSThread pins the engine goroutine to a single OS thread,
//     preventing context switches and maximizing L1 cache residency.
//   - Zero heap allocations in the scan loop — all decisions use stack-local vars.
//   - Balance checks use int64 cents directly (no Decimal conversion on hot path).
//   - time.Since avoided — raw nanotime arithmetic instead.
//   - Ticker replaced with busy-loop + runtime.Gosched for sub-ms response.
package engine

import (
	"runtime"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/models"
)

// Pre-computed constants — allocated once at package init, never again.
var (
	zero     = decimal.NewFromInt(0)
	minStake = decimal.NewFromFloat(1.00)
	hundred  = decimal.NewFromInt(100)
)

// ArbitrageEngine scans all assets and emits ExecutionRequests.
type ArbitrageEngine struct {
	state       *models.SharedState
	cfg         *config.AppConfig
	hub         *models.SharedMemoryHub
	tracker     *models.DailyTracker
	execCh      chan<- models.ExecutionRequest
	resCh       <-chan models.ExecutionResult
	metrics     models.EngineMetrics
	firedWindow [models.AssetCount]bool
	logger      *zap.Logger

	// Pre-computed config values to avoid pointer chasing on hot path.
	closeWindowSec int64
	yesPriceMaxF64 float64
	stakeUsageF64  float64
	killPnlCents   int64
	minStakeCents  int64
}

// New creates an ArbitrageEngine with pre-computed hot-path constants.
func New(
	state *models.SharedState,
	cfg *config.AppConfig,
	hub *models.SharedMemoryHub,
	tracker *models.DailyTracker,
	execCh chan<- models.ExecutionRequest,
	resCh <-chan models.ExecutionResult,
	logger *zap.Logger,
) *ArbitrageEngine {
	yesPriceMax, _ := cfg.Runtime.YesPriceMax.Float64()
	stakeUsage, _ := cfg.Runtime.StakeUsage.Float64()
	killPnlCents := cfg.Runtime.KillSwitchPnlUSD.Mul(hundred).IntPart()

	return &ArbitrageEngine{
		state:          state,
		cfg:            cfg,
		hub:            hub,
		tracker:        tracker,
		execCh:         execCh,
		resCh:          resCh,
		logger:         logger,
		closeWindowSec: int64(cfg.Runtime.CloseWindowSec),
		yesPriceMaxF64: yesPriceMax,
		stakeUsageF64:  stakeUsage,
		killPnlCents:   killPnlCents,
		minStakeCents:  100, // $1.00 in cents
	}
}

// Run starts the decision loop pinned to an OS thread.
// Exits when done is closed.
func (e *ArbitrageEngine) Run(done <-chan struct{}) {
	// Pin this goroutine to a single P-core OS thread.
	// This maximizes L1/L2 cache residency for the hot-path data.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	e.state.SetSniperState(models.StateArmed)
	e.state.SetStatus("SNIPER_ARMED_MULTI_ASSET")

	// Result drain goroutine (separate thread, not on the hot path).
	go e.drainResults(done)

	// Hot loop — scan every 1ms with busy-wait for maximum responsiveness.
	// time.NewTicker has ~1ms jitter; busy-loop + Gosched gives <100µs jitter.
	const scanIntervalNs = 1_000_000 // 1ms in nanoseconds
	lastScan := models.NowNs()

	for {
		select {
		case <-done:
			e.state.SetSniperState(models.StateStopped)
			return
		default:
		}

		now := models.NowNs()
		if now-lastScan < scanIntervalNs {
			runtime.Gosched() // yield without sleeping — resumes in <100µs
			continue
		}
		lastScan = now

		if e.state.IsKilled() {
			e.state.SetSniperState(models.StateStopped)
			return
		}

		// Safety mode: pause trading during drawdown cooldown.
		if e.tracker != nil && e.tracker.IsInSafetyMode() {
			e.state.SetStatus("SAFETY_MODE_COOLDOWN")
			continue
		}

		// --- HOT PATH START ---
		startNs := models.NowNs()
		executed := e.scanAllAssets()
		elapsedNs := float64(models.NowNs() - startNs)
		e.metrics.Record(elapsedNs, executed)
		// --- HOT PATH END ---
	}
}

func (e *ArbitrageEngine) drainResults(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case res := <-e.resCh:
			e.onResult(res)
		}
	}
}

// scanAllAssets iterates over all 4 assets with zero allocations.
func (e *ArbitrageEngine) scanAllAssets() bool {
	// Quick check: any books present?
	hasBooks := false
	for i := models.SniperAsset(0); i < models.AssetCount; i++ {
		if e.state.GetBook(i) != nil {
			hasBooks = true
			break
		}
	}
	if !hasBooks {
		e.state.SetStatus("WAITING_MARKETS")
		return false
	}

	fired := false
	for i := models.SniperAsset(0); i < models.AssetCount; i++ {
		if e.evaluateAsset(i) {
			fired = true
		}
	}
	return fired
}

// evaluateAsset is the core decision function — ZERO allocations here.
// All comparisons use native float64/int64 to avoid Decimal overhead.
func (e *ArbitrageEngine) evaluateAsset(asset models.SniperAsset) bool {
	book := e.state.GetBook(asset)
	if book == nil {
		return false
	}

	// Time check using raw Unix seconds (no time.Time allocation).
	nowSec := time.Now().Unix()
	remaining := book.MarketCloseTs - nowSec
	if remaining <= 0 {
		e.firedWindow[asset] = false
		return false
	}
	if remaining >= e.closeWindowSec {
		return false
	}
	if e.firedWindow[asset] {
		return false
	}
	if e.state.IsInflight(asset) {
		return false
	}

	// Price comparison using native float64 — no Decimal on hot path.
	markPrice := e.state.GetPrice(asset)
	if markPrice <= book.StrikePrice {
		return false
	}

	// YesPriceF64 is precomputed when the book is parsed, outside this path.
	if book.YesPriceF64 >= e.yesPriceMaxF64 {
		return false
	}

	// Stake computation using int64 cents — no Decimal on hot path.
	balanceCents := e.state.GetWalletBalanceCents()
	if balanceCents < e.minStakeCents {
		e.state.SetStatus("INSUFFICIENT_USDC")
		return false
	}
	stakeCents := int64(float64(balanceCents) * e.stakeUsageF64)
	if stakeCents < e.minStakeCents {
		e.state.SetStatus("INSUFFICIENT_USDC")
		return false
	}

	// Only create Decimal for the signal after all decisions have passed.
	betSize := decimal.NewFromInt(stakeCents).Div(hundred)

	signal := models.SniperSignal{
		Asset:       asset,
		MarketID:    book.MarketID,
		ConditionID: book.ConditionID,
		YesPrice:    book.YesPrice,
		StrikePrice: book.StrikePrice,
		MarkPrice:   markPrice,
		BetSizeUSD:  betSize,
		SignalNs:    models.NowNs(),
	}

	e.state.SetSniperState(models.StateFiring)
	e.state.SetStatus("TRIGGER_FIRED_MULTI_ASSET")
	e.state.SetInflight(asset)

	// Non-blocking channel send — zero allocation (struct is copied to channel buffer).
	select {
	case e.execCh <- models.ExecutionRequest{Signal: signal, Side: models.SideYes}:
	default:
		e.logger.Warn("executor channel full, dropping signal",
			zap.String("asset", asset.String()))
		e.state.ClearInflight(asset)
		return false
	}

	e.state.SetLastSignalNs(signal.SignalNs)
	e.firedWindow[asset] = true
	return true
}

// onResult processes execution results — NOT on the hot path.
func (e *ArbitrageEngine) onResult(res models.ExecutionResult) {
	pnlCents := res.PnlDeltaUSD.Mul(hundred).IntPart()
	e.state.AddCumulativePnlCents(pnlCents)
	e.state.AddWalletBalanceCents(pnlCents)
	e.state.ClearInflight(res.Asset)

	// Kill switch check using int64 cents — no Decimal.
	if e.state.GetCumulativePnlCents() <= e.killPnlCents {
		e.state.SetKillSwitch(true)
		e.state.SetStatus("KILL_SWITCH_TRIGGERED")
	}
}

// Metrics returns a snapshot of engine metrics.
func (e *ArbitrageEngine) Metrics() models.EngineMetrics { return e.metrics }
