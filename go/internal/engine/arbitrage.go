// Package engine implements the arbitrage decision loop.
// This is the HOT PATH – every nanosecond counts here.
package engine

import (
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/models"
)

var (
	one   = decimal.NewFromInt(1)
	zero  = decimal.NewFromInt(0)
	oneCent = decimal.NewFromFloat(0.01)
	minStake = decimal.NewFromFloat(1.00)
)

// ArbitrageEngine scans all assets and emits ExecutionRequests.
type ArbitrageEngine struct {
	state   *models.SharedState
	cfg     *config.AppConfig
	execCh  chan<- models.ExecutionRequest
	resCh   <-chan models.ExecutionResult
	metrics models.EngineMetrics
	firedWindow [models.AssetCount]bool
	logger  *zap.Logger
}

// New creates an ArbitrageEngine.
func New(
	state *models.SharedState,
	cfg *config.AppConfig,
	execCh chan<- models.ExecutionRequest,
	resCh <-chan models.ExecutionResult,
	logger *zap.Logger,
) *ArbitrageEngine {
	return &ArbitrageEngine{
		state:  state,
		cfg:    cfg,
		execCh: execCh,
		resCh:  resCh,
		logger: logger,
	}
}

// Run starts the decision loop. It also drains the result channel.
// Call from a goroutine. Exits when done is closed.
func (e *ArbitrageEngine) Run(done <-chan struct{}) {
	e.state.SetSniperState(models.StateArmed)
	e.state.SetStatus("SNIPER_ARMED_MULTI_ASSET")

	// Result drain goroutine.
	go func() {
		for {
			select {
			case <-done:
				return
			case res := <-e.resCh:
				e.onResult(res)
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			e.state.SetSniperState(models.StateStopped)
			return
		case <-ticker.C:
			if e.state.IsKilled() {
				e.state.SetSniperState(models.StateStopped)
				return
			}
			start := time.Now()
			executed := e.scanAllAssets()
			ms := float64(time.Since(start).Nanoseconds()) / 1e6
			e.metrics.Record(ms, executed)
		}
	}
}

func (e *ArbitrageEngine) scanAllAssets() bool {
	hasBooksAt := false
	for i := models.SniperAsset(0); i < models.AssetCount; i++ {
		if e.state.GetBook(i) != nil {
			hasBooksAt = true
			break
		}
	}
	if !hasBooksAt {
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

func (e *ArbitrageEngine) evaluateAsset(asset models.SniperAsset) bool {
	book := e.state.GetBook(asset)
	if book == nil {
		return false
	}

	nowSec := time.Now().Unix()
	remaining := book.MarketCloseTs - nowSec
	if remaining <= 0 {
		e.firedWindow[asset] = false
		return false
	}
	if remaining >= int64(e.cfg.Runtime.CloseWindowSec) {
		return false
	}
	if e.firedWindow[asset] {
		return false
	}
	if e.state.IsInflight(asset) {
		return false
	}

	yesPrice := book.YesPrice
	strike := book.StrikePrice
	markPrice := e.state.GetPrice(asset)

	if markPrice <= strike {
		return false
	}
	if yesPrice.GreaterThanOrEqual(e.cfg.Runtime.YesPriceMax) {
		return false
	}

	betSize := e.computeStake()
	if betSize.LessThanOrEqual(zero) {
		e.state.SetStatus("INSUFFICIENT_USDC")
		return false
	}

	signal := models.SniperSignal{
		Asset:       asset,
		MarketID:    book.MarketID,
		ConditionID: book.ConditionID,
		YesPrice:    yesPrice,
		StrikePrice: strike,
		MarkPrice:   markPrice,
		BetSizeUSD:  betSize,
		SignalNs:    models.NowNs(),
	}

	e.state.SetSniperState(models.StateFiring)
	e.state.SetStatus("TRIGGER_FIRED_MULTI_ASSET")
	e.state.SetInflight(asset)

	// Non-blocking send – drop if executor is full.
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

func (e *ArbitrageEngine) computeStake() decimal.Decimal {
	balance := e.state.GetWalletBalanceUSD()
	if balance.LessThan(minStake) {
		return zero
	}
	stake := balance.Mul(e.cfg.Runtime.StakeUsage).Round(2)
	if stake.LessThan(minStake) {
		return zero
	}
	return stake
}

func (e *ArbitrageEngine) onResult(res models.ExecutionResult) {
	pnlCents := res.PnlDeltaUSD.Mul(decimal.NewFromInt(100)).IntPart()
	e.state.AddCumulativePnlCents(pnlCents)
	e.state.AddWalletBalanceCents(pnlCents)
	e.state.ClearInflight(res.Asset)

	if e.state.GetCumulativePnlUSD().LessThanOrEqual(e.cfg.Runtime.KillSwitchPnlUSD) {
		e.state.SetKillSwitch(true)
		e.state.SetStatus("KILL_SWITCH_TRIGGERED")
	}
}

// Metrics returns a snapshot of engine metrics.
func (e *ArbitrageEngine) Metrics() models.EngineMetrics { return e.metrics }
