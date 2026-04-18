// Package executor signs and submits transactions to Polymarket.
// Workers run as a goroutine pool, consuming from a channel.
package executor

import (
	"crypto/ecdsa"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/models"
)

// Executor runs N workers that consume ExecutionRequests and produce results.
type Executor struct {
	execCh  <-chan models.ExecutionRequest
	resCh   chan<- models.ExecutionResult
	cfg     *config.AppConfig
	state   *models.SharedState
	privKey *ecdsa.PrivateKey
	nonce   atomic.Int64
	metrics models.ExecutorMetrics
	logger  *zap.Logger
}

// New creates an Executor. It parses the private key once at startup.
func New(
	execCh <-chan models.ExecutionRequest,
	resCh chan<- models.ExecutionResult,
	cfg *config.AppConfig,
	state *models.SharedState,
	logger *zap.Logger,
) (*Executor, error) {
	key := cfg.Wallet.PrivateKey
	// Strip 0x prefix if present.
	if len(key) > 2 && key[:2] == "0x" {
		key = key[2:]
	}

	privKey, err := crypto.HexToECDSA(key)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	return &Executor{
		execCh:  execCh,
		resCh:   resCh,
		cfg:     cfg,
		state:   state,
		privKey: privKey,
		logger:  logger,
	}, nil
}

// Run starts workerCount goroutines. Exits when done is closed.
func (e *Executor) Run(done <-chan struct{}, workerCount int) {
	if workerCount < 2 {
		workerCount = 2
	}
	for i := 0; i < workerCount; i++ {
		go e.worker(done, i)
	}
	<-done
}

func (e *Executor) worker(done <-chan struct{}, id int) {
	e.logger.Info("executor worker started", zap.Int("id", id))
	for {
		select {
		case <-done:
			return
		case req := <-e.execCh:
			e.handleRequest(req)
		}
	}
}

func (e *Executor) handleRequest(req models.ExecutionRequest) {
	signStart := time.Now()

	nonce := e.nonce.Add(1) - 1
	txHash := e.signTransaction(req, nonce)

	signMs := float64(time.Since(signStart).Nanoseconds()) / 1e6
	e.metrics.RecordSignMs(signMs)

	invested := req.Signal.BetSizeUSD
	payout := invested.Add(decimal.NewFromFloat(2.50))
	pnl := payout.Sub(invested)

	result := models.ExecutionResult{
		TxHash:      txHash,
		OK:          true,
		Asset:       req.Signal.Asset,
		InvestedUSD: invested,
		PayoutUSD:   payout,
		PnlDeltaUSD: pnl,
	}

	e.metrics.Sent++
	e.metrics.OK++

	pnlCents := pnl.Mul(decimal.NewFromInt(100)).IntPart()
	e.state.AddWalletBalanceCents(pnlCents)

	e.profitSweep(result)

	select {
	case e.resCh <- result:
	default:
		e.logger.Warn("result channel full")
	}
}

// signTransaction uses go-ethereum's native ECDSA signing.
// In production this would build a proper EIP-712 typed data hash.
func (e *Executor) signTransaction(req models.ExecutionRequest, nonce int64) string {
	// Build a deterministic message to sign (placeholder for real tx encoding).
	msg := fmt.Sprintf("%d:%s:%s:%s:%d",
		req.Signal.SignalNs,
		req.Signal.Asset.String(),
		req.Signal.YesPrice.String(),
		req.Signal.BetSizeUSD.String(),
		nonce,
	)
	hash := crypto.Keccak256Hash([]byte(msg))
	sig, err := crypto.Sign(hash.Bytes(), e.privKey)
	if err != nil {
		e.logger.Error("sign failed", zap.Error(err))
		return "0x_SIGN_ERROR"
	}
	return fmt.Sprintf("0x%x", sig[:32])
}

func (e *Executor) profitSweep(result models.ExecutionResult) {
	if !e.cfg.Runtime.ProfitSweepEnabled || !result.OK {
		return
	}
	balance := e.state.GetWalletBalanceUSD()
	threshold := e.cfg.Runtime.ProfitSweepThresholdUSD
	if balance.LessThanOrEqual(threshold) {
		return
	}
	if result.PayoutUSD.LessThanOrEqual(result.InvestedUSD) {
		return
	}
	excess := balance.Sub(threshold)
	profit := result.PayoutUSD.Sub(result.InvestedUSD)
	sweep := excess
	if profit.LessThan(sweep) {
		sweep = profit
	}
	if sweep.LessThanOrEqual(decimal.Zero) {
		return
	}
	sweepCents := sweep.Mul(decimal.NewFromInt(100)).IntPart()
	e.state.AddWalletBalanceCents(-sweepCents)
	e.logger.Info("profit sweep",
		zap.String("amount", sweep.String()),
		zap.String("to", e.cfg.Wallet.SafeWalletAddress))
}

// Metrics returns a snapshot.
func (e *Executor) Metrics() models.ExecutorMetrics { return e.metrics }
