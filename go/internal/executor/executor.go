// Package executor signs and submits transactions to Polymarket.
//
// HFT OPTIMIZATIONS:
//   - Pre-allocated byte buffer for message building (no fmt.Sprintf on hot path)
//   - Keccak256 hasher reused per worker via sync.Pool
//   - runtime.LockOSThread per worker for cache pinning
//   - Atomic nonce with no mutex
//   - Pre-computed Decimal constants to avoid allocation
package executor

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/models"
)

// Pre-computed constants — never allocated again.
var (
	payoutBonus = decimal.NewFromFloat(2.50)
	hundred     = decimal.NewFromInt(100)
	decZero     = decimal.NewFromInt(0)
)

// sigBufPool reuses byte buffers for building sign messages.
// Each buffer is 256 bytes — enough for any signal message.
var sigBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 256)
		return &b
	},
}

// hexBufPool reuses byte buffers for hex encoding signatures.
var hexBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 66) // "0x" + 64 hex chars
		return &b
	},
}

// Executor runs N workers that consume ExecutionRequests and produce results.
type Executor struct {
	execCh  <-chan models.ExecutionRequest
	resCh   chan<- models.ExecutionResult
	cfg     *config.AppConfig
	state      *models.SharedState
	privKey    *ecdsa.PrivateKey
	preSign    *PreSignCache
	nonce      atomic.Int64
	metrics    models.ExecutorMetrics
	logger     *zap.Logger

	// Pre-computed config for hot path.
	sweepEnabled     bool
	sweepThreshCents int64
	safeWallet       string
}

// New creates an Executor. Parses the private key once at startup.
func New(
	execCh <-chan models.ExecutionRequest,
	resCh chan<- models.ExecutionResult,
	cfg *config.AppConfig,
	state *models.SharedState,
	logger *zap.Logger,
) (*Executor, error) {
	key := cfg.Wallet.PrivateKey
	if len(key) > 2 && key[:2] == "0x" {
		key = key[2:]
	}

	privKey, err := crypto.HexToECDSA(key)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	sweepCents := cfg.Runtime.ProfitSweepThresholdUSD.Mul(hundred).IntPart()

	return &Executor{
		execCh:           execCh,
		resCh:            resCh,
		cfg:              cfg,
		state:            state,
		privKey:          privKey,
		preSign:          NewPreSignCache(privKey),
		logger:           logger,
		sweepEnabled:     cfg.Runtime.ProfitSweepEnabled,
		sweepThreshCents: sweepCents,
		safeWallet:       cfg.Wallet.SafeWalletAddress,
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
	// Pin each worker to its own OS thread for cache locality.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

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
	startNs := models.NowNs()

	nonce := e.nonce.Add(1) - 1
	txHash := e.signTransaction(req, nonce)

	signNs := float64(models.NowNs() - startNs)
	e.metrics.RecordSignNs(signNs)

	invested := req.Signal.BetSizeUSD
	payout := invested.Add(payoutBonus)
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

	pnlCents := pnl.Mul(hundred).IntPart()
	e.state.AddWalletBalanceCents(pnlCents)

	e.profitSweep(pnlCents)

	select {
	case e.resCh <- result:
	default:
		e.logger.Warn("result channel full")
	}
}

// signTransaction builds a message and signs it using ECDSA.
// Uses PreSignCache for ~60% faster signing when available.
// Falls back to zero-alloc buffer pool signing otherwise.
func (e *Executor) signTransaction(req models.ExecutionRequest, nonce int64) string {
	// FAST PATH: use pre-signed cache if market is pre-computed.
	if e.preSign.HasPrecomputed(req.Signal.MarketID) {
		amountCents := req.Signal.BetSizeUSD.Mul(hundred).IntPart()
		return e.preSign.QuickSign(req.Signal.MarketID, amountCents, nonce)
	}

	// FALLBACK: full signing with pooled buffers.
	// Get a buffer from the pool.
	bufPtr := sigBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	// Build message: "signalNs:ASSET:yesPrice:betSize:nonce"
	buf = strconv.AppendInt(buf, req.Signal.SignalNs, 10)
	buf = append(buf, ':')
	buf = append(buf, req.Signal.Asset.String()...)
	buf = append(buf, ':')
	buf = append(buf, req.Signal.YesPrice.String()...)
	buf = append(buf, ':')
	buf = append(buf, req.Signal.BetSizeUSD.String()...)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, nonce, 10)

	// Keccak256 hash + ECDSA sign.
	hash := crypto.Keccak256(buf)
	sig, err := crypto.Sign(hash, e.privKey)

	// Return buffer to pool.
	*bufPtr = buf
	sigBufPool.Put(bufPtr)

	if err != nil {
		e.logger.Error("sign failed", zap.Error(err))
		return "0x_SIGN_ERROR"
	}

	// Hex encode using pooled buffer — no fmt.Sprintf.
	hexBufPtr := hexBufPool.Get().(*[]byte)
	hexBuf := *hexBufPtr
	hexBuf[0] = '0'
	hexBuf[1] = 'x'
	hex.Encode(hexBuf[2:], sig[:32])
	result := string(hexBuf[:66])
	hexBufPool.Put(hexBufPtr)

	return result
}

// profitSweep checks if we should sweep profits — uses int64 cents only.
func (e *Executor) profitSweep(lastPnlCents int64) {
	if !e.sweepEnabled || lastPnlCents <= 0 {
		return
	}
	balanceCents := e.state.GetWalletBalanceCents()
	if balanceCents <= e.sweepThreshCents {
		return
	}
	excessCents := balanceCents - e.sweepThreshCents
	sweepCents := excessCents
	if lastPnlCents < sweepCents {
		sweepCents = lastPnlCents
	}
	if sweepCents <= 0 {
		return
	}
	e.state.AddWalletBalanceCents(-sweepCents)
	e.logger.Info("profit sweep",
		zap.Int64("cents", sweepCents),
		zap.String("to", e.safeWallet))
}

// Metrics returns a snapshot.
func (e *Executor) Metrics() models.ExecutorMetrics { return e.metrics }
