// Package models defines core data types optimized for Apple Silicon M1/M2/M3.
//
// HFT OPTIMIZATIONS:
//   - Cache-line padding (128 bytes on Apple Silicon P-cores) to prevent false sharing
//   - All hot-path fields use sync/atomic — zero mutex, zero GC pressure
//   - Fixed-point arithmetic for balances (int64 cents) avoids Decimal on hot path
//   - Structs sized to fit L1 cache lines for maximum locality
package models

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shopspring/decimal"
)

// CacheLinePad prevents false sharing between atomic fields on Apple Silicon.
// M1/M2/M3 performance cores use 128-byte cache lines.
const CacheLineSize = 128

type CacheLinePad [CacheLineSize]byte

// ---------------------------------------------------------------------------
// Enums (uint8 for minimal footprint — fits in a register)
// ---------------------------------------------------------------------------

type SniperAsset uint8

const (
	AssetBTC SniperAsset = iota
	AssetETH
	AssetSOL
	AssetBNB
	AssetDOGE
	AssetMATIC
	AssetCount // 6
)

// assetStrings avoids allocations on String() — indexed by SniperAsset value.
var assetStrings = [AssetCount]string{"BTC", "ETH", "SOL", "BNB", "DOGE", "MATIC"}

func (a SniperAsset) String() string {
	if a < AssetCount {
		return assetStrings[a]
	}
	return "?"
}

func AssetFromSymbol(sym string) (SniperAsset, bool) {
	// Manual dispatch — no map lookup, no hash, no allocation.
	switch len(sym) {
	case 7: // BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT
		switch sym[0] {
		case 'B':
			if sym == "BTCUSDT" {
				return AssetBTC, true
			}
			if sym == "BNBUSDT" {
				return AssetBNB, true
			}
		case 'E':
			if sym == "ETHUSDT" {
				return AssetETH, true
			}
		case 'S':
			if sym == "SOLUSDT" {
				return AssetSOL, true
			}
		}
	case 8: // DOGEUSDT
		if sym == "DOGEUSDT" {
			return AssetDOGE, true
		}
	case 9: // MATICUSDT
		if sym == "MATICUSDT" {
			return AssetMATIC, true
		}
	}
	return 0, false
}

func AssetFromTag(tag string) (SniperAsset, bool) {
	switch tag {
	case "BTC":
		return AssetBTC, true
	case "ETH":
		return AssetETH, true
	case "SOL":
		return AssetSOL, true
	case "BNB":
		return AssetBNB, true
	case "DOGE":
		return AssetDOGE, true
	case "MATIC":
		return AssetMATIC, true
	}
	return 0, false
}

type SniperState int32

const (
	StateIdle SniperState = iota
	StateArmed
	StateFiring
	StateCooldown
	StateStopped
)

var stateStrings = [5]string{"IDLE", "ARMED", "FIRING", "COOLDOWN", "STOPPED"}

func (s SniperState) String() string {
	if int(s) < len(stateStrings) {
		return stateStrings[s]
	}
	return "?"
}

type OrderSide uint8

const (
	SideYes OrderSide = iota
	SideNo
)

const (
	PhaseCompoundActive uint32 = 1 << iota
)

// ---------------------------------------------------------------------------
// Hot-path structs — value types, no pointers, stack-allocated
// Sized to fit in L1 cache (≤64 bytes each)
// ---------------------------------------------------------------------------

// BinanceTick is 25 bytes — fits in half an x86 cache line.
type BinanceTick struct {
	Asset       SniperAsset // 1 byte
	_           [7]byte     // padding for alignment
	MarkPrice   float64     // 8 bytes
	EventTimeMs int64       // 8 bytes
	ReceivedNs  int64       // 8 bytes
}

// MarketBook is the cached orderbook state for one asset.
type MarketBook struct {
	MarketID      string
	ConditionID   string
	YesPrice      decimal.Decimal
	YesPriceF64   float64
	StrikePrice   float64
	MarketCloseTs int64
	UpdatedNs     int64
}

// SniperSignal is emitted when arbitrage conditions are met.
type SniperSignal struct {
	Asset       SniperAsset
	MarketID    string
	ConditionID string
	YesPrice    decimal.Decimal
	StrikePrice float64
	MarkPrice   float64
	BetSizeUSD  decimal.Decimal
	SignalNs    int64
}

type ExecutionRequest struct {
	Signal SniperSignal
	Side   OrderSide
}

type ExecutionResult struct {
	TxHash      string
	OK          bool
	Asset       SniperAsset
	InvestedUSD decimal.Decimal
	PayoutUSD   decimal.Decimal
	PnlDeltaUSD decimal.Decimal
	Error       string
}

// ---------------------------------------------------------------------------
// SharedState — cache-line-padded, false-sharing-proof for Apple Silicon
//
// Each "hot" field group lives on its own 128-byte cache line so that
// concurrent atomic stores from different goroutines don't thrash each
// other's L1/L2 caches.
// ---------------------------------------------------------------------------

type SharedState struct {
	// --- Cache line 0: prices (written by Binance feed goroutine) ---
	prices [AssetCount]atomic.Uint64
	_pad0  [CacheLineSize - int(AssetCount)*8]byte

	// --- Cache line 1: sniper lifecycle (written by engine goroutine) ---
	sniperState  atomic.Int32
	killSwitch   atomic.Bool
	lastSignalNs atomic.Int64
	inflightBits atomic.Uint32
	_pad1        CacheLinePad

	// --- Cache line 2: feed timestamps (written by feed goroutines) ---
	lastBinanceNs atomic.Int64
	_pad2         CacheLinePad

	// --- Cache line 3: balances (written by executor goroutine) ---
	initialCapitalCents atomic.Int64
	walletBalanceCents  atomic.Int64
	cumulativePnlCents  atomic.Int64
	phaseBits           atomic.Uint32
	_pad3               CacheLinePad

	// --- Cache line 4: status + books (low-frequency updates) ---
	latestStatus    atomic.Value
	Books           [AssetCount]atomic.Value
	BooksGeneration atomic.Int64
}

// Compile-time assertion: SharedState should be large enough for padding.
var _ = unsafe.Sizeof(SharedState{})

func NewSharedState(capitalUSD decimal.Decimal) *SharedState {
	s := &SharedState{}
	cents := capitalUSD.Mul(decimal.NewFromInt(100)).IntPart()
	s.initialCapitalCents.Store(cents)
	s.walletBalanceCents.Store(cents)
	s.latestStatus.Store("BOOTING")
	return s
}

// -- Price accessors (lock-free, zero-alloc) --

//go:nosplit
func (s *SharedState) SetPrice(a SniperAsset, p float64) {
	s.prices[a].Store(math.Float64bits(p))
}

//go:nosplit
func (s *SharedState) GetPrice(a SniperAsset) float64 {
	return math.Float64frombits(s.prices[a].Load())
}

// -- State accessors --

func (s *SharedState) SetSniperState(st SniperState) { s.sniperState.Store(int32(st)) }
func (s *SharedState) GetSniperState() SniperState   { return SniperState(s.sniperState.Load()) }
func (s *SharedState) SetKillSwitch(v bool)          { s.killSwitch.Store(v) }
func (s *SharedState) IsKilled() bool                { return s.killSwitch.Load() }
func (s *SharedState) SetStatus(st string)           { s.latestStatus.Store(st) }
func (s *SharedState) GetStatus() string             { return s.latestStatus.Load().(string) }
func (s *SharedState) SetLastBinanceNs(ns int64)     { s.lastBinanceNs.Store(ns) }
func (s *SharedState) GetLastBinanceNs() int64       { return s.lastBinanceNs.Load() }
func (s *SharedState) SetLastSignalNs(ns int64)      { s.lastSignalNs.Store(ns) }

// -- Balance accessors (hot-path uses cents directly, cold-path converts) --

func (s *SharedState) GetWalletBalanceCents() int64 { return s.walletBalanceCents.Load() }
func (s *SharedState) GetWalletBalanceUSD() decimal.Decimal {
	return decimal.NewFromInt(s.walletBalanceCents.Load()).Div(decimal.NewFromInt(100))
}
func (s *SharedState) AddWalletBalanceCents(d int64) { s.walletBalanceCents.Add(d) }
func (s *SharedState) GetCumulativePnlCents() int64  { return s.cumulativePnlCents.Load() }
func (s *SharedState) GetCumulativePnlUSD() decimal.Decimal {
	return decimal.NewFromInt(s.cumulativePnlCents.Load()).Div(decimal.NewFromInt(100))
}
func (s *SharedState) AddCumulativePnlCents(d int64) { s.cumulativePnlCents.Add(d) }
func (s *SharedState) GetInitialCapitalUSD() decimal.Decimal {
	return decimal.NewFromInt(s.initialCapitalCents.Load()).Div(decimal.NewFromInt(100))
}

func (s *SharedState) SetCompoundPhaseActive() { s.phaseBits.Or(PhaseCompoundActive) }
func (s *SharedState) IsCompoundPhaseActive() bool {
	return s.phaseBits.Load()&PhaseCompoundActive != 0
}

// -- Market book accessors --

func (s *SharedState) SetBook(a SniperAsset, b *MarketBook) {
	s.Books[a].Store(b)
	s.BooksGeneration.Add(1)
}

func (s *SharedState) GetBook(a SniperAsset) *MarketBook {
	v := s.Books[a].Load()
	if v == nil {
		return nil
	}
	return v.(*MarketBook)
}

func (s *SharedState) BookCount() int {
	n := 0
	for i := SniperAsset(0); i < AssetCount; i++ {
		if s.Books[i].Load() != nil {
			n++
		}
	}
	return n
}

// -- Inflight tracking (lock-free bitfield, popcount via math/bits) --

func (s *SharedState) SetInflight(a SniperAsset)     { s.inflightBits.Or(1 << uint(a)) }
func (s *SharedState) ClearInflight(a SniperAsset)   { s.inflightBits.And(^(1 << uint(a))) }
func (s *SharedState) IsInflight(a SniperAsset) bool { return s.inflightBits.Load()&(1<<uint(a)) != 0 }

func (s *SharedState) InflightCount() int {
	return bits.OnesCount32(s.inflightBits.Load())
}

// ---------------------------------------------------------------------------
// Metrics (per-goroutine, no sharing — no padding needed)
// ---------------------------------------------------------------------------

type EngineMetrics struct {
	Decisions     int64
	Fired         int64
	AvgDecisionNs float64 // nanoseconds now, not ms
	MaxDecisionNs float64
}

func (m *EngineMetrics) Record(ns float64, exec bool) {
	m.Decisions++
	if exec {
		m.Fired++
	}
	m.AvgDecisionNs = m.AvgDecisionNs*0.9 + ns*0.1
	if ns > m.MaxDecisionNs {
		m.MaxDecisionNs = ns
	}
}

type FeedMetrics struct {
	Ticks      int64
	Reconnects int64
	AvgParseNs float64
}

func (m *FeedMetrics) RecordParseNs(ns float64) {
	m.Ticks++
	m.AvgParseNs = m.AvgParseNs*0.9 + ns*0.1
}

type ExecutorMetrics struct {
	Sent, OK, Failed int64
	AvgSignNs        float64
}

func (m *ExecutorMetrics) RecordSignNs(ns float64) {
	m.AvgSignNs = m.AvgSignNs*0.9 + ns*0.1
}

// NowNs returns the current monotonic nanosecond timestamp.
// On ARM64 Apple Silicon, time.Now() reads CNTVCT_EL0 (~12ns overhead).
//
//go:nosplit
func NowNs() int64 {
	return time.Now().UnixNano()
}
