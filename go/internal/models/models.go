package models

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

type SniperAsset uint8

const (
	AssetBTC SniperAsset = iota
	AssetETH
	AssetSOL
	AssetBNB
	AssetCount
)

func (a SniperAsset) String() string {
	switch a {
	case AssetBTC:
		return "BTC"
	case AssetETH:
		return "ETH"
	case AssetSOL:
		return "SOL"
	case AssetBNB:
		return "BNB"
	default:
		return "?"
	}
}

func AssetFromSymbol(sym string) (SniperAsset, bool) {
	switch sym {
	case "BTCUSDT":
		return AssetBTC, true
	case "ETHUSDT":
		return AssetETH, true
	case "SOLUSDT":
		return AssetSOL, true
	case "BNBUSDT":
		return AssetBNB, true
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

func (s SniperState) String() string {
	names := [...]string{"IDLE", "ARMED", "FIRING", "COOLDOWN", "STOPPED"}
	if int(s) < len(names) {
		return names[s]
	}
	return "?"
}

type OrderSide uint8

const (
	SideYes OrderSide = iota
	SideNo
)

type BinanceTick struct {
	Asset       SniperAsset
	MarkPrice   float64
	EventTimeMs int64
	ReceivedNs  int64
}

type PolymarketTick struct {
	Asset         SniperAsset
	MarketID      string
	ConditionID   string
	YesPrice      decimal.Decimal
	StrikePrice   float64
	MarketCloseTs int64
}

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

type MarketBook struct {
	MarketID      string
	ConditionID   string
	YesPrice      decimal.Decimal
	StrikePrice   float64
	MarketCloseTs int64
	UpdatedNs     int64
}

// SharedState – lock-free atomic state visible to all goroutines.
type SharedState struct {
	prices              [AssetCount]atomic.Uint64
	sniperState         atomic.Int32
	killSwitch          atomic.Bool
	lastBinanceNs       atomic.Int64
	lastSignalNs        atomic.Int64
	initialCapitalCents atomic.Int64
	walletBalanceCents  atomic.Int64
	cumulativePnlCents  atomic.Int64
	latestStatus        atomic.Value
	Books               [AssetCount]atomic.Value
	BooksGeneration     atomic.Int64
	inflightBits        atomic.Uint32
}

func NewSharedState(capitalUSD decimal.Decimal) *SharedState {
	s := &SharedState{}
	cents := capitalUSD.Mul(decimal.NewFromInt(100)).IntPart()
	s.initialCapitalCents.Store(cents)
	s.walletBalanceCents.Store(cents)
	s.latestStatus.Store("BOOTING")
	return s
}

func (s *SharedState) SetPrice(a SniperAsset, p float64)  { s.prices[a].Store(math.Float64bits(p)) }
func (s *SharedState) GetPrice(a SniperAsset) float64      { return math.Float64frombits(s.prices[a].Load()) }
func (s *SharedState) SetSniperState(st SniperState)       { s.sniperState.Store(int32(st)) }
func (s *SharedState) GetSniperState() SniperState         { return SniperState(s.sniperState.Load()) }
func (s *SharedState) SetKillSwitch(v bool)                { s.killSwitch.Store(v) }
func (s *SharedState) IsKilled() bool                      { return s.killSwitch.Load() }
func (s *SharedState) SetStatus(st string)                 { s.latestStatus.Store(st) }
func (s *SharedState) GetStatus() string                   { return s.latestStatus.Load().(string) }
func (s *SharedState) SetLastBinanceNs(ns int64)           { s.lastBinanceNs.Store(ns) }
func (s *SharedState) GetLastBinanceNs() int64             { return s.lastBinanceNs.Load() }
func (s *SharedState) SetLastSignalNs(ns int64)            { s.lastSignalNs.Store(ns) }

func (s *SharedState) GetWalletBalanceUSD() decimal.Decimal {
	return decimal.NewFromInt(s.walletBalanceCents.Load()).Div(decimal.NewFromInt(100))
}
func (s *SharedState) AddWalletBalanceCents(d int64) { s.walletBalanceCents.Add(d) }
func (s *SharedState) GetCumulativePnlUSD() decimal.Decimal {
	return decimal.NewFromInt(s.cumulativePnlCents.Load()).Div(decimal.NewFromInt(100))
}
func (s *SharedState) AddCumulativePnlCents(d int64)  { s.cumulativePnlCents.Add(d) }
func (s *SharedState) GetInitialCapitalUSD() decimal.Decimal {
	return decimal.NewFromInt(s.initialCapitalCents.Load()).Div(decimal.NewFromInt(100))
}

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

func (s *SharedState) SetInflight(a SniperAsset)    { s.inflightBits.Or(1 << uint(a)) }
func (s *SharedState) ClearInflight(a SniperAsset)  { s.inflightBits.And(^(1 << uint(a))) }
func (s *SharedState) IsInflight(a SniperAsset) bool { return s.inflightBits.Load()&(1<<uint(a)) != 0 }
func (s *SharedState) InflightCount() int {
	bits := s.inflightBits.Load()
	n := 0
	for bits != 0 {
		n += int(bits & 1)
		bits >>= 1
	}
	return n
}

type EngineMetrics struct {
	Decisions     int64
	Fired         int64
	AvgDecisionMs float64
	MaxDecisionMs float64
}

func (m *EngineMetrics) Record(ms float64, exec bool) {
	m.Decisions++
	if exec {
		m.Fired++
	}
	m.AvgDecisionMs = m.AvgDecisionMs*0.9 + ms*0.1
	if ms > m.MaxDecisionMs {
		m.MaxDecisionMs = ms
	}
}

type FeedMetrics struct {
	Ticks      int64
	Reconnects int64
	AvgParseMs float64
}

func (m *FeedMetrics) RecordParseMs(ms float64) {
	m.Ticks++
	m.AvgParseMs = m.AvgParseMs*0.9 + ms*0.1
}

type ExecutorMetrics struct {
	Sent, OK, Failed int64
	AvgSignMs        float64
}

func (m *ExecutorMetrics) RecordSignMs(ms float64) {
	m.AvgSignMs = m.AvgSignMs*0.9 + ms*0.1
}

func NowNs() int64 { return time.Now().UnixNano() }
