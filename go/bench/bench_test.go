// Package bench provides latency benchmarks for the hot path.
//
// Run with:
//
//	cd go && go test -bench=. -benchmem -count=5 -cpu=1 ./bench/
//
// Profile with pprof:
//
//	cd go && go test -bench=BenchmarkTimeToTrade -cpuprofile=cpu.prof -memprofile=mem.prof ./bench/
//	go tool pprof -http=:8080 cpu.prof
package bench

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/polymarket-arb-bot/internal/fastlog"
	"github.com/polymarket-arb-bot/internal/models"
	"github.com/polymarket-arb-bot/internal/simd"
)

// BenchmarkPriceUpdate measures the cost of a single atomic price write+read.
// Target: <20ns
func BenchmarkPriceUpdate(b *testing.B) {
	state := models.NewSharedState(decimal.NewFromFloat(1000.0))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		state.SetPrice(models.AssetBTC, 67543.12)
		_ = state.GetPrice(models.AssetBTC)
	}
}

// BenchmarkInflightBitfield measures the cost of set/check/clear inflight.
// Target: <15ns
func BenchmarkInflightBitfield(b *testing.B) {
	state := models.NewSharedState(decimal.NewFromFloat(1000.0))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		asset := models.SniperAsset(i % int(models.AssetCount))
		state.SetInflight(asset)
		_ = state.IsInflight(asset)
		state.ClearInflight(asset)
	}
}

// BenchmarkFastParseFloat measures the custom float parser vs strconv.
// Target: <30ns (vs strconv ~150ns)
func BenchmarkFastParseFloat(b *testing.B) {
	s := "67543.12000000"
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = fastParseFloatBench(s)
	}
}

// BenchmarkDecisionLoop simulates the full engine scan for 4 assets.
// This is the core metric: how fast can we decide to fire or not.
// Target: <1µs for all 4 assets
func BenchmarkDecisionLoop(b *testing.B) {
	state := models.NewSharedState(decimal.NewFromFloat(1000.0))

	// Set up realistic market state.
	state.SetPrice(models.AssetBTC, 67543.0)
	state.SetPrice(models.AssetETH, 3456.0)
	state.SetPrice(models.AssetSOL, 145.0)
	state.SetPrice(models.AssetBNB, 567.0)

	now := time.Now().Unix()
	for i := models.SniperAsset(0); i < models.AssetCount; i++ {
		state.SetBook(i, &models.MarketBook{
			MarketID:      "mkt_" + i.String(),
			ConditionID:   "cond_" + i.String(),
			YesPrice:      decimal.NewFromFloat(0.65),
			YesPriceF64:   0.65,
			StrikePrice:   67000.0,
			MarketCloseTs: now + 10, // 10 seconds to close
			UpdatedNs:     models.NowNs(),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate scanning all 4 assets.
		for a := models.SniperAsset(0); a < models.AssetCount; a++ {
			book := state.GetBook(a)
			if book == nil {
				continue
			}
			_ = state.GetPrice(a)
			_ = book.StrikePrice
			_ = state.IsInflight(a)
			_ = state.GetWalletBalanceCents()
		}
	}
}

// BenchmarkTimeToTrade measures the full pipeline from signal creation
// to channel send — the "time to trade" metric.
// Target: <5µs
func BenchmarkTimeToTrade(b *testing.B) {
	state := models.NewSharedState(decimal.NewFromFloat(10000.0))

	// Pre-fill prices and books.
	state.SetPrice(models.AssetBTC, 67543.0)
	now := time.Now().Unix()
	state.SetBook(models.AssetBTC, &models.MarketBook{
		MarketID:      "mkt_btc",
		ConditionID:   "cond_btc",
		YesPrice:      decimal.NewFromFloat(0.65),
		YesPriceF64:   0.65,
		StrikePrice:   67000.0,
		MarketCloseTs: now + 10,
		UpdatedNs:     models.NowNs(),
	})

	// Buffered channel so sends don't block.
	execCh := make(chan models.ExecutionRequest, 8192)
	betSize := decimal.NewFromInt(8000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		startNs := models.NowNs()

		// Simulate the hot path: read state → decide → build signal → send.
		book := state.GetBook(models.AssetBTC)
		markPrice := state.GetPrice(models.AssetBTC)

		if book != nil && markPrice > book.StrikePrice {
			balanceCents := state.GetWalletBalanceCents()
			var stakeCents int64
			if balanceCents < 10000 {
				if balanceCents < 2000 {
					stakeCents = 150
				} else {
					stakeCents = balanceCents >> 3
				}
			} else {
				stakeCents = (balanceCents * 8) / 10
			}
			if stakeCents > balanceCents {
				stakeCents = balanceCents
			}
			_ = stakeCents

			signal := models.SniperSignal{
				Asset:       models.AssetBTC,
				MarketID:    book.MarketID,
				ConditionID: book.ConditionID,
				YesPrice:    book.YesPrice,
				StrikePrice: book.StrikePrice,
				MarkPrice:   markPrice,
				BetSizeUSD:  betSize,
				SignalNs:    startNs,
			}

			select {
			case execCh <- models.ExecutionRequest{Signal: signal, Side: models.SideYes}:
			default:
			}
		}

		// Drain the channel to avoid filling up.
		select {
		case <-execCh:
		default:
		}
	}
}

// BenchmarkNowNs measures the overhead of our timestamp function.
// Target: <25ns on Apple Silicon
func BenchmarkNowNs(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = models.NowNs()
	}
}

// BenchmarkSIMDSpread measures the NEON-accelerated spread calculation.
// Target: <5ns for 4 assets simultaneously
func BenchmarkSIMDSpread(b *testing.B) {
	prices := [4]uint64{
		uint64(models.FPFromFloat(67543.0)),
		uint64(models.FPFromFloat(3456.0)),
		uint64(models.FPFromFloat(145.0)),
		uint64(models.FPFromFloat(567.0)),
	}
	strikes := [4]uint64{
		uint64(models.FPFromFloat(67000.0)),
		uint64(models.FPFromFloat(3400.0)),
		uint64(models.FPFromFloat(140.0)),
		uint64(models.FPFromFloat(600.0)), // above price → should be 0
	}
	yesPrices := [4]uint64{
		uint64(models.FPFromFloat(0.65)),
		uint64(models.FPFromFloat(0.72)),
		uint64(models.FPFromFloat(0.58)),
		uint64(models.FPFromFloat(0.80)),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = simd.ComputeSpreadsAndRank(prices, strikes, yesPrices)
	}
}

// BenchmarkRingLogger measures lock-free ring buffer log write latency.
// Target: <30ns per write (no blocking, no allocation)
func BenchmarkRingLogger(b *testing.B) {
	rlog := fastlog.NewRingLogger()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rlog.Log(fastlog.LevelInfo, "tick processed")
	}
}

// BenchmarkFixedPointMul measures fixed-point multiplication.
// Target: <10ns
func BenchmarkFixedPointMul(b *testing.B) {
	a := models.FPFromFloat(67543.12)
	c := models.FPFromFloat(0.65)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = a.Mul(c)
	}
}

// BenchmarkHubCorrelation measures cross-asset correlation computation.
// Target: <500ns for N=60
func BenchmarkHubCorrelation(b *testing.B) {
	hub := models.NewSharedMemoryHub()
	// Fill with 100 price ticks per asset.
	for i := 0; i < 100; i++ {
		hub.RecordPrice(models.AssetBTC, models.FPFromFloat(67000.0+float64(i)*10))
		hub.RecordPrice(models.AssetETH, models.FPFromFloat(3400.0+float64(i)*5))
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = hub.Correlation(models.AssetBTC, models.AssetETH, 60)
	}
}

// fastParseFloatBench is a copy for benchmarking (feed package is internal).
func fastParseFloatBench(s string) float64 {
	var result float64
	var divisor float64
	dec := false
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			dec = true
			divisor = 1
			continue
		}
		if c < '0' || c > '9' {
			break
		}
		if dec {
			divisor *= 10
			result += float64(c-'0') / divisor
		} else {
			result = result*10 + float64(c-'0')
		}
	}
	return result
}
