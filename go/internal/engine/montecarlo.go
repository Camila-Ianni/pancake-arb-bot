// Package engine provides a Monte Carlo simulator for profit projection.
//
// Run with: ./sniper --analyze
//
// Simulates 10,000 trade sequences using recent price data from the
// SharedMemoryHub ring buffers. Projects daily profit distribution and
// estimates P50/P90/P99 outcomes.
package engine

import (
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/polymarket-arb-bot/internal/models"
)

const (
	DefaultSimulations = 10_000
	TradesPerDay       = 200 // estimated number of 5-min windows per day
)

// SimConfig holds parameters for the Monte Carlo simulation.
type SimConfig struct {
	Simulations   int
	TradesPerDay  int
	BaseStakeUSD  float64
	CompoundPct   float64 // e.g. 0.05 = 5% per win
	WinRate       float64 // estimated win rate (0.0-1.0)
	AvgProfitPct  float64 // average profit per winning trade (0.0-1.0)
	AvgLossPct    float64 // average loss per losing trade (0.0-1.0)
	DrawdownLimit float64 // max drawdown before safety mode (0.0-1.0)
}

// SimResult holds the result of one Monte Carlo run.
type SimResult struct {
	FinalPnL float64
	MaxPnL   float64
	MaxDD    float64 // max drawdown percentage
	Trades   int
	Wins     int
}

// RunSimulation runs the full Monte Carlo simulation and prints results.
func RunSimulation(hub *models.SharedMemoryHub, cfg SimConfig) {
	if cfg.Simulations == 0 {
		cfg.Simulations = DefaultSimulations
	}
	if cfg.TradesPerDay == 0 {
		cfg.TradesPerDay = TradesPerDay
	}
	if cfg.BaseStakeUSD == 0 {
		cfg.BaseStakeUSD = 10.0
	}
	if cfg.CompoundPct == 0 {
		cfg.CompoundPct = 0.05
	}
	if cfg.WinRate == 0 {
		cfg.WinRate = estimateWinRate(hub)
	}
	if cfg.AvgProfitPct == 0 {
		cfg.AvgProfitPct = 0.35 // 35% average profit on YES contracts
	}
	if cfg.AvgLossPct == 0 {
		cfg.AvgLossPct = 1.0 // lose entire stake on bad trades
	}
	if cfg.DrawdownLimit == 0 {
		cfg.DrawdownLimit = 0.20
	}

	results := make([]SimResult, cfg.Simulations)

	for i := 0; i < cfg.Simulations; i++ {
		results[i] = simulateDay(cfg)
	}

	// Sort by final PnL for percentile analysis.
	sort.Slice(results, func(i, j int) bool {
		return results[i].FinalPnL < results[j].FinalPnL
	})

	// Compute statistics.
	var totalPnL, totalDD float64
	profitable := 0
	target100 := 0

	for _, r := range results {
		totalPnL += r.FinalPnL
		totalDD += r.MaxDD
		if r.FinalPnL > 0 {
			profitable++
		}
		if r.FinalPnL >= 100.0 {
			target100++
		}
	}

	n := float64(cfg.Simulations)
	avgPnL := totalPnL / n
	avgDD := totalDD / n

	p5 := results[int(n*0.05)]
	p25 := results[int(n*0.25)]
	p50 := results[int(n*0.50)]
	p75 := results[int(n*0.75)]
	p90 := results[int(n*0.90)]
	p95 := results[int(n*0.95)]
	p99 := results[int(n*0.99)]

	// Print report.
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              🎲 MONTE CARLO SIMULATION RESULTS                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Simulations:  %6d  |  Trades/Day: %4d                       ║\n", cfg.Simulations, cfg.TradesPerDay)
	fmt.Printf("║ Base Stake:  $%6.2f  |  Win Rate:  %5.1f%%                      ║\n", cfg.BaseStakeUSD, cfg.WinRate*100)
	fmt.Printf("║ Compound:     %5.1f%%  |  Drawdown Limit: %4.0f%%                  ║\n", cfg.CompoundPct*100, cfg.DrawdownLimit*100)
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Average Daily PnL:     $%8.2f                                ║\n", avgPnL)
	fmt.Printf("║ Average Max Drawdown:  %7.1f%%                                 ║\n", avgDD*100)
	fmt.Printf("║ Profitable Days:       %6.1f%%                                  ║\n", float64(profitable)/n*100)
	fmt.Printf("║ Days Hitting $100:     %6.1f%%                                  ║\n", float64(target100)/n*100)
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Println("║                    PERCENTILE DISTRIBUTION                      ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  P5  (worst 5%%):    $%8.2f                                  ║\n", p5.FinalPnL)
	fmt.Printf("║  P25 (lower qrt):   $%8.2f                                  ║\n", p25.FinalPnL)
	fmt.Printf("║  P50 (median):      $%8.2f                                  ║\n", p50.FinalPnL)
	fmt.Printf("║  P75 (upper qrt):   $%8.2f                                  ║\n", p75.FinalPnL)
	fmt.Printf("║  P90:               $%8.2f                                  ║\n", p90.FinalPnL)
	fmt.Printf("║  P95:               $%8.2f                                  ║\n", p95.FinalPnL)
	fmt.Printf("║  P99 (best 1%%):     $%8.2f                                  ║\n", p99.FinalPnL)
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	// Visual distribution.
	printDistribution(results)

	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
}

// simulateDay runs one simulated trading day.
func simulateDay(cfg SimConfig) SimResult {
	stake := cfg.BaseStakeUSD
	pnl := 0.0
	peak := 0.0
	maxDD := 0.0
	wins := 0
	safetyUntil := 0

	for t := 0; t < cfg.TradesPerDay; t++ {
		// Safety mode check.
		if t < safetyUntil {
			continue
		}

		// Decide win/loss.
		if rand.Float64() < cfg.WinRate {
			// Win: profit is stake × avgProfitPct.
			profit := stake * cfg.AvgProfitPct
			pnl += profit
			wins++

			// Compound: increase stake by CompoundPct.
			stake *= (1 + cfg.CompoundPct)
		} else {
			// Loss: lose avgLossPct of stake.
			loss := stake * cfg.AvgLossPct
			pnl -= loss

			// Reset stake to base.
			stake = cfg.BaseStakeUSD
		}

		// Track peak and drawdown.
		if pnl > peak {
			peak = pnl
		}
		if peak > 0 {
			dd := (peak - pnl) / peak
			if dd > maxDD {
				maxDD = dd
			}
			// Safety mode: 20% drawdown triggers 30-min pause.
			if dd >= cfg.DrawdownLimit {
				safetyUntil = t + 6 // ~30 min = 6 × 5-min windows
				stake = cfg.BaseStakeUSD
			}
		}
	}

	return SimResult{
		FinalPnL: pnl,
		MaxPnL:   peak,
		MaxDD:    maxDD,
		Trades:   cfg.TradesPerDay,
		Wins:     wins,
	}
}

// estimateWinRate uses historical price data from the hub to estimate win rate.
func estimateWinRate(hub *models.SharedMemoryHub) float64 {
	if hub == nil {
		return 0.55 // default 55% assumed win rate
	}

	// Count how many 5-min windows BTC was "predictable"
	// (price moved in same direction as deviation from SMA).
	ring := &hub.Rings[models.AssetBTC]
	count := ring.Count()
	if count < 60 {
		return 0.55
	}

	wins := 0
	total := 0
	n := uint64(60)
	if count < n {
		n = count
	}

	for i := uint64(1); i < n; i++ {
		prev := ring.Get(i)
		curr := ring.Get(i - 1)
		sma := ring.SMA(i)

		if prev == 0 || curr == 0 || sma == 0 {
			continue
		}

		// "Win" = if price was below SMA and then moved up (mean reversion).
		if prev < sma && curr > prev {
			wins++
		} else if prev > sma && curr < prev {
			wins++
		}
		total++
	}

	if total == 0 {
		return 0.55
	}
	rate := float64(wins) / float64(total)
	if rate < 0.40 {
		rate = 0.40
	}
	if rate > 0.80 {
		rate = 0.80
	}
	return rate
}

// printDistribution prints a visual histogram of results.
func printDistribution(results []SimResult) {
	// Build 10 buckets.
	minPnL := results[0].FinalPnL
	maxPnL := results[len(results)-1].FinalPnL
	spread := maxPnL - minPnL
	if spread <= 0 {
		return
	}

	const buckets = 10
	counts := [buckets]int{}
	bucketSize := spread / buckets

	for _, r := range results {
		b := int((r.FinalPnL - minPnL) / bucketSize)
		if b >= buckets {
			b = buckets - 1
		}
		counts[b]++
	}

	maxCount := 0
	for _, c := range counts {
		if c > maxCount {
			maxCount = c
		}
	}

	fmt.Println("║                   PROFIT DISTRIBUTION                           ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	for i := 0; i < buckets; i++ {
		lo := minPnL + float64(i)*bucketSize
		barLen := 0
		if maxCount > 0 {
			barLen = int(math.Round(float64(counts[i]) / float64(maxCount) * 30))
		}
		bar := ""
		for j := 0; j < barLen; j++ {
			bar += "█"
		}
		fmt.Printf("║ $%7.0f │%-30s│%5d ║\n", lo, bar, counts[i])
	}
}
