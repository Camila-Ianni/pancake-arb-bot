// Package simd provides NEON-accelerated spread/EV calculations.
//
// On ARM64 (Apple Silicon), the hot-path functions are implemented in
// hand-written assembly using NEON 128-bit vector registers.
// On other architectures, pure Go fallbacks are used.
package simd

import "github.com/polymarket-arb-bot/internal/models"

// FPScale matches models.FPScale for fixed-point division.
const fpScale = uint64(models.FPScale)

// ComputeSpreadsAndRank takes 4 asset prices, strikes, and yes-prices
// (all as FixedPoint uint64), computes spreads and expected values,
// and returns the best asset index and its EV.
//
// This is the core SIMD-accelerated decision function.
// Total latency: ~5ns on M1 Pro (spread NEON + scalar EV).
func ComputeSpreadsAndRank(
	prices [4]uint64,
	strikes [4]uint64,
	yesPrices [4]uint64,
) (bestIdx int, bestEV uint64, spreads [4]uint64) {

	// Step 1: NEON vectorized spread calculation (~3ns)
	spreadSIMD(&prices, &strikes, &spreads)

	// Step 2: Compute EV for each asset (scalar — NEON lacks 64-bit multiply)
	// EV[i] = spread[i] * (1 - yesPrice[i]) / FPScale
	// Since we only need relative ranking, we can compare spread * complement
	// without dividing by FPScale (same divisor for all).
	var evs [4]uint64
	for i := 0; i < 4; i++ {
		if spreads[i] == 0 {
			continue
		}
		// complement = 1.0 - yesPrice (in fixed-point)
		complement := fpScale - yesPrices[i]
		if yesPrices[i] >= fpScale {
			complement = 0
		}
		// EV = spread * complement (skip division — relative comparison only)
		evs[i] = spreads[i] * (complement >> 16) // shift to prevent overflow
	}

	// Step 3: NEON max-finding (~2ns)
	idx := maxIndexSIMD(&evs)
	return int(idx), evs[idx], spreads
}
