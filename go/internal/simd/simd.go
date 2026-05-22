package simd

import "github.com/polymarket-arb-bot/internal/models"

const fpScale = uint64(models.FPScale)

// ComputeSpreadsAndRank calcula spreads y rangos con zero allocations.
func ComputeSpreadsAndRank(
	prices [4]uint64,
	strikes [4]uint64,
	yesPrices [4]uint64,
) (bestIdx int, bestEV uint64, spreads [4]uint64) {

	// 1. Vectorizacion ASM NEON para la resta saturada.
	spreadSIMD(&prices, &strikes, &spreads)

	// 2. Loop unrolling estricto para evadir escapes al heap.
	var ev0, ev1, ev2, ev3 uint64

	if spreads[0] > 0 {
		comp := fpScale - yesPrices[0]
		if yesPrices[0] >= fpScale {
			comp = 0
		}
		ev0 = spreads[0] * (comp >> 16)
	}
	if spreads[1] > 0 {
		comp := fpScale - yesPrices[1]
		if yesPrices[1] >= fpScale {
			comp = 0
		}
		ev1 = spreads[1] * (comp >> 16)
	}
	if spreads[2] > 0 {
		comp := fpScale - yesPrices[2]
		if yesPrices[2] >= fpScale {
			comp = 0
		}
		ev2 = spreads[2] * (comp >> 16)
	}
	if spreads[3] > 0 {
		comp := fpScale - yesPrices[3]
		if yesPrices[3] >= fpScale {
			comp = 0
		}
		ev3 = spreads[3] * (comp >> 16)
	}

	// 3. Arbol de decision escalar estatico para encontrar el maximo.
	bestIdx = 0
	bestEV = ev0

	if ev1 > bestEV {
		bestIdx = 1
		bestEV = ev1
	}
	if ev2 > bestEV {
		bestIdx = 2
		bestEV = ev2
	}
	if ev3 > bestEV {
		bestIdx = 3
		bestEV = ev3
	}

	return bestIdx, bestEV, spreads
}
