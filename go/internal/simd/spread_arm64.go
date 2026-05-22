//go:build arm64

package simd

// spreadSIMD computes 4 saturating subtractions using NEON.
// result[i] = max(prices[i] - strikes[i], 0)
//
//go:noescape
func spreadSIMD(prices, strikes, result *[4]uint64)

// maxIndexSIMD finds the index of the maximum value among 4 uint64s.
//
//go:noescape
func maxIndexSIMD(values *[4]uint64) uint64
