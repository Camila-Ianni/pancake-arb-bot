// Package panel renders a professional terminal dashboard.
//
// Features:
//   - Per-asset breakdown: price, latency, win/loss ratio
//   - Daily progress bar toward $100 USD target
//   - Compounding stake display
//   - Safety mode indicator
package panel

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/polymarket-arb-bot/internal/models"
)

// Clear clears the terminal screen.
func Clear() {
	if runtime.GOOS == "windows" {
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		_ = cmd.Run()
	} else {
		fmt.Print("\033[2J\033[H")
	}
}

// Render prints the pro sniper dashboard to stdout.
func Render(state *models.SharedState, hub *models.SharedMemoryHub, tracker *models.DailyTracker) {
	Clear()

	sniperLabel := "\033[31m● STOPPED\033[0m"
	st := state.GetSniperState()
	switch st {
	case models.StateArmed:
		sniperLabel = "\033[32m● ARMED\033[0m"
	case models.StateFiring:
		sniperLabel = "\033[33m⚡ FIRING\033[0m"
	case models.StateCooldown:
		sniperLabel = "\033[36m● COOLDOWN\033[0m"
	}

	safetyLabel := ""
	if tracker != nil && tracker.IsInSafetyMode() {
		safetyLabel = " \033[31m[SAFETY MODE]\033[0m"
	}

	fmt.Println("\033[1m╔══════════════════════════════════════════════════════════════════════╗\033[0m")
	fmt.Println("\033[1m║     🎯 POLYMARKET MULTI-ASSET SNIPER v2.0 — Go HFT Edition        ║\033[0m")
	fmt.Println("\033[1m╠══════════════════════════════════════════════════════════════════════╣\033[0m")

	// — Header stats —
	fmt.Printf("║ Estado: %s%s | Status: %-25s  ║\n",
		sniperLabel, safetyLabel, state.GetStatus())
	fmt.Printf("║ Capital: $%-8s | Wallet: $%-8s | Kill: %-5v           ║\n",
		state.GetInitialCapitalUSD().StringFixed(2),
		state.GetWalletBalanceUSD().StringFixed(2),
		state.IsKilled())

	// — Daily progress —
	if tracker != nil {
		pnl := tracker.DailyPnL()
		target := tracker.DailyTarget()
		progress := tracker.DailyProgress()
		stake := tracker.CurrentStake()
		winRate := tracker.WinRate()

		fmt.Println("\033[1m╠══════════════════════════════════════════════════════════════════════╣\033[0m")
		fmt.Printf("║ 📊 Daily PnL: \033[32m$%-8s\033[0m | Target: $%-8s | Win Rate: %5.1f%%     ║\n",
			pnl.StringDollars()[1:], target.StringDollars()[1:], winRate)
		fmt.Printf("║ 💰 Stake: $%-10s (compounds +5%%/win)                          ║\n",
			stake.StringDollars()[1:])

		// Progress bar.
		bar := progressBar(progress, 40)
		pctStr := fmt.Sprintf("%.1f%%", progress)
		fmt.Printf("║ [%s] %-6s → $100/day                        ║\n", bar, pctStr)
	}

	// — Per-asset table —
	fmt.Println("\033[1m╠══════════════════════════════════════════════════════════════════════╣\033[0m")
	fmt.Println("║ Asset  │  Mark Price   │  SMA(60)   │  Dev   │  W/L    │ Corr/BTC ║")
	fmt.Println("╠════════╪══════════════╪═══════════╪════════╪═════════╪══════════╣")

	for i := models.SniperAsset(0); i < models.AssetCount; i++ {
		price := state.GetPrice(i)
		if price == 0 {
			fmt.Printf("║ %-6s │     ---      │    ---    │   ---  │   ---   │   ---    ║\n", i.String())
			continue
		}

		smaStr := "---"
		devStr := "---"
		corrStr := "---"

		if hub != nil {
			sma := hub.Rings[i].SMA(60)
			if sma > 0 {
				smaStr = sma.StringDollars()
				dev := hub.Rings[i].Deviation(60)
				devPct := float64(dev) / float64(sma) * 100.0
				if devPct >= 0 {
					devStr = fmt.Sprintf("+%.2f%%", devPct)
				} else {
					devStr = fmt.Sprintf("%.2f%%", devPct)
				}
			}

			if i != models.AssetBTC {
				corr := hub.Correlation(models.AssetBTC, i, 60)
				corrStr = fmt.Sprintf("%.3f", corr)
			} else {
				corrStr = "1.000"
			}
		}

		wlStr := "---"
		if tracker != nil {
			wins := tracker.AssetWins[i].Load()
			losses := tracker.AssetLosses[i].Load()
			if wins+losses > 0 {
				wlStr = fmt.Sprintf("%d/%d", wins, losses)
			}
		}

		priceStr := fmt.Sprintf("$%.2f", price)
		fmt.Printf("║ %-6s │ %12s │ %9s │ %6s │ %7s │ %8s ║\n",
			i.String(), priceStr, smaStr, devStr, wlStr, corrStr)
	}

	// — Footer —
	fmt.Println("\033[1m╠══════════════════════════════════════════════════════════════════════╣\033[0m")
	fmt.Printf("║ Markets: %d | Inflight: %d | Updated: %s              ║\n",
		state.BookCount(), state.InflightCount(),
		time.Now().Format("15:04:05"))
	fmt.Println("\033[1m╚══════════════════════════════════════════════════════════════════════╝\033[0m")
}

// progressBar generates a visual progress bar.
func progressBar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100.0 * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled

	bar := "\033[32m" + strings.Repeat("█", filled) + "\033[0m" + strings.Repeat("░", empty)
	return bar
}

// RunLoop renders the panel every 500ms until done is closed.
func RunLoop(state *models.SharedState, hub *models.SharedMemoryHub, tracker *models.DailyTracker, done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if !state.IsKilled() {
				Render(state, hub, tracker)
			}
		}
	}
}
