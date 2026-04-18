// Package panel renders a terminal dashboard.
package panel

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
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

// Render prints the sniper dashboard to stdout.
func Render(state *models.SharedState) {
	Clear()

	sniperLabel := "🔴 STOPPED"
	if state.GetSniperState() != models.StateStopped {
		sniperLabel = "🟢 ARMADO"
	}

	btc := state.GetPrice(models.AssetBTC)
	eth := state.GetPrice(models.AssetETH)
	sol := state.GetPrice(models.AssetSOL)
	bnb := state.GetPrice(models.AssetBNB)

	dir := func(p float64) string {
		if p > 0 {
			return "UP"
		}
		return "DOWN"
	}

	fmt.Println("======================================================================================")
	fmt.Println("🎯 POLYMARKET MULTI-ASSET SNIPER (5m CLOSE) — Go Edition")
	fmt.Println("======================================================================================")
	fmt.Printf("Capital Inicial: $%s | PnL: $%s | Estado: %s\n",
		state.GetInitialCapitalUSD().StringFixed(2),
		state.GetCumulativePnlUSD().StringFixed(2),
		sniperLabel)
	fmt.Printf("Wallet USDC: $%s | Status: %s\n",
		state.GetWalletBalanceUSD().StringFixed(2),
		state.GetStatus())
	fmt.Printf("Binance Mark | BTC: %.2f | ETH: %.2f | SOL: %.2f | BNB: %.2f\n",
		btc, eth, sol, bnb)
	fmt.Printf("Binance Dirs | BTC: %s | ETH: %s | SOL: %s | BNB: %s\n",
		dir(btc), dir(eth), dir(sol), dir(bnb))
	fmt.Printf("Markets cached: %d | Inflight: %d | Kill Switch: %v\n",
		state.BookCount(), state.InflightCount(), state.IsKilled())
	fmt.Println("======================================================================================")
}

// RunLoop renders the panel every 500ms until done is closed.
func RunLoop(state *models.SharedState, done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if !state.IsKilled() {
				Render(state)
			}
		}
	}
}
