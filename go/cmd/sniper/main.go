// Command sniper is the entry point for the Polymarket multi-asset arb bot.
//
// Architecture:
//
//	┌─────────────┐     ┌──────────────┐
//	│ Binance WS  │────▶│              │
//	│ (goroutine) │     │  Arbitrage   │──── chan ExecReq ────▶ Executor Pool
//	│             │     │   Engine     │◀─── chan ExecRes ──── (N goroutines)
//	└─────────────┘     │ (goroutine)  │
//	┌─────────────┐     │              │
//	│ Polymarket  │────▶│              │
//	│ WS (gortn)  │     └──────────────┘
//	└─────────────┘            │
//	                    ┌──────┴──────┐
//	                    │ Panel TUI   │
//	                    │ (goroutine) │
//	                    └─────────────┘
package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	json "github.com/goccy/go-json"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/engine"
	"github.com/polymarket-arb-bot/internal/executor"
	"github.com/polymarket-arb-bot/internal/feed"
	"github.com/polymarket-arb-bot/internal/models"
	"github.com/polymarket-arb-bot/internal/panel"
)

func main() {
	// ── Logger ──────────────────────────────────────────────────────────
	logCfg := zap.NewProductionConfig()
	logCfg.EncoderConfig.TimeKey = "ts"
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := logCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// ── Config ──────────────────────────────────────────────────────────
	cfg, err := config.Load(logger)
	if err != nil {
		logger.Fatal("config load failed", zap.Error(err))
	}

	// ── Preflight connectivity ──────────────────────────────────────────
	if err := preflightChecks(cfg, logger); err != nil {
		logger.Fatal("preflight failed", zap.Error(err))
	}

	// ── Initial capital ─────────────────────────────────────────────────
	capital := askInitialCapital()
	state := models.NewSharedState(capital)
	state.SetSniperState(models.StateArmed)
	logger.Info("session started",
		zap.String("capital", capital.String()),
		zap.Bool("dry_run", cfg.Execution.DryRun))

	// ── Channels ────────────────────────────────────────────────────────
	execCh := make(chan models.ExecutionRequest, cfg.Performance.QueueMaxSize)
	resCh := make(chan models.ExecutionResult, cfg.Performance.QueueMaxSize)

	// ── Shutdown ─────────────────────────────────────────────────────────
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OS signal handler.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigCh:
			logger.Info("shutdown signal received")
			close(done)
			cancel()
		case <-ctx.Done():
		}
	}()

	// ── Start goroutines ────────────────────────────────────────────────

	// 1. Binance feed
	go feed.RunBinanceFeed(state, logger.Named("binance"), done)

	// 2. Polymarket feed
	go feed.RunPolymarketFeed(state, cfg, logger.Named("polymarket"), done)

	// 3. Executor pool
	exec, err := executor.New(execCh, resCh, cfg, state, logger.Named("executor"))
	if err != nil {
		logger.Fatal("executor init failed", zap.Error(err))
	}
	go exec.Run(done, cfg.Runtime.MaxParallelSignals)

	// 4. Arbitrage engine
	eng := engine.New(state, cfg, execCh, resCh, logger.Named("engine"))
	go eng.Run(done)

	// 5. Panel TUI
	go panel.RunLoop(state, done)

	// ── Main loop: wait for kill switch or OS signal ─────────────────────
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			goto shutdown
		case <-ticker.C:
			if state.IsKilled() {
				close(done)
				goto shutdown
			}
		}
	}

shutdown:
	state.SetSniperState(models.StateStopped)
	panel.Render(state)
	logger.Info("sniper shut down cleanly",
		zap.String("pnl", state.GetCumulativePnlUSD().String()))
}

// ─── Preflight ──────────────────────────────────────────────────────────────

func preflightChecks(cfg *config.AppConfig, logger *zap.Logger) error {
	// In DRY_RUN mode, skip connectivity checks — credentials may be dummy.
	if cfg.Execution.DryRun {
		logger.Info("⏩ DRY_RUN mode — skipping preflight connectivity checks")
		return nil
	}

	client := &http.Client{Timeout: 5 * time.Second}

	// Check Polygon RPC.
	rpcOK, err := checkPolygonRPC(client, cfg.Wallet.RPCUrl)
	if err != nil || !rpcOK {
		return fmt.Errorf("polygon RPC check failed: %w", err)
	}
	logger.Info("✅ Polygon RPC OK")

	// Check Polymarket API key.
	if cfg.Polymarket.APIKey != "" {
		keyOK, err := checkPolymarketKey(client, cfg.Polymarket.APIKey)
		if err != nil || !keyOK {
			return fmt.Errorf("polymarket API key check failed: %w", err)
		}
		logger.Info("✅ Polymarket API Key OK")
	}

	return nil
}

func checkPolygonRPC(client *http.Client, rpcURL string) (bool, error) {
	payload := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`
	resp, err := client.Post(rpcURL, "application/json", strings.NewReader(payload))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}
	r := strings.ToLower(result.Result)
	return r == "0x89" || r == "137", nil
}

func checkPolymarketKey(client *http.Client, apiKey string) (bool, error) {
	req, _ := http.NewRequest("GET", "https://gamma-api.polymarket.com/markets", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200 || resp.StatusCode == 401 || resp.StatusCode == 403, nil
}

// ─── Capital prompt ─────────────────────────────────────────────────────────

func askInitialCapital() decimal.Decimal {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("🚀 [SESSION CONFIG] Ingrese capital inicial (USD): ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		d, err := decimal.NewFromString(line)
		if err != nil || d.LessThanOrEqual(decimal.Zero) {
			fmt.Println("Entrada inválida. Debe ser un número mayor a 0.")
			continue
		}
		return d
	}
}
