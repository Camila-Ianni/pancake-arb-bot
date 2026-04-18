// Package config loads and validates all environment variables at startup.
// Values are cached in a frozen struct for O(1) access on the hot path.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Config sub-structs (all read-only after load)
// ---------------------------------------------------------------------------

// WalletConfig holds RPC and wallet credentials.
type WalletConfig struct {
	PrivateKey         string
	RPCUrl             string
	RPCUrlFailover     string
	WalletAddress      string
	SafeWalletAddress  string
}

// PolymarketConfig holds Polymarket CLOB connection details.
type PolymarketConfig struct {
	APIKey      string
	ConditionID string
	MarketIDs   []string
	// Parsed market map: asset -> {market_id, condition_id}
	Markets map[string]MarketEntry
}

// MarketEntry represents a single Polymarket market.
type MarketEntry struct {
	Asset       string
	MarketID    string
	ConditionID string
}

// TradingConfig holds execution parameters.
type TradingConfig struct {
	BetSizeUSD           float64
	MinROIThreshold      float64
	MaxSlippageTolerance float64
	MaxGasPriceGwei      float64
	PriorityFeeGwei      float64
}

// RiskConfig holds risk management limits.
type RiskConfig struct {
	MaxConsecutiveLosses    int
	MaxFeedLatencyMs       int
	MaxFailedTransactions   int
	CircuitBreakerCooldown int // seconds
}

// ExecutionConfig holds runtime flags.
type ExecutionConfig struct {
	DryRun      bool
	LogLevel    string
	LogFilePath string
}

// PerformanceConfig holds tuning parameters.
type PerformanceConfig struct {
	QueueMaxSize     int
	NetworkTimeoutS  int
	MaxRetries       int
	RetryDelayMs     int // milliseconds
}

// RuntimeConfig holds dynamic runtime parameters (can be adjusted mid-run).
type RuntimeConfig struct {
	KillSwitchPnlUSD         decimal.Decimal
	YesPriceMax              decimal.Decimal
	CloseWindowSec           int
	TargetInternalLatencyMs  float64
	StakeUsage               decimal.Decimal
	ProfitSweepThresholdUSD  decimal.Decimal
	ProfitSweepEnabled       bool
	MaxParallelSignals       int
}

// AppConfig is the top-level immutable config loaded once at startup.
type AppConfig struct {
	Wallet      WalletConfig
	Polymarket  PolymarketConfig
	Trading     TradingConfig
	Risk        RiskConfig
	Execution   ExecutionConfig
	Performance PerformanceConfig
	Runtime     RuntimeConfig
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

// Load reads the .env file (if present) and returns a validated AppConfig.
func Load(logger *zap.Logger) (*AppConfig, error) {
	// Try to load .env from the project root (one level up from go/)
	envPaths := []string{
		filepath.Join("..", ".env"),
		".env",
	}
	for _, p := range envPaths {
		if _, err := os.Stat(p); err == nil {
			_ = godotenv.Load(p)
			logger.Info("loaded .env", zap.String("path", p))
			break
		}
	}

	// -- Determine DRY_RUN early (needed to decide which vars are required) --
	dryRun := getBoolEnv("DRY_RUN", true)

	// -- Wallet --
	// In DRY_RUN mode, PRIVATE_KEY and RPC_URL are optional.
	// A dummy 64-hex-char key is used so the executor can still init.
	privateKey := getEnv("PRIVATE_KEY", "")
	rpcURL := getEnv("RPC_URL", "")

	if !dryRun {
		if privateKey == "" {
			return nil, fmt.Errorf("PRIVATE_KEY is required when DRY_RUN=false")
		}
		if rpcURL == "" {
			return nil, fmt.Errorf("RPC_URL is required when DRY_RUN=false")
		}
	}
	if privateKey == "" {
		// Dummy key for dry-run: valid 32-byte hex so crypto.HexToECDSA won't panic.
		privateKey = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
		logger.Warn("using dummy PRIVATE_KEY (DRY_RUN mode)")
	}
	if rpcURL == "" {
		rpcURL = "https://polygon-rpc.com"
		logger.Warn("using public RPC_URL fallback (DRY_RUN mode)")
	}

	wallet := WalletConfig{
		PrivateKey:        privateKey,
		RPCUrl:            rpcURL,
		RPCUrlFailover:    getEnv("RPC_URL_FAILOVER", ""),
		WalletAddress:     getEnv("WALLET_ADDRESS", ""),
		SafeWalletAddress: getEnv("SAFE_WALLET_ADDRESS", "0xSAFE_WALLET"),
	}

	// -- Polymarket --
	polymarket := PolymarketConfig{
		APIKey:      getEnv("POLYMARKET_API_KEY", ""),
		ConditionID: getEnv("CONDITION_ID", ""),
		MarketIDs:   parseListEnv("MARKET_IDS", ","),
		Markets:     parsePolymarketMarkets(getEnv("POLYMARKET_MARKETS", "")),
	}

	// -- Trading --
	trading := TradingConfig{
		BetSizeUSD:           getFloatEnv("BET_SIZE_USD", 100.0),
		MinROIThreshold:      getFloatEnv("MIN_ROI_THRESHOLD", 0.08),
		MaxSlippageTolerance: getFloatEnv("MAX_SLIPPAGE_TOLERANCE", 0.02),
		MaxGasPriceGwei:      getFloatEnv("MAX_GAS_PRICE_GWEI", 150.0),
		PriorityFeeGwei:      getFloatEnv("PRIORITY_FEE_GWEI", 2.0),
	}

	// -- Risk --
	risk := RiskConfig{
		MaxConsecutiveLosses:    getIntEnv("MAX_CONSECUTIVE_LOSSES", 3),
		MaxFeedLatencyMs:       getIntEnv("MAX_FEED_LATENCY_MS", 500),
		MaxFailedTransactions:   getIntEnv("MAX_FAILED_TRANSACTIONS", 5),
		CircuitBreakerCooldown: getIntEnv("CIRCUIT_BREAKER_COOLDOWN_SEC", 300),
	}

	// -- Execution --
	execution := ExecutionConfig{
		DryRun:      dryRun,
		LogLevel:    getEnv("LOG_LEVEL", "INFO"),
		LogFilePath: getEnv("LOG_FILE_PATH", ""),
	}

	// -- Performance --
	performance := PerformanceConfig{
		QueueMaxSize:    getIntEnv("QUEUE_MAX_SIZE", 2000),
		NetworkTimeoutS: getIntEnv("NETWORK_TIMEOUT_SEC", 5),
		MaxRetries:      getIntEnv("MAX_RETRIES", 3),
		RetryDelayMs:    int(getFloatEnv("RETRY_DELAY_SEC", 0.1) * 1000),
	}

	// -- Runtime --
	runtime := RuntimeConfig{
		KillSwitchPnlUSD:        decimal.NewFromFloat(-30.0),
		YesPriceMax:             decimal.NewFromFloat(0.94),
		CloseWindowSec:          20,
		TargetInternalLatencyMs: 10.0,
		StakeUsage:              decimal.NewFromFloat(0.95),
		ProfitSweepThresholdUSD: decimalFromEnv("PROFIT_SWEEP_THRESHOLD_USD", decimal.NewFromFloat(500.0)),
		ProfitSweepEnabled:      getBoolEnv("PROFIT_SWEEP_ENABLED", true),
		MaxParallelSignals:      8,
	}

	// -- Cross-validation --
	if trading.MinROIThreshold <= trading.MaxSlippageTolerance {
		logger.Warn("MIN_ROI_THRESHOLD should exceed MAX_SLIPPAGE_TOLERANCE",
			zap.Float64("roi", trading.MinROIThreshold),
			zap.Float64("slippage", trading.MaxSlippageTolerance))
	}
	if performance.QueueMaxSize < 100 {
		logger.Warn("QUEUE_MAX_SIZE is very small, may cause data loss",
			zap.Int("size", performance.QueueMaxSize))
	}

	cfg := &AppConfig{
		Wallet:      wallet,
		Polymarket:  polymarket,
		Trading:     trading,
		Risk:        risk,
		Execution:   execution,
		Performance: performance,
		Runtime:     runtime,
	}

	logger.Info("config loaded successfully",
		zap.Bool("dry_run", cfg.Execution.DryRun),
		zap.Float64("min_roi", cfg.Trading.MinROIThreshold),
		zap.String("condition_id", cfg.Polymarket.ConditionID))

	return cfg, nil
}

// ---------------------------------------------------------------------------
// Helpers (no reflection, no generics overhead)
// ---------------------------------------------------------------------------

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var missing: %s", key))
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getFloatEnv(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func getBoolEnv(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

func parseListEnv(key, sep string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func decimalFromEnv(key string, fallback decimal.Decimal) decimal.Decimal {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := decimal.NewFromString(v)
	if err != nil {
		return fallback
	}
	return d
}

func parsePolymarketMarkets(raw string) map[string]MarketEntry {
	m := make(map[string]MarketEntry)
	if raw == "" {
		return m
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 {
			continue
		}
		asset := strings.ToUpper(strings.TrimSpace(parts[0]))
		m[asset] = MarketEntry{
			Asset:       asset,
			MarketID:    strings.TrimSpace(parts[1]),
			ConditionID: strings.TrimSpace(parts[2]),
		}
	}
	return m
}
