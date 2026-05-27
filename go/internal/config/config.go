package config

import (
	"os"
	"sync"

	"github.com/polymarket-arb-bot/internal/models"
	"github.com/shopspring/decimal"
)

type RuntimeConfig struct {
	Velocity                float64           `json:"velocity"`
	IsKilled                bool              `json:"is_killed"`
	YesPriceMax             models.FixedPoint `json:"yes_price_max"`
	KillSwitchPnlUSD        decimal.Decimal   `json:"kill_switch_pnl_usd"`
	CloseWindowSec          int               `json:"close_window_sec"`
	ProfitSweepThresholdUSD decimal.Decimal   `json:"profit_sweep_threshold_usd"`
	ProfitSweepEnabled      bool              `json:"profit_sweep_enabled"`
}

type WalletConfig struct {
	Address           string `json:"address"`
	PrivateKey        string `json:"private_key"`
	SafeWalletAddress string `json:"safe_wallet_address"`
}

type Config struct {
	DryRun   bool          `json:"dry_run"`
	ApiKey   string        `json:"api_key"`
	Markets  string        `json:"markets"`
	LogPath  string        `json:"log_path"`
	Proxy    string        `json:"proxy"`
	ClobURL  string        `json:"clob_url"`
	Runtime  RuntimeConfig `json:"runtime"`
	Wallet   WalletConfig  `json:"wallet"`
}

var (
	cfg  *Config
	once sync.Once
)

func Load() *Config {
	once.Do(func() {
		dryRun := os.Getenv("DRY_RUN")
		cfg = &Config{
			DryRun:  dryRun == "true",
			ApiKey:  os.Getenv("POLYMARKET_API_KEY"),
			Markets: os.Getenv("POLYMARKET_MARKETS"),
			LogPath: os.Getenv("LOG_FILE_PATH"),
			Proxy:   os.Getenv("HTTPS_PROXY"),
			ClobURL: "wss://clob.polymarket.com/subscribe",
			Wallet: WalletConfig{
				Address:           os.Getenv("WALLET_ADDRESS"),
				PrivateKey:        os.Getenv("WALLET_PRIVATE_KEY"),
				SafeWalletAddress: os.Getenv("SAFE_WALLET_ADDRESS"),
			},
			Runtime: RuntimeConfig{
				Velocity:         1.0,
				IsKilled:         false,
				YesPriceMax:      models.FPFromFloat(0.99),
				KillSwitchPnlUSD: decimal.NewFromFloat(100.0),
				CloseWindowSec:   30,
				ProfitSweepThresholdUSD: decimal.NewFromFloat(50.0),
				ProfitSweepEnabled:      false,
			},
		}
	})
	return cfg
}

type AppConfig = Config
