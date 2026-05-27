package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"github.com/polymarket-arb-bot/internal/models"
	"go.uber.org/zap"
)

type BinanceFeed struct {
	logger *zap.Logger
}

type binanceTicker struct {
	Symbol string `json:"s"`
	Price  string `json:"c"`
}

type binanceCombinedStream struct {
	Stream string        `json:"stream"`
	Data   binanceTicker `json:"data"`
}

func NewBinanceFeed(logger *zap.Logger) *BinanceFeed {
	return &BinanceFeed{logger: logger}
}

func (b *BinanceFeed) Start(ctx context.Context, hub *models.SharedMemoryHub) {
	b.logger.Info("binance_ws: conectando al streaming de producción...")
	
	url := "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker/solusdt@ticker/bnbusdt@ticker/dogeusdt@ticker/maticusdt@ticker"
	
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		b.logger.Error("binance_ws: error crítico de handshake", zap.Error(err))
		return
	}
	defer conn.Close()

	b.logger.Info("binance_ws: ¡CONECTADO! Extrayendo ticks reales...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			var combined binanceCombinedStream
			if err := json.Unmarshal(message, &combined); err == nil && combined.Data.Price != "" {
				var f float64
				if _, err := fmt.Sscanf(combined.Data.Price, "%f", &f); err == nil {
					rawPrice := models.FPFromFloat(f)
					
					// Mapeo estricto al enum HFT SniperAsset de tu hub.go
					var asset models.SniperAsset
					switch combined.Data.Symbol {
					case "BTCUSDT":
						asset = 0 // AssetBTC / Primer slot
					case "ETHUSDT":
						asset = 1 // AssetETH / Segundo slot
					case "SOLUSDT":
						asset = 2 // AssetSOL
					case "BNBUSDT":
						asset = 3 // AssetBNB
					case "DOGEUSDT":
						asset = 4 // AssetDOGE
					case "MATICUSDT":
						asset = 5 // AssetMATIC
					default:
						continue
					}

					// Ejecuta el método real de tu arquitectura original de GitHub
					hub.RecordPrice(asset, rawPrice)
				}
			}
		}
	}
}
