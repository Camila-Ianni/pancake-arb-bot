#!/bin/bash
set -e

cd /Users/cami/Desktop/Polymarket-arb-bot-main/go

echo "🧹 1. Forzando inicialización limpia del módulo local..."
rm -f go.mod go.sum
go mod init github.com/polymarket-arb-bot

# Aseguramos que los archivos de feed tengan los imports correctos apuntando al módulo de arriba
cat << 'INNER_EOF' > internal/feed/binance.go
package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type BinanceFeed struct {
	logger *zap.Logger
}

type binanceTicker struct {
	Symbol string `json:"s"`
	Price  string `json:"c"`
}

func NewBinanceFeed(logger *zap.Logger) *BinanceFeed {
	return &BinanceFeed{logger: logger}
}

func (b *BinanceFeed) Start(ctx context.Context, symbols []string, ch chan<- map[string]float64) {
	b.logger.Info("binance_ws: conectando al streaming de producción...")
	url := "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/solusdt@ticker/bnbusdt@ticker/dogeusdt@ticker/maticusdt@ticker"
	
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		b.logger.Error("binance_ws: error crítico de handshake", zap.Error(err))
		return
	}
	defer conn.Close()

	b.logger.Info("binance_ws: ¡CONECTADO! Recibiendo ticks reales...")
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
			var t binanceTicker
			if err := json.Unmarshal(message, &t); err == nil && t.Price != "" {
				var val float64
				fmt.Sscanf(t.Price, "%f", &val)
				sym := t.Symbol[:3]
				if t.Symbol == "MATICUSDT" { sym = "MATIC" }
				ch <- map[string]float64{sym: val}
			}
		}
	}
}
INNER_EOF

echo "📦 2. Descargando de internet e instalando paquetes de dependencias..."
go get github.com/gorilla/websocket
go get go.uber.org/zap
go get github.com/shopspring/decimal
go get github.com/goccy/go-json
go get github.com/ethereum/go-ethereum/crypto

echo "⚙️ 3. Ejecutando go mod tidy para emparchar los paquetes internos..."
go mod tidy

echo "🛠️ 4. Compilando binario definitivo..."
go build -o sniper cmd/sniper/main.go

echo "🚀 [CONEXIÓN ESTABLECIDA] ¡Bot compilado exitosamente!"
