// Package feed implements the Binance WebSocket mark-price feed.
// Runs as a dedicated goroutine, pushing BinanceTick values into a channel.
package feed

import (
	"strings"
	"time"

	json "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/models"
)

const binanceWSURL = "wss://stream.binance.com:9443/stream?streams=btcusdt@markPrice/ethusdt@markPrice/solusdt@markPrice/bnbusdt@markPrice"

// binanceMsg is the minimal struct for zero-reflect JSON parsing.
type binanceMsg struct {
	Data struct {
		S string `json:"s"` // symbol
		P string `json:"p"` // mark price
		E int64  `json:"E"` // event time ms
	} `json:"data"`
}

// RunBinanceFeed connects to Binance and publishes ticks.
// It reconnects automatically on failure. Cancel via context or close the done channel.
func RunBinanceFeed(state *models.SharedState, logger *zap.Logger, done <-chan struct{}) {
	metrics := &models.FeedMetrics{}
	_ = metrics // expose via state if needed

	for {
		select {
		case <-done:
			return
		default:
		}

		logger.Info("binance_ws: connecting")
		conn, _, err := websocket.DefaultDialer.Dial(binanceWSURL, nil)
		if err != nil {
			logger.Warn("binance_ws: dial failed", zap.Error(err))
			metrics.Reconnects++
			time.Sleep(250 * time.Millisecond)
			continue
		}
		conn.SetReadLimit(2_000_000)

		func() {
			defer conn.Close()
			var msg binanceMsg

			for {
				select {
				case <-done:
					return
				default:
				}

				parseStart := time.Now()
				_, raw, err := conn.ReadMessage()
				if err != nil {
					logger.Warn("binance_ws: read error", zap.Error(err))
					metrics.Reconnects++
					return
				}

				if err := json.Unmarshal(raw, &msg); err != nil {
					continue
				}

				sym := strings.ToUpper(msg.Data.S)
				asset, ok := models.AssetFromSymbol(sym)
				if !ok {
					continue
				}

				price := fastParseFloat(msg.Data.P)
				recvNs := models.NowNs()
				parseMs := float64(time.Since(parseStart).Nanoseconds()) / 1e6
				metrics.RecordParseMs(parseMs)

				state.SetPrice(asset, price)
				state.SetLastBinanceNs(recvNs)
			}
		}()
	}
}

// fastParseFloat is a minimal float parser avoiding strconv overhead for
// simple decimal strings like "67543.12000000".
func fastParseFloat(s string) float64 {
	var result float64
	var divisor float64
	decimal := false
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			decimal = true
			divisor = 1
			continue
		}
		if c < '0' || c > '9' {
			break
		}
		if decimal {
			divisor *= 10
			result += float64(c-'0') / divisor
		} else {
			result = result*10 + float64(c-'0')
		}
	}
	if neg {
		result = -result
	}
	return result
}
