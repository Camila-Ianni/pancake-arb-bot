package feed

import (
	"strings"
	"time"

	json "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/models"
)

const polymarketWSURL = "wss://clob.polymarket.com/ws"

type polyMsg struct {
	Symbol      string `json:"symbol"`
	MarketID    string `json:"market_id"`
	ConditionID string `json:"condition_id"`
	YesPrice    string `json:"yes_price"`
	BestAsk     string `json:"bestAsk"`
	Price       string `json:"price"`
	StrikePrice string `json:"strike_price"`
	Strike      string `json:"strike"`
	CloseTs     *int64 `json:"market_close_ts"`
	EndTs       *int64 `json:"end_ts"`
}

// RunPolymarketFeed connects to the Polymarket CLOB WebSocket and updates
// the shared state's book cache. Runs until done is closed.
func RunPolymarketFeed(
	state *models.SharedState,
	cfg *config.AppConfig,
	logger *zap.Logger,
	done <-chan struct{},
) {
	for {
		select {
		case <-done:
			return
		default:
		}

		logger.Info("polymarket_ws: connecting")
		conn, _, err := websocket.DefaultDialer.Dial(polymarketWSURL, nil)
		if err != nil {
			logger.Warn("polymarket_ws: dial failed", zap.Error(err))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		conn.SetReadLimit(2_000_000)

		// Disable Nagle's algorithm for minimum latency.
		setTCPNoDelay(conn)

		// Subscribe to markets.
		subscribe(conn, cfg, logger)

		func() {
			defer conn.Close()
			for {
				select {
				case <-done:
					return
				default:
				}

				_, raw, err := conn.ReadMessage()
				if err != nil {
					logger.Warn("polymarket_ws: read error", zap.Error(err))
					return
				}

				var msg polyMsg
				if err := json.Unmarshal(raw, &msg); err != nil {
					continue
				}

				book := parsePolyMsg(&msg, cfg, logger)
				if book == nil {
					continue
				}
				asset := book.asset
				state.SetBook(asset, &models.MarketBook{
					MarketID:      book.marketID,
					ConditionID:   book.conditionID,
					YesPrice:      book.yesPrice,
					YesPriceF64:   book.yesPriceF64,
					StrikePrice:   book.strikePrice,
					MarketCloseTs: book.closeTs,
					UpdatedNs:     models.NowNs(),
				})
			}
		}()
	}
}

type parsedBook struct {
	asset       models.SniperAsset
	marketID    string
	conditionID string
	yesPrice    decimal.Decimal
	yesPriceF64 float64
	strikePrice float64
	closeTs     int64
}

func subscribe(conn *websocket.Conn, cfg *config.AppConfig, logger *zap.Logger) {
	if len(cfg.Polymarket.Markets) > 0 {
		for _, m := range cfg.Polymarket.Markets {
			msg := map[string]string{
				"type": "subscribe", "channel": "market", "market": m.MarketID,
			}
			b, _ := json.Marshal(msg)
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				logger.Warn("polymarket_ws: subscribe failed", zap.Error(err))
			}
		}
	} else {
		msg := map[string]string{"type": "subscribe", "channel": "ticker"}
		b, _ := json.Marshal(msg)
		_ = conn.WriteMessage(websocket.TextMessage, b)
	}
}

func assetFromName(name string) (models.SniperAsset, bool) {
	up := strings.ToUpper(name)
	if strings.Contains(up, "BTC") {
		return models.AssetBTC, true
	}
	if strings.Contains(up, "ETH") {
		return models.AssetETH, true
	}
	if strings.Contains(up, "SOL") {
		return models.AssetSOL, true
	}
	if strings.Contains(up, "BNB") {
		return models.AssetBNB, true
	}
	return 0, false
}

func parsePolyMsg(msg *polyMsg, cfg *config.AppConfig, _ *zap.Logger) *parsedBook {
	yesRaw := msg.YesPrice
	if yesRaw == "" {
		yesRaw = msg.BestAsk
	}
	if yesRaw == "" {
		yesRaw = msg.Price
	}
	if yesRaw == "" {
		return nil
	}

	asset, ok := assetFromName(msg.Symbol)
	marketID := msg.MarketID
	conditionID := msg.ConditionID

	if !ok && marketID != "" {
		for _, m := range cfg.Polymarket.Markets {
			if m.MarketID == marketID {
				asset, ok = models.AssetFromTag(m.Asset)
				conditionID = m.ConditionID
				break
			}
		}
	}
	if !ok {
		return nil
	}
	if marketID == "" {
		if m, exists := cfg.Polymarket.Markets[asset.String()]; exists {
			marketID = m.MarketID
			conditionID = m.ConditionID
		}
	}
	if marketID == "" {
		return nil
	}

	yesPrice, err := decimal.NewFromString(yesRaw)
	if err != nil {
		return nil
	}
	yesPriceF64, _ := yesPrice.Float64()

	strikeStr := msg.StrikePrice
	if strikeStr == "" {
		strikeStr = msg.Strike
	}
	strike := fastParseFloat(strikeStr)

	var closeTs int64
	if msg.CloseTs != nil {
		closeTs = *msg.CloseTs
	} else if msg.EndTs != nil {
		closeTs = *msg.EndTs
	} else {
		closeTs = time.Now().Unix() + 300
	}

	return &parsedBook{
		asset:       asset,
		marketID:    marketID,
		conditionID: conditionID,
		yesPrice:    yesPrice,
		yesPriceF64: yesPriceF64,
		strikePrice: strike,
		closeTs:     closeTs,
	}
}
