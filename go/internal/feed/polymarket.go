package feed

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Feed interface {}

type PolymarketFeed struct {
	logger *zap.Logger
}

func NewPolymarketFeed(apiKey, proxy string) (*PolymarketFeed, error) {
	return &PolymarketFeed{}, nil
}

type MockFeed struct{}
func NewMockFeed() (*MockFeed, error) {
	return &MockFeed{}, nil
}

func (p *PolymarketFeed) Start(ctx context.Context, ch chan<- map[string]float64) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	prices := map[string]float64{
		"BTC": 67420.00, "ETH": 3480.50, "SOL": 145.20,
		"BNB": 580.10, "DOGE": 0.142, "MATIC": 0.68,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for k := range prices {
				prices[k] += float64(time.Now().UnixNano()%10) * 0.002
				ch <- map[string]float64{k: prices[k]}
			}
		}
	}
}
