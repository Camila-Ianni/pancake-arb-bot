package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/polymarket-arb-bot/internal/config"
	"github.com/polymarket-arb-bot/internal/engine"
	"github.com/polymarket-arb-bot/internal/executor"
	"github.com/polymarket-arb-bot/internal/feed"
	"github.com/polymarket-arb-bot/internal/models"
	"github.com/polymarket-arb-bot/internal/panel"
	"github.com/polymarket-arb-bot/internal/risk"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logFile, err := os.OpenFile("sniper_core.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	var logger *zap.Logger
	if err != nil {
		logger = zap.NewNop()
	} else {
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.AddSync(logFile),
			zap.InfoLevel,
		)
		logger = zap.New(core)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load()

	// Inicialización de las estructuras de control reales de tu GitHub
	state := models.NewSharedState()
	hub := models.NewSharedMemoryHub()
	tracker := models.NewDailyTracker()

	execReq := make(chan models.ExecutionRequest, 1000)
	execRes := make(chan models.ExecutionResult, 1000)

	// Módulos de riesgo y ejecución de fábrica
	cb := risk.NewCircuitBreaker(cfg, logger)
	exec := executor.NewWeb3Executor(cfg, cb, logger)
	go exec.Start(ctx, execReq, execRes)

	// Invocación del Motor pasándole las 7 dependencias exactas de producción
	arbEngine := engine.New(state, cfg, hub, tracker, execReq, execRes, logger)
	stopChan := make(chan struct{})
	go arbEngine.Run(stopChan)

	// Feeds asincrónicos alimentando la memoria compartida
	binanceFeed := feed.NewBinanceFeed(logger)
	go binanceFeed.Start(ctx, hub)

	polyFeed := feed.NewPolymarketFeed(cfg, logger)
	go polyFeed.Start(ctx, hub)

	// LEVANTAR EL PANEL GRÁFICO INTERACTIVO ORIGINAL DE TU REPO
	uiPanel := panel.NewPanel(state, hub, tracker)
	if err := uiPanel.Init(); err != nil {
		fmt.Printf("Error crítico al inicializar la UI: %v\n", err)
		return
	}

	// Loop asincrónico que fuerza el refresco de los cuadros y te pide el input inicial
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				uiPanel.Render()
			case <-ctx.Done():
				return
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	uiPanel.Close()
	close(stopChan)
	cancel()
	time.Sleep(200 * time.Millisecond)
	fmt.Println("\n▶ Sniper apagado de forma limpia. Terminal liberada.")
}
