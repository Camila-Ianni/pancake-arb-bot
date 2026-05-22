# Polymarket HFT Arbitrage Bot - Production Reference

Este documento es la guia tecnica de referencia para operar el nucleo Go del bot de arbitraje HFT de Polymarket.

## Arquitectura Core

- Estrategia: arbitraje de cierre cripto de 5 minutos.
- Feeds: Binance WebSocket multi-stream contra Polymarket CLOB.
- Activos primarios: BTC, ETH, SOL y BNB.
- Implementacion de produccion: Go en `go/`.
- Logica obsoleta: cualquier feed climatico o referencia WeatherAPI no forma parte de la estrategia operativa.

Repositorios:

```text
HTTPS: https://github.com/Camila-Ianni/polymarket-arb-bot.git
SSH: git@github.com:Camila-Ianni/polymarket-arb-bot.git
GitHub CLI: gh repo clone Camila-Ianni/polymarket-arb-bot
```

## Hot Path

### SIMD Ranking

`go/internal/simd/simd.go` usa loop unrolling estricto para evitar arrays intermedios en heap:

```go
package simd

import "github.com/polymarket-arb-bot/internal/models"

const fpScale = uint64(models.FPScale)

func ComputeSpreadsAndRank(
	prices [4]uint64,
	strikes [4]uint64,
	yesPrices [4]uint64,
) (bestIdx int, bestEV uint64, spreads [4]uint64) {
	spreadSIMD(&prices, &strikes, &spreads)

	var ev0, ev1, ev2, ev3 uint64

	if spreads[0] > 0 {
		comp := fpScale - yesPrices[0]
		if yesPrices[0] >= fpScale {
			comp = 0
		}
		ev0 = spreads[0] * (comp >> 16)
	}
	if spreads[1] > 0 {
		comp := fpScale - yesPrices[1]
		if yesPrices[1] >= fpScale {
			comp = 0
		}
		ev1 = spreads[1] * (comp >> 16)
	}
	if spreads[2] > 0 {
		comp := fpScale - yesPrices[2]
		if yesPrices[2] >= fpScale {
			comp = 0
		}
		ev2 = spreads[2] * (comp >> 16)
	}
	if spreads[3] > 0 {
		comp := fpScale - yesPrices[3]
		if yesPrices[3] >= fpScale {
			comp = 0
		}
		ev3 = spreads[3] * (comp >> 16)
	}

	bestIdx = 0
	bestEV = ev0
	if ev1 > bestEV {
		bestIdx = 1
		bestEV = ev1
	}
	if ev2 > bestEV {
		bestIdx = 2
		bestEV = ev2
	}
	if ev3 > bestEV {
		bestIdx = 3
		bestEV = ev3
	}

	return bestIdx, bestEV, spreads
}
```

En `go/internal/simd/spread_arm64.go`, las funciones ASM deben conservar `//go:noescape` para que el compilador no fuerce escapes al heap al pasar punteros a arrays:

```go
//go:noescape
func spreadSIMD(prices, strikes, result *[4]uint64)

//go:noescape
func maxIndexSIMD(values *[4]uint64) uint64
```

### Evaluacion de Activos

`go/internal/engine/arbitrage.go` evita conversiones `decimal.Decimal` durante las condiciones del hot path:

- `YesPriceF64` se calcula al parsear el book en `go/internal/feed/polymarket.go`.
- `MarketBook` conserva `YesPrice` para la senal y `YesPriceF64` para comparacion rapida.
- Las comprobaciones de stake usan centavos `int64`.
- `decimal.Decimal` solo se crea despues de superar todas las condiciones de disparo.

## Benchmarks Locales

Ejecutar en macOS Apple Silicon:

```bash
cd go
go mod tidy
go test -bench=BenchmarkTimeToTrade -benchmem -count=3 ./bench/
go test -bench=BenchmarkSIMDSpread -benchmem -count=3 ./bench/
go build -tags arm64 -ldflags="-s -w" -o sniper cmd/sniper/main.go
cd ..
```

Metas obligatorias:

```text
BenchmarkTimeToTrade: ~78 ns/op, 0 B/op, 0 allocs/op
BenchmarkSIMDSpread: ~6.2 ns/op, 0 B/op, 0 allocs/op
```

## Configuracion `.env`

`.env` reside en la raiz y esta ignorado por Git. En `DRY_RUN=true`, dejar `PRIVATE_KEY` vacio permite que el loader use una clave dummy valida. Para operacion real, reemplazarla por una clave hex privada real sin `0x`.

```env
PRIVATE_KEY=
WALLET_ADDRESS=
SAFE_WALLET_ADDRESS=

RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/tu_api_key_de_alchemy
RPC_URL_FAILOVER=https://polygon-mainnet.g.alchemy.com/v2/tu_api_key_de_backup
USDC_ADDRESS=0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359

POLYMARKET_API_KEY=tu_api_key_de_polymarket_clob
POLYMARKET_MARKETS=BTC:0x_mkt_btc_id:0x_cond_btc_id,ETH:0x_mkt_eth_id:0x_cond_eth_id,SOL:0x_mkt_sol_id:0x_cond_sol_id,BNB:0x_mkt_bnb_id:0x_cond_bnb_id

DRY_RUN=true
LOG_LEVEL=INFO
LOG_FILE_PATH=/var/log/polymarket-arb/sniper.log

MAX_CONSECUTIVE_LOSSES=3
MAX_FEED_LATENCY_MS=500
MAX_FAILED_TRANSACTIONS=5
CIRCUIT_BREAKER_COOLDOWN_SEC=300

PROFIT_SWEEP_ENABLED=true
PROFIT_SWEEP_THRESHOLD_USD=500

QUEUE_MAX_SIZE=2000
NETWORK_TIMEOUT_SEC=5
MAX_RETRIES=3
RETRY_DELAY_SEC=0.1
```

## Linux 24/7

El script `deploy_production.sh` compila para Linux amd64, crea logs persistentes y registra `sniper.service` en systemd con:

```text
GOGC=1600
GOMEMLIMIT=256MiB
```

## Checklist Operativo

1. Mantener `DRY_RUN=true` durante preflight.
2. Ejecutar `./sniper` y cargar capital virtual.
3. Verificar panel TUI y feeds BTC, ETH, SOL y BNB.
4. Observar al menos un ciclo completo de cierre de mercado.
5. Confirmar que EV y spreads sean consistentes antes de capital real.
6. En Linux, cambiar `DRY_RUN=false` y ejecutar `sudo systemctl restart sniper`.
