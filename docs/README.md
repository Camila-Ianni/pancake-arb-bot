# Guía de uso - Polymarket Multi-Asset Sniper

Esta guía explica, paso a paso, qué instalar, qué configurar y cómo ejecutar el bot para operar correctamente.

## 1. Qué hace este bot

El bot detecta oportunidades de arbitraje de cierre en mercados de 5 minutos de Polymarket comparando precios con Binance para:
- BTC
- ETH
- SOL
- BNB

Flujo técnico:
1. `CryptoFeed` recibe mark price de Binance por websocket multi-stream.
2. `PolymarketMonitor` cachea mercados de Polymarket por activo.
3. `ArbitrageEngine` evalúa gatillos en ventana `< 20s`.
4. `Web3Executor` ejecuta órdenes con nonce local y sweep condicional.

---

## 2. Requisitos para funcionar bien

## Hardware / sistema
- Linux recomendado para 24/7 (Ubuntu 22.04+).
- CPU moderna (2+ cores).
- RAM: mínimo 2 GB (recomendado 4 GB).
- Conexión de red estable y baja latencia.

## Software
- Python 3.11+ recomendado.
- `pip`
- `venv`

## Cuentas / accesos necesarios
- RPC de Polygon (`RPC_URL`) con buen SLA (Alchemy/QuickNode).
- API key de Polymarket (`POLYMARKET_API_KEY`).
- Wallet en Polygon con:
  - USDC para operar.
  - MATIC para gas.

---

## 3. Instalación paso a paso

1. Clonar repositorio y entrar al proyecto:

```bash
cd polymarket-arb-bot
```

2. Crear entorno virtual:

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Instalar dependencias:

```bash
pip install -r requirements.txt
```

---

## 4. Configuración (.env)

1. Copiar plantilla:

```bash
cp .env.example .env
```

2. Completar variables críticas:

```env
PRIVATE_KEY=0x...
WALLET_ADDRESS=0x...
SAFE_WALLET_ADDRESS=0x...

RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/...
RPC_URL_FAILOVER=https://polygon-mainnet.g.alchemy.com/v2/...
USDC_ADDRESS=0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359

POLYMARKET_API_KEY=...
POLYMARKET_MARKETS=BTC:market_id_btc:condition_id_btc,ETH:market_id_eth:condition_id_eth,SOL:market_id_sol:condition_id_sol,BNB:market_id_bnb:condition_id_bnb

PROFIT_SWEEP_THRESHOLD_USD=500
PROFIT_SWEEP_ENABLED=true
```

Notas:
- `POLYMARKET_MARKETS` define el mapeo activo → mercado.
- Si falta o está mal, el monitor no va a cachear mercados correctamente.
- El bot hace preflight y corta si falla RPC o API key.

---

## 5. Cómo ejecutarlo

```bash
python main.py
```

Orden de arranque:
1. Preflight de conectividad (RPC Polygon + API key Polymarket).
2. Prompt de capital inicial.
3. Inicio concurrente de módulos.
4. Panel en tiempo real.

---

## 6. Qué muestra el panel

- Capital inicial.
- PnL acumulado.
- Balance USDC en tiempo real.
- Precios Binance BTC/ETH/SOL/BNB.
- Estado de los 4 feeds Binance (`UP/DOWN`).
- Mercados cacheados e inflight.
- Estado general del sniper.

---

## 7. Reglas de trading activas

- Ventana de disparo: `< 20s` al cierre.
- Condición: `mark_price > strike` y `YES < 0.94`.
- Stake dinámico: `95%` del balance USDC.
- Sanity check: no dispara si balance/stake útil `< $1`.
- Kill switch: corta todo si PnL acumulado cruza umbral negativo.
- Profit sweep: solo si el balance supera `PROFIT_SWEEP_THRESHOLD_USD`.

---

## 8. Operación 24/7 recomendada

Usar un supervisor de procesos:
- `systemd` (recomendado)
- `supervisord`
- `pm2`

Y además:
- reinicio automático,
- logs persistentes,
- alertas (CPU, memoria, caídas, latencia de red).

---

## 9. Checklist rápido antes de producción

1. `DRY_RUN=true` en pruebas.
2. Verificar RPC estable.
3. Confirmar `POLYMARKET_MARKETS` correcto.
4. Confirmar fondos USDC + MATIC.
5. Validar que el panel muestre los 4 feeds en `UP`.
6. Recién después pasar a modo real.
