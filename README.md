# Polymarket BTC/ETH 5m Sniper

Sniper HFT para mercados de 5 minutos en Polymarket, orientado a capturar desfases de precio contra Binance en los últimos 20 segundos de cierre.

## Arquitectura

- `main.py`: interfaz, panel en tiempo real y orquestación async.
- `modules/crypto_feed.py`: WebSocket Binance (`btcusdt@markPrice`) con hot cache en memoria.
- `modules/polymarket_monitor.py`: WebSocket Polymarket con keep-alive y parsing tolerante.
- `modules/arbitrage_engine.py`: lógica sniper (trigger de cierre, stake fijo, kill switch).
- `modules/web3_executor.py`: ejecución, nonce local y profit sweep.
- `models.py`: modelos de runtime con `__slots__` para menor overhead.

## Lógica sniper implementada

- Ventana: dispara solo si faltan `< 20s` para el cierre.
- Condición: `Binance mark price > strike` y `YES < 0.94`.
- Tamaño fijo por trade: **$25.00**.
- Kill switch global: si `PnL acumulado <= -$30.00`, se detienen todos los procesos.
- Profit sweep: si un trade da ganancia, el excedente sobre $25 se envía a `SAFE_WALLET_ADDRESS` (hook listo para integración on-chain real).

## Requisitos

- Python 3.11+ recomendado (en 3.9 no está soportado `dataclass(slots=True)`).
- Dependencias:

```bash
pip install -r requirements.txt
```

## Variables de entorno recomendadas

```env
SAFE_WALLET_ADDRESS=0xYourSafeAddress
POLYMARKET_MARKET_ID=your_market_id
```

## Ejecución

```bash
python main.py
```

Al iniciar:
- limpia la terminal,
- pide `🚀 [SESSION CONFIG] Ingrese capital inicial (USD):`,
- lanza en paralelo `CryptoFeed`, `MarketMonitor`, `ArbitrageEngine` y `Web3Executor`,
- renderiza panel en tiempo real con capital inicial, PnL y estado del sniper.

## Producción 24/7

Recomendado correr con supervisor (`systemd`, `supervisord` o `pm2`), reinicio automático y logs persistentes.
