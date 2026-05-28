"""CryptoFeed multi-activo para Binance spot prices."""

from __future__ import annotations

import asyncio
import time
import os
import sys
from typing import Dict
import aiohttp
import json

# Agregar el directorio raíz al path para que el IDE (Pylance) y Python resuelvan 'models'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import BinanceTick, CryptoFeedMetrics, SharedMarketState, SniperAsset

# ─── Binance spot miniTicker multi-stream ─────────────────────────────────
# @markPrice requiere el futures endpoint (fstream.binance.com) que puede
# estar bloqueado por la red.  @miniTicker funciona de forma fiable en el
# endpoint spot y nos da el campo "c" (close / last price) en tiempo real.
BINANCE_MULTI_WS = (
    "wss://stream.binance.com:9443/stream?"
    "streams=btcusdt@miniTicker/ethusdt@miniTicker/solusdt@miniTicker/bnbusdt@miniTicker"
)

# Fallback: REST API para obtener precios si el websocket falla
BINANCE_REST_PRICES = "https://api.binance.com/api/v3/ticker/price"

SYMBOL_MAP = {
    "BTCUSDT": SniperAsset.BTC,
    "ETHUSDT": SniperAsset.ETH,
    "SOLUSDT": SniperAsset.SOL,
    "BNBUSDT": SniperAsset.BNB,
}


class CryptoFeed:
    """El Ojo multi-activo: un websocket, múltiples símbolos."""

    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.metrics = CryptoFeedMetrics()
        self._running = False
        self._ws = None  # type: ignore

    async def start(self) -> None:
        self._running = True
        # Obtener precios iniciales via REST para no empezar en 0
        await self._seed_prices_rest()
        import websockets
        while self._running:
            try:
                async with websockets.connect(
                    BINANCE_MULTI_WS,
                    ping_interval=15,
                    ping_timeout=8,
                    close_timeout=3,
                    max_size=2_000_000,
                ) as ws:
                    self._ws = ws
                    await self._loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.metrics.reconnects += 1
                if self._running:
                    # Intentar REST como fallback antes de reconectar WS
                    await self._seed_prices_rest()
                    await asyncio.sleep(1.0)

    async def _loop(self, ws) -> None:
        while self._running:
            raw = await ws.recv()
            parse_start = time.perf_counter_ns()
            payload = json.loads(raw)
            data = payload.get("data", payload)
            symbol_raw = str(data.get("s", "")).upper()
            asset = SYMBOL_MAP.get(symbol_raw)
            if asset is None:
                continue
            # miniTicker usa "c" para close/last price
            price_raw = data.get("c") or data.get("p") or 0.0
            tick = BinanceTick(
                symbol=asset,
                mark_price=float(price_raw),
                event_time_ms=int(data.get("E", 0)),
                received_ns=time.time_ns(),
            )
            self.metrics.ticks += 1
            self.shared_state.last_binance_update_ns = time.time_ns()
            self.metrics.record_parse_ms((time.perf_counter_ns() - parse_start) / 1_000_000)
            self._publish(tick)

    async def _seed_prices_rest(self) -> None:
        """Obtiene precios via REST API como seed / fallback."""
        try:
            async with aiohttp.ClientSession() as session:
                params = [
                    ("symbols", '["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT"]'),
                ]
                async with session.get(
                    BINANCE_REST_PRICES,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        return
                    items = await resp.json()
                    for item in items:
                        sym = str(item.get("symbol", "")).upper()
                        asset = SYMBOL_MAP.get(sym)
                        if asset is not None:
                            price = float(item.get("price", 0))
                            if price > 0:
                                self.shared_state.asset_prices[asset] = price
                    self.shared_state.last_binance_update_ns = time.time_ns()
        except Exception:
            pass  # Best-effort fallback

    def _publish(self, tick: BinanceTick) -> None:
        self.shared_state.asset_prices[tick.symbol] = tick.mark_price
        self.shared_state.last_binance_update_ns = tick.received_ns

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass

    def get_status(self) -> Dict[str, float]:
        fresh = 1.0 if (time.time_ns() - self.shared_state.last_binance_update_ns) < 5_000_000_000 else 0.0
        return {
            "btc_price": self.shared_state.asset_prices[SniperAsset.BTC],
            "eth_price": self.shared_state.asset_prices[SniperAsset.ETH],
            "sol_price": self.shared_state.asset_prices[SniperAsset.SOL],
            "bnb_price": self.shared_state.asset_prices[SniperAsset.BNB],
            "avg_parse_ms": self.metrics.avg_parse_ms,
            "ticks": float(self.metrics.ticks),
            "reconnects": float(self.metrics.reconnects),
            "crypto_fresh": fresh,
        }
