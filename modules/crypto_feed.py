"""CryptoFeed multi-activo para Binance mark price."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import websockets

try:
    import ujson as json
except ImportError:  # pragma: no cover
    import json  # type: ignore[no-redef]

from models import BinanceTick, CryptoFeedMetrics, SharedMarketState, SniperAsset

BINANCE_MULTI_WS = (
    "wss://stream.binance.com:9443/stream?"
    "streams=btcusdt@markPrice/ethusdt@markPrice/solusdt@markPrice/bnbusdt@markPrice"
)
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
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def start(self) -> None:
        self._running = True
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
                    await self._loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.metrics.reconnects += 1
                await asyncio.sleep(0.25)

    async def _loop(self) -> None:
        assert self._ws is not None
        while self._running:
            raw = await self._ws.recv()
            parse_start = time.perf_counter_ns()
            payload = json.loads(raw)
            data = payload.get("data", payload)
            symbol_raw = str(data.get("s", "")).upper()
            asset = SYMBOL_MAP.get(symbol_raw)
            if asset is None:
                continue
            tick = BinanceTick(
                symbol=asset,
                mark_price=float(data.get("p", 0.0)),
                event_time_ms=int(data.get("E", 0)),
                received_ns=time.time_ns(),
            )
            self.metrics.record_parse_ms((time.perf_counter_ns() - parse_start) / 1_000_000)
            self._publish(tick)

    def _publish(self, tick: BinanceTick) -> None:
        self.shared_state.asset_prices[tick.symbol] = tick.mark_price
        self.shared_state.last_binance_update_ns = tick.received_ns

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    def get_status(self) -> dict[str, float]:
        fresh = 1.0 if (time.time_ns() - self.shared_state.last_binance_update_ns) < 2_000_000_000 else 0.0
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
