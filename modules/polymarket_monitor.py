"""Polymarket monitor multi-mercado con cache paralelo por activo."""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from typing import Dict, List, Optional

import websockets

try:
    import ujson as json
except ImportError:  # pragma: no cover
    import json  # type: ignore[no-redef]

from models import MarketMonitorMetrics, PolymarketTick, SharedMarketState, SniperAsset

POLYMARKET_WS = "wss://clob.polymarket.com/ws"


def _asset_from_symbol(symbol: str) -> Optional[SniperAsset]:
    up = symbol.upper()
    if "BTC" in up:
        return SniperAsset.BTC
    if "ETH" in up:
        return SniperAsset.ETH
    if "SOL" in up:
        return SniperAsset.SOL
    if "BNB" in up:
        return SniperAsset.BNB
    return None


class PolymarketMonitor:
    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.metrics = MarketMonitorMetrics()
        self._running = False
        self._ws = None
        self._market_map = self._load_market_map()

    def _load_market_map(self) -> Dict[SniperAsset, Dict[str, str]]:
        # Formato: "BTC:market_id:condition_id,ETH:...,SOL:...,BNB:..."
        raw = os.getenv("POLYMARKET_MARKETS", "")
        markets = {}
        for entry in [x.strip() for x in raw.split(",") if x.strip()]:
            parts = entry.split(":")
            if len(parts) != 3:
                continue
            asset_raw, market_id, condition_id = parts
            asset = _asset_from_symbol(asset_raw)
            if asset is None:
                continue
            markets[asset] = {"market_id": market_id, "condition_id": condition_id}
        return markets

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                async with websockets.connect(
                    POLYMARKET_WS,
                    ping_interval=10,
                    ping_timeout=8,
                    close_timeout=3,
                    max_size=2_000_000,
                ) as ws:
                    self._ws = ws
                    await self._subscribe(ws)
                    await self._loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.metrics.reconnects += 1
                await asyncio.sleep(0.5)

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        if self._market_map:
            for data in self._market_map.values():
                await ws.send(
                    json.dumps(
                        {"type": "subscribe", "channel": "market", "market": data["market_id"]}
                    )
                )
        else:
            await ws.send(json.dumps({"type": "subscribe", "channel": "ticker"}))

    async def _loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        while self._running:
            started = time.perf_counter_ns()
            raw = await ws.recv()
            message = json.loads(raw)
            tick = self._parse_message(message)
            if tick is not None:
                self._publish(tick)
            self.metrics.record_latency_ms((time.perf_counter_ns() - started) / 1_000_000)

    def _parse_message(self, message: dict) -> Optional[PolymarketTick]:
        yes_raw = message.get("yes_price") or message.get("bestAsk") or message.get("price")
        if yes_raw is None:
            return None
        symbol = str(message.get("symbol", ""))
        asset = _asset_from_symbol(symbol)
        market_id = str(message.get("market_id", ""))
        condition_id = str(message.get("condition_id", ""))

        if asset is None and market_id:
            for k, v in self._market_map.items():
                if v["market_id"] == market_id:
                    asset = k
                    condition_id = v["condition_id"]
                    break
        if asset is None:
            return None
        if not market_id and asset in self._market_map:
            market_id = self._market_map[asset]["market_id"]
            condition_id = self._market_map[asset]["condition_id"]
        if not market_id:
            return None

        try:
            yes_price = Decimal(str(yes_raw))
            strike_raw = message.get("strike_price") or message.get("strike") or 0
            close_raw = message.get("market_close_ts") or message.get("end_ts")
            strike = float(strike_raw) if strike_raw else 0.0
            close_ts = int(close_raw) if close_raw else int(time.time()) + 300
            return PolymarketTick(
                asset=asset,
                market_id=market_id,
                condition_id=condition_id,
                yes_price=yes_price,
                strike_price=strike,
                market_close_ts=close_ts,
            )
        except Exception:
            return None

    def _publish(self, tick: PolymarketTick) -> None:
        self.shared_state.polymarket_books[tick.asset] = {
            "market_id": tick.market_id,
            "condition_id": tick.condition_id,
            "yes_price": tick.yes_price,
            "strike_price": tick.strike_price,
            "market_close_ts": tick.market_close_ts,
            "updated_ns": time.time_ns(),
        }

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    def get_status(self) -> dict[str, float]:
        return {
            "markets_cached": float(len(self.shared_state.polymarket_books)),
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "updates": float(self.metrics.updates),
        }
