"""Polymarket monitor multi-mercado con cache paralelo por activo.

Conecta al CLOB websocket de Polymarket para recibir actualizaciones
de precios en tiempo real, con fallback a polling REST.

WebSocket: wss://ws-subscriptions-clob.polymarket.com/ws/market
REST API:  https://clob.polymarket.com/markets/<condition_id>
"""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from typing import Dict, Optional
import sys

import aiohttp
import json

# Agregar el directorio raíz al path para que el IDE (Pylance) y Python resuelvan 'models'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import MarketMonitorMetrics, PolymarketTick, SharedMarketState, SniperAsset

# ─── Endpoints ────────────────────────────────────────────────────────────
POLYMARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_REST = "https://clob.polymarket.com"


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
        self._ws_connected = False

    def _load_market_map(self) -> Dict[SniperAsset, Dict[str, str]]:
        """Carga el mapeo activo → mercado desde POLYMARKET_MARKETS.

        Formato: "BTC:market_id:condition_id,ETH:...,SOL:...,BNB:..."
        """
        raw = os.getenv("POLYMARKET_MARKETS", "")
        markets: Dict[SniperAsset, Dict[str, str]] = {}
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
        # Intentar REST polling primero para tener datos inmediatos
        await self._rest_poll_once()
        # Lanzar WS y REST polling en paralelo
        ws_task = asyncio.create_task(self._ws_loop())
        rest_task = asyncio.create_task(self._rest_poll_loop())
        try:
            await asyncio.gather(ws_task, rest_task)
        except asyncio.CancelledError:
            ws_task.cancel()
            rest_task.cancel()
            raise

    # ── WebSocket ──────────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        """Loop de reconexión del WebSocket."""
        while self._running:
            try:
                import websockets
                async with websockets.connect(
                    POLYMARKET_WS,
                    ping_interval=10,
                    ping_timeout=8,
                    close_timeout=3,
                    max_size=2_000_000,
                ) as ws:
                    self._ws = ws
                    self._ws_connected = True
                    await self._subscribe(ws)
                    await self._ws_recv_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._ws_connected = False
                self.metrics.reconnects += 1
                if self._running:
                    await asyncio.sleep(2.0)

    async def _subscribe(self, ws) -> None:
        """Suscribe a los mercados configurados.

        Formato Polymarket WS:
        {"type":"market","assets_ids":["TOKEN_ID_1",...],"custom_feature_enabled":true}
        """
        if not self._market_map:
            return
        # Recopilar todos los token IDs / condition IDs para suscripción
        asset_ids = []
        for data in self._market_map.values():
            asset_ids.append(data["condition_id"])
        msg = {
            "type": "market",
            "assets_ids": asset_ids,
        }
        await ws.send(json.dumps(msg))

    async def _ws_recv_loop(self, ws) -> None:
        """Recibe mensajes del WebSocket y los procesa."""
        while self._running:
            started = time.perf_counter_ns()
            raw = await ws.recv()
            if isinstance(raw, str) and raw.strip().upper() == "PONG":
                continue
            try:
                message = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            tick = self._parse_ws_message(message)
            if tick is not None:
                self._publish(tick)
            self.metrics.record_latency_ms((time.perf_counter_ns() - started) / 1_000_000)

    def _parse_ws_message(self, message: dict) -> Optional[PolymarketTick]:
        """Parsea un mensaje del WebSocket de Polymarket.

        Los mensajes pueden venir en varios formatos según el evento:
        - price_change: {"event_type":"price_change","asset_id":"...","price":"0.55",...}
        - book: {"event_type":"book","asset_id":"...","bids":[...],"asks":[...]}
        - last_trade_price: {"event_type":"last_trade_price","asset_id":"...","price":"0.60"}
        """
        event_type = message.get("event_type", "")

        # Extraer precio YES de diferentes formatos
        yes_raw = None
        if event_type in ("price_change", "last_trade_price"):
            yes_raw = message.get("price")
        elif event_type == "book":
            # Tomar el mejor ask como precio YES
            asks = message.get("asks", [])
            if asks:
                yes_raw = asks[0].get("price") if isinstance(asks[0], dict) else asks[0]
        else:
            # Formato genérico
            yes_raw = (
                message.get("yes_price")
                or message.get("bestAsk")
                or message.get("price")
            )

        if yes_raw is None:
            return None

        # Determinar el asset
        asset_id = message.get("asset_id", "") or message.get("condition_id", "")
        market_id = str(message.get("market_id", ""))
        condition_id = str(message.get("condition_id", ""))

        asset: Optional[SniperAsset] = None

        # Intentar por asset_id / condition_id
        if asset_id:
            for k, v in self._market_map.items():
                if v["condition_id"] == asset_id or v["market_id"] == asset_id:
                    asset = k
                    market_id = v["market_id"]
                    condition_id = v["condition_id"]
                    break

        # Intentar por symbol
        if asset is None:
            symbol = str(message.get("symbol", ""))
            asset = _asset_from_symbol(symbol)

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

    # ── REST Polling (fallback) ────────────────────────────────────────────

    async def _rest_poll_loop(self) -> None:
        """Polling REST como fallback cuando el WS no funciona o para seed."""
        while self._running:
            # Si el WS está conectado, poll menos frecuente
            interval = 30.0 if self._ws_connected else 5.0
            await asyncio.sleep(interval)
            if not self._running:
                break
            await self._rest_poll_once()

    async def _rest_poll_once(self) -> None:
        """Consulta REST API para obtener precios de los mercados configurados."""
        if not self._market_map:
            return
        try:
            async with aiohttp.ClientSession() as session:
                for asset, data in self._market_map.items():
                    condition_id = data["condition_id"]
                    url = f"{POLYMARKET_REST}/markets/{condition_id}"
                    try:
                        async with session.get(
                            url,
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as resp:
                            if resp.status != 200:
                                continue
                            market_data = await resp.json()
                            self._process_rest_market(asset, data, market_data)
                    except Exception:
                        continue
        except Exception:
            pass

    def _process_rest_market(
        self, asset: SniperAsset, map_data: Dict[str, str], market_data: dict
    ) -> None:
        """Procesa datos de un mercado desde la REST API."""
        tokens = market_data.get("tokens", [])
        yes_price = Decimal("0")
        for token in tokens:
            outcome = str(token.get("outcome", "")).upper()
            if outcome == "YES":
                yes_price = Decimal(str(token.get("price", 0)))
                break
        if yes_price <= 0:
            # Si no hay outcome YES, usar el primer token
            if tokens:
                yes_price = Decimal(str(tokens[0].get("price", 0)))

        end_date = market_data.get("end_date_iso", "")
        close_ts = int(time.time()) + 300  # default 5 min
        if end_date:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                close_ts = int(dt.timestamp())
            except Exception:
                pass

        tick = PolymarketTick(
            asset=asset,
            market_id=map_data["market_id"],
            condition_id=map_data["condition_id"],
            yes_price=yes_price,
            strike_price=0.0,
            market_close_ts=close_ts,
        )
        self._publish(tick)

    # ── Publish & Status ───────────────────────────────────────────────────

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
            try:
                await self._ws.close()
            except Exception:
                pass

    def get_status(self) -> Dict[str, float]:
        return {
            "markets_cached": float(len(self.shared_state.polymarket_books)),
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "updates": float(self.metrics.updates),
            "ws_connected": 1.0 if self._ws_connected else 0.0,
        }
