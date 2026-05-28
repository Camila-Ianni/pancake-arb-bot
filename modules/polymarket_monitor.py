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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://polymarket.com/"
}


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
        self._session: Optional[aiohttp.ClientSession] = None
        self._market_map = self._load_market_map()
        self._ws_connected = False

    def _load_market_map(self) -> Dict[SniperAsset, Dict[str, str]]:
        # La carga inicial está vacía, se actualizará dinámicamente en el bucle
        return {}

    async def start(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession(headers=HEADERS)
        updater_task = asyncio.create_task(self._dynamic_market_updater_loop())
        await asyncio.sleep(2.0)  # Dar tiempo para la carga inicial
        
        await self._rest_poll_once()
        ws_task = asyncio.create_task(self._ws_loop())
        rest_task = asyncio.create_task(self._rest_poll_loop())
        try:
            await asyncio.gather(ws_task, rest_task, updater_task)
        except asyncio.CancelledError:
            ws_task.cancel()
            rest_task.cancel()
            updater_task.cancel()
            raise

    async def _dynamic_market_updater_loop(self) -> None:
        """Loop que calcula matemáticamente el bloque HFT de 5 minutos actual
        e inyecta dinámicamente los hashes reales al motor de arbitraje."""
        last_interval = 0
        while self._running:
            now = int(time.time())
            current_interval = (now // 300) * 300
            if current_interval != last_interval:
                await self._fetch_deterministic_markets(current_interval)
                last_interval = current_interval
            await asyncio.sleep(10)

    async def _fetch_deterministic_markets(self, interval: int) -> None:
        if not self._session:
            return
        assets_to_check = {
            SniperAsset.BTC: f"btc-updown-5m-{interval}",
            SniperAsset.ETH: f"eth-updown-5m-{interval}"
        }
        for asset, slug in assets_to_check.items():
            url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
            try:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets_list = data.get("markets", [])
                            if not markets_list:
                                self.shared_state.log_messages.append(f"⚠️ [API VACÍA] {slug} sin mercados")
                                continue
                            m = markets_list[0]
                            m_id = m.get("id") or m.get("market_id")
                            c_id = m.get("conditionId") or m.get("condition_id")
                            
                            import re
                            title_str = m.get("title", "")
                            slug_str = m.get("slug", "")
                            strike_val = 0.0

                            match = re.search(r'(?:above|below)\s+([\d,.]+)', title_str, re.IGNORECASE)
                            if match:
                                try:
                                    strike_val = float(match.group(1).replace(",", ""))
                                except ValueError:
                                    pass

                            if strike_val == 0.0:
                                nums = re.findall(r'\d+(?:\.\d+)?', slug_str)
                                if nums:
                                    valid_nums = [float(n) for n in nums if float(n) < 200000 or (asset == SniperAsset.BTC and float(n) > 20000)]
                                    if valid_nums:
                                        strike_val = valid_nums[0]

                            if strike_val == 0.0:
                                self.shared_state.log_messages.append(f"⚠️ [STRIKE ERROR] Título: {title_str[:20]} | Slug: {slug_str[:20]}")

                            if m_id and c_id:
                                self._market_map[asset] = {
                                    "market_id": str(m_id), 
                                    "condition_id": str(c_id),
                                    "market_close_ts": interval + 300,
                                    "strike_price": strike_val
                                }
                                if self._ws and self._ws_connected:
                                    asyncio.create_task(self._subscribe(self._ws))
                        else:
                            self.shared_state.log_messages.append(f"❌ [API ERR] {slug} Status: {resp.status}")
                except Exception as e:
                    self.shared_state.log_messages.append(f"❌ [NET EXCEPTION] -> {repr(e)}")

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
            
            cached_data = self._market_map.get(asset, {})
            strike = cached_data.get("strike_price", 0.0)
            close_ts = cached_data.get("market_close_ts", int(time.time()) + 300)
            
            return PolymarketTick(
                asset=asset,
                market_id=market_id,
                condition_id=condition_id,
                yes_price=yes_price,
                strike_price=strike,
                market_close_ts=close_ts,
            )
        except Exception as e:
            self.shared_state.log_messages.append(f"❌ [MONITOR] Error WS _parse_ws_message: {e} | asset: {asset}")
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
        """Consulta REST API y auto-actualiza mercados de 5m basándose en el reloj del sistema."""
        now = int(time.time())
        current_interval = (now // 300) * 300
        intervals_to_check = [current_interval, current_interval + 300]
        
        self.shared_state.log_messages.append(f"🔄 [POLLING] Escaneando Gamma API... Int: {current_interval}")
        
        if not hasattr(self, "_last_interval") or self._last_interval != current_interval:
            print(f"\n🔄 [MONITOR] Resolviendo IDs dinámicamente para intervalos HFT...")
            new_map = {}
            if self._session:
                for asset_name in ["btc", "eth"]:
                    for interval in intervals_to_check:
                        slug = f"{asset_name}-updown-5m-{interval}"
                        url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
                        try:
                            async with self._session.get(url, timeout=3) as resp:
                                if resp.status == 200:
                                    d = await resp.json()
                                    markets_list = d.get("markets", [])
                                    if not markets_list:
                                        self.shared_state.log_messages.append(f"⚠️ [API VACÍA] {slug} sin mercados")
                                        continue
                                    m = markets_list[0]
                                    m_id = str(m.get("id") or m.get("market_id"))
                                    c_id = str(m.get("conditionId") or m.get("condition_id"))
                                    
                                    import re
                                    title_str = m.get("title", "")
                                    slug_str = m.get("slug", "")
                                    strike_val = 0.0

                                    match = re.search(r'(?:above|below)\s+([\d,.]+)', title_str, re.IGNORECASE)
                                    if match:
                                        try:
                                            strike_val = float(match.group(1).replace(",", ""))
                                        except ValueError:
                                            pass

                                    if strike_val == 0.0:
                                        nums = re.findall(r'\d+(?:\.\d+)?', slug_str)
                                        if nums:
                                            valid_nums = [float(n) for n in nums if float(n) < 200000 or (asset_name == "btc" and float(n) > 20000)]
                                            if valid_nums:
                                                strike_val = valid_nums[0]

                                    if strike_val == 0.0:
                                        self.shared_state.log_messages.append(f"⚠️ [STRIKE ERROR] Título: {title_str[:20]} | Slug: {slug_str[:20]}")
                                    asset_enum = SniperAsset.BTC if asset_name == "btc" else SniperAsset.ETH
                                    
                                    if asset_enum not in new_map or interval == current_interval:
                                        new_map[asset_enum] = {
                                            "market_id": m_id,
                                            "condition_id": c_id,
                                            "market_close_ts": interval + 300,
                                            "strike_price": strike_val
                                        }
                                        label = "Current" if interval == current_interval else "Next/Pre-cache"
                                        print(f"  ✅ Enlazado {asset_name.upper()} 5m dinámico ({label}). ID: {m_id} | Strike: {strike_val}")
                                else:
                                    self.shared_state.log_messages.append(f"❌ [API ERR] {slug} Status: {resp.status}")
                        except Exception as e:
                            self.shared_state.log_messages.append(f"❌ [NET EXCEPTION] -> {repr(e)}")
            if new_map:
                self._market_map.update(new_map)
                self._last_interval = current_interval
                if self._ws_connected and self._ws:
                    try:
                        await self._subscribe(self._ws)
                    except Exception:
                        pass

        if not self._market_map:
            return

        now_sec = int(time.time())
        expired = []
        for a, d in self._market_map.items():
            close_ts = d.get("market_close_ts", 0)
            if now_sec > close_ts + 2:
                expired.append(a)
                self.shared_state.log_messages.append(f"🧹 [CLEANUP] Desalojando {a.name} | CloseTS: {close_ts} | Now: {now_sec} | Diff: {now_sec - close_ts}s")
                
        for a in expired:
            self._market_map.pop(a, None)
            self.shared_state.polymarket_books.pop(a, None)

        if not self._market_map or not self._session:
            return

        try:
            for asset, data in list(self._market_map.items()):
                condition_id = data["condition_id"]
                url = f"{POLYMARKET_REST}/markets/{condition_id}"
                try:
                    async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status != 200:
                            continue
                        market_data = await resp.json()
                        self._process_rest_market(asset, data, market_data)
                except Exception as e:
                    self.shared_state.log_messages.append(f"❌ [REST EXCEPTION] -> {repr(e)}")
                    continue
        except Exception:
            pass

    def _process_rest_market(self, asset: SniperAsset, map_data: Dict[str, str], market_data: dict) -> None:
        """Procesa datos de un mercado desde la REST API utilizando el caché purificado."""
        now_sec = int(time.time())
        close_ts = map_data.get("market_close_ts", 0)
        if now_sec > close_ts + 2:
            self.shared_state.log_messages.append(f"🧹 [REST CLEANUP] Desalojando {asset.name} | CloseTS: {close_ts} | Now: {now_sec} | Diff: {now_sec - close_ts}s")
            self._market_map.pop(asset, None)
            self.shared_state.polymarket_books.pop(asset, None)
            return

        tokens = market_data.get("tokens", [])
        yes_price = Decimal("0")
        for token in tokens:
            outcome = str(token.get("outcome", "")).upper()
            if outcome == "YES":
                yes_price = Decimal(str(token.get("price", 0)))
                break
        if yes_price <= 0 and tokens:
            yes_price = Decimal(str(tokens[0].get("price", 0)))

        close_ts = map_data.get("market_close_ts", int(time.time()) + 300)
        strike_val = map_data.get("strike_price", 0.0)

        tick = PolymarketTick(
            asset=asset,
            market_id=map_data["market_id"],
            condition_id=map_data["condition_id"],
            yes_price=yes_price,
            strike_price=strike_val,
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
        if self._session is not None:
            await self._session.close()

    def get_status(self) -> Dict[str, float]:
        return {
            "markets_cached": float(len(self.shared_state.polymarket_books)),
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "updates": float(self.metrics.updates),
            "ws_connected": 1.0 if self._ws_connected else 0.0,
        }
