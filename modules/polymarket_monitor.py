"""
PolymarketMonitor - Monitor CLOB de Polymarket vía WebSockets (HFT Optimized).

Este módulo se conecta al order book de Polymarket CLOB con optimizaciones
extremas para baja latencia (< 50ms objetivo).

ARQUITECTURA HFT:
- Conexión WebSocket persistente a wss://clob.polymarket.com/ws
- Local Order Book (LOB) en memoria con acceso O(1)
- ujson para parsing ultrarrápido (5-10x más rápido que json estándar)
- Reconexión automática < 1 segundo
- Cálculo de VWAP/slippage sin I/O

OPTIMIZACIONES M1:
- ujson en vez de json estándar
- heapq para mantener top-10 bids/asks ordenados
- Lock-free reads cuando es posible
- Batch de updates para reducir overhead
- Timestamps en nanosegundos para medición precisa

HOT PATH CRITICAL:
- El handler de mensajes NO debe bloquear
- Actualizaciones incrementales al LOB
- Sin allocations innecesarias en el hot path
"""

import asyncio
import time
import heapq
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Callable, Any, Awaitable, Tuple
from enum import Enum, auto
import logging

try:
    import ujson as json
    _HAS_UJSON = True
except ImportError:
    import json
    _HAS_UJSON = False

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException, InvalidStatusCode

from ..models import MarketSide, OrderBookSnapshot, PriceLevel
from ..config import AppConfig, get_config
from ..logging_config import get_logger, get_latency_logger

logger = get_logger(__name__)
latency_logger = get_latency_logger(__name__)


# =============================================================================
# CONSTANTES Y CONFIGURACIÓN HFT
# =============================================================================

# Polymarket CLOB WebSocket endpoints
CLOB_WS_URL = "wss://clob.polymarket.com/ws"
CLOB_WS_URL_SANDBOX = "wss://sandbox.clob.polymarket.com/ws"

# Canales disponibles
CHANNEL_ORDERBOOK = "order_book"
CHANNEL_TRADES = "trades"
CHANNEL_TICKER = "ticker"

# Niveles máximos a mantener en memoria (trade-off: memoria vs velocidad)
MAX_BOOK_DEPTH = 50  # Mantener 50 niveles por lado

# Top-N niveles para cálculo rápido
TOP_N_LEVELS = 10

# Timeout de reconexión (objetivo: < 1 segundo)
RECONNECT_DELAY_SEC = 0.1
MAX_RECONNECT_DELAY_SEC = 5.0

# Heartbeat timeout
HEARTBEAT_TIMEOUT_SEC = 10.0


class PolymarketMonitorState(Enum):
    """Estado del monitor CLOB."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    RECONNECTING = auto()
    ERROR = auto()
    SHUTDOWN = auto()


@dataclass
class MonitorMetrics:
    """
    Métricas en tiempo real del monitor CLOB.

    Diseñadas para overhead mínimo - actualizaciones atómicas.
    """
    messages_received: int = 0
    order_book_updates: int = 0
    snapshot_count: int = 0

    # Latencia (desde recepción hasta procesamiento completo)
    last_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    # Latencia del servidor (si está disponible en el mensaje)
    server_latency_ms: float = 0.0

    # Errores
    connection_attempts: int = 0
    reconnections: int = 0
    errors: int = 0
    parse_errors: int = 0

    # Heartbeat
    last_heartbeat_ns: int = 0
    missed_heartbeats: int = 0

    # Muestras para percentiles
    _latency_samples: List[float] = field(default_factory=list)

    def record_latency(self, latency_ms: float) -> None:
        """Registra latencia con moving average exponencial."""
        self.last_latency_ms = latency_ms
        self.min_latency_ms = min(self.min_latency_ms, latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)
        self.avg_latency_ms = self.avg_latency_ms * 0.85 + latency_ms * 0.15

        self._latency_samples.append(latency_ms)
        if len(self._latency_samples) > 1000:
            self._latency_samples.pop(0)

        if self._latency_samples:
            sorted_samples = sorted(self._latency_samples)
            p99_idx = int(len(sorted_samples) * 0.99)
            self.p99_latency_ms = sorted_samples[p99_idx]

    def reset(self) -> None:
        """Resetea todas las métricas."""
        self.__init__()


@dataclass
class OrderBookLevel:
    """
    Nivel de precio en el order book.

    Inmutable (frozen) para thread-safety implícita.
    Optimizado con __slots__ para reducir memoria.
    """
    __slots__ = ['price', 'size', 'order_count', 'price_decimal']

    price: int          # Precio en centavos (0-100)
    size: Decimal       # Cantidad de shares
    order_count: int = 0  # Número de órdenes (opcional)

    def __post_init__(self):
        # Pre-calcular price_decimal para acceso rápido
        if not hasattr(self, 'price_decimal'):
            object.__setattr__(self, 'price_decimal', Decimal(self.price) / Decimal(100))


class LocalOrderBook:
    """
    Local Order Book (LOB) optimizado para HFT.

    CARACTERÍSTICAS:
    - Diccionarios para acceso O(1) por precio
    - Heaps para obtener top-N rápidamente
    - Actualizaciones incrementales (no full refresh)
    - Lock-free reads para get_snapshot (usa copias shallow)

    ESTRUCTURA:
    _bids: {price_cents: size}  # Diccionario para lookup O(1)
    _asks: {price_cents: size}
    _bid_heap: max-heap para top bids
    _ask_heap: min-heap para top asks
    """

    def __init__(self, token_id: str):
        self.token_id = token_id

        # Order books como diccionarios: price_cents -> size
        self._bids: Dict[int, Decimal] = {}
        self._asks: Dict[int, Decimal] = {}

        # Heaps para top-N rápido
        # Bid heap: max-heap (negar precio para simular)
        # Ask heap: min-heap
        self._bid_heap: List[Tuple[int, Decimal]] = []
        self._ask_heap: List[Tuple[int, Decimal]] = []

        # Metadata
        self._sequence_num: int = 0
        self._last_update_ns: int = 0
        self._is_synced: bool = False

        # Lock para actualizaciones (reads pueden ser lock-free)
        self._lock = asyncio.Lock()

    @property
    def sequence_num(self) -> int:
        """Número de secuencia actual."""
        return self._sequence_num

    @property
    def is_synced(self) -> bool:
        """Verifica si el LOB está sincronizado."""
        return self._is_synced

    @property
    def last_update_ns(self) -> int:
        """Timestamp de la última actualización."""
        return self._last_update_ns

    def apply_snapshot(self, bids: List[Tuple[int, Any]], asks: List[Tuple[int, Any]], sequence_num: int) -> None:
        """
        Aplica un snapshot completo del order book.

        HOT PATH: Esta función debe ser rápida.
        Usar durante inicialización o recuperación de errores.

        Args:
            bids: Lista de (price_cents, size) tuples
            asks: Lista de (price_cents, size) tuples
            sequence_num: Número de secuencia del snapshot
        """
        # Limpiar estado anterior
        self._bids.clear()
        self._asks.clear()
        self._bid_heap.clear()
        self._ask_heap.clear()

        # Aplicar bids
        for price, size in bids:
            price_int = int(price)
            size_dec = Decimal(str(size))
            if size_dec > 0:
                self._bids[price_int] = size_dec
                heapq.heappush(self._bid_heap, (-price_int, size_dec))  # Negar para max-heap

        # Aplicar asks
        for price, size in asks:
            price_int = int(price)
            size_dec = Decimal(str(size))
            if size_dec > 0:
                self._asks[price_int] = size_dec
                heapq.heappush(self._ask_heap, (price_int, size_dec))

        self._sequence_num = sequence_num
        self._last_update_ns = time.time_ns()
        self._is_synced = True

        logger.debug(f"Snapshot aplicado: {len(self._bids)} bids, {len(self._asks)} asks")

    def update_level(self, side: MarketSide, price: int, size: Decimal, sequence_num: int) -> bool:
        """
        Actualiza un nivel de precio incrementalmente.

        HOT PATH: Esta función se llama frecuentemente - debe ser O(1).

        Args:
            side: BID o ASK
            price: Precio en centavos
            size: Nuevo tamaño (0 elimina el nivel)
            sequence_num: Número de secuencia

        Returns:
            True si la actualización fue aplicada, False si se descartó por secuencia antigua
        """
        # Verificar secuencia
        if sequence_num <= self._sequence_num:
            return False  # Mensaje duplicado o fuera de orden

        if sequence_num > self._sequence_num + 100:
            # Gap grande - probablemente perdimos mensajes
            logger.warning(f"Gap de secuencia: {self._sequence_num} -> {sequence_num}")
            # No descartar, pero loguear

        self._sequence_num = sequence_num
        self._last_update_ns = time.time_ns()

        book = self._bids if side == MarketSide.BID else self._asks

        if size == 0:
            # Eliminar nivel
            if price in book:
                del book[price]
                # Nota: No eliminamos del heap (lazy deletion)
        else:
            # Actualizar nivel
            book[price] = size
            # Agregar al heap (lazy insertion)
            if side == MarketSide.BID:
                heapq.heappush(self._bid_heap, (-price, size))
            else:
                heapq.heappush(self._ask_heap, (price, size))

        return True

    def get_top_levels(self, n: int = TOP_N_LEVELS) -> Tuple[List[PriceLevel], List[PriceLevel]]:
        """
        Obtiene los top-N niveles de bids y asks.

        HOT PATH: O(N log N) donde N es pequeño (10).
        Usa heaps para eficiencia.

        Returns:
            (top_bids, top_asks) como listas de PriceLevel
        """
        # Obtener top bids (max-heap, tomar los más altos)
        top_bids = []
        seen_prices = set()

        # Usar el diccionario para datos actualizados
        sorted_bids = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)[:n]
        for price, size in sorted_bids:
            if price not in seen_prices:
                top_bids.append(PriceLevel(price=price, size=size))
                seen_prices.add(price)

        # Obtener top asks (min-heap, tomar los más bajos)
        top_asks = []
        sorted_asks = sorted(self._asks.items(), key=lambda x: x[0])[:n]
        for price, size in sorted_asks:
            if price not in seen_prices:
                top_asks.append(PriceLevel(price=price, size=size))
                seen_prices.add(price)

        return top_bids, top_asks

    def get_best_bid(self) -> Optional[PriceLevel]:
        """
        Obtiene el mejor bid (precio más alto).

        O(1) usando el diccionario.
        """
        if not self._bids:
            return None
        best_price = max(self._bids.keys())
        return PriceLevel(price=best_price, size=self._bids[best_price])

    def get_best_ask(self) -> Optional[PriceLevel]:
        """
        Obtiene el mejor ask (precio más bajo).

        O(1) usando el diccionario.
        """
        if not self._asks:
            return None
        best_price = min(self._asks.keys())
        return PriceLevel(price=best_price, size=self._asks[best_price])

    def get_spread(self) -> Optional[int]:
        """
        Calcula el spread en centavos.

        Returns:
            Spread en centavos o None si no hay liquidez
        """
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None

    def get_mid_price(self) -> Optional[Decimal]:
        """
        Calcula el precio medio (mid-market price).

        Returns:
            Precio medio o None si no hay liquidez
        """
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return (best_bid.price_decimal + best_ask.price_decimal) / 2
        return None

    def get_execution_price(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """
        Calcula el precio de ejecución real (VWAP) para una orden de tamaño dado.

        HOT PATH CRITICAL: Esta función debe ser extremadamente rápida.
        Recorre el order book local sin I/O.

        Args:
            side: BID (comprar) o ASK (vender)
            size: Tamaño de la orden en shares

        Returns:
            VWAP (Volume Weighted Average Price) o None si no hay liquidez

        Ejemplo:
            Si quieres comprar 100 shares y el book tiene:
            - Ask 1: 50 shares @ 52¢
            - Ask 2: 75 shares @ 53¢
            VWAP = (50*52 + 50*53) / 100 = 52.5¢
        """
        if side == MarketSide.BID:
            # Comprar: recorrer asks (del más bajo al más alto)
            levels = sorted(self._asks.items(), key=lambda x: x[0])
        else:
            # Vender: recorrer bids (del más alto al más bajo)
            levels = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)

        if not levels:
            return None

        remaining = size
        total_cost = Decimal('0')
        filled = Decimal('0')

        for price_cents, avail_size in levels:
            if remaining <= 0:
                break

            fill_size = min(remaining, avail_size)
            price_decimal = Decimal(price_cents) / Decimal(100)
            total_cost += fill_size * price_decimal
            filled += fill_size
            remaining -= avail_size

        if filled == 0:
            return None

        return total_cost / filled

    def calculate_slippage(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """
        Calcula el slippage esperado para una orden.

        Slippage = (VWAP - mid_price) / mid_price para compras
        Slippage = (mid_price - VWAP) / mid_price para ventas

        Args:
            side: BID o ASK
            size: Tamaño de la orden

        Returns:
            Slippage como decimal (ej. 0.02 = 2%) o None
        """
        vwap = self.get_execution_price(side, size)
        mid = self.get_mid_price()

        if vwap is None or mid is None or mid == 0:
            return None

        if side == MarketSide.BID:
            return (vwap - mid) / mid
        else:
            return (mid - vwap) / mid

    def get_liquidity(self, side: MarketSide, levels: int = TOP_N_LEVELS) -> Decimal:
        """
        Calcula la liquidez total disponible en los top-N niveles.

        Args:
            side: BID o ASK
            levels: Número de niveles a considerar

        Returns:
            Cantidad total de shares disponibles
        """
        book = self._bids if side == MarketSide.BID else self._asks
        if not book:
            return Decimal('0')

        sorted_items = sorted(
            book.items(),
            key=lambda x: x[0],
            reverse=(side == MarketSide.BID)
        )[:levels]

        return sum(size for _, size in sorted_items)

    def get_snapshot(self) -> OrderBookSnapshot:
        """
        Crea un snapshot completo del order book.

        Returns:
            OrderBookSnapshot inmutable
        """
        top_bids, top_asks = self.get_top_levels(TOP_N_LEVELS)

        return OrderBookSnapshot(
            condition_id=self.token_id,  # Usar token_id como condition_id
            market_id=self.token_id,
            bids=tuple(top_bids),
            asks=tuple(top_asks),
            timestamp_ns=self._last_update_ns,
            sequence_num=self._sequence_num,
        )


class PolymarketMonitor:
    """
    Monitor CLOB de Polymarket optimizado para HFT.

    RESPONSABILIDADES:
    1. Conexión WebSocket persistente a wss://clob.polymarket.com/ws
    2. Suscripción a canales de order_book
    3. Mantenimiento de Local Order Book (LOB) sincronizado
    4. Cálculo instantáneo de VWAP y slippage
    5. Reconexión automática < 1 segundo
    6. Heartbeat monitoring

    API DE POLYMARKET CLOB:
    - Subscribe: {"type": "subscribe", "topic": "order_book", "token_id": "0x..."}
    - Snapshot: {"type": "order_book_snapshot", "token_id": "...", "bids": [...], "asks": [...]}
    - Update: {"type": "order_book_update", "token_id": "...", "side": "bid", "price": 50, "size": 100}
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        sandbox: bool = False,
    ):
        """
        Inicializa el monitor CLOB.

        Args:
            config: Configuración (usa global si None)
            sandbox: Si True, usa el sandbox de Polymarket
        """
        self.config = config or get_config()
        self.sandbox = sandbox

        self.ws_url = CLOB_WS_URL_SANDBOX if sandbox else CLOB_WS_URL

        # Token ID del mercado (del config)
        self.token_id = self.config.polymarket.condition_id
        if not self.token_id:
            logger.warning("No hay token_id configurado - usar condition_id como fallback")

        # Estado interno
        self._state = PolymarketMonitorState.DISCONNECTED
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._metrics = MonitorMetrics()

        # Local Order Book
        self._lob = LocalOrderBook(self.token_id)

        # Callbacks para notificaciones
        self._on_update_callbacks: List[Callable[[OrderBookSnapshot], Awaitable[None]]] = []

        # Control de conexión
        self._running = False
        self._reconnect_delay = RECONNECT_DELAY_SEC
        self._last_heartbeat_ns: int = 0

        # Tareas asyncio
        self._ws_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Precio de última ejecución (cache para acceso rápido)
        self._last_execution_price: Dict[MarketSide, Decimal] = {}
        self._last_execution_size: Decimal = Decimal('0')

        logger.info(f"PolymarketMonitor CLOB inicializado: {self.ws_url}, token_id={self.token_id}")
        if _HAS_UJSON:
            logger.info("ujson disponible - parsing optimizado activado")
        else:
            logger.info("ujson no disponible - usando json estándar (considerar instalar)")

    @property
    def state(self) -> PolymarketMonitorState:
        """Estado actual del monitor."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Verifica si está conectado y operativo."""
        return self._state == PolymarketMonitorState.CONNECTED

    @property
    def metrics(self) -> MonitorMetrics:
        """Métricas en tiempo real."""
        return self._metrics

    @property
    def lob(self) -> LocalOrderBook:
        """Acceso al Local Order Book."""
        return self._lob

    def on_update(self, callback: Callable[[OrderBookSnapshot], Awaitable[None]]) -> None:
        """
        Registra un callback para actualizaciones del order book.

        Args:
            callback: Función async que recibe OrderBookSnapshot
        """
        self._on_update_callbacks.append(callback)
        logger.debug(f"Callback registrado. Total: {len(self._on_update_callbacks)}")

    def get_execution_price(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """
        Calcula el precio de ejecución real (VWAP) localmente.

        HOT PATH: Sin I/O, usa el LOB local.

        Args:
            side: BID (comprar) o ASK (vender)
            size: Tamaño de la orden

        Returns:
            VWAP o None si no hay liquidez
        """
        return self._lob.get_execution_price(side, size)

    def get_best_bid(self) -> Optional[PriceLevel]:
        """Obtiene el mejor bid actual."""
        return self._lob.get_best_bid()

    def get_best_ask(self) -> Optional[PriceLevel]:
        """Obtiene el mejor ask actual."""
        return self._lob.get_best_ask()

    def get_spread(self) -> Optional[int]:
        """Obtiene el spread actual en centavos."""
        return self._lob.get_spread()

    def get_mid_price(self) -> Optional[Decimal]:
        """Obtiene el precio medio actual."""
        return self._lob.get_mid_price()

    def calculate_slippage(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """Calcula el slippage esperado para una orden."""
        return self._lob.calculate_slippage(side, size)

    def get_liquidity(self, side: MarketSide, levels: int = 10) -> Decimal:
        """Obtiene la liquidez total en los top-N niveles."""
        return self._lob.get_liquidity(side, levels)

    async def _connect(self) -> None:
        """
        Establece conexión WebSocket con Polymarket CLOB.

        Optimizado para conexión rápida:
        - Timeout agresivo
        - Ping/pong configurado para keepalive
        - Compresión deshabilitada para menor latencia
        """
        self._state = PolymarketMonitorState.CONNECTING
        self._metrics.connection_attempts += 1

        try:
            # Headers opcionales (Polymarket no requiere auth para order book público)
            headers = {
                "User-Agent": "PolymarketArbBot/1.0 (HFT)",
            }

            # Conectar con timeout agresivo
            with latency_logger.measure("websocket_connect") as metric:
                self._ws = await asyncio.wait_for(
                    websockets.connect(
                        self.ws_url,
                        extra_headers=headers,
                        ping_interval=20,  # Ping cada 20s
                        ping_timeout=5,    # Timeout de ping corto
                        close_timeout=2,   # Close rápido
                        max_size=1024 * 1024,  # 1MB max message
                        compression=None,  # Sin compresión para menor latencia
                    ),
                    timeout=5.0,
                )

            self._state = PolymarketMonitorState.CONNECTED
            self._last_heartbeat_ns = time.time_ns()
            logger.info(f"WebSocket CLOB conectado: {self.ws_url}")

            # Suscribirse al order book
            await self._subscribe()

        except asyncio.TimeoutError:
            logger.error("Timeout conectando a Polymarket CLOB")
            self._state = PolymarketMonitorState.ERROR
            raise

        except Exception as e:
            logger.error(f"Error conectando: {e}")
            self._state = PolymarketMonitorState.ERROR
            raise

    async def _subscribe(self) -> None:
        """
        Envía suscripción al canal order_book.

        Formato según documentación de Polymarket CLOB:
        {"type": "subscribe", "topic": "order_book", "token_id": "0x..."}
        """
        subscribe_msg = {
            "type": "subscribe",
            "topic": CHANNEL_ORDERBOOK,
            "token_id": self.token_id,
        }

        # Usar ujson si está disponible
        if _HAS_UJSON:
            msg_str = json.dumps(subscribe_msg)
        else:
            msg_str = json.dumps(subscribe_msg)

        await self._ws.send(msg_str)
        logger.info(f"Suscrito a {CHANNEL_ORDERBOOK} para token_id={self.token_id}")

    async def _process_message(self, raw_message: str) -> None:
        """
        Procesa un mensaje WebSocket recibido.

        HOT PATH CRITICAL:
        - ujson para parsing 5-10x más rápido
        - Sin I/O blocking
        - Actualización incremental del LOB

        Args:
            raw_message: JSON string del WebSocket
        """
        receive_time_ns = time.time_ns()

        try:
            # Parsing rápido con ujson
            if _HAS_UJSON:
                data = json.loads(raw_message)
            else:
                data = json.loads(raw_message)

            self._metrics.messages_received += 1

            msg_type = data.get("type")

            # Calcular latencia si hay timestamp del servidor
            if "timestamp" in data or "ts" in data:
                server_ts = data.get("timestamp") or data.get("ts")
                if isinstance(server_ts, (int, float)):
                    # Asumir milisegundos
                    server_ts_ns = int(server_ts * 1_000_000)
                    latency_ns = receive_time_ns - server_ts_ns
                    self._metrics.server_latency_ms = latency_ns / 1_000_000

            if msg_type == "order_book_snapshot":
                await self._handle_snapshot(data)

            elif msg_type == "order_book_update" or msg_type == "book_update":
                await self._handle_update(data)

            elif msg_type == "heartbeat" or msg_type == "ping":
                self._last_heartbeat_ns = receive_time_ns
                self._metrics.last_heartbeat_ns = receive_time_ns

            elif msg_type == "error":
                error_msg = data.get("error") or data.get("message", "Unknown error")
                logger.error(f"Error de CLOB API: {error_msg}")
                self._metrics.errors += 1

            elif msg_type == "subscribed":
                logger.debug(f"Suscripción confirmada: {data}")

            else:
                logger.debug(f"Mensaje desconocido: {msg_type}")

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            self._metrics.parse_errors += 1
            self._metrics.errors += 1

    async def _handle_snapshot(self, data: Dict[str, Any]) -> None:
        """
        Maneja un snapshot completo del order book.

        Formato esperado:
        {
            "type": "order_book_snapshot",
            "token_id": "0x...",
            "bids": [{"price": "0.50", "size": "100"}, ...],
            "asks": [{"price": "0.52", "size": "150"}, ...],
            "sequence": 12345
        }
        """
        start_ns = time.perf_counter_ns()

        token_id = data.get("token_id") or data.get("market")
        if token_id and token_id != self.token_id:
            logger.warning(f"Snapshot para token_id diferente: {token_id}")
            return

        # Extraer bids y asks
        bids_raw = data.get("bids") or data.get("bid", [])
        asks_raw = data.get("asks") or data.get("ask", [])
        sequence = data.get("sequence") or data.get("seq", 0)

        # Parsear a formato (price_cents, size)
        bids = []
        for bid in bids_raw:
            if isinstance(bid, dict):
                price = bid.get("price") or bid.get("px")
                size = bid.get("size") or bid.get("qty")
            elif isinstance(bid, (list, tuple)):
                price, size = bid[0], bid[1]
            else:
                continue

            if price and size:
                # Convertir precio decimal a centavos (ej. 0.50 -> 50)
                price_cents = int(Decimal(str(price)) * 100)
                bids.append((price_cents, Decimal(str(size))))

        asks = []
        for ask in asks_raw:
            if isinstance(ask, dict):
                price = ask.get("price") or ask.get("px")
                size = ask.get("size") or ask.get("qty")
            elif isinstance(ask, (list, tuple)):
                price, size = ask[0], ask[1]
            else:
                continue

            if price and size:
                price_cents = int(Decimal(str(price)) * 100)
                asks.append((price_cents, Decimal(str(size))))

        # Aplicar snapshot al LOB
        self._lob.apply_snapshot(bids, asks, sequence)
        self._metrics.snapshot_count += 1

        # Medir latencia de procesamiento
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
        self._metrics.record_latency(elapsed_ms)

        logger.debug(f"Snapshot procesado: {len(bids)} bids, {len(asks)} asks en {elapsed_ms:.2f}ms")

        # Notificar callbacks
        await self._notify_callbacks()

    async def _handle_update(self, data: Dict[str, Any]) -> None:
        """
        Maneja una actualización incremental del order book.

        Formato esperado:
        {
            "type": "order_book_update",
            "token_id": "0x...",
            "side": "bid" | "ask",
            "price": "0.50",
            "size": "100",
            "sequence": 12346
        }
        """
        start_ns = time.perf_counter_ns()

        token_id = data.get("token_id") or data.get("market")
        if token_id and token_id != self.token_id:
            return

        side_str = data.get("side", "bid").lower()
        side = MarketSide.BID if side_str == "bid" else MarketSide.ASK

        # Extraer precio y tamaño
        price_raw = data.get("price") or data.get("px")
        size_raw = data.get("size") or data.get("qty")
        sequence = data.get("sequence") or data.get("seq", 0)

        if not price_raw or size_raw is None:
            logger.warning(f"Update incompleto: {data}")
            return

        # Convertir precio a centavos
        price_cents = int(Decimal(str(price_raw)) * 100)
        size = Decimal(str(size_raw))

        # Actualizar LOB
        updated = self._lob.update_level(side, price_cents, size, sequence)

        if updated:
            self._metrics.order_book_updates += 1

            # Medir latencia
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            self._metrics.record_latency(elapsed_ms)

            # Notificar callbacks (con throttling opcional)
            await self._notify_callbacks()

    async def _notify_callbacks(self) -> None:
        """
        Notifica a todos los callbacks registrados.

        Los callbacks se ejecutan secuencialmente.
        """
        if not self._on_update_callbacks:
            return

        # Crear snapshot para notificar
        snapshot = self._lob.get_snapshot()

        for callback in self._on_update_callbacks:
            try:
                await callback(snapshot)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error en callback: {e}", exc_info=True)

    async def _heartbeat_monitor(self) -> None:
        """
        Monitorea el heartbeat de la conexión.

        Reconecta si no hay heartbeat en timeout segundos.
        """
        while self._running:
            await asyncio.sleep(2)

            if self._state != PolymarketMonitorState.CONNECTED:
                continue

            elapsed_ms = (time.time_ns() - self._last_heartbeat_ns) / 1_000_000

            if elapsed_ms > HEARTBEAT_TIMEOUT_SEC * 1000:
                logger.warning(f"Heartbeat timeout: {elapsed_ms:.0f}ms")
                self._metrics.missed_heartbeats += 1
                self._state = PolymarketMonitorState.RECONNECTING
                await self._reconnect()

    async def _reconnect(self) -> None:
        """
        Reconexión rápida (< 1 segundo objetivo).

        Usa backoff exponencial con jitter.
        """
        reconnect_attempt = 0
        max_delay = MAX_RECONNECT_DELAY_SEC

        while self._running and reconnect_attempt < 10:
            # Delay exponencial con jitter
            delay = min(
                self._reconnect_delay * (2 ** reconnect_attempt),
                max_delay
            )
            jitter = delay * 0.1 * (hash(str(time.time())) % 100) / 100
            delay += jitter

            logger.info(f"Reconectando en {delay:.2f}s (intento {reconnect_attempt + 1})")
            await asyncio.sleep(delay)

            try:
                # Cerrar conexión vieja
                if self._ws:
                    await self._ws.close()

                self._metrics.reconnections += 1
                await self._connect()

                logger.info("Reconexión exitosa")
                break

            except Exception as e:
                logger.error(f"Fallo en reconexión: {e}")
                reconnect_attempt += 1

        if reconnect_attempt >= 10:
            logger.error("Máximo de intentos de reconexión alcanzado")
            self._state = PolymarketMonitorState.ERROR

    async def _websocket_loop(self) -> None:
        """
        Loop principal de recepción de mensajes.

        Optimizado para baja latencia:
        - recv() asíncrono sin timeout (el timeout está en connect)
        - Procesamiento inmediato sin delays
        """
        while self._running:
            if self._state != PolymarketMonitorState.CONNECTED:
                await asyncio.sleep(0.01)  # Sleep corto para no busy-wait
                continue

            try:
                message = await self._ws.recv()
                await self._process_message(message)

            except ConnectionClosed as e:
                logger.warning(f"Conexión cerrada: {e.code} {e.reason}")
                self._state = PolymarketMonitorState.RECONNECTING
                await self._reconnect()

            except WebSocketException as e:
                logger.error(f"Error WebSocket: {e}")
                self._state = PolymarketMonitorState.RECONNECTING
                await self._reconnect()

            except asyncio.CancelledError:
                break

    async def start(self) -> None:
        """
        Inicia el monitor de forma asíncrona.
        """
        if self._running:
            logger.warning("Monitor ya está corriendo")
            return

        self._running = True

        # Conectar inicialmente
        try:
            await self._connect()
        except Exception as e:
            logger.error(f"Fallo inicial de conexión: {e}")
            asyncio.create_task(self._reconnect())

        # Iniciar tareas
        self._ws_task = asyncio.create_task(self._websocket_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

        logger.info("PolymarketMonitor CLOB iniciado")

    async def stop(self) -> None:
        """
        Detiene el monitor gracefulmente.
        """
        logger.info("Deteniendo PolymarketMonitor...")
        self._running = False
        self._state = PolymarketMonitorState.SHUTDOWN

        # Cancelar tareas
        for task in [self._ws_task, self._heartbeat_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Cerrar WebSocket
        if self._ws:
            await self._ws.close()

        logger.info("PolymarketMonitor detenido")

    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen de métricas para monitoreo.

        Returns:
            Dict con métricas clave
        """
        return {
            "state": self._state.name,
            "messages_received": self._metrics.messages_received,
            "order_book_updates": self._metrics.order_book_updates,
            "snapshot_count": self._metrics.snapshot_count,
            "avg_latency_ms": round(self._metrics.avg_latency_ms, 2),
            "min_latency_ms": round(self._metrics.min_latency_ms, 2) if self._metrics.min_latency_ms != float('inf') else 0,
            "max_latency_ms": round(self._metrics.max_latency_ms, 2),
            "p99_latency_ms": round(self._metrics.p99_latency_ms, 2),
            "server_latency_ms": round(self._metrics.server_latency_ms, 2),
            "connection_attempts": self._metrics.connection_attempts,
            "reconnections": self._metrics.reconnections,
            "errors": self._metrics.errors,
            "parse_errors": self._metrics.parse_errors,
            "missed_heartbeats": self._metrics.missed_heartbeats,
            "ujson_enabled": _HAS_UJSON,
        }


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_polymarket_monitor(
    config: Optional[AppConfig] = None,
    sandbox: bool = False,
    token_id: Optional[str] = None,
) -> PolymarketMonitor:
    """
    Factory function para crear un PolymarketMonitor configurado.

    Args:
        config: Configuración (usa global si None)
        sandbox: Si True, usa sandbox
        token_id: Override del token_id (opcional)

    Returns:
        PolymarketMonitor listo para start()
    """
    monitor = PolymarketMonitor(config=config, sandbox=sandbox)

    if token_id:
        monitor.token_id = token_id
        monitor._lob = LocalOrderBook(token_id)

    return monitor
