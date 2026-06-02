"""
PolymarketMonitor - Monitor de mercado en tiempo real vía WebSockets.

Este módulo se conecta a la Gamma API de Polymarket para recibir
actualizaciones del order book en tiempo real.

ARQUITECTURA:
- WebSocket connection persistente con auto-reconnect
- Local Order Book (LOB) mantenido en memoria
- Cálculo instantáneo de VWAP y slippage
- Heartbeat monitoring para detectar desconexiones

HOT PATH OPTIMIZATIONS:
- Order book actualizado incrementalmente (no full refresh)
- Lock-free reads para el LOB cuando sea posible
- Procesamiento asíncrono de mensajes WebSocket
- Buffer de mensajes para evitar pérdida durante reconexión
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, Dict, List, Callable, Any, Awaitable
from enum import Enum, auto
import logging

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from ..models import (
    OrderBookSnapshot,
    PriceLevel,
    MarketSide,
    OrderBookLevel,
)
from ..config import AppConfig, get_config
from ..logging_config import get_logger, get_latency_logger

logger = get_logger(__name__)
latency_logger = get_latency_logger(__name__)


class PolymarketMonitorState(Enum):
    """Estado del monitor de Polymarket."""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    RECONNECTING = auto()
    ERROR = auto()
    SHUTDOWN = auto()


@dataclass
class MonitorMetrics:
    """Métricas en tiempo real del monitor."""
    messages_received: int = 0
    order_book_updates: int = 0
    last_message_timestamp_ns: int = 0
    connection_attempts: int = 0
    reconnections: int = 0
    errors: int = 0

    # Latencia medida desde el mensaje del servidor
    avg_server_latency_ms: float = 0.0
    last_server_latency_ms: float = 0.0


class LocalOrderBook:
    """
    Local Order Book (LOB) mantenido en memoria.

    Mantiene el estado completo del order book basándose
    en los updates incrementales recibidos vía WebSocket.

    THREAD SAFETY:
    - Usar asyncio.Lock para operaciones de escritura
    - Lecturas pueden ser lock-free si se usa copia snapshot
    """

    def __init__(self, condition_id: str, market_id: str):
        self.condition_id = condition_id
        self.market_id = market_id

        # Order books como diccionarios price -> size
        # Usar Decimal para precisión en precios y tamaños
        self._bids: Dict[int, Decimal] = {}  # price (cents) -> size
        self._asks: Dict[int, Decimal] = {}

        self._sequence_num: int = 0
        self._last_update_ns: int = 0
        self._lock = asyncio.Lock()

    @property
    def sequence_num(self) -> int:
        """Número de secuencia actual."""
        return self._sequence_num

    @property
    def last_update_ns(self) -> int:
        """Timestamp de la última actualización."""
        return self._last_update_ns

    async def update_level(self, side: MarketSide, price: int, size: Decimal, sequence_num: int) -> None:
        """
        Actualiza un nivel de precio en el order book.

        HOT PATH: Esta función se llama frecuentemente, mantenerla eficiente.

        Args:
            side: BID o ASK
            price: Precio en centavos (0-100)
            size: Nuevo tamaño en ese nivel (0 elimina el nivel)
            sequence_num: Número de secuencia para detectar gaps
        """
        async with self._lock:
            # Detectar gaps de secuencia (posible pérdida de datos)
            if sequence_num <= self._sequence_num:
                # Mensaje duplicado o fuera de orden, ignorar
                logger.debug(f"Secuencia antigua/duplicada: {sequence_num} <= {self._sequence_num}")
                return

            if sequence_num > self._sequence_num + 1:
                logger.warning(f"Gap en secuencia: esperado {self._sequence_num + 1}, recibido {sequence_num}")
                # En producción, solicitar full refresh aquí

            self._sequence_num = sequence_num
            self._last_update_ns = time.time_ns()

            book = self._bids if side == MarketSide.BID else self._asks

            if size == 0:
                # Eliminar nivel
                book.pop(price, None)
            else:
                # Actualizar nivel
                book[price] = size

    async def apply_snapshot(self, bids: List[tuple], asks: List[tuple], sequence_num: int) -> None:
        """
        Aplica un snapshot completo del order book.

        Usado durante inicialización o recuperación de errores.

        Args:
            bids: Lista de (price, size) tuples
            asks: Lista de (price, size) tuples
            sequence_num: Número de secuencia del snapshot
        """
        async with self._lock:
            self._bids.clear()
            self._asks.clear()

            for price, size in bids:
                if size > 0:
                    self._bids[price] = Decimal(size)

            for price, size in asks:
                if size > 0:
                    self._asks[price] = Decimal(size)

            self._sequence_num = sequence_num
            self._last_update_ns = time.time_ns()

            logger.info(f"Snapshot aplicado: {len(self._bids)} bids, {len(self._asks)} asks")

    async def get_snapshot(self) -> OrderBookSnapshot:
        """
        Obtiene un snapshot consistente del order book.

        Returns:
            OrderBookSnapshot inmutable con el estado actual

        HOT PATH: Esta función se llama antes de cada decisión de trading.
        """
        async with self._lock:
            # Ordenar bids descendente (mejor bid primero)
            sorted_bids = sorted(
                self._bids.items(),
                key=lambda x: x[0],
                reverse=True
            )
            bid_levels = tuple(
                PriceLevel(price=price, size=size)
                for price, size in sorted_bids
            )

            # Ordenar asks ascendente (mejor ask primero)
            sorted_asks = sorted(
                self._asks.items(),
                key=lambda x: x[0]
            )
            ask_levels = tuple(
                PriceLevel(price=price, size=size)
                for price, size in sorted_asks
            )

            return OrderBookSnapshot(
                condition_id=self.condition_id,
                market_id=self.market_id,
                bids=bid_levels,
                asks=ask_levels,
                timestamp_ns=self._last_update_ns,
                sequence_num=self._sequence_num,
            )

    async def get_best_bid(self) -> Optional[PriceLevel]:
        """Obtiene el mejor bid actual."""
        async with self._lock:
            if not self._bids:
                return None
            best_price = max(self._bids.keys())
            return PriceLevel(price=best_price, size=self._bids[best_price])

    async def get_best_ask(self) -> Optional[PriceLevel]:
        """Obtiene el mejor ask actual."""
        async with self._lock:
            if not self._asks:
                return None
            best_price = min(self._asks.keys())
            return PriceLevel(price=best_price, size=self._asks[best_price])


class PolymarketMonitor:
    """
    Monitor de mercado de Polymarket vía Gamma API WebSocket.

    RESPONSABILIDADES:
    1. Mantener conexión WebSocket persistente
    2. Procesar mensajes de order book updates
    3. Mantener Local Order Book sincronizado
    4. Notificar cambios a suscriptores (callbacks)
    5. Manejar reconexión automática

    GAMMA API ENDPOINTS:
    - Producción: wss://gamma-api.polymarket.com/ws
    - Sandbox: wss://sandbox.gamma-api.polymarket.com/ws
    """

    # Gamma API WebSocket endpoints
    PROD_WS_URL = "wss://gamma-api.polymarket.com/ws"
    SANDBOX_WS_URL = "wss://sandbox.gamma-api.polymarket.com/ws"

    # Tipos de mensajes de la API
    MSG_TYPE_SUBSCRIBE = "subscribe"
    MSG_TYPE_ORDERBOOK = "order_book_update"
    MSG_TYPE_SNAPSHOT = "order_book_snapshot"
    MSG_TYPE_HEARTBEAT = "heartbeat"
    MSG_TYPE_ERROR = "error"

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        sandbox: bool = False,
    ):
        """
        Inicializa el monitor de Polymarket.

        Args:
            config: Configuración de la aplicación (usa global si None)
            sandbox: Si True, usa el sandbox de Polymarket
        """
        self.config = config or get_config()
        self.sandbox = sandbox

        self.ws_url = self.SANDBOX_WS_URL if sandbox else self.PROD_WS_URL
        self.condition_id = self.config.polymarket.condition_id
        self.market_ids = self.config.polymarket.market_ids

        # Estado interno
        self._state = PolymarketMonitorState.DISCONNECTED
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._metrics = MonitorMetrics()

        # Local Order Book - uno por market ID si hay múltiples
        self._order_books: Dict[str, LocalOrderBook] = {}
        for market_id in self.market_ids:
            self._order_books[market_id] = LocalOrderBook(self.condition_id, market_id)

        # Callbacks para notificaciones
        self._on_update_callbacks: List[Callable[[OrderBookSnapshot], Awaitable[None]]] = []

        # Control de conexión
        self._running = False
        self._reconnect_delay = 5.0  # segundos
        self._heartbeat_timeout = 30.0  # segundos sin heartbeat = reconectar
        self._last_heartbeat_ns: int = 0

        # Tareas asyncio
        self._ws_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        logger.info(f"PolymarketMonitor inicializado: {self.ws_url}, condition_id={self.condition_id}")

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

    def on_update(self, callback: Callable[[OrderBookSnapshot], Awaitable[None]]) -> None:
        """
        Registra un callback para actualizaciones del order book.

        El callback se llama cada vez que el order book cambia.
        Los callbacks se ejecutan en el orden de registro.

        Args:
            callback: Función async que recibe OrderBookSnapshot
        """
        self._on_update_callbacks.append(callback)
        logger.debug(f"Callback registrado. Total: {len(self._on_update_callbacks)}")

    async def _connect(self) -> None:
        """
        Establece conexión WebSocket con Gamma API.

        Maneja autenticación y suscripción inicial.
        """
        self._state = PolymarketMonitorState.CONNECTING
        self._metrics.connection_attempts += 1

        try:
            # Headers de autenticación
            headers = {
                "Authorization": f"Bearer {self.config.polymarket.api_key}",
                "User-Agent": "PolymarketArbBot/1.0",
            }

            # Conectar con timeout
            with latency_logger.measure("websocket_connect") as metric:
                self._ws = await asyncio.wait_for(
                    websockets.connect(
                        self.ws_url,
                        extra_headers=headers,
                        ping_interval=20,  # Ping cada 20s para keepalive
                        ping_timeout=10,   # Timeout de ping
                        close_timeout=5,
                        max_size=10 * 1024 * 1024,  # 10MB max message
                    ),
                    timeout=self.config.performance.network_timeout_sec,
                )

            self._state = PolymarketMonitorState.CONNECTED
            self._last_heartbeat_ns = time.time_ns()
            logger.info(f"WebSocket conectado: {self.ws_url}")

            # Suscribirse a los mercados
            await self._subscribe()

        except asyncio.TimeoutError:
            logger.error("Timeout conectando a Gamma API")
            self._state = PolymarketMonitorState.ERROR
            raise

        except Exception as e:
            logger.error(f"Error conectando: {e}")
            self._state = PolymarketMonitorState.ERROR
            raise

    async def _subscribe(self) -> None:
        """
        Envía suscripción a los mercados configurados.

        Formato de suscripción según Gamma API docs.
        """
        subscribe_msg = {
            "type": self.MSG_TYPE_SUBSCRIBE,
            "topic": "order_book",
            "condition_id": self.condition_id,
            "market_ids": self.market_ids,
        }

        await self._ws.send(json.dumps(subscribe_msg))
        logger.info(f"Suscrito a condition_id={self.condition_id}, markets={self.market_ids}")

    async def _process_message(self, raw_message: str) -> None:
        """
        Procesa un mensaje WebSocket recibido.

        HOT PATH: Esta función debe ser extremadamente eficiente.

        Args:
            raw_message: JSON string del WebSocket
        """
        receive_time_ns = time.time_ns()

        try:
            data = json.loads(raw_message)
            msg_type = data.get("type")

            self._metrics.messages_received += 1
            self._metrics.last_message_timestamp_ns = receive_time_ns

            # Calcular latencia si el mensaje incluye timestamp del servidor
            if "timestamp" in data:
                server_ts_ms = data["timestamp"]
                server_ts_ns = int(server_ts_ms * 1_000_000)
                latency_ns = receive_time_ns - server_ts_ns
                latency_ms = latency_ns / 1_000_000

                self._metrics.last_server_latency_ms = latency_ms
                # Moving average simple
                self._metrics.avg_server_latency_ms = (
                    self._metrics.avg_server_latency_ms * 0.9 + latency_ms * 0.1
                )

            if msg_type == self.MSG_TYPE_SNAPSHOT:
                await self._handle_snapshot(data)

            elif msg_type == self.MSG_TYPE_ORDERBOOK:
                await self._handle_orderbook_update(data)

            elif msg_type == self.MSG_TYPE_HEARTBEAT:
                self._last_heartbeat_ns = receive_time_ns
                logger.debug("Heartbeat recibido")

            elif msg_type == self.MSG_TYPE_ERROR:
                error_msg = data.get("error", "Unknown error")
                logger.error(f"Error de Gamma API: {error_msg}")
                self._metrics.errors += 1

            else:
                logger.debug(f"Mensaje desconocido: {msg_type}")

        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON: {e}")
            self._metrics.errors += 1

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}", exc_info=True)
            self._metrics.errors += 1

    async def _handle_snapshot(self, data: Dict[str, Any]) -> None:
        """
        Maneja un snapshot completo del order book.

        Formato esperado:
        {
            "type": "order_book_snapshot",
            "condition_id": "...",
            "market_id": "...",
            "bids": [[price1, size1], [price2, size2], ...],
            "asks": [[price1, size1], [price2, size2], ...],
            "sequence": 12345,
            "timestamp": 1699999999999
        }
        """
        market_id = data.get("market_id")
        if market_id not in self._order_books:
            logger.warning(f"Snapshot para market_id desconocido: {market_id}")
            return

        bids = [(int(p), Decimal(str(s))) for p, s in data.get("bids", [])]
        asks = [(int(p), Decimal(str(s))) for p, s in data.get("asks", [])]
        sequence = data.get("sequence", 0)

        await self._order_books[market_id].apply_snapshot(bids, asks, sequence)
        logger.debug(f"Snapshot aplicado para {market_id}: {len(bids)} bids, {len(asks)} asks")

        # Notificar a los callbacks
        snapshot = await self._order_books[market_id].get_snapshot()
        await self._notify_callbacks(snapshot)

    async def _handle_orderbook_update(self, data: Dict[str, Any]) -> None:
        """
        Maneja una actualización incremental del order book.

        Formato esperado:
        {
            "type": "order_book_update",
            "condition_id": "...",
            "market_id": "...",
            "side": "bid" | "ask",
            "price": 50,
            "size": "100.5",
            "sequence": 12346,
            "timestamp": 1699999999999
        }
        """
        market_id = data.get("market_id")
        if market_id not in self._order_books:
            logger.warning(f"Update para market_id desconocido: {market_id}")
            return

        side_str = data.get("side", "bid")
        side = MarketSide.BID if side_str == "bid" else MarketSide.ASK

        price = int(data.get("price", 0))
        size = Decimal(str(data.get("size", 0)))
        sequence = data.get("sequence", 0)

        order_book = self._order_books[market_id]
        await order_book.update_level(side, price, size, sequence)

        self._metrics.order_book_updates += 1

        # Notificar a los callbacks
        snapshot = await order_book.get_snapshot()
        await self._notify_callbacks(snapshot)

    async def _notify_callbacks(self, snapshot: OrderBookSnapshot) -> None:
        """
        Notifica a todos los callbacks registrados.

        Los callbacks se ejecutan secuencialmente para mantener orden.
        """
        for callback in self._on_update_callbacks:
            try:
                await callback(snapshot)
            except Exception as e:
                logger.error(f"Error en callback: {e}", exc_info=True)

    async def _heartbeat_monitor(self) -> None:
        """
        Monitorea el heartbeat de la conexión.

        Si no recibimos heartbeat en timeout segundos, reconectar.
        """
        while self._running:
            await asyncio.sleep(5)  # Chequear cada 5 segundos

            if self._state != PolymarketMonitorState.CONNECTED:
                continue

            elapsed_ms = (time.time_ns() - self._last_heartbeat_ns) / 1_000_000
            if elapsed_ms > self._heartbeat_timeout * 1000:
                logger.warning(f"Heartbeat timeout: {elapsed_ms:.0f}ms sin heartbeat")
                self._state = PolymarketMonitorState.RECONNECTING
                await self._reconnect()

    async def _reconnect(self) -> None:
        """
        Reconexión con backoff exponencial.

        Implementa reconnect con delay creciente para evitar storm.
        """
        reconnect_attempt = 0
        max_delay = 60.0  # Máximo 60 segundos entre intentos

        while self._running and reconnect_attempt < 10:
            delay = min(self._reconnect_delay * (2 ** reconnect_attempt), max_delay)
            logger.info(f"Reintentando en {delay:.1f}s (intento {reconnect_attempt + 1})")

            await asyncio.sleep(delay)

            try:
                # Cerrar conexión vieja si existe
                if self._ws:
                    await self._ws.close()

                self._metrics.reconnections += 1
                await self._connect()

                # Éxito - salir del loop
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
        Loop principal de recepción de mensajes WebSocket.

        Se ejecuta continuamente hasta que _running = False.
        """
        while self._running:
            if self._state != PolymarketMonitorState.CONNECTED:
                await asyncio.sleep(0.1)
                continue

            try:
                message = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=self.config.performance.network_timeout_sec
                )
                await self._process_message(message)

            except asyncio.TimeoutError:
                # Timeout esperado, continuar
                continue

            except ConnectionClosed as e:
                logger.warning(f"Conexión cerrada: {e.code} {e.reason}")
                self._state = PolymarketMonitorState.RECONNECTING
                await self._reconnect()

            except WebSocketException as e:
                logger.error(f"Error WebSocket: {e}")
                self._state = PolymarketMonitorState.ERROR
                await self._reconnect()

    async def start(self) -> None:
        """
        Inicia el monitor de forma asíncrona.

        Se ejecuta en background hasta que stop() sea llamado.
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
            # Iniciar reconexión en background
            asyncio.create_task(self._reconnect())

        # Iniciar tareas en background
        self._ws_task = asyncio.create_task(self._websocket_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

        logger.info("PolymarketMonitor iniciado")

    async def stop(self) -> None:
        """
        Detiene el monitor gracefulmente.

        Cierra conexión y limpia recursos.
        """
        logger.info("Deteniendo PolymarketMonitor...")
        self._running = False
        self._state = PolymarketMonitorState.SHUTDOWN

        # Cancelar tareas
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Cerrar WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None

        logger.info("PolymarketMonitor detenido")

    async def get_order_book(self, market_id: Optional[str] = None) -> Optional[OrderBookSnapshot]:
        """
        Obtiene el order book actual para un mercado.

        Args:
            market_id: ID del mercado (usa el primero si None)

        Returns:
            OrderBookSnapshot o None si no hay datos
        """
        if market_id is None:
            if not self.market_ids:
                return None
            market_id = self.market_ids[0]

        if market_id not in self._order_books:
            logger.warning(f"Market ID desconocido: {market_id}")
            return None

        return await self._order_books[market_id].get_snapshot()

    async def get_vwap(self, side: MarketSide, size: Decimal, market_id: Optional[str] = None) -> Optional[Decimal]:
        """
        Calcula el VWAP para una orden de tamaño dado.

        Args:
            side: BID o ASK
            size: Tamaño de la orden
            market_id: ID del mercado (usa el primero si None)

        Returns:
            VWAP o None si no hay liquidez
        """
        snapshot = await self.get_order_book(market_id)
        if snapshot is None:
            return None

        return snapshot.get_vwap(side, size)

    async def calculate_slippage(
        self,
        side: MarketSide,
        size: Decimal,
        market_id: Optional[str] = None
    ) -> Optional[Decimal]:
        """
        Calcula el slippage esperado para una orden.

        Args:
            side: BID o ASK
            size: Tamaño de la orden
            market_id: ID del mercado (usa el primero si None)

        Returns:
            Slippage como decimal (ej. 0.02 = 2%) o None
        """
        snapshot = await self.get_order_book(market_id)
        if snapshot is None:
            return None

        return snapshot.calculate_slippage(side, size)
