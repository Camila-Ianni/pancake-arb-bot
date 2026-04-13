"""
FastWeatherFeed - Feed de datos climáticos de baja latencia (WeatherAPI.com).

Este módulo se conecta a WeatherAPI.com para obtener observaciones climáticas
en tiempo real con la menor latencia posible, optimizado para HFT.

ARQUITECTURA:
- aiohttp.ClientSession con connection pooling (evita handshake TCP repetido)
- Polling agresivo cada 500ms-1s (configurable, respeta rate limits)
- Retry con exponential backoff para fallos transitorios
- Detección de rate limiting y notificación al RiskManager
- Timestamps duales: provider_timestamp vs local_timestamp para medir lag

HOT PATH OPTIMIZATIONS:
- JSON parsing mínimo (solo campos necesarios)
- Validación ultrarrápida sin regex ni parsing pesado
- Queue put_nowait para evitar bloqueos
- Sin I/O blocking en el loop de polling
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional, List, Callable, Any, Awaitable, Dict, Tuple
from enum import Enum, auto
import logging

import aiohttp
from aiohttp import ClientSession, ClientTimeout, ClientError, ClientResponseError

from ..models import WeatherObservation
from ..config import AppConfig, get_config
from ..logging_config import get_logger, get_latency_logger

logger = get_logger(__name__)
latency_logger = get_latency_logger(__name__)


# =============================================================================
# CONSTANTES Y CONFIGURACIÓN
# =============================================================================

# WeatherAPI endpoint (usar HTTP en vez de HTTPS para ~10-15ms menos de latencia)
WEATHERAPI_ENDPOINT = "http://api.weatherapi.com/v1/current.json"

# Rate limits de WeatherAPI por plan:
# - Free: 60 calls/min = 1 call/segundo
# - Standard: 600 calls/min = 10 calls/segundo
# - Enterprise: 6000+ calls/min
# Recomendado: 500ms para Standard, 1000ms para Free
DEFAULT_POLL_INTERVAL_SEC = 0.5

# Umbrales de validación de datos
VALID_TEMPERATURE_RANGE = (-90.0, 60.0)  # °C
VALID_HUMIDITY_RANGE = (0.0, 100.0)      # %
VALID_WIND_SPEED_RANGE = (0.0, 400.0)    # km/h
VALID_PRESSURE_RANGE = (870.0, 1084.0)   # hPa (récords mundiales)


class WeatherFeedState(Enum):
    """Estado del feed climático."""
    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    RECONNECTING = auto()
    ERROR = auto()
    HEARTBEAT_TIMEOUT = auto()
    RATE_LIMITED = auto()


@dataclass
class FeedMetrics:
    """
    Métricas en tiempo real del feed.

    Estas métricas se actualizan en cada iteración del loop
    y son usadas para monitoreo y debugging.
    """
    # Contadores
    observations_received: int = 0
    observations_valid: int = 0
    observations_invalid: int = 0
    requests_sent: int = 0

    # Latencia (round-trip local)
    last_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    min_latency_ms: float = float('inf')
    max_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    # Errores por tipo
    connection_errors: int = 0
    parse_errors: int = 0
    validation_errors: int = 0
    rate_limit_errors: int = 0
    timeout_errors: int = 0

    # Heartbeat
    last_heartbeat_ns: int = 0
    consecutive_heartbeat_failures: int = 0

    # Retry tracking
    consecutive_retries: int = 0
    max_consecutive_retries: int = 0

    # Muestras para percentiles (rolling window de últimas 1000)
    _latency_samples: List[float] = field(default_factory=list)

    def record_latency(self, latency_ms: float) -> None:
        """Registra una muestra de latencia y actualiza estadísticas."""
        self.last_latency_ms = latency_ms
        self.min_latency_ms = min(self.min_latency_ms, latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)

        # Moving average exponencial (más peso a lo reciente)
        self.avg_latency_ms = self.avg_latency_ms * 0.85 + latency_ms * 0.15

        # Muestras para percentiles
        self._latency_samples.append(latency_ms)
        if len(self._latency_samples) > 1000:
            self._latency_samples.pop(0)

        # Calcular p99
        if self._latency_samples:
            sorted_samples = sorted(self._latency_samples)
            p99_idx = int(len(sorted_samples) * 0.99)
            self.p99_latency_ms = sorted_samples[p99_idx]

    def record_retry(self) -> None:
        """Registra un reintento y actualiza el máximo consecutivo."""
        self.consecutive_retries += 1
        self.max_consecutive_retries = max(
            self.max_consecutive_retries,
            self.consecutive_retries
        )

    def reset_retry_counter(self) -> None:
        """Resetea el contador de reintentos tras éxito."""
        self.consecutive_retries = 0

    def reset(self) -> None:
        """Resetea todas las métricas."""
        self.__init__()


@dataclass
class RetryConfig:
    """Configuración de reintentos."""
    max_retries: int = 3
    base_delay_ms: int = 100
    max_delay_ms: int = 5000
    exponential_base: float = 2.0


class SensorValidationError(Exception):
    """Excepción para errores de validación de sensor."""
    pass


class FastWeatherFeed:
    """
    Feed de datos climáticos de baja latencia usando WeatherAPI.com.

    RESPONSABILIDADES:
    1. Polling de alta frecuencia (500ms-1s) a WeatherAPI
    2. Validación rápida de datos (rangos físicos razonables)
    3. Heartbeat monitoring para detectar datos stale
    4. Retry con exponential backoff para fallos transitorios
    5. Detección de rate limiting (HTTP 429)
    6. Notificación de nuevas observaciones vía callbacks

    WEATHERAPI RESPONSE FORMAT:
    {
        "location": {
            "name": "New York",
            "lat": 40.71,
            "lon": -74.01,
            ...
        },
        "current": {
            "temp_c": 25.5,
            "humidity": 60,
            "wind_kph": 15.5,
            "pressure_mb": 1013.0,
            "precip_mm": 0.0,
            "last_updated_epoch": 1699999999,
            "last_updated": "2024-01-15 14:00"
        }
    }

    HOT PATH CRITICAL:
    - El loop de polling NO debe tener I/O blocking
    - JSON parsing debe ser mínimo (solo campos necesarios)
    - La validación debe ser O(1) sin loops ni regex
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        poll_interval: Optional[float] = None,
        rate_limit_callback: Optional[Callable[[], Awaitable[None]]] = None,
    ):
        """
        Inicializa el feed climático.

        Args:
            config: Configuración de la aplicación (usa global si None)
            poll_interval: Intervalo de polling en segundos (default: 500ms)
            rate_limit_callback: Callback cuando se detecta rate limiting
        """
        self.config = config or get_config()
        # Usar poll_interval de config si no se proporciona explícitamente
        self.poll_interval = poll_interval or self.config.weather_feed.poll_interval_sec
        self.rate_limit_callback = rate_limit_callback

        # Configuración desde config
        self.api_key = self.config.weather_feed.api_key
        self.latitude = self.config.weather_feed.latitude
        self.longitude = self.config.weather_feed.longitude
        self.heartbeat_timeout_sec = self.config.weather_feed.heartbeat_timeout_sec

        # Configuración de retry
        self.retry_config = RetryConfig(
            max_retries=self.config.performance.max_retries,
            base_delay_ms=int(self.config.performance.retry_delay_sec * 1000),
            max_delay_ms=5000,
        )

        # Estado interno
        self._state = WeatherFeedState.STOPPED
        self._metrics = FeedMetrics()
        self._last_observation: Optional[WeatherObservation] = None

        # Callbacks para notificaciones
        self._on_observation_callbacks: List[Callable[[WeatherObservation], Awaitable[None]]] = []

        # Session HTTP para connection pooling (se crea en start())
        self._session: Optional[ClientSession] = None

        # Control de ejecución
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Buffer circular de últimas observaciones (para debugging/auditoría)
        self._observation_buffer: List[WeatherObservation] = []
        self._buffer_max_size = 100

        # URL de request (construida una vez)
        self._request_url = self._build_request_url()

        logger.info(
            f"FastWeatherFeed inicializado: endpoint={WEATHERAPI_ENDPOINT}, "
            f"lat={self.latitude}, lon={self.longitude}, poll_interval={self.poll_interval}s"
        )

    @property
    def state(self) -> WeatherFeedState:
        """Estado actual del feed."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Verifica si el feed está corriendo."""
        return self._state == WeatherFeedState.RUNNING

    @property
    def metrics(self) -> FeedMetrics:
        """Métricas en tiempo real."""
        return self._metrics

    @property
    def last_observation(self) -> Optional[WeatherObservation]:
        """Última observación recibida."""
        return self._last_observation

    def on_observation(self, callback: Callable[[WeatherObservation], Awaitable[None]]) -> None:
        """
        Registra un callback para nuevas observaciones.

        El callback se llama cada vez que se recibe un dato válido.
        Los callbacks se ejecutan en el orden de registro.

        Args:
            callback: Función async que recibe WeatherObservation
        """
        self._on_observation_callbacks.append(callback)
        logger.debug(f"Callback registrado. Total: {len(self._on_observation_callbacks)}")

    def _build_request_url(self) -> str:
        """
        Construye la URL de request con parámetros.

        URL optimizada para WeatherAPI.com:
        - HTTP (no HTTPS) para menor latencia (~10-15ms menos)
        - Parámetros mínimos necesarios
        - Cache-busting con timestamp para evitar CDN cache

        Returns:
            URL completa para el request
        """
        import urllib.parse

        # Parámetros esenciales (menos parámetros = menos overhead)
        params = {
            "key": self.api_key,
            "q": f"{self.latitude},{self.longitude}",
            "aqi": "no",  # No necesitamos air quality
        }

        # Cache-busting: agregar timestamp para evitar caching
        # WeatherAPI puede cachear por ~1 minuto, queremos datos frescos
        params["_t"] = str(int(time.time() * 1000))

        query_string = urllib.parse.urlencode(params)
        return f"{WEATHERAPI_ENDPOINT}?{query_string}"

    def _validate_sensor_data(self, raw_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Valida que los datos del sensor sean físicamente posibles.

        HOT PATH: Esta función debe ser extremadamente rápida.
        Sin regex, sin loops, solo comparaciones directas.

        Args:
            raw_data: Datos crudos de la API

        Returns:
            (is_valid, error_message) - True si los datos son válidos

        Raises:
            SensorValidationError: Si los datos fallan validación
        """
        current = raw_data.get("current")
        if not current:
            return False, "Campo 'current' faltante"

        # Validar temperatura
        temp = current.get("temp_c")
        if temp is None:
            return False, "Temperatura faltante"
        if not isinstance(temp, (int, float)):
            return False, f"Temperatura no numérica: {temp}"
        if not (VALID_TEMPERATURE_RANGE[0] <= temp <= VALID_TEMPERATURE_RANGE[1]):
            raise SensorValidationError(
                f"Temperatura fuera de rango físico: {temp}°C "
                f"(válido: {VALID_TEMPERATURE_RANGE[0]}-{VALID_TEMPERATURE_RANGE[1]})"
            )

        # Validar humedad (opcional pero si está presente, validar)
        humidity = current.get("humidity")
        if humidity is not None:
            if not isinstance(humidity, (int, float)):
                return False, f"Humedad no numérica: {humidity}"
            if not (VALID_HUMIDITY_RANGE[0] <= humidity <= VALID_HUMIDITY_RANGE[1]):
                raise SensorValidationError(
                    f"Humedad fuera de rango: {humidity}% "
                    f"(válido: {VALID_HUMIDITY_RANGE[0]}-{VALID_HUMIDITY_RANGE[1]})"
                )

        # Validar velocidad del viento
        wind_kph = current.get("wind_kph")
        if wind_kph is not None:
            if not isinstance(wind_kph, (int, float)):
                return False, f"Viento no numérico: {wind_kph}"
            if not (VALID_WIND_SPEED_RANGE[0] <= wind_kph <= VALID_WIND_SPEED_RANGE[1]):
                raise SensorValidationError(
                    f"Viento fuera de rango: {wind_kph} km/h"
                )

        # Validar presión
        pressure = current.get("pressure_mb")
        if pressure is not None:
            if not isinstance(pressure, (int, float)):
                return False, f"Presión no numérica: {pressure}"
            if not (VALID_PRESSURE_RANGE[0] <= pressure <= VALID_PRESSURE_RANGE[1]):
                raise SensorValidationError(
                    f"Presión fuera de rango: {pressure} hPa"
                )

        return True, None

    def _parse_observation(self, raw_data: Dict[str, Any], received_at_ns: int) -> WeatherObservation:
        """
        Parsea datos crudos de WeatherAPI a WeatherObservation.

        HOT PATH: Parsing mínimo, solo campos necesarios.
        No crear objetos intermedios innecesarios.

        Args:
            raw_data: Respuesta cruda de la API
            received_at_ns: Timestamp de recepción en nanosegundos

        Returns:
            WeatherObservation parseado
        """
        current = raw_data.get("current", {})

        # Provider timestamp: cuándo WeatherAPI generó el dato
        # last_updated_epoch viene en segundos desde epoch
        provider_timestamp_sec = current.get("last_updated_epoch", 0)
        provider_timestamp_ns = int(provider_timestamp_sec * 1_000_000_000)

        # Calcular lag entre provider y nosotros
        provider_lag_ms = (received_at_ns - provider_timestamp_ns) / 1_000_000

        # Extraer campos esenciales (minimizar parsing)
        temperature = current.get("temp_c")
        humidity = current.get("humidity")
        wind_speed = current.get("wind_kph")
        precipitation = current.get("precip_mm")
        pressure = current.get("pressure_mb")

        # Determinar si el dato es "live" o histórico
        # WeatherAPI siempre devuelve datos actuales, no históricos
        is_live = True

        # Quality score: 1.0 si todos los campos están presentes
        quality_score = 1.0
        fields_present = sum([
            temperature is not None,
            humidity is not None,
            wind_speed is not None,
            pressure is not None,
        ])
        if fields_present < 3:
            quality_score = 0.7  # Datos incompletos

        return WeatherObservation(
            timestamp_ns=provider_timestamp_ns,
            received_at_ns=received_at_ns,
            source="WeatherAPI",
            temperature_c=temperature,
            humidity_pct=humidity,
            wind_speed_kmh=wind_speed,
            precipitation_mm=precipitation,
            pressure_hpa=pressure,
            quality_score=quality_score,
            is_live=is_live,
        )

    async def _fetch_with_retry(self) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos de la API con retry exponencial.

        Maneja:
        - Timeout de red
        - Errores de conexión
        - Rate limiting (HTTP 429)
        - Errores del servidor (5xx)

        Returns:
            Datos crudos o None después de agotar reintentos
        """
        last_error = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                return await self._fetch_once()

            except ClientResponseError as e:
                last_error = e

                if e.status == 429:
                    # Rate limit - notificar y esperar más
                    self._metrics.rate_limit_errors += 1
                    logger.warning(
                        f"⚠️ RATE LIMIT detectado (attempt {attempt + 1})"
                    )

                    # Notificar al callback si existe
                    if self.rate_limit_callback:
                        await self.rate_limit_callback()

                    # Backoff más agresivo para rate limit
                    delay_ms = min(
                        self.retry_config.max_delay_ms,
                        self.retry_config.base_delay_ms * (4 ** attempt)  # 4x en vez de 2x
                    )
                    await asyncio.sleep(delay_ms / 1000)

                elif e.status >= 500:
                    # Error del servidor - retry normal
                    logger.warning(f"Error del servidor (5xx): {e.status}")
                    await self._calculate_backoff(attempt)

                else:
                    # Error del cliente (4xx) - no retry
                    logger.error(f"Error del cliente (4xx): {e.status}")
                    self._metrics.parse_errors += 1
                    return None

            except asyncio.TimeoutError:
                self._metrics.timeout_errors += 1
                last_error = TimeoutError("Request timeout")
                logger.debug(f"Timeout (attempt {attempt + 1})")
                await self._calculate_backoff(attempt)

            except ClientError as e:
                self._metrics.connection_errors += 1
                last_error = e
                logger.warning(f"Error de conexión (attempt {attempt + 1}): {e}")
                await self._calculate_backoff(attempt)

            except Exception as e:
                last_error = e
                logger.error(f"Error inesperado (attempt {attempt + 1}): {e}", exc_info=True)
                await self._calculate_backoff(attempt)

        # Agotados todos los reintentos
        logger.error(f"Falló después de {self.retry_config.max_retries + 1} intentos: {last_error}")
        self._metrics.record_retry()
        return None

    async def _fetch_once(self) -> Dict[str, Any]:
        """
        Realiza un único request a la API.

        Returns:
            Datos crudos de la API

        Raises:
            ClientResponseError: Si la API retorna error HTTP
            ClientError: Si hay error de conexión
            asyncio.TimeoutError: Si el request timeout
        """
        if not self._session:
            raise RuntimeError("Session no inicializada")

        request_start_ns = time.time_ns()

        try:
            # Usar timeout configurable
            timeout = ClientTimeout(total=self.config.performance.network_timeout_sec)

            # Request GET con timeout
            async with self._session.get(self._request_url, timeout=timeout) as response:
                # Medir latencia de la respuesta
                response_received_ns = time.time_ns()
                round_trip_ms = (response_received_ns - request_start_ns) / 1_000_000

                # Verificar status HTTP
                if response.status != 200:
                    raise ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}",
                    )

                # Parsear JSON
                data = await response.json()

                # Validar que hay datos
                if not data or "current" not in data:
                    raise ValueError("Respuesta inválida: falta 'current'")

                self._metrics.requests_sent += 1
                self._metrics.record_latency(round_trip_ms)

                return data

        finally:
            # Actualizar URL con nuevo timestamp para cache-busting
            self._request_url = self._build_request_url()

    async def _calculate_backoff(self, attempt: int) -> None:
        """
        Calcula y aplica backoff exponencial entre reintentos.

        Args:
            attempt: Número de intento actual (0-indexed)
        """
        if attempt >= self.retry_config.max_retries:
            return

        delay_ms = min(
            self.retry_config.max_delay_ms,
            self.retry_config.base_delay_ms * (
                self.retry_config.exponential_base ** attempt
            )
        )

        # Jitter aleatorio para evitar thundering herd
        jitter_ms = delay_ms * 0.1 * asyncio.get_event_loop().time() % 1
        delay_ms += jitter_ms

        logger.debug(f"Backoff: {delay_ms:.0f}ms (attempt {attempt + 1})")
        await asyncio.sleep(delay_ms / 1000)

    async def _run_loop(self) -> None:
        """
        Loop principal de polling.

        Este es el HOT PATH del feed:
        1. Fetch datos de WeatherAPI
        2. Validar datos (rangos físicos)
        3. Crear WeatherObservation con timestamps
        4. Poner en queue (no bloqueante)
        5. Notificar callbacks
        6. Sleep hasta próximo intervalo

        HOT PATH OPTIMIZATIONS:
        - Sin I/O blocking excepto el request HTTP
        - Validación O(1) sin loops
        - put_nowait para evitar await en queue
        """
        while self._running:
            loop_start_ns = time.perf_counter_ns()

            try:
                # =========================================================================
                # FASE 1: FETCH DATOS (con retry)
                # =========================================================================
                raw_data = await self._fetch_with_retry()

                if raw_data is None:
                    # Fallaron todos los reintentos
                    self._metrics.observations_invalid += 1
                    self._metrics.consecutive_heartbeat_failures += 1
                    logger.warning("Sin datos después de retries")

                    # Calcular sleep restante para mantener intervalo
                    elapsed_ns = time.perf_counter_ns() - loop_start_ns
                    elapsed_ms = elapsed_ns / 1_000_000
                    remaining_ms = (self.poll_interval * 1000) - elapsed_ms
                    if remaining_ms > 0:
                        await asyncio.sleep(remaining_ms / 1000)
                    continue

                # =========================================================================
                # FASE 2: VALIDACIÓN RÁPIDA
                # =========================================================================
                receive_time_ns = time.time_ns()

                try:
                    is_valid, error_msg = self._validate_sensor_data(raw_data)

                    if not is_valid:
                        self._metrics.observations_invalid += 1
                        self._metrics.validation_errors += 1
                        logger.warning(f"Dato inválido: {error_msg}")
                        continue

                except SensorValidationError as e:
                    self._metrics.observations_invalid += 1
                    self._metrics.validation_errors += 1
                    logger.error(f"Error de validación de sensor: {e}")
                    continue

                # =========================================================================
                # FASE 3: PARSE Y CREAR OBSERVATION
                # =========================================================================
                observation = self._parse_observation(raw_data, receive_time_ns)

                # =========================================================================
                # FASE 4: ACTUALIZAR ESTADO Y MÉTRICAS
                # =========================================================================
                self._metrics.observations_valid += 1
                self._metrics.last_heartbeat_ns = receive_time_ns
                self._metrics.consecutive_heartbeat_failures = 0
                self._metrics.reset_retry_counter()
                self._metrics.observations_received += 1

                # =========================================================================
                # FASE 5: BUFFER Y NOTIFICAR (HOT PATH - no bloquear)
                # =========================================================================
                # Agregar al buffer circular
                self._observation_buffer.append(observation)
                if len(self._observation_buffer) > self._buffer_max_size:
                    self._observation_buffer.pop(0)

                # Actualizar última observación
                self._last_observation = observation

                # Notificar callbacks (en paralelo, no bloquear si uno es lento)
                await self._notify_callbacks(observation)

            except asyncio.CancelledError:
                logger.info("Poll loop cancelado")
                break

            except Exception as e:
                logger.error(f"Error en poll loop: {e}", exc_info=True)
                self._metrics.connection_errors += 1
                self._state = WeatherFeedState.ERROR
                # Backoff de emergencia
                await asyncio.sleep(1)

            # =========================================================================
            # FASE 6: MANTENER INTERVALO CONSTANTE
            # =========================================================================
            elapsed_ns = time.perf_counter_ns() - loop_start_ns
            elapsed_ms = elapsed_ns / 1_000_000
            target_interval_ms = self.poll_interval * 1000

            if elapsed_ms < target_interval_ms:
                sleep_ms = target_interval_ms - elapsed_ms
                # Solo sleep si queda tiempo significativo (>1ms)
                if sleep_ms > 1:
                    await asyncio.sleep(sleep_ms / 1000)
            else:
                # Tardamos más que el intervalo - loguear warning
                logger.warning(
                    f"Loop más lento que el intervalo: {elapsed_ms:.1f}ms > {target_interval_ms:.1f}ms"
                )

    async def _heartbeat_monitor(self) -> None:
        """
        Monitorea el heartbeat del feed.

        Si no recibimos datos frescos en timeout segundos:
        1. Cambiar estado a HEARTBEAT_TIMEOUT
        2. Incrementar contador de fallos
        3. Notificar al sistema (vía log por ahora)

        Este monitor corre en paralelo al poll loop.
        """
        while self._running:
            await asyncio.sleep(1)  # Chequear cada segundo

            if self._state != WeatherFeedState.RUNNING:
                continue

            elapsed_ms = (time.time_ns() - self._metrics.last_heartbeat_ns) / 1_000_000

            if elapsed_ms > self.heartbeat_timeout_sec * 1000:
                logger.warning(
                    f"❤️ HEARTBEAT TIMEOUT: {elapsed_ms:.0f}ms sin datos frescos "
                    f"(threshold: {self.heartbeat_timeout_sec * 1000:.0f}ms)"
                )
                self._state = WeatherFeedState.HEARTBEAT_TIMEOUT
                self._metrics.consecutive_heartbeat_failures += 1

            elif elapsed_ms > self.config.risk.max_feed_latency_ms:
                # Advertencia de latencia alta (pero no timeout completo)
                logger.warning(
                    f"⚠️ Latencia del feed alta: {elapsed_ms:.0f}ms > {self.config.risk.max_feed_latency_ms}ms"
                )

    async def _notify_callbacks(self, observation: WeatherObservation) -> None:
        """
        Notifica a todos los callbacks registrados.

        Los callbacks se ejecutan secuencialmente para mantener orden.
        Si un callback falla, se loguea el error pero se continúa con los demás.

        Args:
            observation: Observación a notificar
        """
        for callback in self._on_observation_callbacks:
            try:
                await callback(observation)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error en callback de observación: {e}", exc_info=True)

    async def start(self) -> None:
        """
        Inicia el feed de forma asíncrona.

        Crea la sesión HTTP con connection pooling optimizado:
        - TCPConnector con pool de 10 conexiones
        - DNS cache por 300 segundos
        - Timeout configurable
        """
        if self._running:
            logger.warning("Feed ya está corriendo")
            return

        self._running = True
        self._state = WeatherFeedState.STARTING

        # Crear sesión HTTP con connection pooling optimizado
        timeout = ClientTimeout(total=self.config.performance.network_timeout_sec)
        self._session = ClientSession(
            timeout=timeout,
            connector=aiohttp.TCPConnector(
                limit=10,  # Pool de 10 conexiones
                ttl_dns_cache=300,  # DNS cache por 5 minutos
                enable_cleanup_closed=True,  # Limpiar conexiones cerradas
            ),
        )

        logger.info("Iniciando FastWeatherFeed...")

        # Iniciar tareas en background
        self._poll_task = asyncio.create_task(self._run_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())

        self._state = WeatherFeedState.RUNNING
        logger.info(
            f"✅ FastWeatherFeed iniciado (poll_interval={self.poll_interval}s, "
            f"endpoint={WEATHERAPI_ENDPOINT})"
        )

    async def stop(self) -> None:
        """
        Detiene el feed gracefulmente.

        Cancela tareas y cierra la sesión HTTP.
        """
        logger.info("Deteniendo FastWeatherFeed...")
        self._running = False
        self._state = WeatherFeedState.STOPPED

        # Cancelar tareas
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Cerrar sesión HTTP
        if self._session:
            await self._session.close()
            self._session = None

        logger.info("FastWeatherFeed detenido")

    async def get_latest(self) -> Optional[WeatherObservation]:
        """
        Obtiene la última observación válida.

        Returns:
            WeatherObservation o None si no hay datos
        """
        return self._last_observation

    async def get_temperature(self) -> Optional[float]:
        """
        Obtiene la última temperatura registrada.

        Returns:
            Temperatura en °C o None
        """
        obs = await self.get_latest()
        return obs.temperature_c if obs else None

    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen de métricas para logging/monitoreo.

        Returns:
            Dict con métricas clave
        """
        return {
            "state": self._state.name,
            "observations_received": self._metrics.observations_received,
            "observations_valid": self._metrics.observations_valid,
            "observations_invalid": self._metrics.observations_invalid,
            "requests_sent": self._metrics.requests_sent,
            "avg_latency_ms": round(self._metrics.avg_latency_ms, 2),
            "min_latency_ms": round(self._metrics.min_latency_ms, 2) if self._metrics.min_latency_ms != float('inf') else 0,
            "max_latency_ms": round(self._metrics.max_latency_ms, 2),
            "p99_latency_ms": round(self._metrics.p99_latency_ms, 2),
            "connection_errors": self._metrics.connection_errors,
            "timeout_errors": self._metrics.timeout_errors,
            "rate_limit_errors": self._metrics.rate_limit_errors,
            "validation_errors": self._metrics.validation_errors,
            "consecutive_heartbeat_failures": self._metrics.consecutive_heartbeat_failures,
            "max_consecutive_retries": self._metrics.max_consecutive_retries,
        }


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_weather_feed(
    config: Optional[AppConfig] = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SEC,
    rate_limit_callback: Optional[Callable[[], Awaitable[None]]] = None,
) -> FastWeatherFeed:
    """
    Factory function para crear un FastWeatherFeed configurado.

    Args:
        config: Configuración (usa global si None)
        poll_interval: Intervalo de polling en segundos
        rate_limit_callback: Callback cuando se detecta rate limiting

    Returns:
        FastWeatherFeed inicializado y listo para start()
    """
    return FastWeatherFeed(
        config=config,
        poll_interval=poll_interval,
        rate_limit_callback=rate_limit_callback,
    )
