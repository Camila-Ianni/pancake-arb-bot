"""
config.py - Configuración centralizada y validación de parámetros.

Este módulo carga y valida todas las variables de entorno necesarias.
La validación temprana previene fallos en tiempo de ejecución.

HOT PATH OPTIMIZATION:
- Las configuraciones se cargan UNA VEZ al inicio
- Los valores se cachean en variables globales para acceso O(1)
- No hay I/O durante la ejecución del hot path
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WalletConfig:
    """Configuración de wallet y RPC."""
    private_key: str
    rpc_url: str
    rpc_url_failover: Optional[str]
    wallet_address: str


@dataclass(frozen=True)
class PolymarketConfig:
    """Configuración de conexión a Polymarket."""
    api_key: str
    condition_id: str
    market_ids: List[str]


@dataclass(frozen=True)
class WeatherFeedConfig:
    """Configuración del feed climático."""
    api_url: str
    api_key: str
    latitude: float
    longitude: float
    heartbeat_timeout_sec: int
    poll_interval_sec: float  # Intervalo de polling en segundos


@dataclass(frozen=True)
class TradingConfig:
    """Parámetros de trading y ejecución."""
    bet_size_usd: float
    min_roi_threshold: float
    max_slippage_tolerance: float
    max_gas_price_gwei: float
    priority_fee_gwei: float


@dataclass(frozen=True)
class RiskConfig:
    """Configuración de gestión de riesgos."""
    max_consecutive_losses: int
    max_feed_latency_ms: int
    max_failed_transactions: int
    circuit_breaker_cooldown_sec: int


@dataclass(frozen=True)
class ExecutionConfig:
    """Configuración del modo de ejecución."""
    dry_run: bool
    log_level: str
    log_file_path: Optional[str]


@dataclass(frozen=True)
class TimeConfig:
    """Configuración de sincronización temporal."""
    ntp_servers: List[str]
    time_drift_tolerance_ms: int


@dataclass(frozen=True)
class PerformanceConfig:
    """Configuración de performance y tuning."""
    queue_max_size: int
    network_timeout_sec: int
    max_retries: int
    retry_delay_sec: float


@dataclass(frozen=True)
class AppConfig:
    """
    Configuración maestra de la aplicación.

    Inmutable (frozen) para prevenir modificaciones accidentales
    durante la ejecución que podrían causar comportamientos
    inconsistentes en los módulos.
    """
    wallet: WalletConfig
    polymarket: PolymarketConfig
    weather_feed: WeatherFeedConfig
    trading: TradingConfig
    risk: RiskConfig
    execution: ExecutionConfig
    time_sync: TimeConfig
    performance: PerformanceConfig


class ConfigError(Exception):
    """Excepción para errores de configuración crítica."""
    pass


def _parse_float_env(name: str, default: float = 0.0) -> float:
    """Parsea un float del entorno con validación."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise ConfigError(f"{name} debe ser un número válido, got: {value}")


def _parse_int_env(name: str, default: int = 0) -> int:
    """Parsea un int del entorno con validación."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ConfigError(f"{name} debe ser un entero válido, got: {value}")


def _parse_bool_env(name: str, default: bool = False) -> bool:
    """Parsea un booleano del entorno."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ('true', '1', 'yes', 'on')


def _parse_list_env(name: str, separator: str = ',') -> List[str]:
    """Parsea una lista separada por comas del entorno."""
    value = os.getenv(name)
    if not value:
        return []
    return [item.strip() for item in value.split(separator)]


def _validate_required_env(name: str, value: Optional[str]) -> None:
    """Valida que una variable requerida esté presente."""
    if not value:
        raise ConfigError(f"Variable de entorno requerida faltante: {name}")


def load_config() -> AppConfig:
    """
    Carga y valida toda la configuración desde variables de entorno.

    Returns:
        AppConfig: Configuración validada e inmutable.

    Raises:
        ConfigError: Si falta alguna configuración crítica.
    """
    # Cargar .env si existe
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Cargando configuración desde {env_path}")
    else:
        logger.warning("No se encontró .env, usando variables de entorno del sistema")

    # =========================================================================
    # WALLET CONFIG
    # =========================================================================
    private_key = os.getenv("PRIVATE_KEY", "")
    _validate_required_env("PRIVATE_KEY", private_key if private_key else None)

    rpc_url = os.getenv("RPC_URL", "")
    _validate_required_env("RPC_URL", rpc_url if rpc_url else None)

    wallet_config = WalletConfig(
        private_key=private_key,
        rpc_url=rpc_url,
        rpc_url_failover=os.getenv("RPC_URL_FAILOVER"),
        wallet_address=os.getenv("WALLET_ADDRESS", ""),
    )

    # =========================================================================
    # POLYMARKET CONFIG
    # =========================================================================
    api_key = os.getenv("POLYMARKET_API_KEY", "")
    condition_id = os.getenv("CONDITION_ID", "")

    polymarket_config = PolymarketConfig(
        api_key=api_key,
        condition_id=condition_id,
        market_ids=_parse_list_env("MARKET_IDS"),
    )

    # =========================================================================
    # WEATHER FEED CONFIG
    # =========================================================================
    weather_feed_config = WeatherFeedConfig(
        api_url=os.getenv("WEATHER_API_URL", "http://api.weatherapi.com/v1/current.json"),
        api_key=os.getenv("WEATHER_API_KEY", ""),
        latitude=_parse_float_env("WEATHER_LAT", 40.7128),
        longitude=_parse_float_env("WEATHER_LON", -74.0060),
        heartbeat_timeout_sec=_parse_int_env("HEARTBEAT_TIMEOUT_SEC", 5),
        poll_interval_sec=_parse_float_env("WEATHER_POLL_INTERVAL", 0.5),
    )

    # =========================================================================
    # TRADING CONFIG
    # =========================================================================
    trading_config = TradingConfig(
        bet_size_usd=_parse_float_env("BET_SIZE_USD", 100.0),
        min_roi_threshold=_parse_float_env("MIN_ROI_THRESHOLD", 0.08),
        max_slippage_tolerance=_parse_float_env("MAX_SLIPPAGE_TOLERANCE", 0.02),
        max_gas_price_gwei=_parse_float_env("MAX_GAS_PRICE_GWEI", 150.0),
        priority_fee_gwei=_parse_float_env("PRIORITY_FEE_GWEI", 2.0),
    )

    # =========================================================================
    # RISK CONFIG
    # =========================================================================
    risk_config = RiskConfig(
        max_consecutive_losses=_parse_int_env("MAX_CONSECUTIVE_LOSSES", 3),
        max_feed_latency_ms=_parse_int_env("MAX_FEED_LATENCY_MS", 500),
        max_failed_transactions=_parse_int_env("MAX_FAILED_TRANSACTIONS", 5),
        circuit_breaker_cooldown_sec=_parse_int_env("CIRCUIT_BREAKER_COOLDOWN_SEC", 300),
    )

    # =========================================================================
    # EXECUTION CONFIG
    # =========================================================================
    log_path = os.getenv("LOG_FILE_PATH")
    execution_config = ExecutionConfig(
        dry_run=_parse_bool_env("DRY_RUN", True),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_file_path=log_path if log_path else None,
    )

    # =========================================================================
    # TIME SYNC CONFIG
    # =========================================================================
    time_config = TimeConfig(
        ntp_servers=_parse_list_env("NTP_SERVERS"),
        time_drift_tolerance_ms=_parse_int_env("TIME_DRIFT_TOLERANCE_MS", 50),
    )

    # =========================================================================
    # PERFORMANCE CONFIG
    # =========================================================================
    performance_config = PerformanceConfig(
        queue_max_size=_parse_int_env("QUEUE_MAX_SIZE", 1000),
        network_timeout_sec=_parse_int_env("NETWORK_TIMEOUT_SEC", 5),
        max_retries=_parse_int_env("MAX_RETRIES", 3),
        retry_delay_sec=_parse_float_env("RETRY_DELAY_SEC", 0.1),
    )

    # =========================================================================
    # VALIDACIONES CRUZADAS
    # =========================================================================
    # Validar que min_roi > max_slippage (debe haber margen para ganancias)
    if trading_config.min_roi_threshold <= trading_config.max_slippage_tolerance:
        logger.warning(
            f"MIN_ROI_THRESHOLD ({trading_config.min_roi_threshold}) debe ser mayor que "
            f"MAX_SLIPPAGE_TOLERANCE ({trading_config.max_slippage_tolerance}) "
            "para que haya margen de ganancia después de slippage"
        )

    # Validar queue size suficiente
    if performance_config.queue_max_size < 100:
        logger.warning("QUEUE_MAX_SIZE muy pequeño puede causar pérdida de datos")

    config = AppConfig(
        wallet=wallet_config,
        polymarket=polymarket_config,
        weather_feed=weather_feed_config,
        trading=trading_config,
        risk=risk_config,
        execution=execution_config,
        time_sync=time_config,
        performance=performance_config,
    )

    logger.info("Configuración cargada y validada exitosamente")
    logger.info(f"Modo DRY_RUN: {config.execution.dry_run}")
    logger.info(f"ROI mínimo: {config.trading.min_roi_threshold:.2%}")
    logger.info(f"Condition ID: {config.polymarket.condition_id}")

    return config


# =============================================================================
# CONFIGURACIÓN GLOBAL (Singleton pattern)
# =============================================================================
# Se carga una sola vez al inicio del programa
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """
    Obtiene la configuración global (singleton).

    HOT PATH OPTIMIZATION:
    - Acceso directo a variable global (O(1))
    - Sin I/O después de la primera carga
    - Thread-safe para asyncio (inmutable después de carga)
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config() -> AppConfig:
    """
    Recarga la configuración desde el entorno.

    Usar solo en testing o recarga dinámica (no recomendado en producción).
    """
    global _config
    _config = load_config()
    return _config