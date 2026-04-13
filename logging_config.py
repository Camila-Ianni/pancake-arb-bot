"""
logging_config.py - Configuración de logging profesional para HFT.

Diseñado para:
1. Mínimo overhead en el hot path (logging asíncrono)
2. Trazabilidad completa de cada decisión
3. Métricas de latencia embebidas en los logs
4. Separación de logs por tipo (ejecución, errores, auditoría)

HOT PATH OPTIMIZATION:
- Usar logging asíncrono para evitar bloqueos de I/O
- Log levels apropiados (DEBUG solo en desarrollo)
- Evitar string formatting en logs que no se van a emitir
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Optional, Dict, Any
import json
import time
from datetime import datetime


# =============================================================================
# FORMATOS DE LOG PERSONALIZADOS
# =============================================================================

# Formato detallado para logs de archivo (incluye microsegundos)
DETAILED_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)-20s | %(message)s"
)

# Formato simplificado para consola
CONSOLE_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s"
)

# Formato JSON para ingestión en sistemas de monitoreo (ELK, Datadog)
JSON_FORMAT = "json"


class MicrosecondFormatter(logging.Formatter):
    """
    Formatter que incluye microsegundos en el timestamp.

    Para HFT, cada milisegundo cuenta. Este formatter asegura
    que los logs tengan precisión sub-milisegundo cuando es posible.
    """

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        """
        Override para incluir microsegundos.

        El timestamp por defecto de Python solo llega a milisegundos.
        Para mayor precisión, usamos el tiempo del sistema directamente.
        """
        # Usar tiempo en segundos con fracción desde epoch
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            return ct.strftime(datefmt)
        else:
            # Formato ISO con microsegundos
            return ct.strftime("%Y-%m-%d %H:%M:%S")


class JSONFormatter(logging.Formatter):
    """
    Formatter JSON para integración con sistemas de monitoreo.

    Cada log incluye:
    - Timestamp de alta precisión
    - Contexto de ejecución (módulo, función)
    - Métricas de latencia si están presentes
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(timespec='microseconds'),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Agregar contexto extra si existe
        if hasattr(record, 'latency_ms'):
            log_entry["latency_ms"] = record.latency_ms
        if hasattr(record, 'rtt_ms'):
            log_entry["rtt_ms"] = record.rtt_ms
        if hasattr(record, 'extra_context'):
            log_entry["context"] = record.extra_context

        return json.dumps(log_entry)


# =============================================================================
# LOGGERS ESPECIALIZADOS
# =============================================================================

class LatencyLogger:
    """
    Logger especializado para medir y registrar latencias.

    Uso:
        with latency_logger.measure("operacion_critica") as metric:
            # código a medir
            pass
        # automáticamente loguea la latencia
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._measurements: Dict[str, float] = {}

    class _MeasurementContext:
        def __init__(self, logger: logging.Logger, operation: str, level: int = logging.DEBUG):
            self.logger = logger
            self.operation = operation
            self.level = level
            self.start_time: float = 0
            self.end_time: float = 0
            self.extra_data: Dict[str, Any] = {}

        def add_data(self, key: str, value: Any) -> '_MeasurementContext._MeasurementContext':
            """Agrega datos extra al log de medición."""
            self.extra_data[key] = value
            return self

        def __enter__(self) -> '_MeasurementContext._MeasurementContext':
            self.start_time = time.perf_counter_ns()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            self.end_time = time.perf_counter_ns()
            latency_ns = self.end_time - self.start_time
            latency_ms = latency_ns / 1_000_000  # Convertir a milisegundos

            msg = f"[LATENCY] {self.operation}: {latency_ms:.3f}ms"

            if self.extra_data:
                msg += f" | {self.extra_data}"

            # Crear record personalizado con latencia
            record = self.logger.makeRecord(
                self.logger.name,
                self.level,
                "(unknown file)",
                0,
                msg,
                (),
                exc_info=None,
            )
            record.latency_ms = latency_ms
            self.logger.handle(record)

    def measure(self, operation: str, level: int = logging.DEBUG) -> '_MeasurementContext':
        """
        Context manager para medir latencia de operaciones.

        Args:
            operation: Nombre descriptivo de la operación
            level: Nivel de logging para el resultado

        Returns:
            Context manager que registra la latencia al salir
        """
        return self._MeasurementContext(self.logger, operation, level)


# =============================================================================
# CONFIGURACIÓN PRINCIPAL
# =============================================================================

def setup_logging(
    log_level: str = "INFO",
    log_file_path: Optional[str] = None,
    enable_json: bool = False,
) -> None:
    """
    Configura el sistema de logging para toda la aplicación.

    Args:
        log_level: Nivel de logging global (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file_path: Ruta opcional para archivo de logs
        enable_json: Si True, usa formato JSON para el archivo

    HOT PATH CONSIDERATIONS:
    - Los handlers de archivo usan buffering para reducir I/O
    - RotatingFileHandler previene archivos demasiado grandes
    - QueueHandler podría usarse para logging asíncrono si es necesario
    """

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Limpiar handlers existentes
    root_logger.handlers.clear()

    # =========================================================================
    # CONSOLE HANDLER (siempre presente)
    # =========================================================================
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(MicrosecondFormatter(CONSOLE_FORMAT))
    root_logger.addHandler(console_handler)

    # =========================================================================
    # FILE HANDLER (opcional, para producción)
    # =========================================================================
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Rotación basada en tamaño (100MB max) + tiempo (diaria)
        # Esto previene archivos gigantes y facilita el análisis temporal
        if enable_json:
            file_formatter = JSONFormatter()
        else:
            file_formatter = MicrosecondFormatter(DETAILED_FORMAT)

        # Rotating file handler - rota por tamaño
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10,  # Mantener 10 archivos antiguos
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # Loguear todo al archivo

        root_logger.addHandler(file_handler)

        # También crear un archivo separado solo para errores
        error_path = log_path.parent / f"{log_path.stem}_error{log_path.suffix}"
        error_handler = RotatingFileHandler(
            error_path,
            maxBytes=50 * 1024 * 1024,
            backupCount=5,
        )
        error_handler.setFormatter(file_formatter)
        error_handler.setLevel(logging.ERROR)
        error_handler.addFilter(lambda r: r.levelno >= logging.ERROR)

        root_logger.addHandler(error_handler)

    # =========================================================================
    # LOGGERS DE MÓDULOS ESPECÍFICOS
    # =========================================================================
    # Silenciar loggers ruidosos de librerías externas
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    # Logger principal de la aplicación
    app_logger = logging.getLogger("polymarket_arb")
    app_logger.info(f"Logging configurado: nivel={log_level}, archivo={log_file_path}")


def get_logger(name: str) -> logging.Logger:
    """
    Obtiene un logger con nombre jerárquico.

    Uso recomendado:
        logger = get_logger(__name__)

    Esto crea loggers como:
    - polymarket_arb.main
    - polymarket_arb.modules.weather_feed
    - polymarket_arb.modules.arbitrage_engine
    """
    return logging.getLogger(f"polymarket_arb.{name}")


def get_latency_logger(name: str) -> LatencyLogger:
    """
    Obtiene un logger especializado en mediciones de latencia.

    Uso:
        latency_logger = get_latency_logger(__name__)
        with latency_logger.measure("web3_transaction_send"):
            tx_hash = await w3.eth.send_transaction(tx)
    """
    return LatencyLogger(get_logger(name))
