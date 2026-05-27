"""
RiskManager - Gestión de riesgos y circuit breaker.

Este módulo monitorea el estado del sistema y decide si es seguro
ejecutar operaciones de trading.

ARQUITECTURA:
- Circuit breaker pattern para protección contra pérdidas en cascada
- Monitoreo de latencia del feed y ejecución
- Tracking de P&L en tiempo real
- Límites de exposición configurables

HOT PATH OPTIMIZATIONS:
- Estado del circuit breaker en variable atómica
- Contadores incrementales sin locks (usando asyncio-safe operations)
- Validaciones rápidas antes de permitir ejecución
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, Dict, Any, List
from enum import Enum, auto
import logging

logger = logging.getLogger(__name__)


# ─── Tipos locales (antes importados de models con tipos inexistentes) ────

class CircuitBreakerState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class RiskMetrics:
    """Métricas de riesgo en tiempo real."""
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    total_wins: int = 0
    total_losses: int = 0
    total_pnl_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    failed_transactions: int = 0
    successful_transactions: int = 0
    last_feed_latency_ms: float = 0.0
    avg_feed_latency_ms: float = 0.0

    @property
    def win_rate(self) -> float:
        total = self.total_wins + self.total_losses
        return self.total_wins / total if total > 0 else 0.0

    @property
    def transaction_success_rate(self) -> float:
        total = self.successful_transactions + self.failed_transactions
        return self.successful_transactions / total if total > 0 else 0.0

    def record_win(self, pnl: Decimal) -> None:
        self.total_wins += 1
        self.consecutive_wins += 1
        self.consecutive_losses = 0
        self.total_pnl_usd += pnl

    def record_loss(self, pnl: Decimal) -> None:
        self.total_losses += 1
        self.consecutive_losses += 1
        self.consecutive_wins = 0
        self.total_pnl_usd -= pnl

    def record_successful_transaction(self) -> None:
        self.successful_transactions += 1

    def record_failed_transaction(self) -> None:
        self.failed_transactions += 1


@dataclass
class RiskLimits:
    """Límites de riesgo configurables."""
    max_consecutive_losses: int = 3
    max_feed_latency_ms: int = 500
    max_failed_transactions: int = 5
    max_daily_loss_usd: Decimal = field(default_factory=lambda: Decimal("500"))
    max_position_size_usd: Decimal = field(default_factory=lambda: Decimal("1000"))
    circuit_breaker_cooldown_sec: int = 300


class RiskManager:
    """
    Gestor de riesgos con circuit breaker.

    RESPONSABILIDADES:
    1. Monitorear métricas de riesgo en tiempo real
    2. Activar circuit breaker cuando se violan límites
    3. Validar si una operación es segura de ejecutar
    4. Tracking de P&L y estadísticas

    CIRCUIT BREAKER STATES:
    - CLOSED: Operando normalmente, todas las operaciones permitidas
    - OPEN: Detenido por pérdidas/errores, ninguna operación permitida
    - HALF_OPEN: Probando con operación pequeña para recuperar
    """

    def __init__(
        self,
        dry_run: bool = True,
        max_consecutive_losses: int = 3,
        max_feed_latency_ms: int = 500,
        max_failed_transactions: int = 5,
        circuit_breaker_cooldown_sec: int = 300,
    ):
        self.dry_run = dry_run

        # Límites de riesgo
        self.limits = RiskLimits(
            max_consecutive_losses=max_consecutive_losses,
            max_feed_latency_ms=max_feed_latency_ms,
            max_failed_transactions=max_failed_transactions,
            circuit_breaker_cooldown_sec=circuit_breaker_cooldown_sec,
        )

        # Métricas en tiempo real
        self.metrics = RiskMetrics()

        # Estado del circuit breaker
        self._circuit_breaker_state = CircuitBreakerState.CLOSED
        self._circuit_breaker_triggered_at: Optional[int] = None
        self._circuit_breaker_reason: Optional[str] = None

        # Lock para actualizaciones de estado
        self._state_lock = asyncio.Lock()

        # Historial de operaciones (para análisis)
        self._trade_history: List[Dict[str, Any]] = []
        self._max_history = 1000

        # Alertas activas
        self._active_alerts: List[str] = []

        logger.info(
            f"RiskManager inicializado: dry_run={dry_run}, "
            f"max_losses={self.limits.max_consecutive_losses}, "
            f"max_latency_ms={self.limits.max_feed_latency_ms}"
        )

    @property
    def circuit_breaker_state(self) -> CircuitBreakerState:
        """Estado actual del circuit breaker."""
        return self._circuit_breaker_state

    @property
    def is_circuit_open(self) -> bool:
        """Verifica si el circuit breaker está abierto (operaciones bloqueadas)."""
        return self._circuit_breaker_state == CircuitBreakerState.OPEN

    @property
    def is_trading_allowed(self) -> bool:
        """Verifica si se permiten operaciones."""
        if self.dry_run:
            return True  # En dry run, siempre permitir (solo loguear)
        return self._circuit_breaker_state != CircuitBreakerState.OPEN

    def _trigger_circuit_breaker(self, reason: str) -> None:
        """Activa el circuit breaker."""
        self._circuit_breaker_state = CircuitBreakerState.OPEN
        self._circuit_breaker_triggered_at = time.time_ns()
        self._circuit_breaker_reason = reason

        logger.warning(f"⚠️ CIRCUIT BREAKER ACTIVADO: {reason}")

        # Agregar alerta
        alert = f"Circuit Breaker: {reason}"
        self._active_alerts.append(alert)
        if len(self._active_alerts) > 10:
            self._active_alerts.pop(0)

    def _try_reset_circuit_breaker(self) -> bool:
        """Intenta resetear el circuit breaker."""
        if self._circuit_breaker_state != CircuitBreakerState.OPEN:
            return True

        # Verificar cooldown
        if self._circuit_breaker_triggered_at:
            elapsed_sec = (time.time_ns() - self._circuit_breaker_triggered_at) / 1_000_000_000
            if elapsed_sec < self.limits.circuit_breaker_cooldown_sec:
                return False

        # Verificar condiciones para resetear
        should_reset = (
            self.metrics.consecutive_losses < self.limits.max_consecutive_losses and
            self.metrics.failed_transactions < self.limits.max_failed_transactions and
            self.metrics.last_feed_latency_ms < self.limits.max_feed_latency_ms
        )

        if should_reset:
            self._circuit_breaker_state = CircuitBreakerState.HALF_OPEN
            logger.info("Circuit breaker en HALF_OPEN - probando recuperación")
            return True

        return False

    async def check_circuit_breaker(self) -> bool:
        """Chequea y actualiza el estado del circuit breaker."""
        if self._circuit_breaker_state == CircuitBreakerState.OPEN:
            self._try_reset_circuit_breaker()
        return self.is_trading_allowed

    def record_feed_latency(self, latency_ms: float) -> None:
        """Registra latencia del feed."""
        self.metrics.last_feed_latency_ms = latency_ms
        self.metrics.avg_feed_latency_ms = (
            self.metrics.avg_feed_latency_ms * 0.9 + latency_ms * 0.1
        )

        if latency_ms > self.limits.max_feed_latency_ms:
            logger.warning(f"Latencia de feed excede límite: {latency_ms:.0f}ms > {self.limits.max_feed_latency_ms}ms")
            if self._circuit_breaker_state == CircuitBreakerState.CLOSED:
                self._trigger_circuit_breaker(
                    f"Feed latency {latency_ms:.0f}ms > {self.limits.max_feed_latency_ms}ms"
                )

    def record_trade_result(self, is_win: bool, pnl_usd: Decimal) -> None:
        """Registra el resultado de una operación."""
        if is_win:
            self.metrics.record_win(pnl_usd)
            logger.info(f"✅ Trade ganador: +${pnl_usd:.2f}")
        else:
            self.metrics.record_loss(abs(pnl_usd))
            logger.warning(f"❌ Trade perdedor: -${abs(pnl_usd):.2f}")

        # Check circuit breaker por pérdidas
        if self.metrics.consecutive_losses >= self.limits.max_consecutive_losses:
            self._trigger_circuit_breaker(
                f"{self.metrics.consecutive_losses} pérdidas consecutivas"
            )

        # Agregar al historial
        self._trade_history.append({
            "timestamp_ns": time.time_ns(),
            "is_win": is_win,
            "pnl_usd": float(pnl_usd),
        })

        # Limitar historial
        if len(self._trade_history) > self._max_history:
            self._trade_history.pop(0)

    def record_transaction_result(self, success: bool, error_message: Optional[str] = None) -> None:
        """Registra el resultado de una transacción on-chain."""
        if success:
            self.metrics.record_successful_transaction()
        else:
            self.metrics.record_failed_transaction()
            logger.warning(f"Transacción fallida: {error_message or 'Unknown error'}")

            if self.metrics.failed_transactions >= self.limits.max_failed_transactions:
                self._trigger_circuit_breaker(
                    f"{self.metrics.failed_transactions} transacciones fallidas"
                )

    def get_risk_summary(self) -> Dict[str, Any]:
        """Obtiene un resumen del estado de riesgo actual."""
        return {
            "circuit_breaker_state": self._circuit_breaker_state.name,
            "circuit_breaker_reason": self._circuit_breaker_reason,
            "consecutive_losses": self.metrics.consecutive_losses,
            "consecutive_wins": self.metrics.consecutive_wins,
            "win_rate": self.metrics.win_rate,
            "total_pnl_usd": float(self.metrics.total_pnl_usd),
            "failed_transactions": self.metrics.failed_transactions,
            "transaction_success_rate": self.metrics.transaction_success_rate,
            "last_feed_latency_ms": self.metrics.last_feed_latency_ms,
            "avg_feed_latency_ms": self.metrics.avg_feed_latency_ms,
            "active_alerts": self._active_alerts[-5:],
        }

    def reset_metrics(self) -> None:
        """Resetea todas las métricas (para testing o restart)."""
        self.metrics = RiskMetrics()
        self._circuit_breaker_state = CircuitBreakerState.CLOSED
        self._circuit_breaker_triggered_at = None
        self._circuit_breaker_reason = None
        self._trade_history.clear()
        self._active_alerts.clear()
        logger.info("RiskManager metrics reseteadas")
