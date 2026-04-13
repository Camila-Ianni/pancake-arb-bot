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

from ..config import AppConfig, get_config
from ..logging_config import get_logger
from ..models import CircuitBreakerState, RiskMetrics, ArbitrageSignal

logger = get_logger(__name__)


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
        config: Optional[AppConfig] = None,
        dry_run: bool = True,
    ):
        """
        Inicializa el RiskManager.

        Args:
            config: Configuración de la aplicación
            dry_run: Si True, no bloquea operaciones (solo loguea)
        """
        self.config = config or get_config()
        self.dry_run = dry_run

        # Límites de riesgo
        self.limits = RiskLimits(
            max_consecutive_losses=self.config.risk.max_consecutive_losses,
            max_feed_latency_ms=self.config.risk.max_feed_latency_ms,
            max_failed_transactions=self.config.risk.max_failed_transactions,
            max_daily_loss_usd=Decimal("500"),  # Configurable vía env
            max_position_size_usd=self.config.trading.bet_size_usd * 10,
            circuit_breaker_cooldown_sec=self.config.risk.circuit_breaker_cooldown_sec,
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
        """
        Activa el circuit breaker.

        Args:
            reason: Razón de la activación
        """
        self._circuit_breaker_state = CircuitBreakerState.OPEN
        self._circuit_breaker_triggered_at = time.time_ns()
        self._circuit_breaker_reason = reason

        logger.warning(
            f"⚠️ CIRCUIT BREAKER ACTIVADO: {reason}"
        )

        # Agregar alerta
        alert = f"Circuit Breaker: {reason}"
        self._active_alerts.append(alert)
        if len(self._active_alerts) > 10:
            self._active_alerts.pop(0)

    def _try_reset_circuit_breaker(self) -> bool:
        """
        Intenta resetear el circuit breaker.

        Returns:
            True si se reseteó exitosamente
        """
        if self._circuit_breaker_state != CircuitBreakerState.OPEN:
            return True  # Ya está cerrado

        # Verificar cooldown
        if self._circuit_breaker_triggered_at:
            elapsed_sec = (time.time_ns() - self._circuit_breaker_triggered_at) / 1_000_000_000
            if elapsed_sec < self.limits.circuit_breaker_cooldown_sec:
                logger.debug(
                    f"Circuit breaker en cooldown: {elapsed_sec:.0f}s / {self.limits.circuit_breaker_cooldown_sec}s"
                )
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
        """
        Chequea y actualiza el estado del circuit breaker.

        Debe llamarse periódicamente (ej. cada segundo).

        Returns:
            True si las operaciones están permitidas
        """
        if self._circuit_breaker_state == CircuitBreakerState.OPEN:
            self._try_reset_circuit_breaker()

        return self.is_trading_allowed

    def record_feed_latency(self, latency_ms: float) -> None:
        """
        Registra latencia del feed climático.

        Args:
            latency_ms: Latencia en milisegundos
        """
        self.metrics.last_feed_latency_ms = latency_ms

        # Moving average
        self.metrics.avg_feed_latency_ms = (
            self.metrics.avg_feed_latency_ms * 0.9 + latency_ms * 0.1
        )

        # Check si excede límite
        if latency_ms > self.limits.max_feed_latency_ms:
            logger.warning(f"Latencia de feed excede límite: {latency_ms:.0f}ms > {self.limits.max_feed_latency_ms}ms")

            if self._circuit_breaker_state == CircuitBreakerState.CLOSED:
                self._trigger_circuit_breaker(
                    f"Feed latency {latency_ms:.0f}ms > {self.limits.max_feed_latency_ms}ms"
                )

    def record_trade_result(
        self,
        is_win: bool,
        pnl_usd: Decimal,
        signal: Optional[ArbitrageSignal] = None,
    ) -> None:
        """
        Registra el resultado de una operación.

        Args:
            is_win: True si la operación fue ganadora
            pnl_usd: P&L en USD (positivo = ganancia)
            signal: Señal que originó la operación (para tracking)
        """
        async def _record():
            async with self._state_lock:
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
                    "signal_id": signal.signal_id if signal else None,
                })

                # Limitar historial
                if len(self._trade_history) > self._max_history:
                    self._trade_history.pop(0)

        # Ejecutar async
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_record())
        except RuntimeError:
            # No hay loop running (ej. en tests)
            pass

    def record_transaction_result(self, success: bool, error_message: Optional[str] = None) -> None:
        """
        Registra el resultado de una transacción on-chain.

        Args:
            success: True si la tx fue exitosa
            error_message: Mensaje de error si falló
        """
        if success:
            self.metrics.record_successful_transaction()
        else:
            self.metrics.record_failed_transaction()
            logger.warning(f"Transacción fallida: {error_message or 'Unknown error'}")

            # Check circuit breaker
            if self.metrics.failed_transactions >= self.limits.max_failed_transactions:
                self._trigger_circuit_breaker(
                    f"{self.metrics.failed_transactions} transacciones fallidas"
                )

    def validate_signal(self, signal: ArbitrageSignal) -> tuple[bool, Optional[str]]:
        """
        Valida si una señal de arbitraje es segura de ejecutar.

        Args:
            signal: Señal a validar

        Returns:
            (is_valid, reason) - True si es segura de ejecutar
        """
        # En dry run, siempre validar (pero loguear warnings)
        if self.dry_run:
            if not signal.is_profitable:
                logger.debug(f"[DRY_RUN] Señal no rentable: ROI={signal.expected_roi:.2%}")
            return True, None

        # Check circuit breaker
        if self._circuit_breaker_state == CircuitBreakerState.OPEN:
            return False, f"Circuit breaker abierto: {self._circuit_breaker_reason}"

        # Check rentabilidad
        if not signal.is_profitable:
            return False, "Señal no rentable después de costos"

        # Check ROI mínimo
        min_roi = self.config.trading.min_roi_threshold
        if signal.expected_roi < min_roi:
            return False, f"ROI {signal.expected_roi:.2%} < mínimo {min_roi:.2%}"

        # Check slippage
        max_slippage = self.config.trading.max_slippage_tolerance
        if signal.estimated_slippage > max_slippage:
            return False, f"Slippage {signal.estimated_slippage:.2%} > máximo {max_slippage:.2%}"

        # Check gas price
        # (asumir que signal incluye gas estimate)
        # if signal.estimated_gas_cost > MAX_GAS_COST:
        #     return False, "Gas cost demasiado alto"

        # Check freshness de la señal
        time_remaining_ms = signal.time_remaining_ns / 1_000_000
        if time_remaining_ms < 50:  # Menos de 50ms
            return False, "Señal muy antigua, posible stale"

        return True, None

    def get_risk_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen del estado de riesgo actual.

        Returns:
            Dict con métricas clave de riesgo
        """
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
            "active_alerts": self._active_alerts[-5:],  # Últimas 5 alertas
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
