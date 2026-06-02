"""
models.py - Modelos de datos inmutables para el sistema de trading.

Todos los modelos usan dataclasses frozen para:
1. Inmutabilidad (previene bugs por estado compartido mutable)
2. Hashability (pueden usarse en sets y como keys de dict)
3. Thread-safety implícita para asyncio

HOT PATH OPTIMIZATION:
- __slots__ para reducir memoria y mejorar acceso a atributos
- Tipos anotados para mypy y mejor IDE support
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum, auto
from decimal import Decimal
import time


# =============================================================================
# ENUMS DE ESTADO
# =============================================================================

class MarketSide(Enum):
    """Lado del order book."""
    BID = "bid"      # Compradores (quieren comprar)
    ASK = "ask"      # Vendedores (quieren vender)


class OrderStatus(Enum):
    """Estado de una orden."""
    PENDING = auto()
    SUBMITTED = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELLED = auto()
    REJECTED = auto()
    FAILED = auto()


class ArbitrageSignalType(Enum):
    """Tipo de señal de arbitraje detectada."""
    PRICE_MISMATCH = auto()      # El precio del mercado no refleja el dato real
    LIQUIDITY_IMBALANCE = auto() # Hay liquidez asimétrica para explotar
    ORACLE_LAG = auto()          # El oráculo está desactualizado


class CircuitBreakerState(Enum):
    """Estado del circuit breaker de riesgo."""
    CLOSED = auto()      # Operando normalmente
    OPEN = auto()        # Detenido por pérdidas/errores
    HALF_OPEN = auto()   # Probando si recuperar


# =============================================================================
# MODELOS DE DATOS DE MERCADO
# =============================================================================

@dataclass(frozen=True, slots=True)
class PriceLevel:
    """
    Un nivel de precio en el order book.

    Representa una cantidad específica de shares a un precio dado.
    En Polymarket, los shares van de 0-100 (representando probabilidades).
    """
    price: int          # Precio en centavos (0-100)
    size: Decimal       # Cantidad de shares disponibles
    order_count: int = 0  # Número de órdenes en este nivel (opcional)

    @property
    def price_decimal(self) -> Decimal:
        """Precio como decimal (0.00 - 1.00)."""
        return Decimal(self.price) / Decimal(100)

    @property
    def notional_value(self) -> Decimal:
        """Valor nocional en USD (price * size)."""
        return self.price_decimal * self.size


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    """Snapshot de un nivel del order book."""
    side: MarketSide
    price: int
    size: Decimal
    timestamp_ns: int  # Timestamp en nanosegundos para precisión


@dataclass(frozen=True)
class OrderBookSnapshot:
    """
    Snapshot completo del order book en un momento dado.

    Inmutable para asegurar consistencia durante el procesamiento.
    """
    condition_id: str
    market_id: str
    bids: tuple[PriceLevel, ...]  # Tuple frozen para inmutabilidad
    asks: tuple[PriceLevel, ...]
    timestamp_ns: int
    sequence_num: int  # Para detectar gaps en updates

    @property
    def best_bid(self) -> Optional[PriceLevel]:
        """Mejor precio de compra (más alto)."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[PriceLevel]:
        """Mejor precio de venta (más bajo)."""
        return self.asks[0] if self.asks else None

    @property
    def mid_price(self) -> Optional[Decimal]:
        """Precio medio del spread."""
        if self.best_bid and self.best_ask:
            return (self.best_bid.price_decimal + self.best_ask.price_decimal) / 2
        return None

    @property
    def spread(self) -> Optional[int]:
        """Spread en centavos (None si no hay liquidez)."""
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None

    def get_vwap(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """
        Calcula el VWAP (Volume Weighted Average Price) para una orden de tamaño dado.

        Args:
            side: BID para comprar, ASK para vender
            size: Cantidad de shares a ejecutar

        Returns:
            Precio promedio ponderado por volumen, o None si no hay liquidez suficiente

        HOT PATH NOTE: Este cálculo es O(n) donde n es el número de niveles.
        Para order books profundos, considerar caching parcial.
        """
        levels = self.bids if side == MarketSide.BID else self.asks
        if not levels:
            return None

        remaining_size = size
        total_cost = Decimal(0)
        filled_size = Decimal(0)

        for level in levels:
            if remaining_size <= 0:
                break

            fill_size = min(remaining_size, level.size)
            total_cost += fill_size * level.price_decimal
            filled_size += fill_size
            remaining_size -= fill_size

        if filled_size == 0:
            return None

        return total_cost / filled_size

    def calculate_slippage(self, side: MarketSide, size: Decimal) -> Optional[Decimal]:
        """
        Calcula el slippage esperado para una orden de tamaño dado.

        El slippage es la diferencia entre el precio "mark" (mid) y el VWAP
        de ejecución, expresada como porcentaje.

        Returns:
            Slippage como decimal (ej. 0.02 = 2% de slippage)
            None si no hay liquidez suficiente
        """
        vwap = self.get_vwap(side, size)
        mid = self.mid_price

        if vwap is None or mid is None:
            return None

        # Slippage = (VWAP - mid) / mid para compras
        # Slippage = (mid - VWAP) / mid para ventas
        if side == MarketSide.BID:
            return (vwap - mid) / mid
        else:
            return (mid - vwap) / mid


# =============================================================================
# MODELOS DE SEÑALES DE ARBITRAJE
# =============================================================================

@dataclass(frozen=True)
class ArbitrageSignal:
    """
    Señal de oportunidad de arbitraje detectada.

    Generada por el ArbitrageEngine cuando se detecta una discrepancia
    entre el dato climático y el precio del mercado.
    """
    signal_id: str                          # UUID único para tracking
    signal_type: ArbitrageSignalType
    condition_id: str
    market_id: str

    # Datos que dispararon la señal
    market_data: OrderBookSnapshot

    # Análisis de rentabilidad
    expected_roi: Decimal                   # ROI esperado (decimal)
    estimated_gas_cost: Decimal             # Costo estimado de gas en USD
    estimated_slippage: Decimal             # Slippage estimado (decimal)
    net_expected_profit: Decimal            # Profit después de costos

    # Timestamps para medición de latencia
    signal_generated_ns: int
    decision_deadline_ns: int               # Cuándo expire la oportunidad

    @property
    def is_profitable(self) -> bool:
        """Verifica si la señal es rentable después de costos."""
        return self.net_expected_profit > 0

    @property
    def time_remaining_ns(self) -> int:
        """Tiempo restante antes de que expire la señal."""
        return max(0, self.decision_deadline_ns - time.time_ns())

    @property
    def urgency_score(self) -> float:
        """
        Score de urgencia (0-1).

        1 = oportunidad a punto de expirar, ejecutar inmediatamente
        0 = oportunidad fresca, se puede analizar más
        """
        total_window = self.decision_deadline_ns - self.signal_generated_ns
        elapsed = time.time_ns() - self.signal_generated_ns
        return min(1.0, elapsed / total_window) if total_window > 0 else 1.0


# =============================================================================
# MODELOS DE EJECUCIÓN
# =============================================================================

@dataclass(frozen=True)
class ExecutionParams:
    """Parámetros para una ejecución de orden."""
    market_id: str
    side: MarketSide
    size: Decimal
    max_price: Decimal        # Precio máximo a pagar (para bids)
    min_price: Decimal        # Precio mínimo a recibir (para asks)
    priority_fee_gwei: int    # Priority fee para EIP-1559
    max_gas_price_gwei: int   # Gas price máximo aceptable

    # Slippage tolerance
    max_slippage_bps: int = 200  # 200 bps = 2%


@dataclass(frozen=True)
class TransactionResult:
    """Resultado de una transacción enviada."""
    tx_hash: Optional[str]
    status: OrderStatus
    gas_used: Optional[int]
    gas_price_gwei: Optional[int]
    total_cost_usd: Optional[Decimal]
    error_message: Optional[str] = None

    # Timestamps
    submitted_at_ns: int = 0
    confirmed_at_ns: Optional[int] = None

    @property
    def confirmation_time_ms(self) -> Optional[float]:
        """Tiempo de confirmación en milisegundos."""
        if self.confirmed_at_ns:
            return (self.confirmed_at_ns - self.submitted_at_ns) / 1_000_000
        return None


# =============================================================================
# MODELOS DE RIESGO
# =============================================================================

@dataclass
class RiskMetrics:
    """
    Métricas de riesgo en tiempo real.

    Mutable (no frozen) porque se actualiza constantemente.
    Thread-safe acceso vía asyncio lock cuando sea necesario.
    """
    consecutive_losses: int = 0
    consecutive_wins: int = 0
    total_pnl_usd: Decimal = field(default_factory=lambda: Decimal(0))

    failed_transactions: int = 0
    successful_transactions: int = 0

    # Latencia del feed
    last_feed_latency_ms: float = 0.0
    avg_feed_latency_ms: float = 0.0

    # Circuit breaker
    circuit_breaker_state: CircuitBreakerState = CircuitBreakerState.CLOSED
    circuit_breaker_triggered_at: Optional[int] = None  # timestamp_ns

    # P&L histórico
    total_gas_spent_usd: Decimal = field(default_factory=lambda: Decimal(0))
    total_profit_usd: Decimal = field(default_factory=lambda: Decimal(0))
    total_loss_usd: Decimal = field(default_factory=lambda: Decimal(0))

    @property
    def win_rate(self) -> float:
        """Tasa de victorias (wins / total trades)."""
        total = self.consecutive_losses + self.consecutive_wins
        return self.consecutive_wins / total if total > 0 else 0.0

    @property
    def transaction_success_rate(self) -> float:
        """Tasa de éxito de transacciones."""
        total = self.failed_transactions + self.successful_transactions
        return self.successful_transactions / total if total > 0 else 0.0

    def record_win(self, profit_usd: Decimal) -> None:
        """Registra una operación ganadora."""
        self.consecutive_losses = 0
        self.consecutive_wins += 1
        self.total_pnl_usd += profit_usd
        self.total_profit_usd += profit_usd

    def record_loss(self, loss_usd: Decimal) -> None:
        """Registra una operación perdedora."""
        self.consecutive_wins = 0
        self.consecutive_losses += 1
        self.total_pnl_usd -= loss_usd
        self.total_loss_usd += loss_usd

    def record_failed_transaction(self) -> None:
        """Registra una transacción fallida."""
        self.failed_transactions += 1

    def record_successful_transaction(self) -> None:
        """Registra una transacción exitosa."""
        self.successful_transactions += 1
