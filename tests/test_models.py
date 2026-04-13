"""
Tests para los modelos de datos.

Valida que los modelos son inmutables y los cálculos son correctos.
"""

import os
import sys
import time
import pytest
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    PriceLevel,
    OrderBookSnapshot,
    MarketSide,
    WeatherObservation,
    ArbitrageSignal,
    ArbitrageSignalType,
    RiskMetrics,
    CircuitBreakerState,
)


class TestPriceLevel:
    """Tests para PriceLevel."""

    def test_price_level_creation(self):
        """Test creación básica de PriceLevel."""
        level = PriceLevel(price=50, size=Decimal("100.5"))

        assert level.price == 50
        assert level.size == Decimal("100.5")
        assert level.price_decimal == Decimal("0.50")
        assert level.notional_value == Decimal("50.25")  # 0.50 * 100.5

    def test_price_level_immutability(self):
        """Test que PriceLevel es inmutable."""
        level = PriceLevel(price=50, size=Decimal("100"))

        with pytest.raises(AttributeError):
            level.price = 60  # type: ignore


class TestOrderBookSnapshot:
    """Tests para OrderBookSnapshot."""

    def test_snapshot_creation(self):
        """Test creación básica de snapshot."""
        bids = (PriceLevel(price=49, size=Decimal("100")),)
        asks = (PriceLevel(price=51, size=Decimal("100")),)

        snapshot = OrderBookSnapshot(
            condition_id="test_cond",
            market_id="test_market",
            bids=bids,
            asks=asks,
            timestamp_ns=time.time_ns(),
            sequence_num=1,
        )

        assert snapshot.best_bid is not None
        assert snapshot.best_bid.price == 49
        assert snapshot.best_ask is not None
        assert snapshot.best_ask.price == 51
        assert snapshot.spread == 2

    def test_mid_price_calculation(self):
        """Test cálculo del precio medio."""
        bids = (PriceLevel(price=48, size=Decimal("100")),)
        asks = (PriceLevel(price=52, size=Decimal("100")),)

        snapshot = OrderBookSnapshot(
            condition_id="test",
            market_id="test",
            bids=bids,
            asks=asks,
            timestamp_ns=time.time_ns(),
            sequence_num=1,
        )

        assert snapshot.mid_price == Decimal("0.50")

    def test_vwap_calculation(self):
        """Test cálculo de VWAP."""
        # Order book con múltiples niveles
        bids = (
            PriceLevel(price=50, size=Decimal("100")),
            PriceLevel(price=49, size=Decimal("200")),
        )
        asks = (
            PriceLevel(price=51, size=Decimal("100")),
            PriceLevel(price=52, size=Decimal("200")),
        )

        snapshot = OrderBookSnapshot(
            condition_id="test",
            market_id="test",
            bids=bids,
            asks=asks,
            timestamp_ns=time.time_ns(),
            sequence_num=1,
        )

        # VWAP para comprar 150 shares (debería cruzar 2 niveles)
        vwap = snapshot.get_vwap(MarketSide.BID, Decimal("150"))

        assert vwap is not None
        # 100 @ 0.50 + 50 @ 0.49 = (50 + 24.5) / 150 = 0.4967
        expected = (Decimal("100") * Decimal("0.50") + Decimal("50") * Decimal("0.49")) / Decimal("150")
        assert abs(vwap - expected) < Decimal("0.001")

    def test_slippage_calculation(self):
        """Test cálculo de slippage."""
        bids = (
            PriceLevel(price=50, size=Decimal("100")),
            PriceLevel(price=48, size=Decimal("100")),  # Slippage significativo
        )
        asks = (
            PriceLevel(price=52, size=Decimal("100")),
        )

        snapshot = OrderBookSnapshot(
            condition_id="test",
            market_id="test",
            bids=bids,
            asks=asks,
            timestamp_ns=time.time_ns(),
            sequence_num=1,
        )

        # Slippage para vender 150 shares
        slippage = snapshot.calculate_slippage(MarketSide.ASK, Decimal("150"))

        assert slippage is not None
        assert slippage > 0  # Debería haber slippage negativo para ventas grandes


class TestWeatherObservation:
    """Tests para WeatherObservation."""

    def test_observation_creation(self):
        """Test creación básica de observación."""
        now_ns = time.time_ns()

        obs = WeatherObservation(
            timestamp_ns=now_ns,
            received_at_ns=now_ns + 50_000_000,  # 50ms después
            source="TestFeed",
            temperature_c=25.5,
            humidity_pct=60.0,
        )

        assert obs.temperature_c == 25.5
        assert obs.latency_ms == 50.0
        assert obs.is_fresh() is True

    def test_observation_freshness(self):
        """Test verificación de frescura."""
        now_ns = time.time_ns()

        # Observación antigua (600ms)
        old_obs = WeatherObservation(
            timestamp_ns=now_ns - 600_000_000,
            received_at_ns=now_ns,
            source="TestFeed",
            temperature_c=20.0,
        )

        assert old_obs.is_fresh(max_age_ms=500) is False
        assert old_obs.is_fresh(max_age_ms=1000) is True


class TestRiskMetrics:
    """Tests para RiskMetrics."""

    def test_metrics_initialization(self):
        """Test inicialización de métricas."""
        metrics = RiskMetrics()

        assert metrics.consecutive_losses == 0
        assert metrics.consecutive_wins == 0
        assert metrics.total_pnl_usd == Decimal(0)
        assert metrics.circuit_breaker_state == CircuitBreakerState.CLOSED

    def test_record_win(self):
        """Test registro de victoria."""
        metrics = RiskMetrics()

        metrics.record_win(Decimal("10.50"))

        assert metrics.consecutive_wins == 1
        assert metrics.consecutive_losses == 0
        assert metrics.total_pnl_usd == Decimal("10.50")

    def test_record_loss(self):
        """Test registro de pérdida."""
        metrics = RiskMetrics()

        metrics.record_loss(Decimal("5.25"))

        assert metrics.consecutive_losses == 1
        assert metrics.consecutive_wins == 0
        assert metrics.total_pnl_usd == Decimal("-5.25")

    def test_win_rate_calculation(self):
        """Test cálculo de win rate."""
        metrics = RiskMetrics()

        metrics.record_win(Decimal("10"))
        metrics.record_win(Decimal("10"))
        metrics.record_loss(Decimal("5"))
        metrics.record_win(Decimal("10"))

        # 3 wins, 1 loss = 75% win rate
        assert metrics.win_rate == 0.75


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
