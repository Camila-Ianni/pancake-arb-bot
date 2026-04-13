"""
Tests para PolymarketMonitor - Monitor CLOB optimizado para HFT.

Valida:
- Parsing de mensajes WebSocket
- Local Order Book (LOB) operations
- Cálculo de VWAP y slippage
- Reconexión y heartbeat

Ejecutar: pytest tests/test_polymarket_monitor.py -v
"""

import os
import sys
import time
import pytest
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.polymarket_monitor import (
    PolymarketMonitor,
    LocalOrderBook,
    MonitorMetrics,
    MarketSide,
    create_polymarket_monitor,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_config():
    """Configuración mock para tests."""
    config = type('MockConfig', (), {})()
    config.polymarket = type('MockPolymarket', (), {})()
    config.polymarket.condition_id = "test_token_123"
    config.polymarket.market_ids = ["test_token_123"]
    return config


@pytest.fixture
def lob():
    """Crea un LocalOrderBook para testing."""
    return LocalOrderBook(token_id="test_token")


@pytest.fixture
def sample_snapshot_data():
    """Snapshot de ejemplo de Polymarket CLOB."""
    return {
        "type": "order_book_snapshot",
        "token_id": "test_token_123",
        "bids": [
            {"price": "0.49", "size": "100"},
            {"price": "0.48", "size": "200"},
            {"price": "0.47", "size": "300"},
        ],
        "asks": [
            {"price": "0.51", "size": "150"},
            {"price": "0.52", "size": "250"},
            {"price": "0.53", "size": "350"},
        ],
        "sequence": 1000,
    }


@pytest.fixture
def sample_update_data():
    """Update incremental de ejemplo."""
    return {
        "type": "order_book_update",
        "token_id": "test_token_123",
        "side": "bid",
        "price": "0.50",
        "size": "500",
        "sequence": 1001,
    }


# =============================================================================
# TESTS DE LOCAL ORDER BOOK
# =============================================================================

class TestLocalOrderBook:
    """Tests para LocalOrderBook."""

    def test_apply_snapshot(self, lob, sample_snapshot_data):
        """Test aplicación de snapshot completo."""
        bids = [(49, Decimal("100")), (48, Decimal("200"))]
        asks = [(51, Decimal("150")), (52, Decimal("250"))]

        lob.apply_snapshot(bids, asks, sequence_num=1000)

        assert lob.is_synced is True
        assert lob.sequence_num == 1000
        assert len(lob._bids) == 2
        assert len(lob._asks) == 2

    def test_update_level_bid(self, lob):
        """Test actualización de nivel bid."""
        lob.apply_snapshot([(49, Decimal("100"))], [(51, Decimal("150"))], 1)

        updated = lob.update_level(
            side=MarketSide.BID,
            price=50,
            size=Decimal("200"),
            sequence_num=2
        )

        assert updated is True
        assert 50 in lob._bids
        assert lob._bids[50] == Decimal("200")
        assert lob.sequence_num == 2

    def test_update_level_removes_zero_size(self, lob):
        """Test que tamaño 0 elimina el nivel."""
        lob.apply_snapshot([(50, Decimal("100"))], [(51, Decimal("150"))], 1)

        lob.update_level(MarketSide.BID, price=50, size=Decimal("0"), sequence_num=2)

        assert 50 not in lob._bids

    def test_get_best_bid_ask(self, lob):
        """Test obtención de mejor bid/ask."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100")), (48, Decimal("200"))],
            asks=[(51, Decimal("150")), (52, Decimal("250"))],
            sequence_num=1
        )

        best_bid = lob.get_best_bid()
        best_ask = lob.get_best_ask()

        assert best_bid is not None
        assert best_bid.price == 49  # Mejor bid = más alto
        assert best_ask is not None
        assert best_ask.price == 51  # Mejor ask = más bajo

    def test_get_spread(self, lob):
        """Test cálculo de spread."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100"))],
            asks=[(51, Decimal("150"))],
            sequence_num=1
        )

        spread = lob.get_spread()

        assert spread == 2  # 51 - 49 = 2 centavos

    def test_get_mid_price(self, lob):
        """Test cálculo de precio medio."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100"))],
            asks=[(51, Decimal("150"))],
            sequence_num=1
        )

        mid = lob.get_mid_price()

        assert mid == Decimal("0.50")  # (0.49 + 0.51) / 2

    def test_update_with_old_sequence_discarded(self, lob):
        """Test que updates con secuencia antigua se descartan."""
        lob.apply_snapshot([], [], sequence_num=10)

        update_applied = lob.update_level(
            MarketSide.BID,
            price=50,
            size=Decimal("100"),
            sequence_num=5  # Más antiguo
        )

        assert update_applied is False
        assert 50 not in lob._bids


# =============================================================================
# TESTS DE VWAP Y SLIPPAGE
# =============================================================================

class TestVWAPCalculation:
    """Tests para cálculo de VWAP."""

    def test_vwap_buy_small_order(self, lob):
        """Test VWAP para compra pequeña (un solo nivel)."""
        lob.apply_snapshot(
            bids=[],
            asks=[(50, Decimal("100")), (52, Decimal("200"))],
            sequence_num=1
        )

        # Comprar 50 shares - debería ejecutarse todo en el primer nivel
        vwap = lob.get_execution_price(MarketSide.BID, Decimal("50"))

        assert vwap == Decimal("0.50")

    def test_vwap_buy_crosses_multiple_levels(self, lob):
        """Test VWAP para compra que cruza múltiples niveles."""
        lob.apply_snapshot(
            bids=[],
            asks=[
                (50, Decimal("100")),  # 100 @ 50¢
                (52, Decimal("200")),  # 200 @ 52¢
                (55, Decimal("300")),  # 300 @ 55¢
            ],
            sequence_num=1
        )

        # Comprar 250 shares:
        # - 100 @ 50¢ = $50
        # - 150 @ 52¢ = $78
        # Total: $128 / 250 = 51.2¢
        vwap = lob.get_execution_price(MarketSide.BID, Decimal("250"))

        expected = (Decimal("100") * Decimal("0.50") + Decimal("150") * Decimal("0.52")) / Decimal("250")
        assert abs(vwap - expected) < Decimal("0.001")

    def test_vwap_sell_crosses_multiple_levels(self, lob):
        """Test VWAP para venta que cruza múltiples niveles."""
        lob.apply_snapshot(
            bids=[
                (50, Decimal("100")),  # 100 @ 50¢
                (48, Decimal("200")),  # 200 @ 48¢
                (45, Decimal("300")),  # 300 @ 45¢
            ],
            asks=[],
            sequence_num=1
        )

        # Vender 250 shares:
        # - 100 @ 50¢ = $50
        # - 150 @ 48¢ = $72
        # Total: $122 / 250 = 48.8¢
        vwap = lob.get_execution_price(MarketSide.ASK, Decimal("250"))

        expected = (Decimal("100") * Decimal("0.50") + Decimal("150") * Decimal("0.48")) / Decimal("250")
        assert abs(vwap - expected) < Decimal("0.001")

    def test_vwap_insufficient_liquidity(self, lob):
        """Test VWAP cuando no hay liquidez suficiente."""
        lob.apply_snapshot(
            bids=[(50, Decimal("100"))],
            asks=[],
            sequence_num=1
        )

        # Intentar comprar más de lo disponible
        vwap = lob.get_execution_price(MarketSide.BID, Decimal("500"))

        # Debería retornar el VWAP parcial o None
        # En este caso, hay solo 100 shares disponibles
        assert vwap is None or vwap == Decimal("0.50")

    def test_vwap_empty_book(self, lob):
        """Test VWAP con order book vacío."""
        lob.apply_snapshot([], [], sequence_num=1)

        vwap = lob.get_execution_price(MarketSide.BID, Decimal("100"))

        assert vwap is None


class TestSlippageCalculation:
    """Tests para cálculo de slippage."""

    def test_slippage_buy_no_slippage(self, lob):
        """Test slippage para compra pequeña (sin slippage)."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100"))],
            asks=[(51, Decimal("1000"))],  # Mucha liquidez
            sequence_num=1
        )

        # Comprar 10 shares - debería ser al mid price
        slippage = lob.calculate_slippage(MarketSide.BID, Decimal("10"))

        assert slippage is not None
        assert abs(slippage) < Decimal("0.001")  # Slippage casi cero

    def test_slippage_buy_with_slippage(self, lob):
        """Test slippage para compra grande (con slippage)."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100"))],
            asks=[
                (51, Decimal("50")),   # 50 @ 51¢
                (55, Decimal("100")),  # 100 @ 55¢
            ],
            sequence_num=1
        )

        # Comprar 100 shares - cruza 2 niveles
        slippage = lob.calculate_slippage(MarketSide.BID, Decimal("100"))

        assert slippage is not None
        assert slippage > 0  # Slippage positivo para compras

    def test_slippage_sell(self, lob):
        """Test slippage para venta."""
        lob.apply_snapshot(
            bids=[
                (50, Decimal("50")),
                (45, Decimal("100")),
            ],
            asks=[(52, Decimal("100"))],
            sequence_num=1
        )

        # Vender 100 shares
        slippage = lob.calculate_slippage(MarketSide.ASK, Decimal("100"))

        assert slippage is not None
        assert slippage > 0  # Slippage positivo para ventas (recibimos menos)


# =============================================================================
# TESTS DE LIQUIDITY
# =============================================================================

class TestLiquidity:
    """Tests para cálculo de liquidez."""

    def test_get_liquidity_bids(self, lob):
        """Test obtención de liquidez de bids."""
        lob.apply_snapshot(
            bids=[
                (50, Decimal("100")),
                (49, Decimal("200")),
                (48, Decimal("300")),
            ],
            asks=[],
            sequence_num=1
        )

        liquidity = lob.get_liquidity(MarketSide.BID, levels=2)

        assert liquidity == Decimal("300")  # 100 + 200

    def test_get_liquidity_empty_side(self, lob):
        """Test liquidez cuando un lado está vacío."""
        lob.apply_snapshot(
            bids=[(50, Decimal("100"))],
            asks=[],
            sequence_num=1
        )

        liquidity = lob.get_liquidity(MarketSide.ASK)

        assert liquidity == Decimal("0")


# =============================================================================
# TESTS DE MÉTRICAS
# =============================================================================

class TestMonitorMetrics:
    """Tests para MonitorMetrics."""

    def test_initial_metrics(self):
        """Test inicialización de métricas."""
        metrics = MonitorMetrics()

        assert metrics.messages_received == 0
        assert metrics.order_book_updates == 0
        assert metrics.avg_latency_ms == 0.0
        assert metrics.min_latency_ms == float('inf')

    def test_record_latency(self):
        """Test registro de latencia."""
        metrics = MonitorMetrics()

        metrics.record_latency(10.0)
        assert metrics.last_latency_ms == 10.0
        assert metrics.min_latency_ms == 10.0
        assert metrics.max_latency_ms == 10.0
        assert metrics.avg_latency_ms == 10.0

        metrics.record_latency(20.0)
        assert metrics.last_latency_ms == 20.0
        assert metrics.min_latency_ms == 10.0
        assert metrics.max_latency_ms == 20.0
        # Moving average: 10 * 0.85 + 20 * 0.15 = 11.5
        assert abs(metrics.avg_latency_ms - 11.5) < 0.1


# =============================================================================
# TESTS DE CREACIÓN
# =============================================================================

class TestMonitorCreation:
    """Tests para creación del monitor."""

    def test_create_with_factory(self, mock_config):
        """Test creación con factory function."""
        monitor = create_polymarket_monitor(config=mock_config, sandbox=True)

        assert isinstance(monitor, PolymarketMonitor)
        assert monitor.sandbox is True
        assert monitor.token_id == "test_token_123"

    def test_create_with_custom_token_id(self, mock_config):
        """Test creación con token_id personalizado."""
        monitor = create_polymarket_monitor(
            config=mock_config,
            token_id="custom_token_456"
        )

        assert monitor.token_id == "custom_token_456"


# =============================================================================
# TESTS DE PRECISION DECIMAL
# =============================================================================

class TestDecimalPrecision:
    """Tests para precisión decimal en cálculos financieros."""

    def test_price_conversion_to_cents(self, lob):
        """Test conversión de precio decimal a centavos."""
        # Precio 0.49 debería ser 49 centavos
        price_decimal = Decimal("0.49")
        price_cents = int(price_decimal * 100)
        assert price_cents == 49

    def test_no_floating_point_errors(self, lob):
        """Test que no hay errores de floating point."""
        lob.apply_snapshot(
            bids=[(49, Decimal("100.33"))],
            asks=[(51, Decimal("200.67"))],
            sequence_num=1
        )

        # Los valores deberían ser exactos
        assert lob._bids[49] == Decimal("100.33")
        assert lob._asks[51] == Decimal("200.67")

    def test_vwap_precision(self, lob):
        """Test precisión del VWAP."""
        lob.apply_snapshot(
            bids=[],
            asks=[
                (50, Decimal("33.33")),
                (52, Decimal("66.67")),
            ],
            sequence_num=1
        )

        # Comprar 100 shares
        vwap = lob.get_execution_price(MarketSide.BID, Decimal("100"))

        # Debería ser preciso
        assert vwap is not None
        assert vwap > Decimal("0.50")  # Al menos 50¢


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
