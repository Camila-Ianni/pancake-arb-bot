from decimal import Decimal

from models import SharedMarketState
from modules.polymarket_monitor import PolymarketMonitor


def test_monitor_status_shape() -> None:
    shared = SharedMarketState(initial_capital_usd=Decimal("100"))
    monitor = PolymarketMonitor(shared_state=shared)
    status = monitor.get_status()
    assert "markets_cached" in status
    assert "avg_latency_ms" in status
