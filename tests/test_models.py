from decimal import Decimal

from models import RuntimeConfig, SharedMarketState, SniperAsset


def test_runtime_defaults() -> None:
    cfg = RuntimeConfig()
    assert cfg.stake_usage == Decimal("0.95")
    assert cfg.yes_price_max == Decimal("0.94")
    assert cfg.kill_switch_pnl_usd == Decimal("-30.00")


def test_shared_state_initialization() -> None:
    state = SharedMarketState(initial_capital_usd=Decimal("100"))
    assert state.initial_capital_usd == Decimal("100")
    assert state.cumulative_pnl_usd == Decimal("0")
    assert state.wallet_usdc_balance == Decimal("100")
    assert state.asset_prices[SniperAsset.SOL] == 0.0
