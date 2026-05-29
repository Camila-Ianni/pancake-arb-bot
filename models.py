"""Modelos de datos HFT con __slots__ compatibles con Python 3.9+."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, Set
from collections import deque


class SniperAsset(str, Enum):
    BTC = "BTC"
    ETH = "ETH"
    SOL = "SOL"
    BNB = "BNB"


class SniperState(str, Enum):
    IDLE = "IDLE"
    ARMED = "ARMED"
    FIRING = "FIRING"
    COOLDOWN = "COOLDOWN"
    STOPPED = "STOPPED"


class OrderSide(str, Enum):
    YES = "YES"
    NO = "NO"


class SharedMarketState:
    __slots__ = (
        "initial_capital_usd",
        "cumulative_pnl_usd",
        "sniper_state",
        "asset_prices",
        "last_binance_update_ns",
        "pancake_state",
        "last_signal_ns",
        "kill_switch",
        "latest_status",
        "wallet_usdc_balance",
        "inflight_assets",
        "decision_budget_ms",
        "log_messages",
    )

    def __init__(self, initial_capital_usd: Decimal, sniper_state: SniperState = SniperState.IDLE) -> None:
        self.initial_capital_usd = initial_capital_usd
        self.cumulative_pnl_usd = Decimal("0")
        self.sniper_state = sniper_state
        self.asset_prices = {
            SniperAsset.BTC: 0.0,
            SniperAsset.ETH: 0.0,
            SniperAsset.SOL: 0.0,
            SniperAsset.BNB: 0.0,
        }
        self.last_binance_update_ns = 0
        self.pancake_state = {
            "epoch": 0,
            "lock_timestamp": 0,
            "remaining_seconds": 0,
            "bull_amount": Decimal("0"),
            "bear_amount": Decimal("0"),
            "bull_multiplier": Decimal("0"),
            "bear_multiplier": Decimal("0")
        }
        self.last_signal_ns = 0
        self.kill_switch = False
        self.latest_status = "BOOTING"
        self.wallet_usdc_balance = initial_capital_usd
        self.inflight_assets = set()
        self.decision_budget_ms = 10.0
        self.log_messages = deque(maxlen=8)


class PolymarketTick:
    __slots__ = ("asset", "market_id", "condition_id", "yes_price", "strike_price", "market_close_ts")

    def __init__(
        self,
        asset: SniperAsset,
        market_id: str,
        condition_id: str,
        yes_price: Decimal,
        strike_price: float,
        market_close_ts: int,
    ) -> None:
        self.asset = asset
        self.market_id = market_id
        self.condition_id = condition_id
        self.yes_price = yes_price
        self.strike_price = strike_price
        self.market_close_ts = market_close_ts


class BinanceTick:
    __slots__ = ("symbol", "mark_price", "event_time_ms", "received_ns")

    def __init__(self, symbol: SniperAsset, mark_price: float, event_time_ms: int, received_ns: int) -> None:
        self.symbol = symbol
        self.mark_price = mark_price
        self.event_time_ms = event_time_ms
        self.received_ns = received_ns


class SniperSignal:
    __slots__ = (
        "asset",
        "market_id",
        "condition_id",
        "yes_price",
        "strike_price",
        "mark_price",
        "bet_size_usd",
        "signal_ns",
    )

    def __init__(
        self,
        asset: SniperAsset,
        market_id: str,
        condition_id: str,
        yes_price: Decimal,
        strike_price: float,
        mark_price: float,
        bet_size_usd: Decimal,
        signal_ns: int,
    ) -> None:
        self.asset = asset
        self.market_id = market_id
        self.condition_id = condition_id
        self.yes_price = yes_price
        self.strike_price = strike_price
        self.mark_price = mark_price
        self.bet_size_usd = bet_size_usd
        self.signal_ns = signal_ns


class ExecutionRequest:
    __slots__ = ("signal", "side")

    def __init__(self, signal: SniperSignal, side: OrderSide) -> None:
        self.signal = signal
        self.side = side


class ExecutionResult:
    __slots__ = ("tx_hash", "ok", "asset", "invested_usd", "payout_usd", "pnl_delta_usd", "error")

    def __init__(
        self,
        tx_hash: Optional[str],
        ok: bool,
        asset: SniperAsset,
        invested_usd: Decimal,
        payout_usd: Decimal,
        pnl_delta_usd: Decimal,
        error: Optional[str] = None,
    ) -> None:
        self.tx_hash = tx_hash
        self.ok = ok
        self.asset = asset
        self.invested_usd = invested_usd
        self.payout_usd = payout_usd
        self.pnl_delta_usd = pnl_delta_usd
        self.error = error


class EngineMetrics:
    __slots__ = ("decisions", "fired", "avg_decision_ms", "max_decision_ms")

    def __init__(self) -> None:
        self.decisions = 0
        self.fired = 0
        self.avg_decision_ms = 0.0
        self.max_decision_ms = 0.0

    def record(self, decision_ms: float, executed: bool) -> None:
        self.decisions += 1
        if executed:
            self.fired += 1
        self.avg_decision_ms = (self.avg_decision_ms * 0.9) + (decision_ms * 0.1)
        if decision_ms > self.max_decision_ms:
            self.max_decision_ms = decision_ms


class CryptoFeedMetrics:
    __slots__ = ("ticks", "reconnects", "avg_parse_ms")

    def __init__(self) -> None:
        self.ticks = 0
        self.reconnects = 0
        self.avg_parse_ms = 0.0

    def record_parse_ms(self, parse_ms: float) -> None:
        self.ticks += 1
        self.avg_parse_ms = (self.avg_parse_ms * 0.9) + (parse_ms * 0.1)


class ExecutorMetrics:
    __slots__ = ("sent", "ok", "failed", "avg_sign_ms")

    def __init__(self) -> None:
        self.sent = 0
        self.ok = 0
        self.failed = 0
        self.avg_sign_ms = 0.0

    def record_sign_ms(self, sign_ms: float) -> None:
        self.avg_sign_ms = (self.avg_sign_ms * 0.9) + (sign_ms * 0.1)


class MarketMonitorMetrics:
    __slots__ = ("updates", "reconnects", "avg_latency_ms")

    def __init__(self) -> None:
        self.updates = 0
        self.reconnects = 0
        self.avg_latency_ms = 0.0

    def record_latency_ms(self, latency_ms: float) -> None:
        self.updates += 1
        self.avg_latency_ms = (self.avg_latency_ms * 0.9) + (latency_ms * 0.1)


class RuntimeConfig:
    __slots__ = (
        "kill_switch_pnl_usd",
        "yes_price_max",
        "close_window_sec",
        "target_internal_latency_ms",
        "stake_usage",
        "profit_sweep_threshold_usd",
        "profit_sweep_enabled",
        "max_parallel_signals",
    )

    def __init__(
        self,
        kill_switch_pnl_usd: Decimal = Decimal("-30.00"),
        yes_price_max: Decimal = Decimal("0.94"),
        close_window_sec: int = 20,
        target_internal_latency_ms: float = 10.0,
        stake_usage: Decimal = Decimal("0.95"),
        profit_sweep_threshold_usd: Decimal = Decimal("500.00"),
        profit_sweep_enabled: bool = True,
        max_parallel_signals: int = 8,
    ) -> None:
        self.kill_switch_pnl_usd = kill_switch_pnl_usd
        self.yes_price_max = yes_price_max
        self.close_window_sec = close_window_sec
        self.target_internal_latency_ms = target_internal_latency_ms
        self.stake_usage = stake_usage
        self.profit_sweep_threshold_usd = profit_sweep_threshold_usd
        self.profit_sweep_enabled = profit_sweep_enabled
        self.max_parallel_signals = max_parallel_signals
