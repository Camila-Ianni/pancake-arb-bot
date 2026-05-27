"""ArbitrageEngine multi-activo con auto-reinvest y latencia <10ms."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Dict, Optional, Union

from models import (
    EngineMetrics,
    ExecutionRequest,
    OrderSide,
    RuntimeConfig,
    SharedMarketState,
    SniperAsset,
    SniperSignal,
    SniperState,
)


class ArbitrageEngine:
    def __init__(
        self,
        shared_state: SharedMarketState,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        runtime_cfg: Optional[RuntimeConfig] = None,
    ) -> None:
        self.shared_state = shared_state
        self.execution_queue = execution_queue
        self.runtime_cfg = runtime_cfg or RuntimeConfig()
        self.metrics = EngineMetrics()
        self._running = False
        self._fired_window = {}

    async def start(self) -> None:
        self._running = True
        self.shared_state.sniper_state = SniperState.ARMED
        self.shared_state.latest_status = "SNIPER_ARMED_MULTI_ASSET"
        while self._running:
            if self.shared_state.kill_switch:
                break
            started = time.perf_counter_ns()
            executed = await self._scan_all_assets()
            decision_ms = (time.perf_counter_ns() - started) / 1_000_000
            self.metrics.record(decision_ms=decision_ms, executed=executed)
            await asyncio.sleep(0.005)
        self.shared_state.sniper_state = SniperState.STOPPED

    async def _scan_all_assets(self) -> bool:
        if not self.shared_state.polymarket_books:
            self.shared_state.latest_status = "WAITING_MARKETS"
            return False
        tasks = []
        for asset, book in self.shared_state.polymarket_books.items():
            if len(tasks) >= self.runtime_cfg.max_parallel_signals:
                break
            tasks.append(asyncio.create_task(self._evaluate_asset(asset, book)))
        if not tasks:
            return False
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return any(r is True for r in results)

    async def _evaluate_asset(self, asset: SniperAsset, book: dict) -> bool:
        now_s = int(time.time())
        close_ts = int(book["market_close_ts"])
        remaining = close_ts - now_s
        if remaining <= 0:
            self._fired_window[asset] = False
            return False
        if remaining >= self.runtime_cfg.close_window_sec:
            return False
        if self._fired_window.get(asset):
            return False
        if asset in self.shared_state.inflight_assets:
            return False

        yes_price = Decimal(str(book["yes_price"]))
        strike = float(book["strike_price"])
        mark_price = float(self.shared_state.asset_prices.get(asset, 0.0))

        if mark_price <= strike:
            return False
        if yes_price >= self.runtime_cfg.yes_price_max:
            return False

        bet_size = self._compute_dynamic_stake()
        if bet_size <= Decimal("0"):
            self.shared_state.latest_status = "INSUFFICIENT_USDC"
            return False

        signal = SniperSignal(
            asset=asset,
            market_id=str(book["market_id"]),
            condition_id=str(book["condition_id"]),
            yes_price=yes_price,
            strike_price=strike,
            mark_price=mark_price,
            bet_size_usd=bet_size,
            signal_ns=time.time_ns(),
        )
        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = "TRIGGER_FIRED_MULTI_ASSET"
        self.shared_state.inflight_assets.add(asset)
        await self.execution_queue.put(ExecutionRequest(signal=signal, side=OrderSide.YES))
        self.shared_state.last_signal_ns = signal.signal_ns
        self._fired_window[asset] = True
        return True

    def _compute_dynamic_stake(self) -> Decimal:
        # Auto-reinvest: 95% del balance USDC disponible.
        balance = self.shared_state.wallet_usdc_balance
        if balance < Decimal("1.00"):
            return Decimal("0")
        stake = (balance * self.runtime_cfg.stake_usage).quantize(Decimal("0.01"))
        if stake < Decimal("1.00"):
            return Decimal("0")
        return stake

    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.wallet_usdc_balance += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        if self.shared_state.cumulative_pnl_usd <= self.runtime_cfg.kill_switch_pnl_usd:
            self.shared_state.kill_switch = True
            self.shared_state.latest_status = "KILL_SWITCH_TRIGGERED"

    async def stop(self) -> None:
        self._running = False

    def get_engine_summary(self) -> Dict[str, Union[float, str]]:
        return {
            "state": self.shared_state.sniper_state.value,
            "opportunities_detected": float(self.metrics.decisions),
            "opportunities_executed": float(self.metrics.fired),
            "avg_decision_time_ms": self.metrics.avg_decision_ms,
            "sniper_active": 1.0 if self.shared_state.sniper_state != SniperState.STOPPED else 0.0,
            "status": self.shared_state.latest_status,
            "wallet_usdc_balance": float(self.shared_state.wallet_usdc_balance),
        }
