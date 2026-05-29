import asyncio
import time
from decimal import Decimal

from models import SniperAsset, SniperState, ExecutionRequest, ExecutionResult, SniperSignal, OrderSide, SharedMarketState, RuntimeConfig

class ArbitrageEngine:
    def __init__(
        self,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        shared_state: SharedMarketState,
        runtime_cfg: RuntimeConfig | None = None
    ) -> None:
        self.execution_queue = execution_queue
        self.shared_state = shared_state
        self.runtime_cfg = runtime_cfg or RuntimeConfig()
        self._running = False
        self._fired_window = {SniperAsset.BNB: False}
        self.last_pancake_epoch = 0

    async def start(self) -> None:
        self._running = True
        self.shared_state.log_messages.append("🟢 [ARBITRAGE ENGINE] Iniciado para PancakeSwap BSC.")
        asyncio.create_task(self._loop(), name="EngineLoop")

    async def _loop(self) -> None:
        while self._running:
            try:
                if self.shared_state.kill_switch:
                    await asyncio.sleep(1)
                    continue

                if self.shared_state.sniper_state == SniperState.FIRING:
                    await asyncio.sleep(0.01)
                    continue

                await self._evaluate_opportunities()
            except Exception as e:
                self.shared_state.log_messages.append(f"⚠️ [ENGINE ERROR] {e}")
            await asyncio.sleep(0.1)

    async def _evaluate_opportunities(self) -> None:
        p_state = self.shared_state.pancake_state
        epoch = p_state["epoch"]
        rem = p_state["remaining_seconds"]
        
        # Reset window if new epoch
        if epoch > self.last_pancake_epoch:
            self.last_pancake_epoch = epoch
            self._fired_window[SniperAsset.BNB] = False

        # Only evaluate in last 15 seconds
        if rem > 15 or rem < 0:
            return

        if self._fired_window.get(SniperAsset.BNB):
            return

        bnb_price = self.shared_state.asset_prices.get(SniperAsset.BNB, 0.0)
        if bnb_price <= 0:
            return

        # Simple logic: If we have recent Binance tick, we bet Bull or Bear based on some micro-trend.
        # But for now, we just bet Bull if multiplier is high, or just random to test latency.
        # Here we just bet Bull to validate execution.
        side = OrderSide.YES  # Bull
        
        signal = SniperSignal(
            asset=SniperAsset.BNB,
            market_id=str(epoch),
            condition_id=str(epoch),
            yes_price=Decimal("0.5"),
            strike_price=Decimal(bnb_price),
            mark_price=Decimal(bnb_price),
            bet_size_usd=Decimal("0"),
            signal_ns=time.time_ns(),
        )
        
        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = "FIRING_BSC_TX"
        self.shared_state.inflight_assets.add(SniperAsset.BNB)
        
        await self.execution_queue.put(ExecutionRequest(signal=signal, side=side))
        self.shared_state.last_signal_ns = signal.signal_ns
        self._fired_window[SniperAsset.BNB] = True

    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        
        # In PancakeSwap we don't immediately know PnL, but we can reset state
        self.shared_state.sniper_state = SniperState.ARMED
        self.shared_state.latest_status = "ARMED"

    async def stop(self) -> None:
        self._running = False
