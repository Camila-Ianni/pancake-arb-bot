"""ArbitrageEngine multi-activo con auto-reinvest y latencia <10ms."""

from __future__ import annotations

import asyncio
import time
import os
import logging
from decimal import Decimal
from typing import Dict, Optional, Union

logger = logging.getLogger(__name__)

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
        try:
            close_ts = int(book.get("market_close_ts", 0))
            remaining = close_ts - now_s
        except Exception as e:
            self.shared_state.log_messages.append(f"❌ [{asset.name}] Error calculando remaining: {e} | Book: {book}")
            return False

        if remaining <= 0:
            self.shared_state.log_messages.append(f"⏳ [{asset.name}] Vela cerrada. Esperando que Polymarket publique la nueva ronda...")
            self._fired_window[asset] = False
            return False

        if remaining % 10 == 0 or remaining <= 20:
            self.shared_state.log_messages.append(f"🔍 [CHECK {asset.name}] Tick recibido. Remaining: {remaining}s")
            
        self.runtime_cfg.close_window_sec = 20  # Ventana óptima HFT antes del cierre
        
        if self._fired_window.get(asset):
            if remaining <= 20:
                # Limitamos el spam, pero lo mostramos al menos una vez
                if remaining == 19 or remaining == 10:
                    self.shared_state.log_messages.append(f"👉 [DEBUG] {asset.name} omitido porque _fired_window ya es True.")
            return False
        if asset in self.shared_state.inflight_assets:
            if remaining <= 20 and (remaining == 19 or remaining == 10):
                self.shared_state.log_messages.append(f"👉 [DEBUG] {asset.name} omitido porque está inflight.")
            return False

        try:
            yes_price = Decimal(str(book.get("yes_price", "0")))
            strike_val = book.get("strike_price", 0.0)
            strike = float(strike_val) if strike_val is not None else 0.0
            mark_price = float(self.shared_state.asset_prices.get(asset, 0.0))
        except Exception as e:
            self.shared_state.log_messages.append(f"❌ [{asset.name}] Exception parseando precios: {e}")
            return False

        # 2. AUDITAR RETORNOS TEMPRANOS POR PRECIOS VACÍOS
        if not mark_price or mark_price <= 0 or not yes_price or not strike:
            if remaining % 10 == 0 or remaining <= 20:
                self.shared_state.log_messages.append(f"❌ [{asset.name}] Abortado por precio 0 o nulo: Binance={mark_price} | Poly={yes_price} | Strike={strike}")
            return False

        if remaining >= self.runtime_cfg.close_window_sec:
            if remaining % 10 == 0:
                self.shared_state.log_messages.append(f"⏳ [{asset.name}] Monitoreando vela de 5m... Quedan {remaining}s. Spot: {mark_price:.2f}")
            return False

        # --- DENTRO DE LA VENTANA CRÍTICA ---
        self.shared_state.log_messages.append(f"🎯 [ALERTA SNIPER] ¡{asset.name} en ventana crítica de ejecución! Faltan {remaining}s. Spot: {mark_price:.2f} | Strike: {strike:.2f}")

        if mark_price > strike:
            side = OrderSide.YES
            side_str = "YES"
            poly_price = yes_price
        else:
            side = OrderSide.NO
            side_str = "NO"
            poly_price = Decimal("1.00") - yes_price

        decision = f"Ejecutando apuesta ganadora ({side_str})"
            
        bet_size = self._compute_dynamic_stake()
        self.shared_state.log_messages.append(f"👉 [DEBUG] bet_size calculado: {bet_size} | saldo: {self.shared_state.wallet_usdc_balance}")
        if bet_size < Decimal("0.01"):
            decision = "Saltando trade: Liquidez USDC insuficiente (min 0.01)"
            self.shared_state.log_messages.append(f"❌ [{asset.name} 5m] Descartado por saldo insuficiente.")
            self.shared_state.latest_status = "INSUFFICIENT_USDC"
            return False

        self.shared_state.log_messages.append(f"[{asset.name} 5m] -> Segundos restantes: {remaining} | Binance: {mark_price:.2f} | Strike: {strike:.2f}")
        self.shared_state.log_messages.append(f"  └ {decision}")

        is_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
        if is_dry_run:
            simulated_gas = Decimal("0.05")
            pnl_proj = Decimal("2.50") - simulated_gas
            self.shared_state.log_messages.append(f"  ├ [SIMULACIÓN OP] PnL Proyectado Neto: ${pnl_proj:.2f} (Gas deducido)")

        signal = SniperSignal(
            asset=asset,
            market_id=str(book["market_id"]),
            condition_id=str(book["condition_id"]),
            yes_price=poly_price,
            strike_price=strike,
            mark_price=mark_price,
            bet_size_usd=bet_size,
            signal_ns=time.time_ns(),
        )
        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = "TRIGGER_FIRED_MULTI_ASSET"
        self.shared_state.inflight_assets.add(asset)
        await self.execution_queue.put(ExecutionRequest(signal=signal, side=side))
        self.shared_state.last_signal_ns = signal.signal_ns
        self._fired_window[asset] = True
        return True

    def _compute_dynamic_stake(self) -> Decimal:
        balance = self.shared_state.wallet_usdc_balance
        if balance < Decimal("0.01"):
            return Decimal("0")
        stake = (balance * self.runtime_cfg.stake_usage).quantize(Decimal("0.01"))
        if stake < Decimal("0.01"):
            return Decimal("0")
        return stake

    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.wallet_usdc_balance += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        self._fired_window[asset] = False
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
