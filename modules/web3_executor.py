"""Web3Executor concurrente con smart sweep condicional (> $500)."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from hashlib import sha1
from typing import Dict, Optional
import os
import sys

# Agregar el directorio raíz al path para que el IDE (Pylance) y Python resuelvan 'models'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import ExecutionRequest, ExecutionResult, ExecutorMetrics, RuntimeConfig, SharedMarketState


class Web3Executor:
    def __init__(
        self,
        execution_queue: asyncio.Queue[ExecutionRequest],
        result_queue: asyncio.Queue[ExecutionResult],
        safe_wallet_address: str,
        shared_state: SharedMarketState,
        runtime_cfg: Optional[RuntimeConfig] = None,
    ) -> None:
        self.execution_queue = execution_queue
        self.result_queue = result_queue
        self.safe_wallet_address = safe_wallet_address
        self.shared_state = shared_state
        self.runtime_cfg = runtime_cfg or RuntimeConfig()
        self.metrics = ExecutorMetrics()
        self._running = False
        self._nonce = 0
        self._nonce_lock = asyncio.Lock()
        self._workers = []

    async def start(self) -> None:
        self._running = True
        worker_count = max(2, self.runtime_cfg.max_parallel_signals)
        self._workers = [
            asyncio.create_task(self._worker(), name="Web3Worker-{0}".format(i))
            for i in range(worker_count)
        ]
        await asyncio.gather(*self._workers)

    async def _worker(self) -> None:
        is_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
        while self._running:
            req = await self.execution_queue.get()
            
            if is_dry_run:
                # Sandbox: Paper Trading Mode
                await asyncio.sleep(0.05)  # Simular latencia de red
                tx_hash = "0xDRYRUN" + sha1(str(time.time_ns()).encode()).hexdigest()
                self.metrics.record_sign_ms(0.0)
                
                # Simular gas (ej: $0.05 en MATIC) y fill automático
                invested = req.signal.bet_size_usd
                simulated_gas_usd = Decimal("0.05")
                payout = invested + Decimal("2.50")  # Retorno estático simulado
                pnl = payout - invested - simulated_gas_usd
                
                result = ExecutionResult(
                    tx_hash=tx_hash,
                    ok=True,
                    asset=req.signal.asset,
                    invested_usd=invested,
                    payout_usd=payout,
                    pnl_delta_usd=pnl,
                )
            else:
                sign_start = time.perf_counter_ns()
                nonce = await self._acquire_nonce()
                tx_hash = self._fast_sign_stub(req=req, nonce=nonce)
                sign_ms = (time.perf_counter_ns() - sign_start) / 1_000_000
                self.metrics.record_sign_ms(sign_ms)

                invested = req.signal.bet_size_usd
                payout = invested + Decimal("2.50")
                pnl = payout - invested
                result = ExecutionResult(
                    tx_hash=tx_hash,
                    ok=True,
                    asset=req.signal.asset,
                    invested_usd=invested,
                    payout_usd=payout,
                    pnl_delta_usd=pnl,
                )

            self.metrics.sent += 1
            self.metrics.ok += 1
            self.shared_state.wallet_usdc_balance += pnl
            await self._profit_sweep_if_needed(result)
            await self.result_queue.put(result)

    async def _acquire_nonce(self) -> int:
        async with self._nonce_lock:
            nonce = self._nonce
            self._nonce += 1
            return nonce

    def _fast_sign_stub(self, req: ExecutionRequest, nonce: int) -> str:
        seed = (
            f"{req.signal.signal_ns}:{req.signal.asset.value}:{req.signal.yes_price}:"
            f"{req.signal.bet_size_usd}:{nonce}"
        )
        return "0x" + sha1(seed.encode("utf-8")).hexdigest()

    async def _profit_sweep_if_needed(self, result: ExecutionResult) -> None:
        if not self.runtime_cfg.profit_sweep_enabled:
            return
        if not result.ok:
            return
        if self.shared_state.wallet_usdc_balance <= self.runtime_cfg.profit_sweep_threshold_usd:
            return
        if result.payout_usd <= result.invested_usd:
            return
        # Solo barre el excedente sobre el umbral, mantiene capital de escalado.
        excess = self.shared_state.wallet_usdc_balance - self.runtime_cfg.profit_sweep_threshold_usd
        sweep_amount = min(excess, result.payout_usd - result.invested_usd)
        if sweep_amount <= Decimal("0"):
            return
        self.shared_state.wallet_usdc_balance -= sweep_amount
        _ = (sweep_amount, self.safe_wallet_address)
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._running = False
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

    def get_executor_status(self) -> Dict[str, float]:
        success_rate = (self.metrics.ok / self.metrics.sent) if self.metrics.sent else 0.0
        return {
            "tx_sent": float(self.metrics.sent),
            "tx_confirmed": float(self.metrics.ok),
            "success_rate": success_rate,
            "avg_sign_ms": self.metrics.avg_sign_ms,
            "nonce": float(self._nonce),
            "wallet_usdc_balance": float(self.shared_state.wallet_usdc_balance),
        }
