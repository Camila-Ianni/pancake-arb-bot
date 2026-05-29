import asyncio
import time
import os
from decimal import Decimal
from typing import Optional
from web3 import AsyncWeb3
from web3.exceptions import Web3Exception

from models import ExecutionRequest, ExecutionResult, OrderSide, SharedMarketState, RuntimeConfig
from modules.pancake_abi import PANCAKESWAP_PREDICTION_ABI

PANCAKESWAP_CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"

class ExecutorMetrics:
    __slots__ = ("sent", "ok", "latency_history_ms")
    def __init__(self) -> None:
        self.sent = 0
        self.ok = 0
        self.latency_history_ms: list[float] = []

    def record_sign_ms(self, ms: float) -> None:
        self.latency_history_ms.append(ms)
        if len(self.latency_history_ms) > 100:
            self.latency_history_ms.pop(0)

class Web3Executor:
    """
    Motor EVM para firmar y transmitir transacciones a BNB Chain (PancakeSwap).
    """
    def __init__(
        self,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        result_queue: "asyncio.Queue[ExecutionResult]",
        shared_state: SharedMarketState,
        runtime_cfg: Optional[RuntimeConfig] = None
    ) -> None:
        self.execution_queue = execution_queue
        self.result_queue = result_queue
        self.shared_state = shared_state
        self.runtime_cfg = runtime_cfg or RuntimeConfig()
        self.metrics = ExecutorMetrics()
        self._running = False
        
        self.private_key = os.getenv("PRIVATE_KEY")
        self.wallet_address = os.getenv("WALLET_ADDRESS")
        self.rpc_url = os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org/")
        self.bet_amount_bnb = Decimal(os.getenv("BET_AMOUNT_BNB", "0.0005"))
        
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.rpc_url))
        self.contract = self.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(PANCAKESWAP_CONTRACT),
            abi=PANCAKESWAP_PREDICTION_ABI
        )
        
        if not self.private_key or not self.wallet_address:
            self.shared_state.log_messages.append("⚠️ [ERROR CRÍTICO] PRIVATE_KEY o WALLET_ADDRESS no configurados.")
        
        self._workers = []

    async def start(self) -> None:
        self._running = True
        worker_count = max(1, self.runtime_cfg.max_parallel_signals)
        self._workers = [
            asyncio.create_task(self._worker(), name=f"Web3Worker-{i}")
            for i in range(worker_count)
        ]
        await asyncio.gather(*self._workers)

    async def _worker(self) -> None:
        while self._running:
            req = await self.execution_queue.get()
            try:
                sign_start = time.perf_counter_ns()
                
                # Ejecutar
                order_id, ok, invested, payout = await self._execute_live_order(req)
                
                sign_ms = (time.perf_counter_ns() - sign_start) / 1_000_000
                self.metrics.record_sign_ms(sign_ms)

                pnl = payout - invested if ok else Decimal("0")
                
                if ok:
                    self.shared_state.log_messages.append(f"✅ [WEB3 EXECUTOR] TX PancakeSwap ejecutada con éxito. Hash: {order_id}")
                else:
                    self.shared_state.log_messages.append(f"❌ [WEB3 EXECUTOR] TX rechazada.")

                result = ExecutionResult(
                    tx_hash=order_id,
                    ok=ok,
                    asset=req.signal.asset,
                    invested_usd=invested,
                    payout_usd=payout,
                    pnl_delta_usd=pnl,
                )
            except Exception as e:
                self.shared_state.log_messages.append(f"❌ [WEB3 EXECUTOR] Fallo de Ejecución: {e}")
                self.shared_state.latest_status = f"ERROR EJECUCIÓN: {e}"
                
                if req.signal.asset in self.shared_state.inflight_assets:
                    self.shared_state.inflight_assets.remove(req.signal.asset)
                    
                self.metrics.sent += 1
                continue

            self.metrics.sent += 1
            if result.ok:
                self.metrics.ok += 1
                
            if req.signal.asset in self.shared_state.inflight_assets:
                self.shared_state.inflight_assets.remove(req.signal.asset)
                
            await self.result_queue.put(result)

    async def _execute_live_order(self, req: ExecutionRequest) -> tuple[str, bool, Decimal, Decimal]:
        """
        Ejecuta la apuesta en PancakeSwap.
        """
        epoch = self.shared_state.pancake_state["epoch"]
        value_wei = AsyncWeb3.to_wei(self.bet_amount_bnb, 'ether')
        account = AsyncWeb3.to_checksum_address(self.wallet_address)
        
        # Selección de función
        if req.side == OrderSide.YES:  # Bull
            func = self.contract.functions.betBull(epoch)
        else:  # Bear
            func = self.contract.functions.betBear(epoch)
            
        nonce = await self.w3.eth.get_transaction_count(account)
        gas_price = await self.w3.eth.gas_price
        
        # Aumentar gas price ligeramente para prioridad HFT
        gas_price = int(gas_price * 1.1)
        
        tx = await func.build_transaction({
            'from': account,
            'value': value_wei,
            'nonce': nonce,
            'gasPrice': gas_price
        })
        
        try:
            # Estimar gas
            gas_estimate = await self.w3.eth.estimate_gas(tx)
            tx['gas'] = gas_estimate
        except Web3Exception as e:
            raise Exception(f"Gas estimation failed (ronda cerrada?): {e}")
            
        # Firmar
        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
        
        # Enviar
        tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        
        # No esperamos el receipt para HFT, retornamos inmediatamente asumiendo éxito si pasó al mempool
        return tx_hash.to_0x_hex(), True, self.bet_amount_bnb, Decimal("0")

    async def stop(self) -> None:
        self._running = False
        for worker in self._workers:
            worker.cancel()
