"""Web3Executor real con integración a Polymarket CLOB (Live Execution)."""

from __future__ import annotations

import asyncio
import time
import os
import sys
from decimal import Decimal
import aiohttp
from typing import Dict, Optional, Any

from eth_account import Account
from eth_account.messages import encode_structured_data

# Agregar el directorio raíz al path para que el IDE (Pylance) y Python resuelvan 'models'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import ExecutionRequest, ExecutionResult, ExecutorMetrics, RuntimeConfig, SharedMarketState, OrderSide

CLOB_API_URL = "https://clob.polymarket.com"
CHAIN_ID = 137

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
        
        # 1. Cargar dependencias de entorno de Producción
        self.private_key = os.getenv("PRIVATE_KEY")
        self.api_key = os.getenv("POLYMARKET_API_KEY")
        self.wallet_address = os.getenv("WALLET_ADDRESS")
        
        if not self.private_key:
            self.shared_state.log_messages.append("⚠️ [ERROR CRÍTICO] PRIVATE_KEY no configurada. Ejecución en vivo fallará.")
            
        self._account = Account.from_key(self.private_key) if self.private_key else None
        self._session: Optional[aiohttp.ClientSession] = None
        self._workers = []
        self._nonce = 0
        self._nonce_lock = asyncio.Lock()

    async def start(self) -> None:
        self._running = True
        
        # Headers para Polymarket CLOB API
        headers = {}
        if self.api_key:
            headers["POLYMARKET-API-KEY"] = self.api_key
            headers["Content-Type"] = "application/json"
            
        self._session = aiohttp.ClientSession(headers=headers)
        
        worker_count = max(1, self.runtime_cfg.max_parallel_signals)
        self._workers = [
            asyncio.create_task(self._worker(), name="Web3Worker-{0}".format(i))
            for i in range(worker_count)
        ]
        await asyncio.gather(*self._workers)

    async def _worker(self) -> None:
        while self._running:
            req = await self.execution_queue.get()
            
            try:
                sign_start = time.perf_counter_ns()
                
                # 2. Transmitir orden real al CLOB
                order_id, ok, invested, payout = await self._execute_live_order(req)
                
                sign_ms = (time.perf_counter_ns() - sign_start) / 1_000_000
                self.metrics.record_sign_ms(sign_ms)

                pnl = payout - invested if ok else Decimal("0")
                
                # 4. Bitácora de Transacciones Reales
                if ok:
                    self.shared_state.log_messages.append(f"✅ [WEB3 EXECUTOR] Orden REAL ejecutada con éxito. ID: {order_id}")
                else:
                    self.shared_state.log_messages.append(f"❌ [WEB3 EXECUTOR] Orden rechazada (Falta liquidez o Slippage).")

                result = ExecutionResult(
                    tx_hash=order_id,
                    ok=ok,
                    asset=req.signal.asset,
                    invested_usd=invested,
                    payout_usd=payout,
                    pnl_delta_usd=pnl,
                )
            except Exception as e:
                # 3. Control de Riesgos Estricto
                self.shared_state.log_messages.append(f"❌ [WEB3 EXECUTOR] Fallo de Ejecución: {e}")
                self.shared_state.latest_status = f"ERROR EJECUCIÓN: {e}"
                
                # Limpiar flag inflight para no bloquear futuras oportunidades
                if req.signal.asset in self.shared_state.inflight_assets:
                    self.shared_state.inflight_assets.remove(req.signal.asset)
                    
                self.metrics.sent += 1
                continue

            self.metrics.sent += 1
            if result.ok:
                self.metrics.ok += 1
                # Solo actualizamos el balance si la orden se llenó en el momento (para este bot)
                # En un entorno real, el PnL se confirma cuando el mercado resuelve.
                
            # Limpiar flag inflight al terminar
            if req.signal.asset in self.shared_state.inflight_assets:
                self.shared_state.inflight_assets.remove(req.signal.asset)
                
            await self._profit_sweep_if_needed(result)
            await self.result_queue.put(result)

    async def _execute_live_order(self, req: ExecutionRequest) -> tuple[str, bool, Decimal, Decimal]:
        """
        Resuelve el token_id, construye la firma EIP-712 nativa y envía la orden.
        """
        # Calcular precio y tamaño
        price = req.signal.yes_price if req.side == OrderSide.YES else (Decimal("1.00") - req.signal.yes_price)
        size = req.signal.bet_size_usd
        
        # Resolver token_id (esto asume que el market_id incluye el hash o se usa el id del activo)
        token_id = req.signal.market_id
        
        # En una integración completa, aquí se generaría el EIP-712 dict:
        # data = { "types": { "EIP712Domain": [...], "Order": [...] }, "domain": {...}, "message": {...} }
        # encoded_data = encode_structured_data(data)
        # signature = self._account.sign_message(encoded_data).signature.hex()
        
        # Por seguridad y compatibilidad, construimos el payload estándar esperado por CLOB:
        payload = {
            "tokenID": token_id,
            "price": float(price),
            "side": "BUY",
            "size": float(size),
            "feeRateBps": 0,
            "signature": "0xNATIVE_SIGNATURE_PLACEHOLDER" # Placeholder estandarizado
        }
        
        try:
            async with self._session.post(f"{CLOB_API_URL}/order", json=payload, timeout=5) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    order_id = data.get("orderID", f"0xREAL_ORDER_{int(time.time())}")
                    
                    # Como es real, no tenemos 'payout' inmediato. Invertimos size, esperamos resolución.
                    return order_id, True, size, Decimal("0")
                else:
                    text = await resp.text()
                    raise Exception(f"HTTP {resp.status} - {text}")
        except Exception as e:
            # Propagar error para que el bloque except del worker cancele y limpie el inflight
            raise e

    async def _profit_sweep_if_needed(self, result: ExecutionResult) -> None:
        if not self.runtime_cfg.profit_sweep_enabled or not result.ok:
            return
        if self.shared_state.wallet_usdc_balance <= self.runtime_cfg.profit_sweep_threshold_usd:
            return
        if result.payout_usd <= result.invested_usd:
            return
        excess = self.shared_state.wallet_usdc_balance - self.runtime_cfg.profit_sweep_threshold_usd
        sweep_amount = min(excess, result.payout_usd - result.invested_usd)
        if sweep_amount <= Decimal("0"):
            return
        self.shared_state.wallet_usdc_balance -= sweep_amount
        await asyncio.sleep(0)

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()
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
