import asyncio
import time
import os
from decimal import Decimal
from typing import Optional
from web3 import Web3
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

def _sync_setup_web3(primary_rpc_url):
    RPC_POOL = [
        primary_rpc_url,
        "https://binance.llamarpc.com",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-mainnet.nodereal.io/v1/public"
    ]
    
    seen = set()
    pool = [x for x in RPC_POOL if not (x in seen or seen.add(x))]

    for rpc in pool:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 3}))
            if not w3.is_connected():
                continue
                
            try:
                from web3.middleware import ExtraDataToPOAMiddleware
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(PANCAKESWAP_CONTRACT),
                abi=PANCAKESWAP_PREDICTION_ABI
            )
            return w3, contract
        except Exception:
            continue
            
    raise Exception("Fallaron todos los RPCs del Failover Cluster.")

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
        self.rpc_url = os.getenv("BSC_RPC_URL", "https://binance.llamarpc.com")
        self.bet_amount_bnb = Decimal(os.getenv("BET_AMOUNT_BNB", "0.0005"))
        self.dry_run = os.getenv("DRY_RUN", "false").strip().lower() in ("true", "1", "yes", "on")
        
        self.w3 = None
        self.contract = None
        
        if self.dry_run:
            self.shared_state.log_messages.append("🔮 [MODO SIMULACRO] DRY_RUN=true. No se transmitirán transacciones reales.")
        
        if not self.private_key or not self.wallet_address:
            self.shared_state.log_messages.append("⚠️ [ERROR CRÍTICO] PRIVATE_KEY o WALLET_ADDRESS no configurados.")
        
        self._workers = []

    async def start(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            self.w3, self.contract = await loop.run_in_executor(None, _sync_setup_web3, self.rpc_url)
            
            self._running = True
            
            # ── AUTO-HARVESTER al arranque ──────────────────────────────────
            await self.run_harvester()
            # ───────────────────────────────────────────────────────────────
            
            worker_count = max(1, self.runtime_cfg.max_parallel_signals)
            self._workers = [
                asyncio.create_task(self._worker(), name=f"Web3Worker-{i}")
                for i in range(worker_count)
            ]
            await asyncio.gather(*self._workers)
        except Exception as e:
            import traceback
            err_msg = f"⚠️ [WEB3] START FATAL CRASH: {e} | {traceback.format_exc()}"
            self.shared_state.log_messages.append(err_msg)
            with open("crash_report.log", "a") as f:
                f.write(err_msg + "\n")

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
                err_msg = str(e)[:100]  # Truncate error message
                self.shared_state.log_messages.append(f"❌ [WEB3] Fallo TX: {err_msg}")
                self.shared_state.latest_status = f"ERROR EJECUCIÓN: {err_msg}"
                
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
        Ejecuta la apuesta en PancakeSwap usando run_in_executor para no bloquear el HFT loop.
        En modo DRY_RUN, simula la ejecución sin transmitir a la red.
        """
        loop = asyncio.get_event_loop()
        epoch = self.shared_state.pancake_state["epoch"]
        value_wei = Web3.to_wei(self.bet_amount_bnb, 'ether')
        account = Web3.to_checksum_address(self.wallet_address)
        
        direction = "BULL 🟢" if req.side == OrderSide.YES else "BEAR 🔴"
        p_state = self.shared_state.pancake_state
        rem = p_state.get("remaining_seconds", "?")
        
        # ═══════════════════════════════════════════════════════════════
        # 🔮 INTERCEPTOR DRY-RUN: Simula sin transmitir a la blockchain
        # ═══════════════════════════════════════════════════════════════
        if self.dry_run:
            sim_time_ns = time.time_ns()
            sim_hash = f"0xSIMULATED_HASH_{epoch}_{sim_time_ns}"
            self.shared_state.log_messages.append(
                f"🔮 [SIMULACRO DRY-RUN] ¡GATILLO ACCIONADO!"
            )
            self.shared_state.log_messages.append(
                f"├ Epoch Objetivo: {epoch} | Dirección: {direction}"
            )
            self.shared_state.log_messages.append(
                f"├ Tiempo Restante del Contrato: {rem}s"
            )
            self.shared_state.log_messages.append(
                f"└ Simulación TX OK (Monto Protegido: {self.bet_amount_bnb} BNB) | Hash: {sim_hash[:30]}..."
            )
            return sim_hash, True, self.bet_amount_bnb, Decimal("0")

        # ═══════════════════════════════════════════════════════════════
        # 🔴 MODO LIVE: Transmisión real a la blockchain
        # ═══════════════════════════════════════════════════════════════
        
        # Selección de función
        if req.side == OrderSide.YES:  # Bull
            func = self.contract.functions.betBull(epoch)
        else:  # Bear
            func = self.contract.functions.betBear(epoch)
            
        def _build_and_send():
            nonce = self.w3.eth.get_transaction_count(account)
            gas_price = self.w3.eth.gas_price
            
            # Aumentar gas price fuertemente para máxima prioridad HFT (Fast-Inclusion)
            gas_price = int(gas_price * 1.5)
            
            tx = func.build_transaction({
                'from': account,
                'value': value_wei,
                'nonce': nonce,
                'gasPrice': gas_price,
                'gas': 250000  # Hardcoded gas limit to bypass node estimation on low balance
            })
                
            # Firmar
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            
            # Enviar
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Esperar confirmación de la red para evitar falsos positivos
            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                if receipt.status != 1:
                    raise Exception("Transacción revertida por el Smart Contract (¿Monto muy bajo?).")
            except Exception as e:
                raise Exception(f"Fallo al confirmar: {e}")
                
            return tx_hash.to_0x_hex()
            
        tx_hash_hex = await loop.run_in_executor(None, _build_and_send)
        
        return tx_hash_hex, True, self.bet_amount_bnb, Decimal("0")

    async def run_harvester(self) -> None:
        """
        AUTO-HARVESTER: Escanea las últimas 10 rondas cerradas al arrancar.
        Detecta Epochs ganados y pendientes de cobro, y emite un batch claim.
        """
        if self.dry_run:
            print("🔮 [HARVESTER] Modo DRY_RUN: saltando auto-cobro real.")
            return

        if not self.wallet_address or not self.private_key:
            print("⚠️ [HARVESTER] Sin wallet configurada. Saltando harvester.")
            return

        loop = asyncio.get_event_loop()

        def _scan_and_claim():
            print("💰 [HARVESTER] Escaneando rondas pasadas para auto-cobro...")
            account = Web3.to_checksum_address(self.wallet_address)
            current_epoch = self.contract.functions.currentEpoch().call()

            claimable_epochs = []
            for i in range(2, 12):  # Rondas [current-2 .. current-11] (cerradas)
                target = current_epoch - i
                if target <= 0:
                    break
                try:
                    is_claimable = self.contract.functions.claimable(target, account).call()
                    if is_claimable:
                        claimable_epochs.append(target)
                        print(f"  ├ Epoch {target}: GANADO ✅ (pendiente de cobro)")
                    else:
                        ledger = self.contract.functions.ledger(target, account).call()
                        claimed = ledger[2]
                        amount  = ledger[1]
                        if amount > 0 and claimed:
                            print(f"  ├ Epoch {target}: ya cobrado ✔")
                        elif amount == 0:
                            print(f"  ├ Epoch {target}: sin apuesta registrada")
                        else:
                            print(f"  ├ Epoch {target}: PERDIDO ❌")
                except Exception as e:
                    print(f"  ├ Epoch {target}: error al consultar ({e})")

            if not claimable_epochs:
                print("💰 [HARVESTER] No hay ganancias pendientes. Continuando...")
                return None

            print(f"💰 [HARVESTER] ¡Detectadas ganancias pendientes en las rondas: {claimable_epochs}!")

            nonce     = self.w3.eth.get_transaction_count(account)
            gas_price = int(self.w3.eth.gas_price * 1.5)

            tx = self.contract.functions.claim(claimable_epochs).build_transaction({
                'from': account,
                'nonce': nonce,
                'gasPrice': gas_price,
                'gas': 300000,
            })
            signed = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status == 1:
                msg = f"✅ [HARVESTER] Transacción de cobro enviada. Fondos restaurados a tu Trust Wallet. Hash: {tx_hash.hex()}"
            else:
                msg = f"❌ [HARVESTER] Transacción de cobro revertida. Hash: {tx_hash.hex()}"
            print(msg)
            return msg

        try:
            result = await loop.run_in_executor(None, _scan_and_claim)
            if result:
                self.shared_state.log_messages.append(result)
        except Exception:
            import traceback
            print("🚨 [CRITICAL EXCEPTION DETECTED] run_harvester:")
            traceback.print_exc()

    async def execute_auto_claim(self, epoch: int) -> bool:
        """
        Cobra una sola ronda ganada. Llamado por ArbitrageEngine tras resolver outcome.
        Retorna True si el cobro fue exitoso.
        """
        if self.dry_run:
            print(f"🔮 [AUTO-CLAIM] DRY_RUN: simulando cobro de Epoch {epoch}.")
            return True

        loop = asyncio.get_event_loop()
        account = Web3.to_checksum_address(self.wallet_address)

        def _do_claim():
            nonce     = self.w3.eth.get_transaction_count(account)
            gas_price = int(self.w3.eth.gas_price * 1.5)

            tx = self.contract.functions.claim([epoch]).build_transaction({
                'from': account,
                'nonce': nonce,
                'gasPrice': gas_price,
                'gas': 200000,
            })
            signed  = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            return receipt.status == 1, tx_hash.hex()

        try:
            ok, tx_hex = await loop.run_in_executor(None, _do_claim)
            if ok:
                msg = f"✅ [AUTO-CLAIM] Epoch {epoch} cobrado con éxito. Hash: {tx_hex}"
            else:
                msg = f"❌ [AUTO-CLAIM] Cobro de Epoch {epoch} revertido. Hash: {tx_hex}"
            print(msg)
            self.shared_state.log_messages.append(msg)
            return ok
        except Exception:
            import traceback
            print(f"🚨 [CRITICAL EXCEPTION DETECTED] execute_auto_claim(epoch={epoch}):")
            traceback.print_exc()
            return False

    async def stop(self) -> None:
        self._running = False
        for worker in self._workers:
            worker.cancel()
