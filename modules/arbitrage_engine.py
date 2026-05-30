import asyncio
import time
import traceback
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from modules.web3_executor import Web3Executor

from models import SniperAsset, SniperState, ExecutionRequest, ExecutionResult, SniperSignal, OrderSide, SharedMarketState, RuntimeConfig

class ArbitrageEngine:
    def __init__(
        self,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        shared_state: SharedMarketState,
        executor: Optional["Web3Executor"] = None,
        runtime_cfg: Optional[RuntimeConfig] = None
    ) -> None:
        self.execution_queue = execution_queue
        self.shared_state = shared_state
        self.executor = executor  # Referencia inyectada para auto-claim
        self.runtime_cfg = runtime_cfg or RuntimeConfig()
        self._running = False
        self._fired_window = {SniperAsset.BNB: False}
        self.last_pancake_epoch = 0
        self.executed_epochs = {}
        self.placed_bets: dict[int, str] = {}  # {epoch: 'BULL'|'BEAR'}

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

                # Resolver outcomes de apuestas pasadas
                await self._resolve_outcomes()

                await self._evaluate_opportunities()
            except Exception as e:
                print(f"\n🚨 [CRITICAL EXCEPTION DETECTED] ArbitrageEngine Loop")
                traceback.print_exc()
                self.shared_state.log_messages.append(f"⚠️ [ENGINE ERROR] {e}")
            await asyncio.sleep(0.1)

    async def _evaluate_opportunities(self) -> None:
        p_state = self.shared_state.pancake_state
        epoch = p_state["epoch"]
        
        # Reset window if new epoch
        if epoch > self.last_pancake_epoch:
            self.last_pancake_epoch = epoch
            self._fired_window[SniperAsset.BNB] = False

        lock_timestamp = p_state.get("lock_timestamp")
        
        if lock_timestamp is None:
            rem = 250
        else:
            # RELOJ HÍBRIDO CON CORRECCIÓN DE DESFASE:
            # Usa el offset calibrado atómicamente al arranque por el Monitor
            offset = getattr(self.shared_state, 'time_offset', 0.0)
            corrected_time = time.time() + offset
            rem = int(lock_timestamp - corrected_time)

        # Inyección de Log de Depuración Temporal (Verbose Debug)
        self.shared_state.log_messages.append(
            f"DEBUG TIME -> Epoch: {epoch} | Lock: {lock_timestamp} | Corrected: {int(time.time() + getattr(self.shared_state, 'time_offset', 0.0))} | Rem: {rem}s | Offset: {getattr(self.shared_state, 'time_offset', 0.0):+.1f}s"
        )
            
        # Hard validation: Solo permitir ejecuciones entre los 2 y los 4 segundos finales
        if rem > 4 or rem < 2:
            return  # Abortar por completo cualquier análisis o disparo
            
        # Control de Estado Anti-Doble Disparo (Nonce/Epoch Lock)
        if self.executed_epochs.get(epoch):
            self.shared_state.log_messages.append(f"🔒 [EPOCH LOCK] Epoch {epoch} ya ejecutado. Bloqueando doble disparo.")
            return

        bnb_price = Decimal(self.shared_state.asset_prices.get(SniperAsset.BNB, 0.0))
        if bnb_price <= 0:
            self.shared_state.log_messages.append(f"⚠️ [ABORT] BNB price={bnb_price}. Sin datos de Binance.")
            return
            
        lock_price = p_state.get("lock_price", Decimal("0"))
        if lock_price <= 0:
            self.shared_state.log_messages.append(f"⚠️ [KILL SWITCH] lockPrice={lock_price} de la ronda en juego no detectado. Abortando.")
            return

        # Estrategia de Predicción Real:
        # El bot compara el precio SPOT instantáneo de Binance (sin lag)
        # contra el precio de apertura de la ronda activa en PancakeSwap.
        if bnb_price > lock_price:
            side = OrderSide.YES  # Bull
        else:
            side = OrderSide.NO   # Bear
        
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
        
        self.shared_state.log_messages.append(
            f"🎯 [SNIPER FIRE] Epoch: {epoch} | Dirección: {'BULL 🟢' if side == OrderSide.YES else 'BEAR 🔴'} | BNB Spot: {bnb_price} | Lock: {lock_price} | Rem: {rem}s"
        )
        
        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = "FIRING_BSC_TX"
        self.shared_state.inflight_assets.add(SniperAsset.BNB)
        
        # Registrar en el ledger de apuestas activas
        direction = "BULL" if side == OrderSide.YES else "BEAR"
        self.placed_bets[epoch] = direction
        
        await self.execution_queue.put(ExecutionRequest(signal=signal, side=side))
        self.shared_state.last_signal_ns = signal.signal_ns
        self.executed_epochs[epoch] = True

    async def _resolve_outcomes(self) -> None:
        """
        Para cada apuesta en placed_bets, si el epoch ya cerró (currentEpoch >= epoch+2),
        consulta lockPrice y closePrice en el contrato y emite el resultado.
        Si ganó, dispara el auto-claim.
        """
        if not self.placed_bets:
            return

        current_epoch = self.shared_state.pancake_state.get("epoch", 0)
        if current_epoch == 0:
            return

        # Importar contrato del executor
        if self.executor is None or self.executor.contract is None:
            return

        resolved = []
        for target_epoch, direction in list(self.placed_bets.items()):
            if current_epoch < target_epoch + 2:
                continue  # Ronda aún no cerrada definitivamente

            try:
                loop = asyncio.get_event_loop()
                round_data = await loop.run_in_executor(
                    None,
                    self.executor.contract.functions.rounds(target_epoch).call
                )
                lock_price_raw  = round_data[4]   # int256
                close_price_raw = round_data[5]   # int256
                oracle_called   = round_data[13]  # bool

                if not oracle_called:
                    continue  # Datos aún no confirmados on-chain

                lock_price  = lock_price_raw  / 1e8
                close_price = close_price_raw / 1e8

                if direction == "BULL":
                    won = close_price > lock_price
                else:
                    won = close_price < lock_price

                if won:
                    msg = (f"🎉 [BALANCE] Epoch {target_epoch}: ¡APUESTA GANADA! 🟢 "
                           f"(Lock: {lock_price:.4f} | Close: {close_price:.4f})")
                    self.shared_state.log_messages.append(msg)
                    print(msg)
                    # ── Auto-cobro inmediato ──────────────────────────────
                    if self.executor:
                        asyncio.create_task(
                            self.executor.execute_auto_claim(target_epoch),
                            name=f"AutoClaim-{target_epoch}"
                        )
                else:
                    msg = (f"💀 [BALANCE] Epoch {target_epoch}: ¡APUESTA PERDIDA! 🔴 "
                           f"(Lock: {lock_price:.4f} | Close: {close_price:.4f})")
                    self.shared_state.log_messages.append(msg)
                    print(msg)

                resolved.append(target_epoch)

            except Exception:
                print(f"🚨 [CRITICAL EXCEPTION DETECTED] _resolve_outcomes(epoch={target_epoch}):")
                traceback.print_exc()

        for ep in resolved:
            self.placed_bets.pop(ep, None)

    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        
        # In PancakeSwap we don't immediately know PnL, but we can reset state
        self.shared_state.sniper_state = SniperState.ARMED
        self.shared_state.latest_status = "ARMED"

    async def stop(self) -> None:
        self._running = False
