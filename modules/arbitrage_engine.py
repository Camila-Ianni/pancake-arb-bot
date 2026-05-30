import asyncio
import time
import traceback
from collections import deque
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from modules.web3_executor import Web3Executor

from models import (
    SniperAsset, SniperState, ExecutionRequest, ExecutionResult,
    SniperSignal, OrderSide, SharedMarketState, RuntimeConfig
)

# ═══════════════════════════════════════════════════════════════════════════
# PARÁMETROS CUANTITATIVOS - Ajusta estos valores para calibrar la estrategia
# ═══════════════════════════════════════════════════════════════════════════
EMA_PERIOD          = 7       # Período de la EMA de corto plazo
ATR_PERIOD          = 7       # Período del ATR
ATR_MIN_THRESHOLD   = 0.05    # ↓ Reducido: opera en mercados de menor volatilidad
ATR_MAX_THRESHOLD   = 5.00    # Máximo de volatilidad en USD (si ATR > esto, skip)
MOMENTUM_TICKS_REQ  = 2       # ↓ Reducido de 3→2: gatillo más sensible
SAFETY_MARGIN_PCT   = Decimal("0.0004")  # ↓ ~0.04% → ~$0.29 spread mínimo a $720 BNB
KILL_SWITCH_PNL_USD = Decimal("-1.00")  # Límite de pérdida acumulada en simulación


def _calc_ema(prices: deque, period: int) -> Optional[Decimal]:
    """Calcula la EMA de un deque de precios. Retorna None si no hay suficientes datos."""
    if len(prices) < period:
        return None
    k = Decimal("2") / Decimal(period + 1)
    vals = list(prices)[-period:]
    ema = Decimal(str(vals[0]))
    for p in vals[1:]:
        ema = Decimal(str(p)) * k + ema * (1 - k)
    return ema


def _calc_atr(highs: deque, lows: deque, closes: deque, period: int) -> Optional[float]:
    """
    ATR simplificado usando spread high-low de cada tick de precio.
    En este contexto, 'high' y 'low' son la variación máx/mín por bloque.
    """
    if len(highs) < period or len(lows) < period:
        return None
    trs = []
    for i in range(-period, 0):
        tr = float(highs[i]) - float(lows[i])
        trs.append(abs(tr))
    return sum(trs) / len(trs)


class ArbitrageEngine:
    def __init__(
        self,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        shared_state: SharedMarketState,
        executor: Optional["Web3Executor"] = None,
        runtime_cfg: Optional[RuntimeConfig] = None
    ) -> None:
        self.execution_queue  = execution_queue
        self.shared_state     = shared_state
        self.executor         = executor
        self.runtime_cfg      = runtime_cfg or RuntimeConfig()
        self._running         = False
        self._fired_window    = {SniperAsset.BNB: False}
        self.last_pancake_epoch = 0
        self.executed_epochs: dict[int, bool] = {}
        self.placed_bets: dict[int, str] = {}  # {epoch: 'BULL'|'BEAR'}

        # ── Buffers para indicadores técnicos ─────────────────────────────
        maxlen = max(EMA_PERIOD, ATR_PERIOD) * 4
        self._price_history: deque = deque(maxlen=maxlen)   # precios BNB spot (Binance)
        self._tick_highs: deque    = deque(maxlen=maxlen)   # high por bloque
        self._tick_lows: deque     = deque(maxlen=maxlen)   # low por bloque
        self._last_price: Optional[Decimal] = None           # precio previo

        # ── Contador de momentum (ticks consecutivos con spread válido) ────
        self._momentum_counter: int = 0
        self._momentum_side: Optional[OrderSide] = None      # dirección confirmada

    # ──────────────────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._running = True
        self.shared_state.log_messages.append(
            "🟢 [ARBITRAGE ENGINE] Iniciado — Estrategia: EMA Trend + ATR + Momentum Confirmation"
        )
        asyncio.create_task(self._loop(), name="EngineLoop")

    # ──────────────────────────────────────────────────────────────────────
    async def _loop(self) -> None:
        while self._running:
            try:
                # 1. Kill Switch de PnL: si caemos -$1.00 en simulación, congelamos
                if self.shared_state.cumulative_pnl_usd <= KILL_SWITCH_PNL_USD:
                    if not self.shared_state.kill_switch:
                        self.shared_state.kill_switch = True
                        self.shared_state.log_messages.append(
                            f"🚨 [KILL SWITCH] PnL acumulado = ${self.shared_state.cumulative_pnl_usd:.2f} "
                            f"— Límite crítico alcanzado. Bot congelado. Resetea manualmente."
                        )

                if self.shared_state.kill_switch:
                    await asyncio.sleep(1)
                    continue

                if self.shared_state.sniper_state == SniperState.FIRING:
                    await asyncio.sleep(0.01)
                    continue

                # 2. Resolver outcomes de apuestas pasadas
                await self._resolve_outcomes()

                # 3. Evaluar nueva oportunidad
                await self._evaluate_opportunities()

            except Exception as e:
                print(f"\n🚨 [CRITICAL EXCEPTION DETECTED] ArbitrageEngine Loop")
                traceback.print_exc()
                self.shared_state.log_messages.append(f"⚠️ [ENGINE ERROR] {e}")
            await asyncio.sleep(0.1)

    # ──────────────────────────────────────────────────────────────────────
    def _update_price_buffers(self, current_price: Decimal) -> None:
        """Alimenta los buffers de indicadores con el precio más reciente."""
        self._price_history.append(current_price)

        if self._last_price is not None:
            high = max(current_price, self._last_price)
            low  = min(current_price, self._last_price)
        else:
            high = current_price
            low  = current_price

        self._tick_highs.append(high)
        self._tick_lows.append(low)
        self._last_price = current_price

    # ──────────────────────────────────────────────────────────────────────
    async def _evaluate_opportunities(self) -> None:
        p_state = self.shared_state.pancake_state
        epoch   = p_state["epoch"]

        # Reset ventana si nuevo epoch
        if epoch > self.last_pancake_epoch:
            self.last_pancake_epoch = epoch
            self._fired_window[SniperAsset.BNB] = False
            self._momentum_counter = 0
            self._momentum_side    = None

        lock_timestamp = p_state.get("lock_timestamp")
        if lock_timestamp is None:
            return

        # Reloj híbrido con offset atómico
        offset         = getattr(self.shared_state, "time_offset", 0.0)
        corrected_time = time.time() + offset
        rem            = int(lock_timestamp - corrected_time)

        # Solo operar en la ventana de disparo: 2–4 segundos antes del cierre
        if rem > 4 or rem < 2:
            return

        # Anti-doble-disparo
        if self.executed_epochs.get(epoch):
            return

        # ── Datos base ────────────────────────────────────────────────────
        bnb_price = Decimal(str(self.shared_state.asset_prices.get(SniperAsset.BNB, 0.0)))
        if bnb_price <= 0:
            self.shared_state.log_messages.append("⚠️ [ABORT] Sin precio BNB de Binance.")
            return

        lock_price = p_state.get("lock_price", Decimal("0"))
        if lock_price <= 0:
            self.shared_state.log_messages.append("⚠️ [ABORT] lockPrice no disponible.")
            return

        # Alimentar buffers con el tick actual
        self._update_price_buffers(bnb_price)

        # ── FILTRO 1: Safety Margin Spread (0.1%) ─────────────────────────
        spread_abs    = abs(bnb_price - lock_price)
        safety_margin = lock_price * SAFETY_MARGIN_PCT
        if spread_abs <= safety_margin:
            self.shared_state.log_messages.append(
                f"🛡️ [STRATEGY SKIP] Spread insuficiente: Diff=${spread_abs:.2f} < Min=${safety_margin:.2f}"
            )
            self._momentum_counter = 0
            return

        # ── FILTRO 2: EMA Trend Confirmation ──────────────────────────────
        ema = _calc_ema(self._price_history, EMA_PERIOD)
        if ema is None:
            self.shared_state.log_messages.append(
                f"📊 [STRATEGY SKIP] EMA({EMA_PERIOD}) aún calentando ({len(self._price_history)}/{EMA_PERIOD} ticks)"
            )
            return

        # Tendencia: precio debe estar claramente por encima o debajo de la EMA
        if bnb_price > ema:
            candidate_side = OrderSide.YES   # BULL
        elif bnb_price < ema:
            candidate_side = OrderSide.NO    # BEAR
        else:
            self.shared_state.log_messages.append("📊 [STRATEGY SKIP] Precio == EMA. Mercado lateral.")
            self._momentum_counter = 0
            return

        # ── FILTRO 3: ATR Volatility Window ───────────────────────────────
        atr = _calc_atr(self._tick_highs, self._tick_lows, self._price_history, ATR_PERIOD)
        if atr is None:
            self.shared_state.log_messages.append("📈 [STRATEGY SKIP] ATR calentando...")
            return

        if atr < ATR_MIN_THRESHOLD:
            self.shared_state.log_messages.append(
                f"📉 [STRATEGY SKIP] Volatilidad demasiado baja (ATR={atr:.3f} < {ATR_MIN_THRESHOLD}). No cubre gas."
            )
            self._momentum_counter = 0
            return

        if atr > ATR_MAX_THRESHOLD:
            self.shared_state.log_messages.append(
                f"🌪️ [STRATEGY SKIP] Volatilidad extrema (ATR={atr:.3f} > {ATR_MAX_THRESHOLD}). Riesgo de reversal."
            )
            self._momentum_counter = 0
            return

        # ── FILTRO 4: Momentum Confirmation (3 ticks consecutivos) ────────
        if candidate_side == self._momentum_side:
            self._momentum_counter += 1
        else:
            # Cambió la dirección — reiniciar contador
            self._momentum_counter = 1
            self._momentum_side    = candidate_side

        if self._momentum_counter < MOMENTUM_TICKS_REQ:
            self.shared_state.log_messages.append(
                f"⏳ [MOMENTUM] Confirmando señal {'BULL' if candidate_side == OrderSide.YES else 'BEAR'}: "
                f"{self._momentum_counter}/{MOMENTUM_TICKS_REQ} ticks | EMA={ema:.2f} | ATR={atr:.3f}"
            )
            return

        # ── TODOS LOS FILTROS PASADOS → DISPARO ──────────────────────────
        side      = candidate_side
        direction = "BULL 🟢" if side == OrderSide.YES else "BEAR 🔴"

        signal = SniperSignal(
            asset=SniperAsset.BNB,
            market_id=str(epoch),
            condition_id=str(epoch),
            yes_price=Decimal("0.5"),
            strike_price=float(bnb_price),
            mark_price=float(bnb_price),
            bet_size_usd=Decimal("0"),
            signal_ns=time.time_ns(),
        )

        self.shared_state.log_messages.append(
            f"🎯 [SNIPER FIRE] Epoch: {epoch} | {direction} | "
            f"BNB={bnb_price:.2f} | EMA={ema:.2f} | ATR={atr:.3f} | "
            f"Momentum={self._momentum_counter}✓ | Rem={rem}s"
        )

        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = "FIRING_BSC_TX"
        self.shared_state.inflight_assets.add(SniperAsset.BNB)

        self.placed_bets[epoch] = "BULL" if side == OrderSide.YES else "BEAR"
        await self.execution_queue.put(ExecutionRequest(signal=signal, side=side))
        self.shared_state.last_signal_ns = signal.signal_ns
        self.executed_epochs[epoch] = True

        # Resetear momentum tras disparo
        self._momentum_counter = 0
        self._momentum_side    = None

    # ──────────────────────────────────────────────────────────────────────
    async def _resolve_outcomes(self) -> None:
        """
        Consulta on-chain el resultado de cada apuesta registrada.
        Actualiza cumulative_pnl_usd en shared_state para el panel de la UI.
        """
        if not self.placed_bets:
            return

        current_epoch = self.shared_state.pancake_state.get("epoch", 0)
        if current_epoch == 0:
            return

        if self.executor is None or self.executor.contract is None:
            return

        resolved = []
        for target_epoch, direction in list(self.placed_bets.items()):
            if current_epoch < target_epoch + 2:
                continue  # Ronda aún no cerrada

            try:
                loop = asyncio.get_event_loop()
                round_data = await loop.run_in_executor(
                    None,
                    self.executor.contract.functions.rounds(target_epoch).call
                )
                lock_price_raw  = round_data[4]
                close_price_raw = round_data[5]
                oracle_called   = round_data[13]

                if not oracle_called:
                    continue

                lock_price  = lock_price_raw  / 1e8
                close_price = close_price_raw / 1e8

                won = (close_price > lock_price) if direction == "BULL" else (close_price < lock_price)

                # PnL virtual calculado en USD
                bet_bnb   = self.executor.bet_amount_bnb if self.executor else Decimal("0.0005")
                bnb_price = Decimal(str(self.shared_state.asset_prices.get(SniperAsset.BNB, 700.0)))
                bet_usd   = bet_bnb * bnb_price

                if won:
                    p_state    = self.shared_state.pancake_state
                    mult_key   = "bull_multiplier" if direction == "BULL" else "bear_multiplier"
                    multiplier = p_state.get(mult_key, Decimal("1.9"))
                    payout_usd = bet_usd * multiplier
                    pnl_delta  = payout_usd - bet_usd
                    self.shared_state.cumulative_pnl_usd += pnl_delta

                    msg = (f"🎉 [BALANCE] Epoch {target_epoch}: ¡APUESTA GANADA! 🟢 "
                           f"(Lock: {lock_price:.4f} | Close: {close_price:.4f} | PnL: +${pnl_delta:.2f})")
                    self.shared_state.log_messages.append(msg)
                    print(msg)

                    if self.executor:
                        asyncio.create_task(
                            self.executor.execute_auto_claim(target_epoch),
                            name=f"AutoClaim-{target_epoch}"
                        )
                else:
                    pnl_delta = -bet_usd
                    self.shared_state.cumulative_pnl_usd += pnl_delta

                    msg = (f"💀 [BALANCE] Epoch {target_epoch}: ¡APUESTA PERDIDA! 🔴 "
                           f"(Lock: {lock_price:.4f} | Close: {close_price:.4f} | PnL: -${bet_usd:.2f})")
                    self.shared_state.log_messages.append(msg)
                    print(msg)

                resolved.append(target_epoch)

            except Exception:
                print(f"🚨 [CRITICAL EXCEPTION DETECTED] _resolve_outcomes(epoch={target_epoch}):")
                traceback.print_exc()

        for ep in resolved:
            self.placed_bets.pop(ep, None)

    # ──────────────────────────────────────────────────────────────────────
    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        self.shared_state.sniper_state = SniperState.ARMED
        self.shared_state.latest_status = "ARMED"

    async def stop(self) -> None:
        self._running = False
