"""
Arbitrage Engine — Spot-to-DEX (Binance vs PancakeSwap V2)

Estrategia:
  - Monitorea BNB/USDT en Binance (vía WebSocket, ya activo en CryptoFeed)
  - Monitorea BNB/USDT en PancakeSwap V2 Router (vía getAmountsOut)
  - Dispara cuando el spread bruto - costos de tx > PROFIT_THRESHOLD_PCT (0.2%)
  - Gestión de riesgo: bet dinámico del 2% del capital (interés compuesto)
  - Kill Switch automático si PnL acumulado cae bajo KILL_SWITCH_PNL_USD
"""
import asyncio
import time
import traceback
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from modules.web3_executor import Web3Executor

from models import (
    SniperAsset, SniperState, ExecutionRequest, ExecutionResult,
    SniperSignal, OrderSide, SharedMarketState, RuntimeConfig,
)

# ═══════════════════════════════════════════════════════════════════════════
# PARÁMETROS DE ARBITRAJE — ajusta estos para calibrar la estrategia
# ═══════════════════════════════════════════════════════════════════════════
PROFIT_THRESHOLD_PCT  = Decimal("0.002")   # 0.20% — margen mínimo neto para disparar
GAS_COST_USD_EST      = Decimal("0.08")    # ~$0.08 gas en BSC por tx (conservador)
SLIPPAGE_PCT          = Decimal("0.001")   # 0.10% slippage estimado en swap
PANCAKE_FEE_PCT       = Decimal("0.0025")  # 0.25% fee del LP de PancakeSwap V2
KILL_SWITCH_PNL_USD   = Decimal("-1.00")   # Límite de pérdida acumulada en simulación
DEX_POLL_INTERVAL_S   = 3.0               # Cada cuántos segundos consultar el DEX (≈1 bloque BSC)
COOLDOWN_AFTER_FIRE_S = 30.0              # Cooldown entre disparos para evitar over-trading


def calculate_spread(binance_price: Decimal, pancake_price: Decimal) -> dict:
    """
    Compara el precio Binance (spot) vs PancakeSwap DEX y calcula:
      - spread_pct: diferencia porcentual bruta
      - net_profit_pct: spread_pct - costos totales (gas + slippage + fee LP)
      - direction: 'BUY_PANCAKE' si Pancake más barato, 'SELL_PANCAKE' si más caro
      - viable: True si net_profit_pct > PROFIT_THRESHOLD_PCT
    """
    if binance_price <= 0 or pancake_price <= 0:
        return {"viable": False, "spread_pct": Decimal("0"), "net_profit_pct": Decimal("0"), "direction": None}

    # Spread bruto como fracción del precio de referencia (Binance)
    spread_abs = binance_price - pancake_price
    spread_pct = spread_abs / binance_price  # positivo → Pancake más barato (Buy opp)

    direction = "BUY_PANCAKE" if spread_pct > 0 else "SELL_PANCAKE"
    spread_pct_abs = abs(spread_pct)

    # Costos totales estimados como fracción del capital
    # Gas se normaliza sobre el capital apostado para comparar correctamente
    # (se resuelve al momento de ejecutar con la apuesta dinámica)
    total_cost_pct = SLIPPAGE_PCT + PANCAKE_FEE_PCT  # gas se descuenta por separado en USD

    net_profit_pct = spread_pct_abs - total_cost_pct

    return {
        "viable": net_profit_pct > PROFIT_THRESHOLD_PCT,
        "spread_pct": spread_pct_abs,
        "net_profit_pct": net_profit_pct,
        "direction": direction,
        "spread_usd": abs(spread_abs),
    }


class ArbitrageEngine:
    def __init__(
        self,
        execution_queue: "asyncio.Queue[ExecutionRequest]",
        shared_state: SharedMarketState,
        executor: Optional["Web3Executor"] = None,
        runtime_cfg: Optional[RuntimeConfig] = None,
    ) -> None:
        self.execution_queue = execution_queue
        self.shared_state    = shared_state
        self.executor        = executor
        self.runtime_cfg     = runtime_cfg or RuntimeConfig()
        self._running        = False

        self._last_fire_ts: float = 0.0          # timestamp del último disparo
        self._dex_price: Decimal  = Decimal("0") # precio DEX actualizado por el poller
        self._spread_info: dict   = {}           # resultado del último calculate_spread

        # Estado de oportunidad — leído por el panel de UI
        self.arb_status: str       = "WARMING_UP"
        self.binance_price: Decimal = Decimal("0")
        self.pancake_price: Decimal = Decimal("0")
        self.current_spread_pct: Decimal = Decimal("0")
        self.net_profit_pct: Decimal     = Decimal("0")

    # ──────────────────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._running = True
        self.shared_state.log_messages.append(
            "🟢 [ARB ENGINE] Iniciado — Estrategia: Spot-to-DEX Arbitrage (Binance vs PancakeSwap V2)"
        )
        asyncio.create_task(self._dex_price_loop(), name="DexPricePoller")
        asyncio.create_task(self._arb_loop(), name="ArbEngine")

    # ──────────────────────────────────────────────────────────────────────
    async def _dex_price_loop(self) -> None:
        """Consulta el precio DEX cada ~3 s (1 bloque BSC) en thread secundario."""
        loop = asyncio.get_event_loop()

        # Esperar a que el executor tenga el contrato listo
        for _ in range(30):
            if self.executor and self.executor._dex_router:
                break
            await asyncio.sleep(1)
        else:
            self.shared_state.log_messages.append("⚠️ [DEX POLLER] Router V2 no disponible. Poller detenido.")
            return

        while self._running:
            try:
                from modules.dex_monitor import fetch_pancake_bnb_price
                price = await loop.run_in_executor(
                    None, fetch_pancake_bnb_price, self.executor._dex_router
                )
                self._dex_price  = price
                self.pancake_price = price
            except Exception as e:
                pass  # Fallo silencioso — el panel muestra 0 si no hay dato
            await asyncio.sleep(DEX_POLL_INTERVAL_S)

    # ──────────────────────────────────────────────────────────────────────
    async def _arb_loop(self) -> None:
        """Evalúa la oportunidad de arbitraje en cada tick."""
        while self._running:
            try:
                # 1. Kill Switch de PnL
                if self.shared_state.cumulative_pnl_usd <= KILL_SWITCH_PNL_USD:
                    if not self.shared_state.kill_switch:
                        self.shared_state.kill_switch = True
                        self.shared_state.log_messages.append(
                            f"🚨 [KILL SWITCH] PnL = ${self.shared_state.cumulative_pnl_usd:.2f} — "
                            "Límite crítico. Bot congelado."
                        )
                if self.shared_state.kill_switch:
                    self.arb_status = "KILL_SWITCH_ON"
                    await asyncio.sleep(1)
                    continue

                # 2. Leer precio Binance del estado compartido
                bnb_binance = Decimal(str(
                    self.shared_state.asset_prices.get(SniperAsset.BNB, 0.0)
                ))
                self.binance_price = bnb_binance

                # 3. Leer precio DEX (actualizado por _dex_price_loop)
                bnb_pancake = self._dex_price

                # 4. Calcular spread
                if bnb_binance > 0 and bnb_pancake > 0:
                    info = calculate_spread(bnb_binance, bnb_pancake)
                    self._spread_info        = info
                    self.current_spread_pct  = info["spread_pct"]
                    self.net_profit_pct      = info["net_profit_pct"]

                    if info["viable"]:
                        self.arb_status = "OPPORTUNITY_FOUND"
                        await self._try_fire(bnb_binance, bnb_pancake, info)
                    else:
                        self.arb_status = "WAITING_FOR_SPREAD"
                else:
                    self.arb_status = "WARMING_UP"

            except Exception as e:
                print(f"\n🚨 [CRITICAL] ArbLoop: {e}")
                traceback.print_exc()
                self.shared_state.log_messages.append(f"⚠️ [ARB ENGINE] Error: {e}")

            await asyncio.sleep(0.5)

    # ──────────────────────────────────────────────────────────────────────
    async def _try_fire(self, binance_price: Decimal, pancake_price: Decimal, info: dict) -> None:
        """
        Valida el cooldown y los requisitos de capital antes de disparar.
        Aplica el bet dinámico del 2% del capital.
        """
        now = time.time()
        if now - self._last_fire_ts < COOLDOWN_AFTER_FIRE_S:
            remaining_cd = int(COOLDOWN_AFTER_FIRE_S - (now - self._last_fire_ts))
            self.arb_status = f"COOLDOWN ({remaining_cd}s)"
            return

        if self.shared_state.sniper_state == SniperState.FIRING:
            return

        # Apuesta dinámica: 2% del capital actual
        capital_usd = self.shared_state.initial_capital_usd + self.shared_state.cumulative_pnl_usd
        stake_usd   = max(capital_usd * Decimal("0.02"), Decimal("0.01"))

        # Validar que el gas no se lleve toda la ganancia
        expected_gross_usd = stake_usd * info["net_profit_pct"]
        if expected_gross_usd <= GAS_COST_USD_EST:
            self.shared_state.log_messages.append(
                f"⛽ [ARB SKIP] Ganancia bruta ${expected_gross_usd:.4f} ≤ gas estimado ${GAS_COST_USD_EST}. Apuesta insuficiente."
            )
            return

        direction = info["direction"]
        side      = OrderSide.YES if direction == "BUY_PANCAKE" else OrderSide.NO

        signal = SniperSignal(
            asset=SniperAsset.BNB,
            market_id=f"ARB-{int(now)}",
            condition_id=f"ARB-{int(now)}",
            yes_price=Decimal("0.5"),
            strike_price=float(pancake_price),
            mark_price=float(binance_price),
            bet_size_usd=stake_usd,
            signal_ns=time.time_ns(),
        )

        net_pct_display = float(info["net_profit_pct"]) * 100

        self.shared_state.log_messages.append(
            f"🎯 [ARB FIRE] {direction} | "
            f"Binance=${binance_price:.2f} | DEX=${pancake_price:.2f} | "
            f"Spread={float(info['spread_pct'])*100:.3f}% | "
            f"Net={net_pct_display:.3f}% | Stake=${stake_usd:.3f}"
        )

        self.shared_state.sniper_state = SniperState.FIRING
        self.shared_state.latest_status = f"FIRING: {direction}"
        self.shared_state.inflight_assets.add(SniperAsset.BNB)
        self._last_fire_ts = now

        await self.execution_queue.put(ExecutionRequest(signal=signal, side=side))
        self.shared_state.last_signal_ns = signal.signal_ns

    # ──────────────────────────────────────────────────────────────────────
    def on_execution_result(self, asset: SniperAsset, pnl_delta: Decimal) -> None:
        self.shared_state.cumulative_pnl_usd += pnl_delta
        self.shared_state.inflight_assets.discard(asset)
        self.shared_state.sniper_state = SniperState.ARMED
        self.shared_state.latest_status = "SCANNING_SPREADS"

    async def stop(self) -> None:
        self._running = False
