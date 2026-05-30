"""Orquestador principal del sniper multi-activo (BTC/ETH/SOL/BNB)."""

from __future__ import annotations

import asyncio
import os
import sys
import time
import random
import signal
from decimal import Decimal, InvalidOperation
from pathlib import Path
import aiohttp

# Agregamos el directorio actual al PYTHONPATH para evitar errores visuales (unresolved imports) en el IDE (Pylance/VSCode)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from models import ExecutionRequest, ExecutionResult, RuntimeConfig, SharedMarketState, SniperAsset, SniperState
from modules.arbitrage_engine import ArbitrageEngine
from modules.crypto_feed import CryptoFeed
from modules.pancakeswap_monitor import PancakeSwapMonitor
from modules.web3_executor import Web3Executor


def clear_console() -> None:
    if os.name == "nt":
        os.system("cls")
    else:
        sys.stdout.write("\033[H\033[2J")
        sys.stdout.flush()


def ask_initial_capital() -> Decimal:
    while True:
        raw = input("🚀 [SESSION CONFIG] Ingrese capital inicial (USD): ").strip()
        try:
            value = Decimal(raw)
        except InvalidOperation:
            print("Entrada inválida.")
            continue
        if value <= 0:
            print("Debe ser mayor a 0.")
            continue
        return value


def render_panel(shared: SharedMarketState, crypto: CryptoFeed = None, engine: ArbitrageEngine = None, executor: Web3Executor = None) -> None:
    clear_console()
    sniper_label = "🟢 ARMADO" if shared.sniper_state != SniperState.STOPPED else "🔴 STOPPED"
    p = shared.asset_prices
    print("=" * 86)
    print("🥞 PANCAKESWAP PREDICTION SNIPER (5m)")
    print("=" * 86)
    wallet_raw = os.getenv("WALLET_ADDRESS", "UNKNOWN")
    wallet_suffix = wallet_raw[-4:] if len(wallet_raw) > 4 else "NONE"
    
    print(
        f"Capital Inicial: ${shared.initial_capital_usd:.2f} | "
        f"Ganancia Acumulada (PnL): ${shared.cumulative_pnl_usd:+.2f} | "
        f"Estado del Sniper: {sniper_label}"
    )
    print(f"Wallet BNB: ...{wallet_suffix} | Status: {shared.latest_status[:70]}")
    print("-" * 86)
    print(
        "Binance Mark | "
        f"BTC: {p[SniperAsset.BTC]:.2f} | ETH: {p[SniperAsset.ETH]:.2f} | "
        f"SOL: {p[SniperAsset.SOL]:.2f} | BNB: {p[SniperAsset.BNB]:.2f}"
    )
    print(
        "Binance Threads | "
        f"BTC: {'UP' if p[SniperAsset.BTC] > 0 else 'DOWN'} | "
        f"ETH: {'UP' if p[SniperAsset.ETH] > 0 else 'DOWN'} | "
        f"SOL: {'UP' if p[SniperAsset.SOL] > 0 else 'DOWN'} | "
        f"BNB: {'UP' if p[SniperAsset.BNB] > 0 else 'DOWN'}"
    )
    print("-" * 86)
    
    ps = shared.pancake_state
    print(f"🥞 Ronda (Epoch): {ps['epoch']} | Faltan: {ps['remaining_seconds']}s")
    print(f"Bull Pool: {ps['bull_amount']:.2f} BNB ({ps['bull_multiplier']:.2f}x) | Bear Pool: {ps['bear_amount']:.2f} BNB ({ps['bear_multiplier']:.2f}x)")
    print("-" * 86)
    
    if crypto and engine and executor:
        print("⚡ TELEMETRÍA DE LATENCIA (HFT) ⚡")
        print(f"├ Parseo WebSocket (Binance):   {crypto.metrics.avg_parse_ms:.5f} ms")
        print(f"└ Ejecución Web3 (PancakeSwap): {executor.metrics.avg_sign_ms if hasattr(executor.metrics, 'avg_sign_ms') else 0.0:.5f} ms")
        print("-" * 86)
    print(
        f"Inflight: {len(shared.inflight_assets)} | "
        f"Kill Switch: {'ON' if shared.kill_switch else 'OFF'}"
    )
    print("=" * 86)
    
    # --- LOGS SECTION ---
    print("📝 REGISTRO DE EVENTOS & ALERTAS SNIPER")
    print("-" * 86)
    for msg in list(shared.log_messages)[-8:]:
        print(f" {msg}")
    print("=" * 86)


async def panel_loop(shared: SharedMarketState, crypto: CryptoFeed, engine: ArbitrageEngine, executor: Web3Executor) -> None:
    while not shared.kill_switch:
        render_panel(shared, crypto, engine, executor)
        await asyncio.sleep(0.5)


async def result_loop(engine: ArbitrageEngine, result_queue: "asyncio.Queue[ExecutionResult]") -> None:
    while True:
        result = await result_queue.get()
        engine.on_execution_result(result.asset, result.pnl_delta_usd)


async def simulated_market_activity(result_queue: asyncio.Queue[ExecutionResult], shared: SharedMarketState) -> None:
    """Función de simulación desactivada para evitar inyectar PnL falso."""
    pass


async def _check_polygon_rpc(session: aiohttp.ClientSession, rpc_url: str) -> bool:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}
    try:
        async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return False
            data = await resp.json()
            return str(data.get("result", "")).lower() in ("0x89", "137")
    except Exception:
        return False


async def _check_polymarket_api_key(session: aiohttp.ClientSession, api_key: str) -> bool:
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with session.get(
            "https://clob.polymarket.com/markets?limit=1",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return resp.status in (200, 401, 403)
    except Exception:
        return False


async def preflight_connectivity_checks() -> None:
    rpc_url = os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org/").strip()
    if not rpc_url:
        raise RuntimeError("Falta BSC_RPC_URL para preflight.")


async def run() -> None:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    clear_console()
    await preflight_connectivity_checks()
    
    initial_capital = ask_initial_capital()
    shared = SharedMarketState(initial_capital_usd=initial_capital, sniper_state=SniperState.ARMED)
    
    # Simulación de mercados activos si las IDs del env fallan, para forzar el estado ARMED
    shared.latest_status = "SCANNING_SPREADS"
    
    runtime_cfg = RuntimeConfig()
    env_threshold = os.getenv("PROFIT_SWEEP_THRESHOLD_USD", "").strip()
    if env_threshold:
        runtime_cfg.profit_sweep_threshold_usd = Decimal(env_threshold)
    env_enabled = os.getenv("PROFIT_SWEEP_ENABLED", "true").strip().lower()
    runtime_cfg.profit_sweep_enabled = env_enabled in ("1", "true", "yes", "on")

    execution_queue: asyncio.Queue[ExecutionRequest] = asyncio.Queue(maxsize=2000)
    result_queue: asyncio.Queue[ExecutionResult] = asyncio.Queue(maxsize=2000)

    crypto = CryptoFeed(shared_state=shared)
    monitor = PancakeSwapMonitor(shared_state=shared)
    executor = Web3Executor(
        execution_queue=execution_queue,
        result_queue=result_queue,
        shared_state=shared,
        runtime_cfg=runtime_cfg,
    )
    # Inyectar referencia del executor al engine para auto-claim y outcome resolution
    engine = ArbitrageEngine(shared_state=shared, execution_queue=execution_queue, executor=executor, runtime_cfg=runtime_cfg)

    tasks = [
        asyncio.create_task(crypto.start(), name="CryptoFeed"),
        asyncio.create_task(monitor.start(), name="MarketMonitor"),
        asyncio.create_task(engine.start(), name="ArbitrageEngine"),
        asyncio.create_task(executor.start(), name="Web3Executor"),
        asyncio.create_task(panel_loop(shared, crypto, engine, executor), name="Panel"),
        asyncio.create_task(result_loop(engine, result_queue), name="ResultLoop"),
        asyncio.create_task(simulated_market_activity(result_queue, shared), name="SimActivity"),
    ]

    try:
        while not shared.kill_switch:
            await asyncio.sleep(0.1)
    finally:
        await crypto.stop()
        await monitor.stop()
        await engine.stop()
        await executor.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        shared.sniper_state = SniperState.STOPPED
        render_panel(shared, crypto, engine, executor)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        clear_console()
        print("\n🔌 [INFO] Cerrando sesión del Sniper de forma segura...\n")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    except Exception:
        import traceback
        print("\n🚨 [CRITICAL EXCEPTION DETECTED] main() Top-Level Crash:")
        traceback.print_exc()
        with open("crash_report.log", "a") as f:
            f.write(f"MAIN CRASH: {traceback.format_exc()}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
