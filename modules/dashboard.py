import asyncio
import os
from collections import deque
from typing import Deque

class DashboardRenderer:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.events: Deque[str] = deque(maxlen=6)
        self._running = False
        self._task = None

    def add_event(self, event_msg: str):
        # Aseguramos que el evento encaje visualmente
        self.events.append(event_msg)

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self.render_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def render_loop(self):
        import logging
        logger = logging.getLogger("polymarket_arb.dashboard")
        while self._running:
            await asyncio.sleep(0.1)
            try:
                self._render()
            except Exception as e:
                logger.error(f"Crash en render_loop: {e}", exc_info=True)
            await asyncio.sleep(0.4)

    def _render(self):
        os.system('clear' if os.name == 'posix' else 'cls')
        
        # Extraer estados
        dry_run = getattr(self.orchestrator.config.execution, 'dry_run', True) if hasattr(self.orchestrator, 'config') else True
        dry_run_text = "ON" if dry_run else "OFF"
        
        engine_state = "INICIALIZANDO"
        pnl = "0.00"
        
        if hasattr(self.orchestrator, 'arbitrage_engine') and self.orchestrator.arbitrage_engine:
            engine_state_obj = getattr(self.orchestrator.arbitrage_engine, 'state', None)
            engine_state = getattr(engine_state_obj, 'name', "INICIALIZANDO")
            
        if hasattr(self.orchestrator, 'risk_manager') and self.orchestrator.risk_manager:
            pnl_val = getattr(self.orchestrator.risk_manager, '_total_pnl_usd', 0.0)
            pnl = f"{pnl_val:.2f}"
            
        # Simulación de latencias (podrían leerse de metrics en el futuro)
        parse_ms = 0.12
        eval_ms = 0.35
        eip_ms = 2.10

        width = 80
        
        lines = []
        lines.append("╔" + "═" * (width - 2) + "╗")
        lines.append("║" + " 🔮 POLYMARKET HFT TACTICAL DASHBOARD ".center(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Header Info
        header = f"  [DRY_RUN: {dry_run_text}] | Kill Switch: OFF | State: {engine_state}"
        lines.append("║" + header.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Market Status variables
        import time
        round_info = "1"
        t_minus = "N/A"
        money_used = "0.00"
        btc_price = "0.00"
        
        if hasattr(self.orchestrator, 'arbitrage_engine') and self.orchestrator.arbitrage_engine:
            metrics = getattr(self.orchestrator.arbitrage_engine, '_metrics', None)
            if metrics:
                round_info = str(metrics.opportunities_executed + 1)
                
        if hasattr(self.orchestrator, 'execution_engine') and self.orchestrator.execution_engine:
            ee = self.orchestrator.execution_engine
            if hasattr(ee, 'order_size_usdc'):
                money_used = f"{ee.order_size_usdc:.2f}"
            if hasattr(ee, 'current_ctx') and ee.current_ctx:
                ctx = ee.current_ctx
                remaining = max(0, ctx.close_ts - time.time())
                t_minus = f"{int(remaining)}s"
                btc_price = f"{ctx.last_price:,.2f}"

        # Métricas principales
        metrics1 = f"  Wallet PnL : ${pnl} | Execution Mode: FOK (Maker)"
        metrics2 = f"  Asset      : BTC/USD 5m | Strategy: Front-Running EIP-712"
        lines.append("║" + metrics1.ljust(width - 2) + "║")
        lines.append("║" + metrics2.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Dinámica de Ronda
        dyn1 = f"  Round      : #{round_info} | T-Minus: {t_minus}"
        dyn2 = f"  BTC Price  : ${btc_price} | Capital: ${money_used} USDC"
        lines.append("║" + dyn1.ljust(width - 2) + "║")
        lines.append("║" + dyn2.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Telemetría
        telemetry = f"  [LATENCY] Parsing: {parse_ms}ms | Eval: {eval_ms}ms | EIP-712: {eip_ms}ms"
        lines.append("║" + telemetry.ljust(width - 2) + "║")
        lines.append("╠" + "═" * (width - 2) + "╣")
        
        # Registros de eventos
        lines.append("║" + " [RECENT EVENTS] ".center(width - 2) + "║")
        
        current_events = list(self.events)
        while len(current_events) < 6:
            current_events.append("")
            
        for ev in current_events:
            # Ljust to width-4 to account for "  " padding and borders
            lines.append("║  " + ev.ljust(width - 4) + "║")
            
        lines.append("╚" + "═" * (width - 2) + "╝")
        
        # Print atomicamente
        print("\n".join(lines))
