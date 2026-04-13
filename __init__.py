"""
Polymarket Arbitrage Bot - HFT Latency Arbitrage System.

Un sistema de trading de alta frecuencia para explotar la ventana
entre eventos climáticos reales y la actualización del oráculo
de Polymarket.

Arquitectura:
- FastWeatherFeed: Feed climático de baja latencia
- PolymarketMonitor: Monitor de mercado vía WebSocket
- ArbitrageEngine: Motor de decisión y comparación
- RiskManager: Circuit breaker y gestión de riesgo
- Web3Executor: Ejecución de transacciones on-chain

Uso básico:
    from polymarket_arb import BotOrchestrator
    from polymarket_arb.config import get_config

    config = get_config()
    bot = BotOrchestrator(config)
    # await bot.start()  # En un event loop asyncio
"""

__version__ = "0.1.0"
__author__ = "Quant Developer"

from .config import get_config, AppConfig
from .main import BotOrchestrator

__all__ = [
    "get_config",
    "AppConfig",
    "BotOrchestrator",
    "__version__",
]
