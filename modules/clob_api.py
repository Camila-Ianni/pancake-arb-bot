"""
CLOB Client API wrapper using the official py-clob-client SDK.
Handles initialization, API Rate Limiting, and Order Book fetches.
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Any
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType as SDKOrderType, ApiCreds
from py_clob_client.exceptions import PolyApiException

logger = logging.getLogger(__name__)

class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class PolymarketClobClient:
    """Wrapper para el ClobClient oficial de Polymarket."""
    
    def __init__(self, private_key: str, host: str = "https://clob.polymarket.com", chain_id: int = 137, dry_run: bool = True):
        self.private_key = private_key
        self.host = host
        self.chain_id = chain_id
        self.dry_run = dry_run
        
        # Cliente Oficial del SDK
        self.client = ClobClient(
            host=self.host,
            key=self.private_key,
            chain_id=self.chain_id,
        )
        self._is_initialized = False

    async def initialize(self):
        """Inicializa la sesión nativa del SDK y crea/deriva credenciales L1 -> L2."""
        if self._is_initialized:
            return
            
        logger.info("[AUTH] Inicializando sesión con py-clob-client...")
        # create_or_derive_api_creds handles EIP-712 signing internally!
        try:
            creds: ApiCreds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            self._is_initialized = True
            logger.info("[AUTH] Sesión persistente establecida mediante el SDK oficial.")
        except Exception as e:
            logger.error(f"Error autenticando con el SDK: {e}")
            raise

    async def close(self):
        """No es necesario cerrar explícitamente en el SDK sincrónico, pero mantenemos compatibilidad."""
        pass

    async def get_order_book(self, market_id: str) -> Any:
        """Obtiene el OrderBook directamente desde el CLOB via SDK."""
        # Fix del Simulador: Forzar Orderbook falso si es el mercado simulado
        if market_id == "0xSIMULATED_CONDITION_ID" or (isinstance(market_id, dict) and market_id.get("condition_id") == "0xSIMULATED_CONDITION_ID"):
            from dataclasses import dataclass
            from typing import List

            @dataclass
            class FakeLevel:
                price: str
                size: str

            @dataclass
            class FakeOrderBook:
                bids: List[FakeLevel]
                asks: List[FakeLevel]

            # Forzamos un spread que resulte en un ROI alto (ej. bid 0.40, ask 0.60)
            # Para arbitraje de libro, un spread amplio es mejor.
            # Pero el bot busca "Maker" orders, así que simulamos un spread saludable.
            return FakeOrderBook(
                bids=[FakeLevel(price="0.45", size="1000")],
                asks=[FakeLevel(price="0.55", size="1000")]
            )

        return await self._execute_with_backoff(self.client.get_order_book, market_id)

    async def place_order(self, market_id: str, side: Side, price: float, size: float, order_type: SDKOrderType = SDKOrderType.GTC) -> dict:
        """Coloca una orden utilizando el SDK oficial."""
        if self.dry_run:
            logger.info(f"🔮 [DRY RUN - SDK] Orden {order_type} {side.value} | Precio: {price} | Tamaño: {size}")
            return {"status": "simulated", "order_id": f"sim_{time.time_ns()}", "success": True}

        order_args = OrderArgs(
            price=price,
            size=size,
            side=side.value,
            token_id=market_id,
        )

        logger.info(f"📝 [ORDER] Creando orden SDK {order_type} {side.value} @ {price} x {size}")
        
        # client.create_order signs and submits the order
        return await self._execute_with_backoff(
            self.client.create_order,
            order_args,
            {"orderType": order_type}
        )

    async def cancel_order(self, order_id: str) -> dict:
        if self.dry_run:
            logger.info(f"🔮 [DRY RUN - SDK] Cancelando orden {order_id}")
            return {"status": "simulated_cancel", "success": True}
        return await self._execute_with_backoff(self.client.cancel, order_id)

    async def _execute_with_backoff(self, func, *args, **kwargs):
        """Maneja Errores del SDK y Rate Limiting 429."""
        max_retries = 5
        retries = 0
        
        loop = asyncio.get_event_loop()

        while retries < max_retries:
            try:
                # El SDK de Python suele ser sincrónico bajo el capó (usa requests)
                result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                return result
                
            except PolyApiException as e:
                # Capturar excepciones nativas del SDK
                if e.status_code == 429:
                    delay = 1.0 * (2 ** retries)
                    logger.warning(f"⚠️ [Rate Limited SDK - 429] Backoff de {delay}s...")
                    await asyncio.sleep(delay)
                    retries += 1
                    continue
                elif e.status_code in [401, 403]:
                    logger.warning(f"⚠️ [AUTH] Error {e.status_code}. Renovando sesión SDK...")
                    self._is_initialized = False
                    await self.initialize()
                    retries += 1
                    continue
                else:
                    logger.error(f"❌ [API Error] SDK devolvió código {e.status_code}: {e}")
                    raise e
                    
            except Exception as e:
                logger.error(f"❌ [Error Inesperado] {e}")
                retries += 1
                await asyncio.sleep(1)

        raise RuntimeError("Máximo de reintentos excedido en llamadas al SDK py-clob-client.")
