"""
Web3Executor - Ejecución de transacciones on-chain de baja latencia.

Este módulo maneja la firma y envío de transacciones a Polymarket
vía web3.py con optimizaciones para HFT.

ARQUITECTURA:
- Firma offline de transacciones (sin llamadas RPC para signing)
- Gestión de nonces con tracking local
- EIP-1559 para gas pricing dinámico
- Conexión a nodo RPC privado con failover

HOT PATH OPTIMIZATIONS:
- Pre-calcular nonces para reducir round-trips
- Firmar transacciones en thread pool (no bloquear event loop)
- Gas price caching con refresh periódico
- Transaction bundling cuando sea posible
"""

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, Dict, Any, Tuple
from enum import Enum, auto
import logging
from concurrent.futures import ThreadPoolExecutor

from web3 import AsyncWeb3
from web3.contract import AsyncContract
from web3.types import TxParams, TxReceipt, Wei, Nonce
from web3.exceptions import TransactionNotFound, TimeExhausted
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_typing import ChecksumAddress

from ..config import AppConfig, get_config
from ..logging_config import get_logger, get_latency_logger
from ..models import (
    ExecutionParams,
    TransactionResult,
    OrderStatus,
    MarketSide,
)

logger = get_logger(__name__)
latency_logger = get_latency_logger(__name__)


# =============================================================================
# CONSTANTES Y DIRECCIONES DE CONTRATO
# =============================================================================

# Polymarket CTF Exchange (Conditional Token Framework)
# Estas direcciones deben verificarse - pueden cambiar
POLYMARKET_CTf_EXCHANGE_MAINNET = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
POLYMARKET_WALLET_MAINNET = "0xD9DFE096562cE57C1a31c3c1e42e6f88C1F5a5F1"

# Polygon Mainnet RPCs (para failover)
POLYGON_RPC_MAINNET = "https://polygon-rpc.com"

# Gas limits estimados para operaciones de Polymarket
GAS_LIMIT_BUY_SHARES = 150_000
GAS_LIMIT_SELL_SHARES = 150_000
GAS_LIMIT_CANCEL_ORDER = 100_000


class TransactionState(Enum):
    """Estado de una transacción."""
    PENDING = auto()
    SIGNING = auto()
    SUBMITTED = auto()
    CONFIRMED = auto()
    FAILED = auto()
    REVERTED = auto()


@dataclass
class GasEstimate:
    """Estimación de gas para una transacción."""
    gas_limit: int
    max_fee_per_gas: Wei
    max_priority_fee_per_gas: Wei
    estimated_cost_wei: Wei

    @property
    def estimated_cost_gwei(self) -> float:
        """Costo estimado en Gwei."""
        return float(self.estimated_cost_wei) / 1e9

    @property
    def max_fee_gwei(self) -> float:
        """Max fee en Gwei."""
        return float(self.max_fee_per_gas) / 1e9

    @property
    def priority_fee_gwei(self) -> float:
        """Priority fee en Gwei."""
        return float(self.max_priority_fee_per_gas) / 1e9


class Web3Executor:
    """
    Ejecutor de transacciones Web3 optimizado para HFT.

    RESPONSABILIDADES:
    1. Gestión de conexión RPC con failover
    2. Firma offline de transacciones
    3. Gestión de nonces (evitar collisions)
    4. Envío de transacciones con retry
    5. Monitoreo de confirmación

    ARQUITECTURA DE NONCES:
    - Nonce local tracking para evitar esperas RPC
    - Incremento atómico después de cada envío
    - Re-sync con RPC en caso de error
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        dry_run: bool = True,
    ):
        """
        Inicializa el ejecutor Web3.

        Args:
            config: Configuración de la aplicación
            dry_run: Si True, simula transacciones (no envía)
        """
        self.config = config or get_config()
        self.dry_run = dry_run

        # Configuración de wallet
        self.private_key = self.config.wallet.private_key
        self.rpc_url = self.config.wallet.rpc_url
        self.rpc_url_failover = self.config.wallet.rpc_url_failover

        # Configurar cuenta
        Account.enable_unaudited_hdwallet_features()
        self.account = Account.from_key(self.private_key)
        self.address: ChecksumAddress = self.account.address

        logger.info(f"Web3Executor inicializado: address={self.address}, dry_run={dry_run}")

        # Web3 instance (se inicializa en start())
        self.w3: Optional[AsyncWeb3] = None
        self._primary_rpc: bool = True

        # Gestión de nonces
        self._nonce_lock = asyncio.Lock()
        self._current_nonce: Optional[int] = None
        self._pending_nonces: set = set()  # Nonces en vuelo

        # Gas price caching
        self._last_gas_update_ns: int = 0
        self._cached_gas_estimate: Optional[GasEstimate] = None
        self._gas_cache_ttl_ns: int = 2_000_000_000  # 2 segundos

        # Thread pool para signing (CPU-bound)
        self._executor = ThreadPoolExecutor(max_workers=4)

        # Contrato de Polymarket (se inicializa en start())
        self.ctf_contract: Optional[AsyncContract] = None

        # Métricas
        self._transactions_sent = 0
        self._transactions_confirmed = 0
        self._transactions_failed = 0
        self._total_gas_spent_wei = 0

    async def start(self) -> None:
        """Inicializa la conexión Web3."""
        logger.info("Iniciando Web3Executor...")

        # Conectar al nodo RPC
        await self._connect_rpc()

        # Inicializar nonce desde la red
        async with self._nonce_lock:
            self._current_nonce = await self.w3.eth.get_transaction_count(
                self.address,
                "pending"
            )
            logger.info(f"Nonce inicial: {self._current_nonce}")

        # Cargar contrato
        # Nota: Necesitamos el ABI real del contrato CTF
        # Esto es un placeholder
        self.ctf_contract = None  # Se cargará con el ABI real

        logger.info("Web3Executor iniciado")

    async def stop(self) -> None:
        """Detiene el ejecutor y limpia recursos."""
        logger.info("Deteniendo Web3Executor...")

        if self.w3:
            await self.w3.shutdown()

        self._executor.shutdown(wait=False)

        logger.info("Web3Executor detenido")

    async def _connect_rpc(self) -> None:
        """
        Establece conexión con el nodo RPC.

        Intenta el primary primero, luego failover si falla.
        """
        try:
            # Intentar primary
            self.w3 = AsyncWeb3(
                AsyncWeb3.AsyncHTTPProvider(
                    self.rpc_url,
                    request_kwargs={"timeout": self.config.performance.network_timeout_sec}
                )
            )
            self._primary_rpc = True

            # Verificar conexión
            if await self.w3.is_connected():
                chain_id = await self.w3.eth.chain_id
                logger.info(f"Conectado a RPC primary (chain_id={chain_id})")
                return

        except Exception as e:
            logger.warning(f"Fallo en RPC primary: {e}")

        # Intentar failover
        if self.rpc_url_failover:
            try:
                self.w3 = AsyncWeb3(
                    AsyncWeb3.AsyncHTTPProvider(
                        self.rpc_url_failover,
                        request_kwargs={"timeout": self.config.performance.network_timeout_sec}
                    )
                )
                self._primary_rpc = False

                if await self.w3.is_connected():
                    chain_id = await self.w3.eth.chain_id
                    logger.info(f"Conectado a RPC failover (chain_id={chain_id})")
                    return

            except Exception as e:
                logger.error(f"Fallo en RPC failover: {e}")

        raise ConnectionError("No se pudo conectar a ningún RPC")

    async def _get_gas_estimate(self, force_refresh: bool = False) -> GasEstimate:
        """
        Obtiene estimación de gas actualizada.

        HOT PATH: Usa cache para evitar llamadas RPC frecuentes.

        Args:
            force_refresh: Si True, ignora cache y consulta RPC

        Returns:
            GasEstimate con precios actuales
        """
        current_ns = time.time_ns()

        # Usar cache si es válido
        if (
            self._cached_gas_estimate and
            not force_refresh and
            (current_ns - self._last_gas_update_ns) < self._gas_cache_ttl_ns
        ):
            return self._cached_gas_estimate

        # Consultar RPC
        with latency_logger.measure("gas_price_fetch"):
            fee_history = await self.w3.eth.fee_history(
                5,  # últimos 5 bloques
                "latest",
                [50, 75]  # percentiles para priority fee
            )

            # Calcular max_fee_from_base
            base_fees = fee_history["baseFeePerGas"]
            latest_base_fee = base_fees[-1] if base_fees else Wei(0)

            # Priority fee del percentil 75
            priority_fees = fee_history["reward"]
            if priority_fees and priority_fees[-1]:
                avg_priority = sum(priority_fees[-1]) // len(priority_fees[-1])
            else:
                avg_priority = Wei(self.config.trading.priority_fee_gwei * 10**9)

            # EIP-1559: max_fee = base_fee * 2 + priority_fee
            max_fee = Wei(latest_base_fee * 2 + avg_priority)

            # Cap por configuración
            max_configured = Wei(int(self.config.trading.max_gas_price_gwei * 10**9))
            if max_fee > max_configured:
                logger.warning(
                    f"Gas price alto: {max_fee/10**9:.2f} Gwei > {self.config.trading.max_gas_price_gwei} Gwei"
                )
                max_fee = max_configured

            estimate = GasEstimate(
                gas_limit=GAS_LIMIT_BUY_SHARES,
                max_fee_per_gas=max_fee,
                max_priority_fee_per_gas=avg_priority,
                estimated_cost_wei=Wei(GAS_LIMIT_BUY_SHARES * max_fee),
            )

            self._cached_gas_estimate = estimate
            self._last_gas_update_ns = current_ns

            return estimate

    async def _get_next_nonce(self) -> int:
        """
        Obtiene el siguiente nonce disponible.

        HOT PATH: Sin llamadas RPC, usa tracking local.

        Returns:
            Nonce a usar para la próxima transacción
        """
        async with self._nonce_lock:
            if self._current_nonce is None:
                # Sync inicial con RPC
                self._current_nonce = await self.w3.eth.get_transaction_count(
                    self.address, "pending"
                )

            nonce = self._current_nonce
            self._current_nonce += 1
            self._pending_nonces.add(nonce)

            return nonce

    def _release_nonce(self, nonce: int) -> None:
        """Libera un nonce después de confirmación/fallo."""
        self._pending_nonces.discard(nonce)

    async def _sign_transaction(self, tx_params: TxParams) -> bytes:
        """
        Firma una transacción offline.

        Se ejecuta en thread pool para no bloquear el event loop.

        Args:
            tx_params: Parámetros de la transacción

        Returns:
            Transacción firmada (raw bytes)
        """
        loop = asyncio.get_event_loop()

        def _sign_sync() -> bytes:
            signed_tx = self.account.sign_transaction(tx_params)
            return signed_tx.rawTransaction

        with latency_logger.measure("transaction_sign"):
            return await loop.run_in_executor(self._executor, _sign_sync, tx_params)

    async def _send_raw_transaction(self, signed_tx: bytes) -> str:
        """
        Envía una transacción firmada a la red.

        Args:
            signed_tx: Transacción firmada

        Returns:
            Tx hash
        """
        with latency_logger.measure("transaction_send"):
            tx_hash = await self.w3.eth.send_raw_transaction(signed_tx)

        return tx_hash.hex()

    async def _wait_for_confirmation(
        self,
        tx_hash: str,
        timeout_sec: float = 30.0,
    ) -> TxReceipt:
        """
        Espera confirmación de una transacción.

        Args:
            tx_hash: Hash de la transacción
            timeout_sec: Timeout en segundos

        Returns:
            Receipt de la transacción
        """
        try:
            receipt = await asyncio.wait_for(
                self.w3.eth.wait_for_transaction_receipt(
                    tx_hash,
                    poll_latency=0.5,  # Poll cada 500ms
                ),
                timeout=timeout_sec,
            )
            return receipt

        except asyncio.TimeoutError:
            logger.warning(f"Timeout esperando confirmación: {tx_hash}")
            raise
        except TimeExhausted:
            logger.warning(f"TimeExhausted para tx: {tx_hash}")
            raise

    async def execute_buy(
        self,
        market_id: str,
        outcome: str,
        amount: Decimal,
        max_price: Decimal,
    ) -> TransactionResult:
        """
        Ejecuta una orden de compra en Polymarket.

        Args:
            market_id: ID del mercado
            outcome: Outcome a comprar (ej. "YES", "NO")
            amount: Cantidad en USD
            max_price: Precio máximo a pagar (0-1)

        Returns:
            TransactionResult con el resultado
        """
        submitted_at_ns = time.time_ns()

        # En dry run, simular
        if self.dry_run:
            logger.info(
                f"[DRY_RUN] BUY: market={market_id}, outcome={outcome}, "
                f"amount=${amount}, max_price={max_price:.2%}"
            )
            return TransactionResult(
                tx_hash="0x" + "deadbeef" * 8,  # Fake hash
                status=OrderStatus.FILLED,
                gas_used=150000,
                gas_price_gwei=30,
                total_cost_usd=Decimal("0.05"),  # Simulado
                submitted_at_ns=submitted_at_ns,
            )

        try:
            # Obtener gas estimate
            gas_estimate = await self._get_gas_estimate()

            # Check gas price máximo
            if gas_estimate.max_fee_gwei > self.config.trading.max_gas_price_gwei:
                return TransactionResult(
                    tx_hash=None,
                    status=OrderStatus.REJECTED,
                    gas_used=None,
                    gas_price_gwei=None,
                    total_cost_usd=None,
                    error_message=f"Gas price too high: {gas_estimate.max_fee_gwei:.2f} Gwei",
                    submitted_at_ns=submitted_at_ns,
                )

            # Construir transacción
            # NOTA: Esta es una plantilla - necesita el ABI real del contrato CTF
            tx_params = TxParams(
                from_=self.address,
                to=POLYMARKET_CTf_EXCHANGE_MAINNET,  # Contrato CTF
                value=Wei(int(amount * 10**18)),  # Convertir a Wei (asumiendo USDC)
                gas=gas_estimate.gas_limit,
                maxFeePerGas=gas_estimate.max_fee_per_gas,
                maxPriorityFeePerGas=gas_estimate.max_priority_fee_per_gas,
                nonce=await self._get_next_nonce(),
                chainId=await self.w3.eth.chain_id,
            )

            # Firmar transacción
            signed_tx = await self._sign_transaction(tx_params)

            # Enviar
            tx_hash = await self._send_raw_transaction(signed_tx)
            logger.info(f"Transacción enviada: {tx_hash}")

            self._transactions_sent += 1

            # Esperar confirmación
            receipt = await self._wait_for_confirmation(tx_hash)

            # Verificar status
            if receipt["status"] == 1:
                self._transactions_confirmed += 1
                gas_used = receipt["gasUsed"]
                effective_gas = receipt.get("effectiveGasPrice", gas_estimate.max_fee_per_gas)

                # Calcular costo real
                total_cost_wei = gas_used * effective_gas
                self._total_gas_spent_wei += total_cost_wei

                # Liberar nonce
                self._release_nonce(tx_params["nonce"])

                return TransactionResult(
                    tx_hash=tx_hash,
                    status=OrderStatus.FILLED,
                    gas_used=gas_used,
                    gas_price_gwei=int(effective_gas / 10**9),
                    total_cost_usd=Decimal(total_cost_wei / 10**18),  # Aproximado
                    submitted_at_ns=submitted_at_ns,
                    confirmed_at_ns=time.time_ns(),
                )
            else:
                # Transacción revertida
                self._transactions_failed += 1
                self._release_nonce(tx_params["nonce"])

                return TransactionResult(
                    tx_hash=tx_hash,
                    status=OrderStatus.REVERTED,
                    gas_used=receipt["gasUsed"],
                    gas_price_gwei=None,
                    total_cost_usd=None,
                    error_message="Transaction reverted",
                    submitted_at_ns=submitted_at_ns,
                )

        except Exception as e:
            logger.error(f"Error ejecutando compra: {e}", exc_info=True)
            self._transactions_failed += 1

            return TransactionResult(
                tx_hash=None,
                status=OrderStatus.FAILED,
                gas_used=None,
                gas_price_gwei=None,
                total_cost_usd=None,
                error_message=str(e),
                submitted_at_ns=submitted_at_ns,
            )

    async def execute_sell(
        self,
        market_id: str,
        outcome: str,
        amount: Decimal,
        min_price: Decimal,
    ) -> TransactionResult:
        """
        Ejecuta una orden de venta en Polymarket.

        Similar a execute_buy pero para vender shares.
        """
        # Implementación similar a execute_buy
        # Adaptar según la API específica de Polymarket
        logger.warning("execute_sell no implementado completamente")
        return await self.execute_buy(market_id, outcome, amount, min_price)

    async def cancel_order(self, order_id: str) -> TransactionResult:
        """
        Cancela una orden pendiente.

        Args:
            order_id: ID de la orden a cancelar
        """
        logger.warning("cancel_order no implementado completamente")

        if self.dry_run:
            logger.info(f"[DRY_RUN] CANCEL: order_id={order_id}")
            return TransactionResult(
                tx_hash="0x" + "cafe" * 8,
                status=OrderStatus.CANCELLED,
                gas_used=50000,
                gas_price_gwei=30,
                total_cost_usd=Decimal("0.02"),
                submitted_at_ns=time.time_ns(),
            )

        # Implementación real depende del contrato CTF
        raise NotImplementedError("cancel_order no implementado")

    def get_executor_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del ejecutor.

        Returns:
            Dict con métricas clave
        """
        return {
            "transactions_sent": self._transactions_sent,
            "transactions_confirmed": self._transactions_confirmed,
            "transactions_failed": self._transactions_failed,
            "success_rate": (
                self._transactions_confirmed / self._transactions_sent
                if self._transactions_sent > 0 else 0
            ),
            "total_gas_spent_wei": self._total_gas_spent_wei,
            "pending_nonces": len(self._pending_nonces),
            "current_nonce": self._current_nonce,
            "using_primary_rpc": self._primary_rpc,
            "dry_run": self.dry_run,
        }
