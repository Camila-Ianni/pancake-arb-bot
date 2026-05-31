"""
DEX Price Monitor — PancakeSwap V2 BNB/USDT
Consulta el precio en tiempo real del par BNB/USDT directamente desde el Router de PancakeSwap V2
usando la función getAmountsOut(). Completamente independiente del módulo de Prediction Market.
"""
import os
import traceback
from decimal import Decimal
from web3 import Web3

# ─── PancakeSwap V2 Router ────────────────────────────────────────────────────
PANCAKE_V2_ROUTER   = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
BNB_TOKEN           = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"   # WBNB
USDT_TOKEN          = "0x55d398326f99059fF775485246999027B3197955"   # BSC-USDT (18 dec)
ONE_BNB_WEI         = 10 ** 18   # 1 BNB en wei

ROUTER_ABI_MINIMAL = [
    {
        "inputs": [
            {"internalType": "uint256", "name": "amountIn",  "type": "uint256"},
            {"internalType": "address[]", "name": "path",    "type": "address[]"},
        ],
        "name": "getAmountsOut",
        "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def build_dex_reader(primary_rpc_url: str):
    """
    Conecta al primer nodo HTTP disponible y devuelve (w3, router_contract).
    """
    RPC_POOL = [
        primary_rpc_url,
        "https://binance.llamarpc.com",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
    ]
    seen = set()
    pool = [x for x in RPC_POOL if not (x in seen or seen.add(x))]

    for rpc in pool:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 3}))
            if not w3.is_connected():
                continue
            try:
                from web3.middleware import ExtraDataToPOAMiddleware
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)

            router = w3.eth.contract(
                address=Web3.to_checksum_address(PANCAKE_V2_ROUTER),
                abi=ROUTER_ABI_MINIMAL,
            )
            print(f"✅ [DEX MONITOR] Router V2 conectado: {rpc}")
            return w3, router
        except Exception:
            continue

    raise RuntimeError("DEX Monitor: Fallaron todos los nodos RPC.")


def fetch_pancake_bnb_price(router_contract) -> Decimal:
    """
    Llama a getAmountsOut(1 BNB → USDT) y devuelve el precio BNB en USD con 18 decimales.
    """
    path = [
        Web3.to_checksum_address(BNB_TOKEN),
        Web3.to_checksum_address(USDT_TOKEN),
    ]
    amounts = router_contract.functions.getAmountsOut(ONE_BNB_WEI, path).call()
    # amounts[1] = USDT recibido por 1 BNB (18 decimales en BSC-USDT)
    usdt_raw = amounts[1]
    return Decimal(usdt_raw) / Decimal(10 ** 18)
