import asyncio
import time
import os
from decimal import Decimal
from web3 import AsyncWeb3
from modules.pancake_abi import PANCAKESWAP_PREDICTION_ABI
from models import SharedMarketState

PANCAKESWAP_CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"

class PancakeSwapMonitor:
    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.rpc_url = os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org/")
        self.w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(self.rpc_url))
        self.contract = self.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(PANCAKESWAP_CONTRACT),
            abi=PANCAKESWAP_PREDICTION_ABI
        )
        self._running = False

    async def start(self) -> None:
        self._running = True
        self.shared_state.log_messages.append(f"🟢 [PANCAKE] Conectando a {self.rpc_url}")
        
        while self._running:
            try:
                epoch = await self.contract.functions.currentEpoch().call()
                round_data = await self.contract.functions.rounds(epoch).call()
                
                # round_data tuple match con ABI
                # 0: epoch, 1: start, 2: lock, 3: close, 4: lockPrice, 5: closePrice
                # 8: totalAmount, 9: bullAmount, 10: bearAmount
                
                lock_timestamp = round_data[2]
                bull_amount_wei = round_data[9]
                bear_amount_wei = round_data[10]
                
                bull_amt = Decimal(bull_amount_wei) / Decimal(1e18)
                bear_amt = Decimal(bear_amount_wei) / Decimal(1e18)
                
                # Calc multipliers (approx, ignores 3% treasury fee for pure display, or we can include 0.97)
                total = bull_amt + bear_amt
                bull_mult = (total * Decimal("0.97")) / bull_amt if bull_amt > 0 else Decimal("0")
                bear_mult = (total * Decimal("0.97")) / bear_amt if bear_amt > 0 else Decimal("0")
                
                remaining = int(lock_timestamp - time.time())
                
                self.shared_state.pancake_state = {
                    "epoch": epoch,
                    "lock_timestamp": lock_timestamp,
                    "remaining_seconds": remaining,
                    "bull_amount": bull_amt,
                    "bear_amount": bear_amt,
                    "bull_multiplier": bull_mult,
                    "bear_multiplier": bear_mult
                }
                
            except Exception as e:
                self.shared_state.log_messages.append(f"⚠️ [PANCAKE] Error al leer contrato: {e}")
            
            # Polling rápido pero respetuoso
            await asyncio.sleep(2)

    async def stop(self) -> None:
        self._running = False
