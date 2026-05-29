import asyncio
import time
import os
from decimal import Decimal
from web3 import Web3
from modules.pancake_abi import PANCAKESWAP_PREDICTION_ABI
from models import SharedMarketState

PANCAKESWAP_CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"

def _sync_setup_pancake(rpc_url):
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(PANCAKESWAP_CONTRACT),
        abi=PANCAKESWAP_PREDICTION_ABI
    )
    return w3, contract

def _sync_pancake_call(contract):
    epoch = contract.functions.currentEpoch().call()
    round_data = contract.functions.rounds(epoch).call()
    return epoch, round_data

class PancakeSwapMonitor:
    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.rpc_url = os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org/")
        self.w3 = None
        self.contract = None
        self._running = False

    async def start(self) -> None:
        try:
            self._running = True
            self.shared_state.log_messages.append(f"🟢 [PANCAKE] Conectando a {self.rpc_url}")
            
            loop = asyncio.get_event_loop()
            
            # Inicialización en thread secundario
            self.w3, self.contract = await loop.run_in_executor(None, _sync_setup_pancake, self.rpc_url)
            
            while self._running:
                try:
                    epoch, round_data = await loop.run_in_executor(None, _sync_pancake_call, self.contract)
                    
                    lock_timestamp = round_data[2]
                    bull_amount_wei = round_data[9]
                    bear_amount_wei = round_data[10]
                    
                    bull_amt = Decimal(bull_amount_wei) / Decimal(1e18)
                    bear_amt = Decimal(bear_amount_wei) / Decimal(1e18)
                    
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
                    import traceback
                    err_msg = f"⚠️ [PANCAKE] Loop Crash: {e} | {traceback.format_exc()}"
                    self.shared_state.log_messages.append(err_msg)
                    with open("crash_report.log", "a") as f:
                        f.write(err_msg + "\n")
                
                await asyncio.sleep(2)
        except Exception as e:
            import traceback
            err_msg = f"⚠️ [PANCAKE] START FATAL CRASH: {e} | {traceback.format_exc()}"
            self.shared_state.log_messages.append(err_msg)
            with open("crash_report.log", "a") as f:
                f.write(err_msg + "\n")

    async def stop(self) -> None:
        self._running = False
