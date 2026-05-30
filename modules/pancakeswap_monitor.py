import asyncio
import time
import os
import traceback
import urllib.request
import email.utils
from decimal import Decimal
from web3 import Web3
from modules.pancake_abi import PANCAKESWAP_PREDICTION_ABI
from models import SharedMarketState

PANCAKESWAP_CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"

def _calibrate_time_offset():
    """
    Sincronización Atómica: calcula el desfase entre el reloj local y UTC real.
    Extrae el header 'Date' de una petición HTTP ligera a un servidor confiable.
    Retorna el offset en segundos: (tiempo_red - tiempo_local).
    """
    calibration_urls = [
        "https://www.google.com",
        "https://www.cloudflare.com",
    ]
    
    for url in calibration_urls:
        try:
            print(f"⏱️ [CLOCK SYNC] Intentando calibrar contra: {url}")
            before = time.time()
            req = urllib.request.Request(url, method='HEAD')
            resp = urllib.request.urlopen(req, timeout=3)
            after = time.time()
            
            date_header = resp.headers.get('Date')
            if not date_header:
                print(f"⏱️ [CLOCK SYNC] {url} no devolvió header Date. Saltando...")
                continue
            
            # Parsear el header HTTP Date a timestamp Unix
            server_time = email.utils.parsedate_to_datetime(date_header).timestamp()
            
            # Compensar el RTT (Round-Trip Time / 2)
            rtt_half = (after - before) / 2.0
            local_at_server = before + rtt_half
            
            offset = server_time - local_at_server
            print(f"⏱️ [CLOCK SYNC] Resultado de calibración:")
            print(f"  ├ Servidor ({url}): {server_time:.6f}")
            print(f"  ├ Local Mac (corregido RTT): {local_at_server:.6f}")
            print(f"  ├ RTT completo: {(after - before)*1000:.1f}ms")
            print(f"  └ OFFSET CALCULADO: {offset:+.3f}s")
            return offset
        except Exception:
            print(f"🚨 [CLOCK SYNC] Fallo al calibrar contra {url}:")
            traceback.print_exc()
            continue
    
    # Si todo falla, asumir offset 0
    return 0.0

def _sync_setup_pancake(primary_rpc_url):
    WSS_POOL = [
        "wss://bsc-rpc.publicnode.com",
        "wss://entrypoint.blockpi.io/v1/bsc/network",
        "wss://bsc-mainnet.nodereal.io/ws/v1/public"
    ]
    
    # Eliminar duplicados manteniendo orden
    seen = set()
    pool = [x for x in WSS_POOL if not (x in seen or seen.add(x))]

    for rpc in pool:
        try:
            print(f"🌐 [WSS FAILOVER] Intentando conectar a: {rpc}")
            w3 = Web3(Web3.WebsocketProvider(rpc, websocket_timeout=5))
            if not w3.is_connected():
                print(f"🌐 [WSS FAILOVER] {rpc} -> NO CONECTADO. Saltando...")
                continue
            
            try:
                from web3.middleware import ExtraDataToPOAMiddleware
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            except ImportError:
                from web3.middleware import geth_poa_middleware
                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            
            print(f"✅ [WSS FAILOVER] Conectado exitosamente a: {rpc}")
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(PANCAKESWAP_CONTRACT),
                abi=PANCAKESWAP_PREDICTION_ABI
            )
            return w3, contract
        except Exception:
            print(f"🚨 [WSS FAILOVER] Fallo en {rpc}:")
            traceback.print_exc()
            continue
            
    raise Exception("Fallaron todos los WSS del Failover Cluster.")

def _sync_pancake_call(contract):
    epoch = contract.functions.currentEpoch().call()
    round_data = contract.functions.rounds(epoch).call()
    prev_round = contract.functions.rounds(epoch - 1).call() if epoch > 0 else None
    lock_price_raw = prev_round[4] if prev_round else 0
    return epoch, round_data, lock_price_raw

class PancakeSwapMonitor:
    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.rpc_url = os.getenv("BSC_RPC_URL", "https://binance.llamarpc.com")
        self.w3 = None
        self.contract = None
        self._running = False
        self.time_offset = 0.0  # Se calibra en start()

    async def start(self) -> None:
        try:
            self._running = True
            self.shared_state.log_messages.append(f"🟢 [PANCAKE] Conectando a {self.rpc_url}")
            
            loop = asyncio.get_event_loop()
            
            # Calibración Atómica del Reloj al arranque
            self.time_offset = await loop.run_in_executor(None, _calibrate_time_offset)
            self.shared_state.log_messages.append(
                f"⏱️ [CLOCK SYNC] Desfase local calibrado: {self.time_offset:+.2f}s"
            )
            # Compartir el offset con el engine via shared_state
            self.shared_state.time_offset = self.time_offset
            
            # Inicialización en thread secundario de Web3 crudo
            self.w3, self.contract = await loop.run_in_executor(None, _sync_setup_pancake, self.rpc_url)
            
            # WSS POOL para suscripciones crudas
            wss_pool = [
                "wss://bsc-rpc.publicnode.com",
                "wss://entrypoint.blockpi.io/v1/bsc/network",
                "wss://bsc-mainnet.nodereal.io/ws/v1/public"
            ]
            current_ws_idx = 0
            
            import websockets
            import json
            
            while self._running:
                ws_url = wss_pool[current_ws_idx]
                print(f"📡 [WSS STREAM] Conectando a {ws_url} para newHeads...")
                try:
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                        # Enviar suscripción a newHeads
                        sub_msg = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]})
                        await ws.send(sub_msg)
                        print(f"✅ [WSS STREAM] Suscrito a newHeads en {ws_url}. Modo BLOCK-DRIVEN activo.")
                        
                        while self._running:
                            msg = await asyncio.wait_for(ws.recv(), timeout=15.0)
                            data = json.loads(msg)
                            
                            # Ignorar respuesta inicial de suscripción
                            if "result" in data and "params" not in data:
                                continue
                                
                            if "params" in data and "result" in data["params"]:
                                block = data["params"]["result"]
                                block_ts_hex = block.get("timestamp")
                                if block_ts_hex:
                                    block_timestamp = int(block_ts_hex, 16)
                                    
                                    # 2. CALIBRACIÓN AUTOMÁTICA DEL ANCHOR POR BLOQUE
                                    # Al recibir el bloque, calibramos el offset local usando el timestamp on-chain
                                    # Esto elimina cualquier jitter derivado del RTT de la red HTTP.
                                    self.time_offset = block_timestamp - time.time()
                                    self.shared_state.time_offset = self.time_offset
                                    
                                    # Consultar contrato al ritmo del bloque (Block-Driven)
                                    epoch, round_data, lock_price_raw = await loop.run_in_executor(
                                        None, _sync_pancake_call, self.contract
                                    )
                                    
                                    lock_timestamp = round_data[2]
                                    bull_amount_wei = round_data[9]
                                    bear_amount_wei = round_data[10]
                                    
                                    bull_amt = Decimal(bull_amount_wei) / Decimal(1e18)
                                    bear_amt = Decimal(bear_amount_wei) / Decimal(1e18)
                                    
                                    total = bull_amt + bear_amt
                                    bull_mult = (total * Decimal("0.97")) / bull_amt if bull_amt > 0 else Decimal("0")
                                    bear_mult = (total * Decimal("0.97")) / bear_amt if bear_amt > 0 else Decimal("0")
                                    
                                    lock_price = Decimal(lock_price_raw) / Decimal(1e8)
                                    
                                    # El segundero local 'rem' será corregido por la calibración atómica de este bloque
                                    corrected_time = time.time() + self.time_offset
                                    remaining_s = int(lock_timestamp - corrected_time)
                                    
                                    self.shared_state.pancake_state = {
                                        "epoch": epoch,
                                        "lock_timestamp": lock_timestamp,
                                        "remaining_seconds": remaining_s,
                                        "lock_price": lock_price,
                                        "bull_amount": bull_amt,
                                        "bear_amount": bear_amt,
                                        "bull_multiplier": bull_mult,
                                        "bear_multiplier": bear_mult
                                    }
                                    
                except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
                    print(f"⚠️ [WSS STREAM] Conexión caída en {ws_url} ({type(e).__name__}). Reconectando...")
                    current_ws_idx = (current_ws_idx + 1) % len(wss_pool)
                    await asyncio.sleep(1)
                except Exception as e:
                    err_msg = f"⚠️ [WSS STREAM] Error crítico: {e}"
                    self.shared_state.log_messages.append(err_msg)
                    print(f"🚨 [CRITICAL EXCEPTION DETECTED] WSS Stream Loop")
                    traceback.print_exc()
                    current_ws_idx = (current_ws_idx + 1) % len(wss_pool)
                    await asyncio.sleep(1)
        except Exception as e:
            err_msg = f"⚠️ [PANCAKE] START FATAL CRASH: {e}"
            self.shared_state.log_messages.append(err_msg)
            print(f"🚨 [CRITICAL EXCEPTION DETECTED] PancakeSwap Monitor START")
            traceback.print_exc()
            with open("crash_report.log", "a") as f:
                f.write(f"{err_msg} | {traceback.format_exc()}\n")

    async def stop(self) -> None:
        self._running = False
