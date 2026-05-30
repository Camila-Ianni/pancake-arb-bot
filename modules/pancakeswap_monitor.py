import asyncio
import time
import os
import traceback
import urllib.request
import email.utils
import socket
import json
from decimal import Decimal

import websockets
import websockets.exceptions

from web3 import Web3
from modules.pancake_abi import PANCAKESWAP_PREDICTION_ABI
from models import SharedMarketState

PANCAKESWAP_CONTRACT = "0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA"

# ─── Throttle: mínimo de segundos entre logs del mismo tipo ───────────────
_WSS_ERROR_THROTTLE_S = 10.0   # 1 aviso de WSS por cada 10 s máximo
_SKIP_LOG_THROTTLE_S  =  5.0   # 1 aviso de STRATEGY SKIP por cada 5 s

def _calibrate_time_offset() -> float:
    """
    Sincronización Atómica: calcula el desfase entre el reloj local y UTC real.
    Retorna el offset en segundos: (tiempo_red - tiempo_local).
    """
    calibration_urls = [
        "https://www.google.com",
        "https://www.cloudflare.com",
    ]
    for url in calibration_urls:
        try:
            print(f"⏱️ [CLOCK SYNC] Calibrando contra: {url}")
            before = time.time()
            req  = urllib.request.Request(url, method="HEAD")
            resp = urllib.request.urlopen(req, timeout=3)
            after = time.time()

            date_header = resp.headers.get("Date")
            if not date_header:
                continue

            server_time    = email.utils.parsedate_to_datetime(date_header).timestamp()
            rtt_half       = (after - before) / 2.0
            local_at_server = before + rtt_half
            offset         = server_time - local_at_server

            print(f"  └ OFFSET CALCULADO: {offset:+.3f}s  (RTT {(after-before)*1000:.0f}ms)")
            return offset
        except Exception:
            continue

    print("⏱️ [CLOCK SYNC] No se pudo calibrar. Offset = 0.0s")
    return 0.0


def _sync_setup_pancake(primary_rpc_url: str):
    """
    Conecta al primer nodo HTTP disponible del pool y devuelve (w3, contract).
    Imprime solo 1 línea de éxito — los fallos son silenciosos a nivel de log.
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

            print(f"✅ [RPC] Conectado: {rpc}")
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(PANCAKESWAP_CONTRACT),
                abi=PANCAKESWAP_PREDICTION_ABI,
            )
            return w3, contract
        except Exception:
            continue

    raise Exception("Fallaron todos los RPCs del pool.")


def _sync_pancake_call(contract):
    epoch      = contract.functions.currentEpoch().call()
    round_data = contract.functions.rounds(epoch).call()
    prev_round = contract.functions.rounds(epoch - 1).call() if epoch > 0 else None
    lock_price_raw = prev_round[4] if prev_round else 0
    return epoch, round_data, lock_price_raw


class PancakeSwapMonitor:
    def __init__(self, shared_state: SharedMarketState) -> None:
        self.shared_state = shared_state
        self.rpc_url      = os.getenv("BSC_RPC_URL", "https://binance.llamarpc.com")
        self.w3           = None
        self.contract     = None
        self._running     = False
        self.time_offset  = 0.0

        # ── Throttle timestamps ─────────────────────────────────────────
        self._last_wss_error_log: float = 0.0   # última vez que logueamos error WSS
        self._last_skip_log_epoch: int  = -1     # epoch en el que ya mostramos SKIP

    async def start(self) -> None:
        try:
            self._running = True
            self.shared_state.log_messages.append(
                f"🟢 [PANCAKE] Iniciando monitor — modo BLOCK-DRIVEN WSS"
            )

            loop = asyncio.get_event_loop()

            # ── Calibración del reloj ──────────────────────────────────
            self.time_offset = await loop.run_in_executor(None, _calibrate_time_offset)
            self.shared_state.log_messages.append(
                f"⏱️ [CLOCK SYNC] Offset calibrado: {self.time_offset:+.3f}s"
            )
            self.shared_state.time_offset = self.time_offset

            # ── Conexión HTTP para lectura de contratos ────────────────
            self.w3, self.contract = await loop.run_in_executor(
                None, _sync_setup_pancake, self.rpc_url
            )

            # ── WSS POOL para streaming de bloques ────────────────────
            wss_pool = [
                "wss://bsc.publicnode.com",
                "wss://rpc.ankr.com/bsc/ws",
                "wss://bsc-mainnet.public.blastapi.io",
            ]
            current_ws_idx = 0
            _connected_to: str = ""

            while self._running:
                ws_url = wss_pool[current_ws_idx]
                try:
                    async with websockets.connect(
                        ws_url, ping_interval=20, ping_timeout=20
                    ) as ws:
                        # Suscribirse al feed de cabeceras de bloque
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0", "id": 1,
                            "method": "eth_subscribe", "params": ["newHeads"]
                        }))

                        # Solo loguear cuando el nodo realmente cambia
                        if _connected_to != ws_url:
                            _connected_to = ws_url
                            self.shared_state.log_messages.append(
                                f"📡 [WSS] Suscrito a newHeads → {ws_url}"
                            )

                        while self._running:
                            raw  = await asyncio.wait_for(ws.recv(), timeout=15.0)
                            data = json.loads(raw)

                            # Ignorar ack de suscripción
                            if "result" in data and "params" not in data:
                                continue

                            if "params" in data and "result" in data["params"]:
                                block        = data["params"]["result"]
                                block_ts_hex = block.get("timestamp")
                                if not block_ts_hex:
                                    continue

                                # ── Actualizar estado del mercado en cada bloque ──
                                epoch, round_data, lock_price_raw = await loop.run_in_executor(
                                    None, _sync_pancake_call, self.contract
                                )

                                lock_timestamp  = round_data[2]
                                bull_amount_wei = round_data[9]
                                bear_amount_wei = round_data[10]

                                bull_amt = Decimal(bull_amount_wei) / Decimal(1e18)
                                bear_amt = Decimal(bear_amount_wei) / Decimal(1e18)
                                total    = bull_amt + bear_amt

                                bull_mult = (total * Decimal("0.97")) / bull_amt if bull_amt > 0 else Decimal("0")
                                bear_mult = (total * Decimal("0.97")) / bear_amt if bear_amt > 0 else Decimal("0")

                                lock_price     = Decimal(lock_price_raw) / Decimal(1e8)
                                corrected_time = time.time() + self.time_offset
                                remaining_s    = int(lock_timestamp - corrected_time)

                                self.shared_state.pancake_state = {
                                    "epoch":            epoch,
                                    "lock_timestamp":   lock_timestamp,
                                    "remaining_seconds": remaining_s,
                                    "lock_price":       lock_price,
                                    "bull_amount":      bull_amt,
                                    "bear_amount":      bear_amt,
                                    "bull_multiplier":  bull_mult,
                                    "bear_multiplier":  bear_mult,
                                }

                # ── Manejo silencioso de errores WSS (throttle 10 s) ──────
                except (
                    websockets.exceptions.InvalidStatus,
                    websockets.exceptions.ConnectionClosed,
                    socket.gaierror,
                    asyncio.TimeoutError,
                    Exception,
                ) as exc:
                    now = time.time()
                    if now - self._last_wss_error_log >= _WSS_ERROR_THROTTLE_S:
                        self._last_wss_error_log = now
                        short_reason = type(exc).__name__
                        self.shared_state.log_messages.append(
                            f"⚠️ [WSS] {ws_url.split('//')[1][:20]} → {short_reason}. Rotando nodo..."
                        )
                        _connected_to = ""  # Forzar log al reconectar

                    current_ws_idx = (current_ws_idx + 1) % len(wss_pool)
                    await asyncio.sleep(2)

        except Exception as e:
            err_msg = f"⚠️ [PANCAKE] FATAL: {e}"
            self.shared_state.log_messages.append(err_msg)
            with open("crash_report.log", "a") as f:
                f.write(f"{err_msg}\n{traceback.format_exc()}\n")

    async def stop(self) -> None:
        self._running = False
