import aiohttp
import asyncio
import logging
import json
import re
import time
from typing import Optional, Tuple, Dict, Any
from datetime import datetime

logger = logging.getLogger("polymarket_arb.market_scanner")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/"
}

BTC_5MIN_SLUG_RE = re.compile(r'btc.*?5m|btc.*?5-min|bitcoin.*?5m|bitcoin.*?5-min', re.IGNORECASE)

def _is_btc_5min(title: str, slug: str = "") -> bool:
    title_upper = title.upper()
    if 'BTC' in title_upper or 'BITCOIN' in title_upper:
        if '5 M' in title_upper or '5M' in title_upper or '5 MIN' in title_upper:
            return True
    if slug and BTC_5MIN_SLUG_RE.search(slug):
        return True
    return False

def _extract_market_fields(market: Dict[str, Any], title: str, event_end: str = "") -> Tuple[Optional[Dict[str, Any]], str]:
    if not market.get('active') or market.get('closed'):
        return None, ""
    clob_ids = market.get('clobTokenIds', [])
    condition_id = market.get('conditionId', '')
    if clob_ids:
        try:
            if isinstance(clob_ids, str):
                clob_ids = json.loads(clob_ids)
            if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                # Calcular close_ts si hay event_end
                close_ts = time.time() + 50.0  # Default 50s as user requested
                if event_end:
                    try:
                        # event_end format: '2024-06-02T03:35:00Z'
                        dt = datetime.strptime(event_end.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
                        close_ts = dt.timestamp()
                    except:
                        pass
                
                market_dict = {
                    "condition_id": condition_id,
                    "token_id_yes": clob_ids[0],
                    "token_id_no": clob_ids[1],
                    "close_ts": close_ts,
                    "question": market.get('question', title)
                }
                return market_dict, market_dict["question"]
        except Exception as e:
            logger.warning(f"Error parseando clobTokenIds: {e}")
    return None, ""

def _extract_clob_market(market: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    if not market.get('active') or market.get('closed'):
        return None, ""
    question = market.get('question', '')
    if _is_btc_5min(question):
        tokens = market.get('tokens', [])
        condition_id = market.get('condition_id', '')
        if tokens and len(tokens) >= 2:
            market_dict = {
                "condition_id": condition_id,
                "token_id_yes": tokens[0].get('token_id'),
                "token_id_no": tokens[1].get('token_id'),
                "close_ts": time.time() + 50.0,  # default 50s
                "question": question
            }
            return market_dict, question
    return None, ""

class MarketScanner:
    def __init__(self, dashboard=None):
        self.gamma_url = "https://gamma-api.polymarket.com/events?series_slug=btc-up-or-down-5m&active=true&closed=false"
        self.clob_url = "https://clob.polymarket.com/markets"
        self.dashboard = dashboard

    async def _fetch_gamma(self) -> Tuple[Optional[str], str]:
        try:
            async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
                async with session.get(self.gamma_url, timeout=5.0) as response:
                    if response.status == 403:
                        logger.warning("Gamma API retornó 403 (WAF Cloudflare bloqueado).")
                        return None, ""
                    response.raise_for_status()
                    data = await response.json()
                    
                    for event in data:
                        title = event.get('title', '')
                        slug = event.get('slug', '')
                        if _is_btc_5min(title, slug):
                            markets = event.get('markets', [])
                            event_end = event.get('endDate', '')
                            for market in markets:
                                market_dict, name = _extract_market_fields(market, title, event_end)
                                if market_dict:
                                    logger.info(f"Mercado encontrado vía Gamma: {name}")
                                    return market_dict, name
        except Exception as e:
            logger.warning(f"Error en _fetch_gamma: {e}")
        return None, ""

    async def _fetch_clob(self) -> Tuple[Optional[str], str]:
        try:
            async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
                async with session.get(self.clob_url, timeout=5.0) as response:
                    response.raise_for_status()
                    data = await response.json()
                    markets = data.get('data', [])
                    for market in markets:
                        market_dict, name = _extract_clob_market(market)
                        if market_dict:
                            logger.info(f"Mercado encontrado vía CLOB Fallback: {name}")
                            return market_dict, name
        except Exception as e:
            logger.warning(f"Error en _fetch_clob: {e}")
        return None, ""

    async def get_active_btc_5min_market(self) -> Tuple[Optional[Any], str]:
        from config import get_config
        
        logger.info("Iniciando escaneo de mercados (Capa 1: Gamma API)...")
        if self.dashboard:
            self.dashboard.add_event("[SCANNER] Intentando capa 1 (Gamma API)...")
        tid, name = await self._fetch_gamma()
        if tid:
            return tid, name
            
        logger.info("Iniciando escaneo de mercados (Capa 2: CLOB API Fallback)...")
        if self.dashboard:
            self.dashboard.add_event("[SCANNER] Intentando capa 2 (CLOB REST)...")
        tid, name = await self._fetch_clob()
        if tid:
            return tid, name
            
        logger.info("No se encontró ningún mercado activo de BTC a 5 minutos. Reintentando...")
        return None, ""
