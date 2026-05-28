import urllib.request
import json

print("🔄 Conectando con la API de Polymarket para buscar mercados activos...")
try:
    markets = []
    cursor = ""
    for _ in range(10):
        url = "https://clob.polymarket.com/markets"
        if cursor:
            url += f"?next_cursor={cursor}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req) as response:
            raw_data = json.loads(response.read().decode())
            
        page_markets = []
        if isinstance(raw_data, list):
            page_markets = raw_data
        elif isinstance(raw_data, dict):
            page_markets = raw_data.get("data", raw_data.get("markets", raw_data.get("results", [])))
            cursor = raw_data.get("next_cursor", "")
        
        markets.extend(page_markets)
        if not cursor:
            break


    found = {"BTC": [], "ETH": [], "SOL": [], "BNB": []}
    for m in markets:
        if not isinstance(m, dict):
            continue
        question = str(m.get("question", "")).lower()
        slug = str(m.get("slug", "")).lower()
        market_id = m.get("market_id") or m.get("id")
        condition_id = m.get("condition_id")
        
        if not market_id or not condition_id:
            continue
            
        if "bitcoin" in question or "btc" in slug:
            found["BTC"].append((market_id, condition_id, m.get("question")))
        elif "ethereum" in question or "eth" in slug:
            found["ETH"].append((market_id, condition_id, m.get("question")))
        elif "solana" in question or "sol" in slug:
            found["SOL"].append((market_id, condition_id, m.get("question")))
        elif "binance" in question or "bnb" in slug:
            found["BNB"].append((market_id, condition_id, m.get("question")))

    print("\n🚀 ¡Mercados encontrados! Copiá y armá tu línea para el .env:\n")
    for asset, items in found.items():
        print(f"🔹 Opciones para {asset}:")
        if not items:
            print("  No se encontraron mercados de precio activos en este lote.")
        for item in items[:2]:
            print(f"  📌 Pregunta: {item[2]}")
            print(f"  ID completo: {asset}:{item[0]}:{item[1]}")
        print("-" * 50)
except Exception as e:
    print(f"❌ Ocurrió un problema al escanear Polymarket: {e}")
