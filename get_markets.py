import urllib.request
import json
import os

print("🔄 Conectando con la API Gamma de Polymarket para buscar mercados activos de precios...")
url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&q=price"

try:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response:
        markets = json.loads(response.read().decode())
        
    found = {"BTC": None, "ETH": None, "SOL": None, "BNB": None}
    
    for m in markets:
        if not isinstance(m, dict):
            continue
            
        question = str(m.get("question", "")).lower()
        slug = str(m.get("slug", "")).lower()
        title = str(m.get("title", "")).lower()
        
        market_id = m.get("id") or m.get("market_id")
        condition_id = m.get("conditionId") or m.get("condition_id")
        
        if not market_id or not condition_id:
            continue
            
        combined_text = question + " " + slug + " " + title
        
        if not found["BTC"] and ("bitcoin" in combined_text or "btc" in combined_text):
            found["BTC"] = f"BTC:{market_id}:{condition_id}"
        elif not found["ETH"] and ("ethereum" in combined_text or "eth" in combined_text):
            found["ETH"] = f"ETH:{market_id}:{condition_id}"
        elif not found["SOL"] and ("solana" in combined_text or "sol" in combined_text):
            found["SOL"] = f"SOL:{market_id}:{condition_id}"
        elif not found["BNB"] and ("binance" in combined_text or "bnb" in combined_text):
            found["BNB"] = f"BNB:{market_id}:{condition_id}"

    final_parts = []
    print("\n🚀 ¡Mercados encontrados y extraídos!\n")
    for asset, formatted in found.items():
        if formatted:
            print(f"✅ {asset} encontrado -> {formatted}")
            final_parts.append(formatted)
        else:
            print(f"⚠️ {asset} no encontrado en este lote. Usando fallback estático.")
            dummy = f"0x{'0'*61}{asset.lower()}"
            final_parts.append(f"{asset}:{dummy}:{dummy}")
            
    final_string = ",".join(final_parts)
    print(f"\n⚙️ Cadena generada para el .env:\nPOLYMARKET_MARKETS={final_string}\n")
    
    env_path = ".env"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()
            
        with open(env_path, "w") as f:
            for line in lines:
                if line.startswith("POLYMARKET_MARKETS="):
                    f.write(f"POLYMARKET_MARKETS={final_string}\n")
                else:
                    f.write(line)
        print("✅ ¡Archivo .env parcheado automáticamente de forma exitosa!")
    else:
        print("❌ Archivo .env no encontrado en la ruta local.")
        
except Exception as e:
    print(f"❌ Ocurrió un problema al escanear Gamma API: {e}")
