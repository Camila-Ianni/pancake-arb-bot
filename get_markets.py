import urllib.request
import urllib.error
import json
import time
import os

print("🔄 Calculando intervalos determinísticos de 5 minutos...")
now = int(time.time())
current_interval = (now // 300) * 300
intervals_to_check = [current_interval - 300, current_interval, current_interval + 300]

found = {"BTC": None, "ETH": None, "SOL": None, "BNB": None}

def fetch_market_by_slug(slug: str):
    url = f"https://gamma-api.polymarket.com/markets/slug/{slug}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"⚠️ Error HTTP {e.code} al consultar {slug}")
        return None
    except Exception as e:
        print(f"❌ Error al consultar {slug}: {e}")
        return None

for interval in intervals_to_check:
    print(f"🔎 Escaneando intervalo Epoch: {interval}")
    
    # Check BTC
    if not found["BTC"]:
        slug_btc = f"btc-updown-5m-{interval}"
        data_btc = fetch_market_by_slug(slug_btc)
        if data_btc:
            m_id = data_btc.get("id") or data_btc.get("market_id")
            c_id = data_btc.get("conditionId") or data_btc.get("condition_id")
            if m_id and c_id:
                found["BTC"] = f"BTC:{m_id}:{c_id}"
                print(f"  ✅ BTC encontrado en intervalo {interval}")

    # Check ETH
    if not found["ETH"]:
        slug_eth = f"eth-updown-5m-{interval}"
        data_eth = fetch_market_by_slug(slug_eth)
        if data_eth:
            m_id = data_eth.get("id") or data_eth.get("market_id")
            c_id = data_eth.get("conditionId") or data_eth.get("condition_id")
            if m_id and c_id:
                found["ETH"] = f"ETH:{m_id}:{c_id}"
                print(f"  ✅ ETH encontrado en intervalo {interval}")

final_parts = []
print("\n🚀 Resultados de la extracción determinística:\n")
for asset in ["BTC", "ETH", "SOL", "BNB"]:
    formatted = found[asset]
    if formatted:
        print(f"✅ {asset} 5m ID -> {formatted}")
        final_parts.append(formatted)
    else:
        print(f"⚠️ {asset} 5m no activo/encontrado. Usando fallback estructurado.")
        dummy_market = f"0x{asset.lower()}5mfallbackmarketid0000000"
        dummy_cond = f"0x{asset.lower()}5mfallbackconditionid00000"
        final_parts.append(f"{asset}:{dummy_market}:{dummy_cond}")
        
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
