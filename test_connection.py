import sys
import os
import time
from dotenv import load_dotenv

# 1. Verificación de Entorno (Versión Python)
def check_python_version():
    required_version = (3, 9, 10)
    current_version = sys.version_info[:3]
    print(f"🔍 Verificando versión de Python: {current_version[0]}.{current_version[1]}.{current_version[2]}")
    
    if current_version < required_version:
        print(f"❌ ERROR: La versión de Python debe ser mayor o igual a 3.9.10 para soportar py-clob-client.")
        print("💡 Solución: Actualiza tu entorno virtual (venv) a una versión más reciente.")
        sys.exit(1)
    else:
        print("✅ Python Version OK.")

check_python_version()

# Validamos dependencias en runtime
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from py_clob_client.exceptions import PolyApiException
except ImportError as e:
    print(f"\n❌ ERROR: El SDK 'py-clob-client' falló al cargar: {e}")
    print("💡 Solución: Ejecuta 'pip install py_clob_client' antes de continuar.")
    sys.exit(1)

# Cargar variables de entorno
load_dotenv()

# 1. Verificación de Entorno (Claves)
def check_environment():
    private_key = os.getenv("PRIVATE_KEY") or os.getenv("WALLET_PRIVATE_KEY")
    print("🔍 Verificando variables de entorno...")
    
    if not private_key:
        print("❌ ERROR: No se encontró la variable PRIVATE_KEY o WALLET_PRIVATE_KEY en el .env")
        sys.exit(1)
        
    print("✅ PRIVATE_KEY detectada (Oculta por seguridad).")
    return private_key

def main():
    print("\n" + "="*50)
    print("🚀 INICIANDO HEALTH CHECK: POLYMARKET CLOB API")
    print("="*50 + "\n")
    
    private_key = check_environment()
    
    host = "https://clob.polymarket.com"
    chain_id = 137
    
    print(f"\n⚙️  Inicializando ClobClient...")
    print(f"   Host: {host}")
    print(f"   Chain ID: {chain_id}")
    
    try:
        client = ClobClient(host, key=private_key, chain_id=chain_id)
    except Exception as e:
        print(f"❌ ERROR: Fallo al instanciar ClobClient: {e}")
        sys.exit(1)
        
    print("\n🔐 2. Diagnóstico de Conectividad (Autenticación L1 -> L2)...")
    try:
        start_auth = time.time()
        creds: ApiCreds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        auth_time = time.time() - start_auth
        print(f"✅ Autenticación CLOB (EIP-712) exitosa en {auth_time:.2f}s.")
        print(f"   API Key derivada: {creds.api_key[:8]}...{creds.api_key[-4:]}")
    except PolyApiException as e:
        print(f"❌ [PolyApiException] Fallo de Autenticación de API: HTTP {e.status_code}")
        print(f"   Detalle: {e}")
        sys.exit(1)
    except Exception as e:
        error_str = str(e).lower()
        if "signature" in error_str:
            print(f"❌ [Auth Failed] Invalid Signature: Asegúrate de que la Private Key sea correcta.")
        elif "network" in error_str or "connection" in error_str:
            print(f"❌ [Network Error] No se pudo conectar al endpoint de Polymarket.")
        else:
            print(f"❌ [Auth Failed] Error Inesperado durante derivación de credenciales: {e}")
        sys.exit(1)
        
    print("\n📊 3. Validación de Datos en Tiempo Real (Order Book)...")
    
    # Intenta usar un Market ID del config, si no existe, usa un default activo
    market_ids_env = os.getenv("POLYMARKET_MARKET_IDS", "")
    if market_ids_env:
        market_id = market_ids_env.split(",")[0]
        print(f"   Usando mercado desde .env: {market_id[:10]}...")
    else:
        # Default fallback: Un token random o conocido para probar (Si da error, puede estar expirado)
        print("   No se encontró POLYMARKET_MARKET_IDS en el .env, probando mercado dinámico...")
        try:
            # Obtiene mercados activos de la API pública para probar
            markets = client.get_markets()
            if markets and len(markets.get("data", [])) > 0:
                market_data = markets["data"][0]
                market_id = market_data.get("tokens", [{"token_id": ""}])[0].get("token_id", "")
                if not market_id:
                    market_id = market_data.get("condition_id")
                print(f"   Mercado automático detectado (Token/Condition ID).")
            else:
                market_id = "73470541315377973562501025254719659796416871135081220986683321361000395461644"
        except Exception:
            market_id = "73470541315377973562501025254719659796416871135081220986683321361000395461644"
    
    try:
        ob = client.get_order_book(market_id)
        print("✅ Conexión a OrderBook API: OK")
        
        bids = ob.bids if hasattr(ob, 'bids') else ob.get("bids", [])
        asks = ob.asks if hasattr(ob, 'asks') else ob.get("asks", [])
        
        best_bid = float(bids[0].price) if bids else 0.0
        best_ask = float(asks[0].price) if asks else 0.0
        
        if best_bid and best_ask:
            mid_price = (best_bid + best_ask) / 2
            spread = best_ask - best_bid
            print(f"   Precio Mid: {mid_price:.4f}¢")
            print(f"   Mejor Bid:  {best_bid:.4f}¢")
            print(f"   Mejor Ask:  {best_ask:.4f}¢")
            print(f"   Spread Real: {spread:.4f}¢")
        else:
            print("   ⚠️ El mercado especificado no tiene suficiente liquidez para mostrar Bid/Ask completos.")
            
    except PolyApiException as e:
        if e.status_code == 429:
            print("❌ [Rate Limit] HTTP 429: Demasiadas peticiones. La IP está restringida temporalmente.")
        elif e.status_code == 404:
            print(f"❌ [Not Found] El mercado {market_id[:8]}... no existe o está expirado.")
        else:
            print(f"❌ [Market Data Error] HTTP {e.status_code}: {e}")
    except Exception as e:
        print(f"❌ [Data Validation Failed] Error obteniendo el OrderBook: {e}")

    print("\n" + "="*50)
    print("✅ HEALTH CHECK FINALIZADO")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
