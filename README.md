# Polymarket HFT Arbitrage Bot ⚡

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Architecture](https://img.shields.io/badge/architecture-Asynchronous-green.svg)]()
[![Status](https://img.shields.io/badge/status-Production--Ready-success.svg)]()

Motor de arbitraje automatizado de Alta Frecuencia (HFT) diseñado específicamente para los mercados relámpago (*flash markets*) de 5 minutos en Polymarket (ej. "BTC price up or down in 5m"). 

Construido en Python 3.11 bajo una arquitectura 100% asíncrona, este sistema destaca por su tolerancia a fallos, escaneo dinámico y una interfaz de consola (HUD) estilo *Cyber-Industrial* que prioriza la claridad sin sacrificar la profundidad técnica en sus logs.

---

## 🏗 Arquitectura y Módulos Principales

El bot está estructurado en módulos atómicos que interactúan mediante colas asíncronas (`asyncio.Queue`) y despachadores de eventos, asegurando que ninguna operación de I/O bloquee el ciclo de evaluación principal.

- **`BotOrchestrator` (`main.py`)**: El hilo conductor del sistema. Controla el ciclo de vida (start/stop), maneja señales del SO (`SIGINT`/`SIGTERM`) y coordina las dependencias de los demás módulos.
- **`ArbitrageEngine` (`modules/arbitrage_engine.py`)**: El cerebro de trading. Opera exclusivamente utilizando el SDK oficial de Polymarket (`py-clob-client`) para emitir órdenes en modo *Maker* limitando el spread y capturando liquidez con precisión sub-segundo.
- **`RiskManager` (`modules/risk_manager.py`)**: La barrera de defensa. Monitorea PnL, Win Rates y latencia de feeds. Si detecta pérdidas consecutivas o picos de latencia, activa *Circuit Breakers* que pausan temporalmente las operaciones.
- **`MarketScanner` (`modules/market_scanner.py`)**: Sistema de auto-descubrimiento. Consulta dinámicamente la Gamma API para extraer el Token ID de los mercados flash de 5 minutos activos. Cuenta con un mecanismo de fallback robusto hacia el CLOB endpoint (`https://clob.polymarket.com/markets`) que protege la ejecución contra caídas de DNS o bloqueos por Cloudflare.

- **`ExecutionEngine` (`modules/execution_engine.py`)**: El motor de ejecución de sub-milisegundo. Abstrae el *hot path* implementando pre-firmas criptográficas asíncronas (`EIP-712`), conexiones `Keep-Alive` HTTP y salvaguardas estrictas de simulación (`DRY_RUN`).

---

## 🔮 El Oráculo y Estrategia de Front-Running

El verdadero valor de este bot reside en la predicción algorítmica previa al cierre del contrato. Los mercados de 5 minutos en Polymarket se resuelven mediante el oráculo matemático de **Chainlink Data Streams**, evaluando si el precio final (T=300s) es superior al precio de apertura (T=0). 

Acceder a Chainlink Data Streams normalmente requiere credenciales B2B de pago, pero este sistema sortea dicha restricción a través de nuestro módulo `polymarket_chainlink_feed.py`.

### Mecanismo de Intercepción (Piggybacking)
1. **Conexión RTDS**: Nos acoplamos directamente al WebSocket público de Polymarket (`wss://ws-live-data.polymarket.com`).
2. **Suscripción de Tópico**: Nos suscribimos exclusivamente al canal `crypto_prices_chainlink` para capturar el *mismo feed en crudo* (1 tick/segundo) que usa Polymarket para la resolución.
3. **La Ventana Crítica**: A través del `MarketTimer`, el bot sella el precio de apertura del BTC al segundo exacto de la creación del mercado. A medida que avanza el reloj, evalúa matemáticamente el spread y la desviación (Δ%).

### Pre-Firma EIP-712 y Optimización de Latencia
El *overhead* criptográfico y la latencia de red son los peores enemigos del spread. El bot mitiga esto dividiendo la ejecución asimétrica en fases:
1. **Fase 1 (Pre-Firma en T-60s)**: Durante la ventana inactiva, el bot empaqueta y firma (`EIP-712`) en memoria dos órdenes independientes (`YES` y `NO`). Esto delega el cálculo síncrono intensivo a un `ThreadPoolExecutor` para no bloquear el WebSocket de precios.
2. **Fase 2 (Re-firma dinámica)**: Si el precio sufre una deriva alta antes del disparo, las órdenes se regeneran automáticamente para garantizar un *fill* exitoso dentro del *slippage* tolerado.
3. **Fase 3 (Disparo en T-8s - Hot Path)**: En los últimos segundos, si se asegura una dirección algorítmica, el bot lanza la orden pre-construida usando una sesión `aiohttp` pre-establecida. Esto suprime el *TCP Handshake* logrando disparos sub-milisegundos a nivel local.

## 🖥 Interfaz HUD y Graceful Shutdown

Hemos desarrollado un entorno de consola pensado para operadores institucionales, separando radicalmente el ruido técnico de la interfaz de usuario.

- **Segregación de Logs**: Cualquier excepción técnica, fallo de red, traceback crudo, o diccionario anidado es capturado silenciosamente y almacenado en el archivo permanente `polymarket_bot.log`. 
- **Consola Clean**: La terminal principal (STDOUT) no sufre engrillados forzados ni prints sucios. En caso de pérdida de red, solo reportará un alerta discreto: `[!] Enlace con Gamma API interrumpido. Reintentando...`
- **Graceful Shutdown**: Al interrumpir el proceso (`Ctrl+C`), el bot suspende sus tareas asíncronas para evitar procesos zombis, y despliega un reporte tabular ASCII de doble línea con las métricas finales limpias (sin inyectar diccionarios ni timestamps en la interfaz visual).

```text
╔══════════════════════════════════════════════════════════╗
║                    📊 MÉTRICAS FINALES                    ║
╠══════════════════════════════════════════════════════════╣
║  [ENGINE]                                                ║
║    State                    : SHUTDOWN                   ║
║    Opportunities Executed   : 4                          ║
║                                                          ║
║  [RISK]                                                  ║
║    Circuit Breaker State    : CLOSED                     ║
║    Win Rate                 : 0.95                       ║
╚══════════════════════════════════════════════════════════╝
✅ Apagado del sistema completado.
```

---

## ⚙️ Instalación y Uso

### 1. Requisitos Previos
*   Python >= 3.9 (Recomendado 3.11 para asincronismo nativo avanzado).
*   Cuenta de Polymarket (Address fondeada).

### 2. Configuración del Entorno
Clona el repositorio e inicializa el entorno virtual sin pisar configuraciones nativas del OS:
```bash
python3.11 -m venv venv --without-pip
source venv/bin/activate
curl -sS https://bootstrap.pypa.io/get-pip.py | python
```

### 3. Instalar Dependencias
Instala los paquetes mandatorios (el SDK oficial y la librería para el RTDS websocket):
```bash
pip install -r requirements.txt
pip install py_clob_client websockets
```

### 4. Configurar Variables de Entorno (`.env`)
No modifiques el código base. Duplica `.env-examp` como `.env` e inserta tus claves.
No requieres credenciales de Chainlink, solo de Polymarket:
```env
PRIVATE_KEY="tu_private_key_derivada_de_polymarket"
DRY_RUN=True  # Ponlo en False para pasar a producción
POLYMARKET_CHAIN_ID=137
```
*(Nota: Si usas Trust Wallet o cuentas Smart, usa nuestro script `derive_key.py` para obtener la clave hexadecimal real).*

### 5. Ejecución en Producción
Dispara el orquestador principal:
```bash
python main.py
```
*(Para detenerlo de forma segura y evaluar métricas en el panel HUD, utiliza simplemente `Ctrl+C`).*
