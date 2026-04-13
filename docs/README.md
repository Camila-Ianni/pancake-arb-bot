# 📊 Polymarket Arbitrage Bot - Documentación Técnica

Bot de arbitraje de latencia para mercados de predicción climáticos en Polymarket.

---

## ⚠️ ADVERTENCIA IMPORTANTE

Este bot opera con **capital real** cuando `DRY_RUN=false`. Los riesgos incluyen:
- Pérdida total del capital asignado
- Bugs de software no detectados
- Latencia de red que cause ejecuciones desfavorables
- Cambios en el oráculo de Polymarket

**Siempre probar en modo simulación primero.**

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         POLYMARKET ARBITRAGE BOT                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐         ┌─────────────────────────────────────────┐  │
│  │  FastWeatherFeed │────────▶│                                         │  │
│  │  (WeatherAPI)    │         │           ArbitrageEngine               │  │
│  │  Latencia: <50ms │         │         (Zero-Latency HFT)              │  │
│  └──────────────────┘         │                                         │  │
│                               │  ┌─────────────┐      ┌──────────────┐  │  │
│  ┌──────────────────┐         │  │ RiskManager │      │ Web3Executor │  │  │
│  │ PolymarketMonitor│────────▶│  │  (Circuit   │─────▶│ (Firma +     │  │  │
│  │ (CLOB WebSocket) │         │  │   Breaker)  │      │  Envío TX)   │  │  │
│  │  Latencia: <50ms │         │  └─────────────┘      └──────────────┘  │  │
│  └──────────────────┘         └─────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estructura del Proyecto

```
polymarket-arb-bot/
├── main.py                      # Punto de entrada, orquestador principal
├── config.py                    # Configuración centralizada
├── logging_config.py            # Logging con métricas de latencia
├── models.py                    # Modelos de datos inmutables
├── requirements.txt             # Dependencias de Python
├── .env.example                 # Plantilla de configuración
│
├── modules/
│   ├── __init__.py
│   ├── fast_weather_feed.py     # Feed climático (WeatherAPI)
│   ├── polymarket_monitor.py    # Monitor CLOB WebSocket
│   ├── arbitrage_engine.py      # Motor de decisión HFT
│   ├── risk_manager.py          # Circuit breaker y gestión de riesgo
│   └── web3_executor.py         # Ejecución de transacciones
│
├── tests/
│   ├── test_config.py
│   ├── test_models.py
│   ├── test_weather_feed.py
│   └── test_polymarket_monitor.py
│
└── docs/
    └── README.md                # Esta documentación
```

---

## 🚀 Instalación

### 1. Requisitos Previos

- **Python 3.11+** (optimizado para Apple Silicon M1/M2/M3)
- **pip** o **poetry** para gestión de dependencias
- **Cuenta en WeatherAPI.com** (gratis: 60 calls/min)
- **API Key de Polymarket** (opcional, para datos públicos no requiere)
- **Wallet de Polygon** con MATIC para gas (si ejecutas en real)

### 2. Clonar e Instalar

```bash
cd polymarket-arb-bot
python3.11 -m venv venv
source venv/bin/activate  # Linux/Mac
# o: venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 3. Configurar Variables de Entorno

```bash
cp .env.example .env
```

Editar `.env` con tus credenciales:

```ini
# Wallet (solo si ejecutas en real)
PRIVATE_KEY=0x________________________
RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
WALLET_ADDRESS=0x________________________

# Polymarket
POLYMARKET_API_KEY=your_api_key
CONDITION_ID=0x________________________  # Token ID del mercado
MARKET_IDS=market_id_1,market_id_2

# WeatherAPI
WEATHER_API_KEY=your_weatherapi_key
WEATHER_LAT=40.7128
WEATHER_LON=-74.0060
WEATHER_POLL_INTERVAL=0.5  # 500ms

# Trading
BET_SIZE_USD=5
MIN_ROI_THRESHOLD=0.08
MAX_SLIPPAGE_TOLERANCE=0.02

# Risk
MAX_CONSECUTIVE_LOSSES=3
MAX_FEED_LATENCY_MS=500

# Execution
DRY_RUN=true  # ⚠️ TRUE para testing
LOG_LEVEL=INFO
```

---

## 🔧 Características Principales

### 1. Zero-Latency Execution

El motor usa **pre-computación** para reducir la latencia de ejecución:

| Fase | Latencia Típica |
|------|-----------------|
| Pre-compute (antes de señal) | ~50ms |
| Detección de señal | < 1ms |
| Fat-finger check | < 0.1ms |
| Profit lock check | < 0.1ms |
| Ejecución (tx pre-firmada) | ~5ms |
| **Total** | **< 56ms** |

### 2. Bit-Level Processing

Comparaciones directas sin parsing innecesario:

```python
# En vez de:
if weather_data.temperature > threshold and market_price < fair_value:
    ...

# Usamos:
if self._weather_state.temperature_c > 20.0 and state.best_ask < 0.95:
    # Señal detectada en O(1)
```

### 3. Fat-Finger Protection

Valida que tu orden no mueva el mercado:

```python
# Check automático:
# - Orden < 5% de liquidez disponible
# - Slippage < 2%
if order_size / liquidity > 0.05:
    reject("Orden muy grande")
```

### 4. Profit Lock

Garantiza ROI mínimo del 5% después de fees:

```python
MIN_ROI_AFTER_FEES = 0.05  # 5% mínimo

net_roi = gross_roi - (gas_cost / bet_size)
if net_roi < MIN_ROI_AFTER_FEES:
    reject("ROI insuficiente")
```

### 5. Circuit Breaker

Detiene el bot automáticamente si:
- 3 pérdidas consecutivas
- Latencia del feed > 500ms
- 5 transacciones fallidas

---

## 📖 Funcionamiento Paso a Paso

### Flujo de Ejecución

```
1. WEATHER UPDATE
   └─> FastWeatherFeed polls WeatherAPI cada 500ms
       └─> Recibe: {temp: 25.5, humidity: 60, precipitation: 0}
           └─> Actualiza WeatherState (O(1))
               └─> Notifica al ArbitrageEngine

2. MARKET UPDATE
   └─> PolymarketMonitor recibe WebSocket update
       └─> Actualiza Local Order Book (LOB)
           └─> Pre-computa VWAP para $5
               └─> Notifica al ArbitrageEngine

3. DETECCIÓN DE SEÑAL
   └─> Engine compara weather vs market
       └─> Ejemplo: precipitation > 0 BUT market YES price = 0.45
           └─> ¡OPORTUNIDAD! El mercado no sabe que está lloviendo

4. VALIDACIONES
   └─> Fat-Finger Check: ¿$5 mueven el precio?
       └─> Profit Lock Check: ¿ROI >= 5% después de gas?
           └─> Risk Manager: ¿Circuit breaker cerrado?

5. EJECUCIÓN
   └─> Usa transacción pre-computada
       └─> Firma y envía (~5ms)
           └─> Espera confirmación en Polygon
               └─> Registra resultado

6. POST-TRADE
   └─> Actualiza métricas
       └─> P&L tracking
           └─> Log de auditoría
```

---

## 🎯 Configuración de Mercados

### Ejemplo: Mercado de Lluvia

```ini
# Mercado: "¿Va a llover en NYC el Jan 15?"
CONDITION_ID=0x1234567890abcdef
MARKET_IDS=0xabcdef1234567890

# Threshold de lluvia (mm)
# Si precipitation > 0 → YES debería ser 100¢
# Si precipitation = 0 → NO debería ser 100¢
```

### Ejemplo: Mercado de Temperatura

```ini
# Mercado: "Temp máxima en NYC > 25°C el Jan 15?"
CONDITION_ID=0x1234567890abcdef

# Threshold específico
TEMP_THRESHOLD=25.0  # °C

# Si temp > 25 → YES = 100¢
# Si temp < 23 → NO = 100¢ (margen de 2°C)
```

---

## 📊 Monitoreo y Logs

### Logs en Tiempo Real

```bash
# Ver logs en consola
python main.py

# Ver archivo de logs (si configurado)
tail -f /var/log/polymarket-arb/bot.log
```

### Métricas Clave

El bot loguea cada ~30 segundos:

```
📈 MÉTRICAS DEL ENGINE:
{
  "state": "RUNNING",
  "opportunities_detected": 15,
  "opportunities_executed": 8,
  "opportunities_skipped": 7,
  "fat_finger_rejects": 2,
  "avg_decision_time_ms": 0.45,
  "best_roi_seen": "12.5%",
  "weather_valid": true,
  "weather_latency_ms": 35.2,
}
```

### Alertas Importantes

| Log | Significado | Acción |
|-----|-------------|--------|
| `⚠️ RATE LIMIT detectado` | WeatherAPI rate limit | Aumentar `WEATHER_POLL_INTERVAL` |
| `❤️ HEARTBEAT TIMEOUT` | Sin datos del feed | Verificar conexión API |
| `⚠️ CIRCUIT BREAKER ACTIVADO` | Pérdidas/errores máximos | Revisar estrategia |
| `❌ Transacción fallida` | Error en-chain | Verificar gas/nounce |

---

## 🧪 Testing

### Tests Unitarios

```bash
# Todos los tests
pytest tests/ -v

# Con coverage
pytest tests/ --cov=. --cov-report=html

# Test específico
pytest tests/test_weather_feed.py -v
```

### Modo Simulación

```ini
# .env
DRY_RUN=true
```

En este modo:
- ✅ Detecta oportunidades
- ✅ Calcula ROI y slippage
- ✅ Loguea qué **habría** ejecutado
- ❌ **NO** envía transacciones reales
- ❌ **NO** usa capital real

---

## 🔐 Seguridad

### Nunca Commitear

```bash
# Agregar al .gitignore
.env
*.key
private_key.txt
```

### Mejores Prácticas

1. **Usar wallet dedicada** con solo el capital que quieras arriesgar
2. **Nunca compartir** `PRIVATE_KEY`
3. **Usar RPC privado** (Alchemy/QuickNode) en vez de públicos
4. **Rotar API keys** periódicamente

---

## 📈 Estrategia de Arbitraje

### La Ventana de Oportunidad

```
Tiempo 0:00  → Evento climático ocurre (ej. empieza a llover)
Tiempo 0:01  → WeatherAPI lo detecta (~1-5 segundos)
Tiempo 0:05  → Tu bot recibe el dato
Tiempo 0:06  → Tu bot ejecuta orden (YES a 45¢)
Tiempo 2:00  → Oráculo de Polymarket se actualiza
Tiempo 2:01  → YES sube a 95¢
Tiempo 2:02  → Tu bot vende (o espera resolución)

Profit: 50¢ - gas - fees = ~$4.50 por trade de $5
```

### Riesgos

1. **Oracle Risk**: El oráculo de Polymarket puede actualizarse antes de que ejecutes
2. **Liquidity Risk**: Puede no haber contraparte para salir del trade
3. **Latency Risk**: Otro bot más rápido puede tomar la oportunidad primero
4. **Smart Contract Risk**: Bugs en el contrato de Polymarket

---

## 🛠️ Troubleshooting

### Error: `Rate Limit Exceeded`

```ini
# Solución: Aumentar intervalo de polling
WEATHER_POLL_INTERVAL=1.0  # De 500ms a 1s
```

### Error: `Circuit Breaker Activado`

```bash
# Ver logs para causa específica
tail -f bot.log | grep "CIRCUIT BREAKER"

# Causas comunes:
# - 3 pérdidas seguidas → Revisar estrategia
# - Latencia > 500ms → Verificar conexión
# - 5 TXs fallidas → Verificar gas/nonce
```

### Error: `Insufficient Liquidity`

```python
# El bot detecta automáticamente y skipa la oportunidad
# Ver métricas: fat_finger_rejects > 0
# Solución: Reducir BET_SIZE_USD
BET_SIZE_USD=2  # De $5 a $2
```

---

## 📞 Soporte y Contribuciones

### Issues Comunes

1. **¿Cómo obtengo una API key de WeatherAPI?**
   - Gratis en https://www.weatherapi.com/
   - 60 calls/min en plan free

2. **¿Cómo encuentro el CONDITION_ID de un mercado?**
   - Inspeccionar la URL en Polymarket
   - O usar la API: `https://gamma-api.polymarket.com/conditions`

3. **¿Puedo ejecutar en otros mercados (no clima)?**
   - Sí, pero debes adaptar la lógica de `FastWeatherFeed`

### Contribuciones

Pull requests bienvenidos para:
- Mejoras de performance
- Nuevas fuentes de datos
- Estrategias adicionales
- Mejoras en tests

---

## 📄 Licencia

MIT License - Ver `LICENSE` para detalles.

---

## 🎯 Roadmap

- [ ] Backtesting framework con datos históricos
- [ ] Dashboard web para monitoreo
- [ ] Soporte para múltiples mercados simultáneos
- [ ] Machine learning para predicción de thresholds
- [ ] Integración con más APIs climáticas (Meteomatics, NOAA)

---

**Última actualización:** 2026-04-13
**Versión:** 0.1.0
