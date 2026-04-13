# Polymarket Latency Arbitrage Bot

Bot de arbitraje de latencia para mercados de predicción climáticos en Polymarket.

## ⚠️ ADVERTENCIAS IMPORTANTES

Este bot está diseñado para **trading de alta frecuencia (HFT)** y conlleva riesgos significativos:

1. **Riesgo de capital**: Puedes perder dinero real si se ejecuta en modo LIVE
2. **Riesgo técnico**: Latencia de red, bugs de software, o fallos de API pueden causar pérdidas
3. **Riesgo de mercado**: Los mercados pueden comportarse de forma no modelada

**NUNCA ejecutes este bot en modo LIVE sin:**
- Testing extensivo en modo DRY_RUN
- Testing con capital mínimo en sandbox
- Monitoreo constante durante las primeras horas

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────┐     ┌─────────────────────────────────────┐
│  FastWeatherFeed    │────▶│                                     │
│  (Meteomatics API)  │     │                                     │
└─────────────────────┘     │         ArbitrageEngine             │
                            │         (El Cerebro)                │
┌─────────────────────┐     │                                     │
│  PolymarketMonitor  │────▶│  ┌─────────────┐  ┌──────────────┐  │
│  (Gamma API WS)     │     │  │ RiskManager │  │ Web3Executor │  │
└─────────────────────┘     │  │ (Circuit    │  │ (Firma +     │  │
                            │  │  Breaker)   │  │  Envío TX)   │  │
                            │  └─────────────┘  └──────────────┘  │
                            └─────────────────────────────────────┘
```

### Flujo de Decisión

1. **FastWeatherFeed** recibe dato climático (ej. "25°C en NYC")
2. **PolymarketMonitor** mantiene order book local en tiempo real
3. **ArbitrageEngine** compara: ¿El precio del mercado refleja el dato nuevo?
4. **RiskManager** valida: ¿ROI > gas + slippage? ¿Circuit breaker cerrado?
5. **Web3Executor** firma y envía transacción a Polymarket (CTF Exchange)

## 📁 Estructura del Proyecto

```
polymarket-arb-bot/
├── main.py                 # Orquestador principal
├── config.py               # Configuración y validación
├── logging_config.py       # Logging profesional con métricas de latencia
├── models.py               # Modelos de datos inmutables
├── requirements.txt        # Dependencias de Python
├── .env.example            # Plantilla de configuración
│
└── modules/
    ├── __init__.py
    ├── fast_weather_feed.py   # Feed climático de baja latencia
    ├── polymarket_monitor.py  # Monitor de mercado (Gamma WS)
    ├── arbitrage_engine.py    # Motor de decisión
    ├── risk_manager.py        # Circuit breaker y gestión de riesgo
    └── web3_executor.py       # Ejecución de transacciones on-chain
```

## 🚀 Instalación

### 1. Clonar y configurar entorno

```bash
cd polymarket-arb-bot
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o: venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con tus credenciales reales
```

### Variables requeridas (mínimo):

```env
PRIVATE_KEY=0x...
RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYMARKET_API_KEY=your_api_key
CONDITION_ID=market_condition_id
MARKET_IDS=market_id_1,market_id_2
DRY_RUN=true  # ¡IMPORTANTE! True para testing
```

## 🏃 Ejecución

### Modo Simulación (Recomendado inicialmente)

```bash
# Asegurar DRY_RUN=true en .env
python main.py
```

El bot logueará qué operaciones **habría** ejecutado y con qué latencia, sin usar capital real.

### Modo Live (⚠️ Solo después de testing extensivo)

```bash
# Cambiar DRY_RUN=false en .env
python main.py
```

## 📊 Monitoreo

### Logs en tiempo real

```bash
tail -f /var/log/polymarket-arb/bot.log
```

### Métricas clave a monitorear

- `opportunities_detected`: Oportunidades detectadas
- `opportunities_executed`: Operaciones ejecutadas
- `avg_decision_time_ms`: Tiempo promedio de decisión (debe ser < 100ms)
- `circuit_breaker_state`: Estado del circuit breaker
- `feed_latency_ms`: Latencia del feed climático (debe ser < 500ms)

## 🧪 Testing

```bash
# Tests unitarios
pytest tests/ -v

# Tests con coverage
pytest tests/ --cov=. --cov-report=html

# Type checking
mypy .
```

## 🔧 Configuración Avanzada

### Ajustar sensibilidad del circuit breaker

```env
MAX_CONSECUTIVE_LOSSES=3      # Pérdidas antes de parar
MAX_FEED_LATENCY_MS=500       # Latencia máxima del feed
MAX_FAILED_TRANSACTIONS=5     # TXs fallidas antes de parar
CIRCUIT_BREAKER_COOLDOWN_SEC=300  # Pausa después de activar
```

### Optimizar para menor latencia

```env
# Polling más frecuente (más CPU, más latencia)
# Modificar en FastWeatherFeed: DEFAULT_POLL_INTERVAL = 0.05  # 50ms

# Usar RPC privado de baja latencia
RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Aumentar priority fee para TXs más rápidas
PRIORITY_FEE_GWEI=5
```

## 📝 Estrategia de Arbitraje

### La Ventana de Oportunidad

El arbitraje explota el tiempo entre:
1. **Evento físico ocurre** (ej. temperatura cambia)
2. **Feed rápido lo detecta** (Meteomatics, ~segundos)
3. **Oráculo de Polymarket se actualiza** (puede tomar minutos)

Durante esa ventana, el precio del mercado no refleja la realidad → oportunidad de arbitraje.

### Ejemplo Concreto

```
Mercado: "¿Temp en NYC > 20°C el Jan 15?"
- Outcome YES cotiza a 45¢ (45% probabilidad implícita)

14:00:00 - Feed rápido detecta: 25°C (confirmado)
14:00:01 - Bot calcula: fair price debería ser ~100¢
14:00:01 - Bot compra YES a 45¢
14:02:00 - Oráculo Polymarket se actualiza a 25°C
14:02:01 - Outcome YES sube a 95¢
14:02:02 - Bot vende YES a 95¢
Profit: 50¢ por share - gas - slippage
```

## ⚖️ Consideraciones Legales

- Verifica que el arbitraje esté permitido en tu jurisdicción
- Polymarket puede tener restricciones geográficas
- Las ganancias pueden estar sujetas a impuestos

## 🤝 Contribuciones

Este es un esqueleto base. Mejoras bienvenidas:
- Implementación real de APIs de weather (Meteomatics, etc.)
- Integración con el ABI real del contrato CTF de Polymarket
- Backtesting framework
- Live monitoring dashboard

## 📄 Licencia

MIT License - Ver LICENSE para detalles.
