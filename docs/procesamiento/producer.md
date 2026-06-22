# Productor Python

## Ubicación

`03-producer/tickdb_producer.py`

## Descripción

Productor que consume datos del mercado financiero desde **TickDB API** y los publica en **Apache Kafka**. Soporta dos modos de operación: REST polling y WebSocket en tiempo real.

## Modos de operación

### REST (por defecto)

```bash
TICKDB_API_KEY=tu-key python tickdb_producer.py
```

- Polling cada 30s (configurable via `POLL_INTERVAL`)
- Obtiene todos los símbolos en una llamada
- Envía cada ticker a Kafka individualmente
- Actualiza métricas en Prometheus Pushgateway

### WebSocket (tiempo real)

```bash
TICKDB_MODE=websocket TICKDB_API_KEY=tu-key python tickdb_producer.py
```

- Conexión persistente WebSocket
- Reconexión automática cada 5s ante fallos
- Mensajes entrantes se procesan inmediatamente

## Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `TICKDB_API_KEY` | — | API Key de TickDB (requerida) |
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Servidor Kafka |
| `PUSHGATEWAY_URL` | pushgateway:9091 | Prometheus Pushgateway |
| `SCHEMA_REGISTRY_URL` | http://schema-registry:8081 | Schema Registry |
| `POLL_INTERVAL` | 30 | Segundos entre polls (REST) |
| `TICKDB_MODE` | rest | rest o websocket |

## Símbolos Monitoreados (21)

| Mercado | Símbolos |
|---------|----------|
| Forex | XAUUSD, GBPUSD, EURUSD, USDJPY |
| Índices | SPX, NDX, HSI |
| US Stocks | AAPL.US, TSLA.US, MSFT.US, NVDA.US, GOOGL.US, AMZN.US, META.US |
| HK Stocks | 700.HK, 9988.HK, 3690.HK |
| Crypto | BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT |

## Ejemplo de Evento Generado

```json
{
  "symbol": "BTCUSDT",
  "last_price": 76517.32,
  "volume_24h": 13430.51370,
  "high_24h": 76850.00,
  "low_24h": 76200.00,
  "price_change_24h": 39.32,
  "price_change_percent_24h": 0.05,
  "timestamp": 1779201261000,
  "event_timestamp": 1779201261839,
  "market": "crypto"
}
```

## Contrato del Evento (Esquema Avro)

| Campo | Tipo | Nullable | Descripción | Ejemplo |
|-------|------|----------|-------------|---------|
| symbol | STRING | No | Identificador único del activo | "BTCUSDT" |
| last_price | DOUBLE | No | Precio de cierre actual | 76517.32 |
| volume_24h | DOUBLE | Sí | Volumen transado en 24h | 13430.51 |
| high_24h | DOUBLE | Sí | Precio máximo del día | 76850.00 |
| low_24h | DOUBLE | Sí | Precio mínimo del día | 76200.00 |
| price_change_24h | DOUBLE | Sí | Diferencia de precio | 39.32 |
| price_change_percent_24h | DOUBLE | Sí | Porcentaje de cambio | 0.05 |
| timestamp | BIGINT | No | Timestamp origen (epoch ms) | 1779201261000 |
| event_timestamp | BIGINT | No | Timestamp procesamiento | 1779201261839 |
| market | STRING | No | Clasificación de mercado | "crypto" |

## Métricas Prometheus

| Métrica | Tipo | Descripción |
|---------|------|-------------|
| `last_price` | Gauge | Precio actual por símbolo/mercado |
| `kafka_events_total` | Counter | Eventos enviados a Kafka |
| `producer_healthy` | Gauge | Estado del productor (1=ok) |

## Logs de Ejecución

```
[2026-05-19 14:28:26] INFO - Connected to Kafka
[2026-05-19 14:28:27] INFO - Sending batch: 23 events
[2026-05-19 14:28:32] INFO - Sending batch: 23 events
```
