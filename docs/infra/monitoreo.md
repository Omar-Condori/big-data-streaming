# Monitoreo

## Prometheus

- **URL**: http://localhost:49090
- **Scrape interval**: 15s

### Jobs configurados

| Job | Target | Métricas |
|-----|--------|----------|
| prometheus | localhost:9090 | Automonitoreo |
| kafka-exporter | kafka-exporter:9308 | offsets, brokers |
| node-exporter | node-exporter:9100 | CPU, RAM, disco |
| pushgateway | pushgateway:9091 | Métricas del productor |
| hdfs-namenode | hdfs-namenode:9870 | HDFS |

## Grafana

- **URL**: http://localhost:43000
- **Credenciales**: admin / admin

### Dashboard "TickDB Streaming Pipeline"

6 paneles:
1. **Kafka Brokers Active** — Gauge de brokers activos
2. **System CPU Usage** — Uso de CPU del sistema
3. **System RAM Usage** — Uso de memoria
4. **Kafka Ingest Rate** — Mensajes por segundo
5. **Kafka Topic Offsets** — Offsets por partición
6. **Live Market Prices** — Precios en tiempo real (desde Pushgateway)

### Métricas del Productor

| Métrica | Tipo | Labels |
|---------|------|--------|
| `last_price` | Gauge | symbol, market |
| `kafka_events_total` | Counter | market |
| `producer_healthy` | Gauge | — |

### Dashboard Propuesto: TickDB Streaming Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TICKDB STREAMING PIPELINE - MONITOREO EN TIEMPO REAL                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────┐  ┌─────────────────────────┐                  │
│  │  THROUGHPUT (evt/s)    │  │  LATENCIA (ms)         │                  │
│  │  ████████████ 7.66     │  │  ████████ 649 ms      │                  │
│  │  target: >5             │  │  target: <500         │                  │
│  └─────────────────────────┘  └─────────────────────────┘                  │
│                                                                             │
│  ┌─────────────────────────┐  ┌─────────────────────────┐                  │
│  │  EVENTOS/BATCH         │  │  LAG (offsets)         │                  │
│  │  ███████████ 23        │  │  0                     │                  │
│  │  stable                │  │  ✅ HEALTHY            │                  │
│  └─────────────────────────┘  └─────────────────────────┘                  │
│                                                                             │
│  ┌──────────────────────────────┐  ┌────────────────────────────────┐     │
│  │  EVENTOS POR MERCADO         │  │  ESTADO DEL PIPELINE          │     │
│  │  crypto:    ████████████     │  │  ✅ RUNNING                 │     │
│  │  forex:     ████████         │  │  Uptime: 2h 34m            │     │
│  │  indices:   ████████         │  │  Batch: 105                 │     │
│  │  us_stocks: ████████████     │  │  Errors: 0                  │     │
│  └──────────────────────────────┘  └────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Métricas Clave Monitoreadas

| Métrica | Descripción | Fuente de Datos |
|---------|-------------|------------------|
| throughput_in | Eventos entrantes por segundo | Kafka consumer metrics |
| throughput_out | Eventos procesados por segundo | Spark streaming metrics |
| latency_ms | Tiempo de procesamiento del batch | Spark query.lastProgress |
| numInputRows | Cantidad de eventos por batch | Spark query.lastProgress |
| processingTime | Tiempo de ejecución del trigger | Spark durationMs |
| error_count | Número de errores en el pipeline | Logs de Spark |
| backpressure | Presión en el pipeline | Spark streaming UI |

### Logs del Pipeline

| Componente | Ejemplo |
|------------|---------|
| Productor | `[2026-05-19 14:28:26] INFO - Connected to Kafka` |
| Spark | `[INFO] StreamingQuery - Trigger completed in 649 ms` |
