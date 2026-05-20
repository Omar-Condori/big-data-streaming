# TickDB Streaming - Pipeline Big Data en Tiempo Real

Pipeline streaming basado en Kafka y Spark Structured Streaming usando datos reales de mercado financiero desde TickDB API.

## Estructura del Proyecto

```
tickdb-streaming/
├── python/                    # Productor Python (WebSocket -> Kafka)
│   ├── tickdb_producer.py    # Productor principal
│   └── requirements.txt      # Dependencias
├── kafka/                     # Scripts de Kafka
├── spark/                     # Notebooks Jupyter
│   └── tickdb_spark_streaming.ipynb
├── docker/                    # Docker Compose
│   ├── docker-compose.yml
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
└── GUIA_TICKDB_STREAMING.md  # Guía de la sesión
```

## Quick Start

### 1. Levantar infraestructura

```bash
cd docker
docker compose up -d
```

### 2. Verificar servicios

- Kafka: `localhost:49092`
- Kafka UI: `http://localhost:48085`
- Prometheus: `http://localhost:49090`
- Grafana: `http://localhost:43000` (admin/admin)

### 3. Ejecutar Productor

```bash
cd python
pip install -r requirements.txt
python tickdb_producer.py
```

### 4. Ejecutar Spark

Usar el notebook `spark/tickdb_spark_streaming.ipynb` en Jupyter.

## Datos de Mercado

| Mercado | Ejemplos |
|---------|----------|
| Forex | XAUUSD, GBPUSD, EURUSD |
| Índices | SPX, NDX, HSI |
| US Stocks | AAPL.US, TSLA.US, MSFT.US |
| HK Stocks | 700.HK, 9988.HK |
| Crypto | BTCUSDT, ETHUSDT |

## Esquema del Evento

```json
{
  "symbol": "AAPL.US",
  "last_price": 297.84,
  "volume_24h": 34474022,
  "high_24h": 300.66,
  "low_24h": 294.91,
  "price_change_24h": -2.39,
  "price_change_percent_24h": -0.80,
  "timestamp": 1779134400000,
  "event_timestamp": 1779134450000,
  "market": "us_stocks"
}
```

## Métricas del Pipeline

- Latencia: ~50-200ms (TickDB → Spark)
- Throughput: 5-50 eventos/segundo
- Salidas: Console, Parquet, Grafana

## Producción

Para producción considerar:
- 3+ brokers Kafka
- Replication factor 3
- Particiones: 6-12
- Cluster Spark con ejecutores
- Alertas en Grafana