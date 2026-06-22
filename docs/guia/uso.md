# Uso del Pipeline

## Productor

### Modo REST (por defecto)

```bash
cd 03-producer
python tickdb_producer.py
```

Polling cada 30s a la API de TickDB.

### Modo WebSocket (tiempo real)

```bash
TICKDB_MODE=websocket python tickdb_producer.py
```

Conexión persistente WebSocket para latencia mínima.

## Spark Streaming

Abrir el notebook en Jupyter:
- `tickdb_spark_streaming.ipynb` — Streaming con Kafka
- `tickdb_spark_directo.ipynb` — Streaming sin Kafka (socket)
- `tickdb_etl_batch.ipynb` — ETL batch a múltiples formatos
- `tickdb_ml_distribuido.ipynb` — ML distribuido con MLlib

## Monitoreo

| Servicio | URL |
|----------|-----|
| Kafka UI | http://localhost:48085 |
| Schema Registry | http://localhost:48081 |
| Prometheus | http://localhost:49090 |
| Grafana | http://localhost:43000 (admin/admin) |
| HDFS NameNode | http://localhost:49870 |
| MLflow | http://localhost:45000 |
| Jupyter | http://localhost:8888 (token: tickdb) |
