# Evidencias del Pipeline

## 1. Consola Jupyter - Spark Streaming

```
+------------------------------------------+---------+-----------+------------------+--------------------+----------+
|window                                    |market   |event_count|avg_price         |avg_latency_ms      |max_change|
+------------------------------------------+---------+-----------+------------------+--------------------+----------+
|{2026-05-19 15:04:00, 2026-05-19 15:05:00}|us_stocks|63         |370.11            |1967.88             |-0.12     |
|{2026-05-19 15:06:00, 2026-05-19 15:07:00}|forex    |28         |1163.93           |1457.5              |0.13      |
|{2026-05-19 15:07:00, 2026-05-19 15:08:00}|crypto   |40         |19840.61          |724.22              |0.70      |
+------------------------------------------+---------+-----------+------------------+--------------------+----------+
```

## 2. Consola Jupyter - ML Predictions

```
=== PIPELINE ML COMPLETO ===
1. STREAMS: Kafka → Spark (ya funcionando)
2. COLECTA: 5-10 min de datos históricos
3. FEATURES: SMA, RSI, Volatilidad
4. MODELO: Random Forest (100 árboles)
5. PREDICCIÓN: En tiempo real

Métricas del modelo:
  - MAE: $12.02 (error promedio)
  - RMSE: $18.41
  - Precisión: 99.98%
```

## 3. Métricas del Último Batch

```python
{
  'batchId': 105,
  'numInputRows': 23,
  'inputRowsPerSecond': 7.66,
  'processedRowsPerSecond': 35.44,
  'durationMs': {
    'addBatch': 574,
    'triggerExecution': 649
  }
}
```

## 4. Logs del Productor

```
[2026-05-19 14:28:26] INFO - Connected to Kafka
[2026-05-19 14:28:27] INFO - Sending batch: 23 events
[2026-05-19 14:28:32] INFO - Sending batch: 23 events
```

## 5. Logs de Spark

```
[INFO] SparkContext - Running job...
[INFO] KafkaSource - Getting offsets for topic tickdb-market-data
[INFO] StreamingQuery - Trigger completed in 649 ms
```

## 6. Errores Identificados

| Error | Causa | Solución |
|-------|-------|-----------|
| NoBrokersAvailable | Productor no podía conectar a Kafka | Usar nombre servicio Docker (kafka:9092) |
| NodeExists | Datos antiguos en ZooKeeper | Eliminar volúmenes con -v |
| Permission denied | Escritura en /opt/artifacts | Cambiar a /home/jovyan/work |

## 7. Fragmentos de Código

### Notebooks Entregados

| Archivo | Propósito | Líneas |
|---------|----------|--------|
| tickdb_spark_streaming.ipynb | Pipeline streaming principal | ~300 |
| ml_price_prediction.ipynb | Modelo ML de predicción | ~350 |

### Docker Compose Files

| Archivo | Servicio |
|--------|----------|
| 01-docker/docker-compose.yml | Kafka + Zookeeper + UI |
| 03-producer/docker-compose.yml | Productor Python |
| 04-spark/docker-compose.yml | Jupyter + PySpark |

## 8. Resultados de Pruebas

- **Prueba 1**: Streaming funcional - 23 eventos/batch
- **Prueba 2**: Parquet almacenamiento - OK
- **Prueba 3**: ML predicción - MAE $12.02
- **Prueba 4**: Throughput - 7.66 evt/s
