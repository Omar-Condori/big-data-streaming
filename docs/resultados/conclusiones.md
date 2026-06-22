# Conclusiones

## Logros

1. **Pipeline funcional** de extremo a extremo: TickDB → Kafka → Spark → ML
2. **Dos modos de ingesta**: REST polling y WebSocket en tiempo real
3. **Procesamiento streaming** con Spark Structured Streaming, watermarking y ventanas
4. **ETL batch** con 4 formatos de salida (Parquet, Avro, ORC, Delta) + HDFS
5. **ML distribuido** con Spark MLlib, CrossValidator y experiment tracking en MLflow
6. **Monitoreo completo** con Prometheus + Grafana
7. **Documentación** en Material for MkDocs

## Errores Identificados

| Error | Causa | Solución |
|-------|-------|-----------|
| NoBrokersAvailable | Productor no podía conectar a Kafka | Usar nombre servicio Docker (kafka:9092) |
| NodeExists | Datos antiguos en ZooKeeper | Eliminar volúmenes con -v |
| Permission denied | Escritura en /opt/artifacts | Cambiar a /home/jovyan/work |

## Limitaciones

| Limitación | Descripción | Impacto |
|------------|-------------|---------|
| Red Docker | Configuración de red entre contenedores | Requiere extra_hosts |
| Memoria | Limitado a un ejecutor | No escalable horizontalmente |
| Particiones | Solo 1 partición Kafka | Limitado throughput |
| API Key | Rate limits en TickDB | Requiere gestión de quotas |

## Mejoras Futuras

- Múltiples brokers Kafka (3+) con replicación
- Cluster Spark multi-nodo
- Modelos LSTM/XGBoost con MLlib
- Alertas en Grafana
- CI/CD para despliegue automatizado
- Delta Lake con time travel y Z-order
- Feature Store (Feast o Tecton)
- Model Serving (TensorFlow Serving o Triton)

## Checklist de Entrega

| Criterio | Cumple |
|----------|--------|
| Se creó y probó un tópico Kafka | ✅ |
| Se ejecutó productor y consumidor | ✅ |
| Se documentó el contrato de evento | ✅ |
| Se implementó pipeline con Spark Structured Streaming | ✅ |
| Se usaron ventanas y watermarking | ✅ |
| Se midió latencia y throughput | ✅ |
| Se propuso estrategia de observabilidad | ✅ |
| Se definieron métricas, alertas y umbrales | ✅ |
| Se estimaron costos o recursos de operación | ✅ |
| Se propuso estrategia de escalado | ✅ |

## Lecciones Aprendidas

- La ingesta por WebSocket reduce la latencia significativamente vs REST polling
- Spark MLlib escala horizontalmente pero sklearn es más flexible para prototipado
- Schema Registry es esencial para evolución de datos en Kafka
- MLflow simplifica el tracking de experimentos distribuidos
