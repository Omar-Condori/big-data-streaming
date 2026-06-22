# TickDB Streaming Pipeline

Pipeline de Big Data en tiempo real que consume datos del mercado financiero desde **TickDB API**, los transmite por **Apache Kafka**, los procesa con **Spark Structured Streaming** y aplica **Machine Learning distribuido** con **Spark MLlib**.

## Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ARQUITECTURA KAPPA COMPLETA                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ╔══════════════════╗      ╔══════════════════════╗      ╔═══════════════════════════╗  │
│   ║  TICKDB API     ║      ║     KAFKA           ║      ║  SPARK STRUCTURED        ║  │
│   ║  (Fuente)      ║──────║  (Ingesta)         ║──────║  STREAMING               ║  │
│   ║                 ║      ║                     ║      ║  (Procesamiento)         ║  │
│   ║ - Forex         ║      ║ Topic:             ║      ║                          ║  │
│   ║ - Índices       ║      ║ tickdb-market-data ║      ║ - Parseo JSON           ║  │
│   ║ - Acciones US  ║      ║ Partitions: 1      ║      ║ - Ventanas               ║  │
│   ║ - Criptomonedas║      ║                     ║      ║ - Watermarking          ║  │
│   ╚══════════════════╝      ╚══════════════════════╝      ║ - Agregaciones           ║  │
│                                    │                        ╚═══════════════════════════╝  │
│                                    │                                   │                  │
│                                    ▼                                   ▼                  │
│                          ╔══════════════════════╗      ╔═══════════════════════════╗  │
│                          ║  CONSUMIDORES       ║      ║  SALIDAS MÚLTIPLES       ║  │
│                          ║                     ║      ║                          ║  │
│                          ║ - Producer (Python)║      ║ 1. Console (Debug)       ║  │
│                          ║ - Jupyter (Spark)  ║      ║ 2. Parquet (Storage)    ║  │
│                          ║ - ML Prediction    ║      ║ 3. ML Model (Real-time)  ║  │
│                          ╚══════════════════════╝      ╚═══════════════════════════╝  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Resultados Obtenidos

| Componente | Resultado |
|------------|-----------|
| Pipeline Streaming | ✅ Operativo - 23 eventos/batch |
| Throughput | 7.66 eventos/segundo |
| Latencia | ~600ms por batch |
| Modelo ML (MAE) | $12.02 de error promedio |
| Modelo ML (Precisión) | 99.98% |
| Almacenamiento Parquet | ✅ Funcionando |

## Tecnologías

| Componente | Tecnología |
|------------|------------|
| Ingesta | TickDB API (REST + WebSocket) |
| Mensajería | Apache Kafka 7.5 + Schema Registry |
| Procesamiento Streaming | Spark Structured Streaming 3.5 |
| Procesamiento Batch | Spark ETL (Parquet, Avro, ORC, Delta) |
| Almacenamiento | HDFS + Local Filesystem |
| ML Distribuido | Spark MLlib (Pipeline, CrossValidator) |
| Experiment Tracking | MLflow |
| Monitoreo | Prometheus + Grafana |
| Contenedores | Docker Compose |

## Componentes

- **01-docker**: Infraestructura (Kafka, ZK, Schema Registry, HDFS, Prometheus, Grafana, MLflow)
- **02-kafka**: Scripts de administración Kafka
- **03-producer**: Productor Python (REST + WebSocket)
- **04-spark**: Notebooks Jupyter (Streaming, ETL Batch, ML Distribuido)
- **05-documentacion**: Documentación completa del proyecto
