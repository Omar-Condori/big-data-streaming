# ETL Batch

## Notebook

`tickdb_etl_batch.ipynb`

## Flujo

1. **Lectura**: Desde Parquet (output del streaming)
2. **Transformaciones**:
   - Particionado por fecha de ingesta (`ingestion_date`)
   - Hora de ingesta (`ingestion_hour`)
   - Categorización de precio (`price_range`: low/medium/high/very_high)
   - Cálculo de volatilidad
   - Deduplicación por `(symbol, timestamp)`
3. **Escritura**: A 4 formatos diferentes

## Formatos de Salida

| Formato | Biblioteca Spark | Uso |
|---------|-----------------|-----|
| Parquet | Nativo | Almacenamiento principal, BI |
| Avro | spark-avro | Interoperabilidad con Kafka |
| ORC | Nativo | Optimizado para Hive |
| Delta | delta-spark | ACID, time travel, upserts |

## Destinos

- **Local**: `/home/jovyan/work/etl_output/{formato}/tickdb/`
- **HDFS**: `hdfs://hdfs-namenode:9000/tickdb/{formato}/`

Particionado por `ingestion_date` y `market`.
