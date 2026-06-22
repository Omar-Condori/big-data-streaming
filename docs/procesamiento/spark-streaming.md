# Spark Structured Streaming

## Notebooks

| Notebook | Descripción |
|----------|-------------|
| `tickdb_spark_streaming.ipynb` | Streaming con Kafka |
| `tickdb_spark_directo.ipynb` | Streaming sin Kafka (socket TCP) |

## Configuración

```python
spark = SparkSession.builder \
    .appName("TickDBStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
```

## Lectura desde Kafka

```python
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tickdb-market-data") \
    .option("startingOffsets", "earliest") \
    .load()
```

| Parámetro | Valor | Justificación |
|----------|-------|----------------|
| kafka.bootstrap.servers | kafka:9092 | Nombre del servicio Docker para acceso interno |
| subscribe | tickdb-market-data | Nombre del tópico a consumir |
| startingOffsets | earliest | Leer desde el inicio del topic |

## Procesamiento

1. **Lectura**: `readStream.format("kafka")` desde tópico `tickdb-market-data`
2. **Parseo**: `from_json` con esquema tipado (10 campos)
3. **Métricas**: Latencia = `event_timestamp - timestamp`
4. **Filtrado**: Eliminar nulos en `symbol` y `last_price`
5. **Windowing**: Ventanas de 1 minuto con watermark de 10 minutos
6. **Agregaciones**: count, avg_price, avg_latency, max_change
7. **Sinks**: Console + Parquet

### Parseo JSON

```python
event_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("last_price", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percent_24h", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("market", StringType(), True)
])

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
    col("topic"),
    col("partition"),
    col("offset")
).select("data.*", "kafka_timestamp", "topic", "partition", "offset")
```

### Ventana y Watermark

```python
df_windowed = df_valid \
    .withWatermark("timestamp_ts", "10 minutes") \
    .groupBy(
        window("timestamp_ts", "1 minute"),
        col("market")
    ) \
    .agg(
        count("*").alias("event_count"),
        avg("last_price").alias("avg_price"),
        avg("latency_ms").alias("avg_latency_ms"),
        max("price_change_percent_24h").alias("max_change")
    )
```

## Trigger

- Console: cada 5 segundos
- Parquet: cada 10 segundos, modo append

## Parámetros de Configuración

| Parámetro | Valor | Justificación |
|-----------|-------|----------------|
| trigger | 5 seconds | Frecuencia de procesamiento |
| watermark | 10 minutes | Tolerancia para datos tardíos |
| ventana | 1 minute | Aggregation window |
| outputMode | append | Solo agregar nuevos resultados |
| checkpointLocation | /home/jovyan/work/checkpoint | Persistencia de estado |

## Métricas de rendimiento

```python
query.lastProgress  # numInputRows, inputRowsPerSecond, processedRowsPerSecond
```

### Detalle del Último Batch Procesado

```json
{
  "batchId": 105,
  "numInputRows": 23,
  "inputRowsPerSecond": 7.66,
  "processedRowsPerSecond": 35.44,
  "durationMs": {
    "addBatch": 574,
    "commitOffsets": 17,
    "getBatch": 0,
    "latestOffset": 8,
    "queryPlanning": 14,
    "triggerExecution": 649,
    "walCommit": 35
  }
}
```

### Salidas del Stream

```python
# Console Output (Debug)
query_console = df_valid \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

# Parquet (Almacenamiento)
query_parquet = df_windowed \
    .writeStream \
    .format("parquet") \
    .option("path", "/home/jovyan/work/tickdb_parquet") \
    .option("checkpointLocation", "/home/jovyan/work/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
```
