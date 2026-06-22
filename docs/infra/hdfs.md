# HDFS

## Componentes

| Servicio | Puerto | Propósito |
|----------|--------|-----------|
| NameNode | 9870 (web UI) + 9000 | Metadatos del sistema de archivos |
| DataNode | — | Almacenamiento de datos |

## Estructura de directorios

```
hdfs://hdfs-namenode:9000/tickdb/
├── parquet/
│   └── ingestion_date=YYYY-MM-DD/
│       └── market=crypto/
├── avro/
├── orc/
├── delta/
└── models/
    └── btc_rf_model/
```

## Acceso Web

NameNode UI: `http://localhost:49870`

## Uso desde Spark

```python
df.write.format("parquet").save("hdfs://hdfs-namenode:9000/tickdb/parquet")
```
