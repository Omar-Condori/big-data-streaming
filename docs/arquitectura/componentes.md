# Componentes del Pipeline

## 1. Productor Python

- **Modo REST**: Polling cada 30s a `api.tickdb.ai/v1/market/ticker`
- **Modo WebSocket**: Conexión persistente a `wss://api.tickdb.ai/v1/ws/market`
- **Serialización**: JSON (con soporte para Avro vía Schema Registry)
- **Métricas**: Prometheus Pushgateway (precio por símbolo, total de eventos)
- **21 símbolos** monitoreados: Forex, Índices, US Stocks, HK Stocks, Crypto

## 2. Apache Kafka

| Configuración | Valor |
|--------------|-------|
| Versión | 7.5.0 (Confluent) |
| Tópico | `tickdb-market-data` |
| Particiones | 3 |
| Retención | 7 días |
| ACKs | all |
| UI | Kafka UI en `:48085` |

## 3. Schema Registry

- Registro de esquemas Avro para evolución de datos
- Endpoint: `http://schema-registry:8081`
- Sujeto: `tickdb-market-data-value`

## 4. Spark Structured Streaming

- **App Name**: TickDBStreaming
- **Trigger**: 5 segundos (processing time)
- **Watermark**: 10 minutos
- **Windowing**: Ventanas de 1 minuto por mercado
- **Outputs**: Console, Parquet, HDFS, ML Inference

## 5. ETL Batch

- Lectura desde Parquet (output del streaming)
- Transformaciones: particionado por fecha, limpieza de duplicados
- Formatos de salida: **Parquet**, **Avro**, **ORC**, **Delta Lake**
- Destino: Local + HDFS (`hdfs://hdfs-namenode:9000/tickdb/`)

## 6. HDFS

- NameNode: `hdfs-namenode:9870` (web UI)
- DataNode: Almacenamiento distribuido
- Datos organizados por `ingestion_date/market/`

## 7. MLflow

- Tracking de experimentos ML
- Registro de parámetros, métricas y modelos
- UI: `http://localhost:45000`

## 8. Monitoreo

| Servicio | Puerto | Propósito |
|----------|--------|-----------|
| Prometheus | 49090 | Métricas del pipeline |
| Pushgateway | 49091 | Métricas del productor |
| Node Exporter | 49100 | Métricas del sistema |
| Grafana | 43000 | Dashboards visuales |
