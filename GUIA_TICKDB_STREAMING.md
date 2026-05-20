# TickDB Market Data Streaming - Guía de Sesión U2

## 1. Título

Pipeline streaming en tiempo real con datos de mercado financiero usando TickDB, Kafka y Spark Structured Streaming.

## 2. Objetivo

Implementar un pipeline de datos en tiempo real que ingiera datos de mercado financiero desde la API de TickDB, los publique en Kafka y los procese con Spark Structured Streaming, midiendo latencia, throughput y generando salidas para BI/ML.

## 3. Herramientas utilizadas

- **TickDB API**: WebSocket para datos reales de mercado (forex, índices, acciones, crypto)
- **Apache Kafka**: Sistema de mensajería para ingesta en tiempo real
- **Spark Structured Streaming**: Motor de procesamiento de flujo
- **Python**: Productor WebSocket -> Kafka
- **Jupyter Notebook**: Consola Spark
- **Docker Compose**: Infraestructura completa
- **Prometheus + Grafana**: Observabilidad

## 4. Datos disponibles en TickDB

| Mercado | Símbolos ejemplo | Frecuencia |
|---------|------------------|------------|
| Forex | XAUUSD, GBPUSD, EURUSD, USDJPY | Tiempo real |
| Índices | SPX, NDX, HSI, DJIA, FTSE, DAX | Tiempo real |
| US Stocks | AAPL.US, TSLA.US, MSFT.US, NVDA.US | Tiempo real |
| HK Stocks | 700.HK, 9988.HK, 3690.HK | Tiempo real |
| Crypto | BTCUSDT, ETHUSDT, SOLUSDT | Tiempo real |

## 5. Entorno de trabajo

- Kafka desde host: `localhost:49092`
- Kafka interno: `kafka:9092`
- Kafka UI: `http://localhost:48085`
- Prometheus: `http://localhost:49090`
- Grafana: `http://localhost:43000`
- Tópico: `tickdb-market-data`

## 6. Arquitectura del pipeline

```
TickDB WebSocket → Productor Python → Kafka → Spark Structured Streaming
                                                            ↓
                                              ┌─────────────┴─────────────┐
                                              ↓                           ↓
                                         Console                    Parquet
                                              ↓                           ↓
                                         Métricas                  BI/ML
```

## 7. Desarrollo de la práctica

### 7.1 Levantar infraestructura

```bash
cd tickdb-streaming/docker
docker compose up -d
```

Verificar:
```bash
docker compose ps
```

### 7.2 Verificar tópico en Kafka

Entrar al contenedor:
```bash
docker compose exec kafka bash
```

Crear tópico:
```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --topic tickdb-market-data \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

Verificar:
```bash
/opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server kafka:9092
```

### 7.3 Ejecutar Productor Python

```bash
cd tickdb-streaming/python
pip install websocket-client kafka-python
python tickdb_producer.py
```

El productor se conectará a TickDB WebSocket y publicará eventos como:
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

### 7.4 Verificar mensajes en Kafka

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic tickdb-market-data \
  --bootstrap-server kafka:9092 \
  --from-beginning
```

### 7.5 Ejecutar Spark Streaming

Usar el notebook: `tickdb-streaming/spark/tickdb_spark_streaming.ipynb`

Ejecutar las celdas para:
1. Conectar a Kafka
2. Parsear JSON
3. Calcular latencia
4. Mostrar en consola
5. Guardar en Parquet

### 7.6 Medir latencia y throughput

Revisar en el notebook:
- `latency_ms`: diferencia entre timestamp del evento y procesamiento
- `numInputRows`: eventos por micro-batch
- `inputRowsPerSecond`: throughput de entrada
- `processedRowsPerSecond`: throughput de procesamiento

## 8. Contrato del evento

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| symbol | String | Símbolo del activo | AAPL.US |
| last_price | Double | Último precio | 297.84 |
| volume_24h | Double | Volumen 24h | 34474022 |
| high_24h | Double | Precio más alto 24h | 300.66 |
| low_24h | Double | Precio más bajo 24h | 294.91 |
| price_change_24h | Double | Cambio de precio | -2.39 |
| price_change_percent_24h | Double | Cambio % | -0.80 |
| timestamp | Long | Timestamp TickDB | 1779134400000 |
| event_timestamp | Long | Timestamp procesamiento | 1779134450000 |
| market | String | Categoría mercado | us_stocks |

## 9. Configuración de Spark

| Parámetro | Valor | Justificación |
|-----------|-------|----------------|
| Trigger | 5 segundos | Balance latencia/recursos |
| Watermark | 10 minutos | Tolerancia tardanza eventos |
| Ventana | 1 minuto | Agregación por tiempo |
| Output mode | Append | Solo eventos nuevos |
| Checkpoint | /opt/artifacts/checkpoint | Recuperación ante fallos |

## 10. Métricas esperadas

| Métrica | Descripción | Umbral esperado |
|---------|-------------|------------------|
| Latencia | Tiempo desde TickDB hasta Spark | < 500ms |
| Throughput | Eventos por segundo | 5-50 eventos/s |
| Backpressure | Lag del consumidor | < 100 eventos |
| Errores | Eventos inválidos | 0% |

## 11. Observabilidad

### Métricas Kafka (Prometheus)
- `kafka_brokers`: 1
- `kafka_consumergroup_lag`: 0
- `up{job="kafka-exporter"}`: 1

### Métricas Spark
- `numInputRows` por micro-batch
- `latency_ms` promedio
- `event_count` por ventana

### Dashboard Grafana
- Panel Kafka Brokers
- Panel Consumer Lag
- Panel Spark Throughput
- Panel Latencia promedio

## 12. Evidencias a entregar

- Captura del producerpublicando eventos
- Captura del tópico en Kafka UI
- Captura del notebook procesando datos
- Tabla de métricas (latencia, throughput)
- Captura del dashboard Grafana
- Archivo Parquet generado

## 13. Actividad de aprendizaje autónomo

1. Agregar más símbolos al producer
2. Probar diferentes intervalos de trigger
3. Crear alertas en Grafana
4. Calcular volumen diario estimado
5. Proponer estrategia de escalado