# Requisitos

## Software

- **Docker** 24+ y **Docker Compose** v2
- **Python** 3.10+
- **Git**

## Hardware Recomendado

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disco | 20 GB | 50 GB |

## Puertos Necesarios

| Puerto | Servicio |
|--------|----------|
| 42181 | Zookeeper |
| 49092 | Kafka |
| 48081 | Schema Registry |
| 48085 | Kafka UI |
| 49870 | HDFS NameNode |
| 49000 | HDFS |
| 49090 | Prometheus |
| 49091 | Pushgateway |
| 49100 | Node Exporter |
| 43000 | Grafana |
| 45000 | MLflow |
| 8888 | Jupyter |

## API Key

Necesitas una API Key de [TickDB](https://tickdb.ai). Configúrala como variable de entorno:

```bash
export TICKDB_API_KEY="tu-api-key"
```
