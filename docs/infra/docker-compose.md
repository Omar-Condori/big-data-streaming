# Docker Compose

Todos los servicios se definen en `01-docker/docker-compose.yml`. Incluye:

## Servicios

| Servicio | Imagen | Propósito |
|----------|--------|-----------|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | Coordinación Kafka |
| kafka | confluentinc/cp-kafka:7.5.0 | Broker de mensajes |
| schema-registry | confluentinc/cp-schema-registry:7.5.0 | Registro de esquemas Avro |
| kafka-ui | provectuslabs/kafka-ui:v0.7.1 | UI de administración |
| hdfs-namenode | apache/hadoop:3.3.6 | NameNode HDFS |
| hdfs-datanode | apache/hadoop:3.3.6 | DataNode HDFS |
| prometheus | prom/prometheus:v2.48.0 | Métricas |
| kafka-exporter | danielqsj/kafka-exporter:v1.6.0 | Métricas de Kafka |
| node-exporter | prom/node-exporter:v1.6.1 | Métricas del sistema |
| pushgateway | prom/pushgateway:v1.6.2 | Gateway para métricas |
| grafana | grafana/grafana:10.2.0 | Dashboards |
| mlflow | ghcr.io/mlflow/mlflow:v2.9.0 | Experiment tracking |

## Red

Todos los servicios comparten la red `tickdb-net` (bridge).

## Volúmenes

- `prometheus_data`: Datos de Prometheus
- `grafana_data`: Datos de Grafana
- `hdfs_namenode`: Metadatos HDFS
- `hdfs_datanode`: Datos HDFS
- `mlflow_data`: Experimentos ML

## Healthchecks

Cada servicio tiene healthcheck configurado para garantizar dependencias ordenadas.
