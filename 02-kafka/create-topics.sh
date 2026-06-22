#!/bin/bash
# Script para crear tópicos en Kafka con configuración optimizada
KAFKA_CONTAINER="tickdb-kafka"

docker exec $KAFKA_CONTAINER kafka-topics \
  --create \
  --topic tickdb-market-data \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

echo "✓ Tópico 'tickdb-market-data' creado (3 particiones, 7 días retención)"
