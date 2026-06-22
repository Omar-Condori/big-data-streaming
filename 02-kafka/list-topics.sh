#!/bin/bash
# Listar tópicos y sus detalles
KAFKA_CONTAINER="tickdb-kafka"

echo "=== TÓPICOS ==="
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "=== DETALLE ==="
docker exec $KAFKA_CONTAINER kafka-topics --describe --bootstrap-server localhost:9092
