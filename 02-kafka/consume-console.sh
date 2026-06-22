#!/bin/bash
# Consumir mensajes desde Kafka y mostrarlos en consola formateados
KAFKA_CONTAINER="tickdb-kafka"

docker exec -it $KAFKA_CONTAINER kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tickdb-market-data \
  --from-beginning \
  --property print.key=false \
  --property print.timestamp=true \
  --property print.partition=true \
  --property print.offset=true
