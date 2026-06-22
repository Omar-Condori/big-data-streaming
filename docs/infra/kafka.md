# Kafka

## Configuración

| Parámetro | Valor |
|-----------|-------|
| Versión | Confluent 7.5.0 |
| Broker ID | 1 |
| Puerto interno | 9092 |
| Puerto externo | 49092 |
| Particiones por defecto | 3 |
| Factor de replicación | 1 |
| Auto-creación de tópicos | true |

## Tópico Principal

- **Nombre**: `tickdb-market-data`
- **Particiones**: 3
- **Retención**: 7 días (604800000 ms)
- **Cleanup policy**: delete

## Schema Registry

- **URL**: `http://schema-registry:8081`
- **Sujeto**: `tickdb-market-data-value`

## Scripts de Administración

```bash
# Listar tópicos
cd 02-kafka && ./list-topics.sh

# Consumir mensajes
cd 02-kafka && ./consume-console.sh

# Crear tópico manualmente
cd 02-kafka && ./create-topics.sh
```

## UI

Kafka UI disponible en `http://localhost:48085`.
