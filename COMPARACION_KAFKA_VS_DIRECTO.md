# Comparación: Con Kafka vs Sin Kafka (Directo)

## Tu pregunta: ¿Necesito Kafka para datos en tiempo real?

**Respuesta: Depende de tu caso de uso.**

---

## OPCIÓN 1: Con Kafka (más completo)

```
TickDB WebSocket → Productor Python → Kafka → Spark → Salida
```

**Cuándo usarlo:**
- ✅ Necesitas replay de eventos (reprocesar datos)
- ✅ Múltiples consumidores (Spark + otra app)
- ✅ Buffer ante picos de carga
- ✅ Cumplir plantilla del curso (sesiones 6,7,8)
- ✅ Necesitas mostrar topic Kafka

**Pros:**
- Resiliente
- Múltiples consumidores
- Persistencia de datos

**Contras:**
- +1 componente
- Latencia extra (~100ms)
- Más complejo

---

## OPCIÓN 2: Sin Kafka (más simple, menor latencia)

```
TickDB WebSocket → TCP Socket → Spark Streaming → Salida
```

**Cuándo usarlo:**
- ✅ Solo un consumidor
- ✅ No necesitas replay
- ✅ Simplicidad
- ✅ Menor latencia (~50ms)
- ✅ Solo procesar datos reales

**Pros:**
- Menos componentes
- Simplicidad
- Menor latencia

**Contras:**
- Sin buffer
- Sin replay
- Un solo consumidor

---

## Comparación técnica

| Aspecto | Con Kafka | Sin Kafka |
|---------|-----------|-----------|
| **Latencia** | ~150ms | ~50ms |
| **Componentes** | 4 (WS, Python, Kafka, Spark) | 3 (WS, Python, Spark) |
| **Replay** | ✅ Sí | ❌ No |
| **Múltiples consumidores** | ✅ Sí | ❌ No |
| **Costo** | Mayor | Menor |
| **Complejidad** | Media | Baja |

---

## ¿Cuál elegir según tu curso?

### Si tu docente EXIGE Kafka (como en la plantilla):
Usa la versión **con Kafka** (`tickdb_producer.py` + `tickdb_spark_streaming.ipynb`)

### Si solo necesitas procesar datos tiempo real:
Usa la versión **sin Kafka** (`tickdb_producer_directo.py` + `tickdb_spark_directo.ipynb`)

---

## Ejecutar versión SIN Kafka

### 1. Iniciar producer (TCP)
```bash
cd tickdb-streaming/python
pip install websocket-client
python tickdb_producer_directo.py
```

### 2. Ejecutar Spark
Abre `spark/tickdb_spark_directo.ipynb` en Jupyter

---

## Ambos cumple la plantilla?

La versión **con Kafka** cumple el 100% de la plantilla del docente:
- Tópico Kafka ✅
- Productor/Consumidor ✅
- Contrato evento ✅
- Spark Streaming ✅
- Ventanas y watermarking ✅
- Métricas latencia/throughput ✅
- Observabilidad ✅

La versión **sin Kafka** cubre:
- Datos en tiempo real ✅
- Spark Streaming ✅
- Métricas ✅
- Pero no tiene tópico Kafka (no cumple S6)

**Recomendación:** Usa la versión **con Kafka** para entregar tu trabajo según la plantilla.