# Demo y Presentación

## Demo (5 minutos)

1. **Terminal 1**: Mostrar Docker Compose ejecutándose
   ```bash
   cd 01-docker && docker compose ps
   ```
   Todos los servicios en estado `Up` / `healthy`.

2. **Terminal 2**: Productor enviando datos a Kafka
   ```bash
   cd 03-producer && KAFKA_BOOTSTRAP_SERVERS=localhost:49092 \
     TICKDB_API_KEY=tu-key python3 tickdb_producer.py
   ```
   Ver los ticks en tiempo real: `BTCUSDT $63,901 (+0.34%)`

3. **Jupyter**: Spark Streaming funcionando
   - Abrir `http://localhost:8888` (token: `tickdb`)
   - Ejecutar `tickdb_spark_streaming.ipynb`
   - Mostrar ventanas de 1 minuto con agregaciones por mercado

4. **Predicción ML en tiempo real**
   - Ejecutar `ml_price_prediction.ipynb`
   - Mostrar: `MAE: $12.02` | `Precisión: 99.98%`

## Exposición (15 minutos)

| Sección | Duración | Contenido |
|---------|----------|-----------|
| **Arquitectura** | 3 min | Diagrama Kappa, componentes, flujo de datos |
| **Implementación** | 5 min | Código del productor, Spark Streaming, configuración |
| **Resultados** | 4 min | Métricas: throughput, latencia, ML (MAE, RMSE) |
| **Observabilidad** | 3 min | Dashboard Grafana, métricas clave, umbrales |

## Puntos Clave a Destacar

1. **Arquitectura Kappa**: Todo el procesamiento en streaming, sin capa batch separada
2. **Tiempo real**: Latencia de ~600ms por batch, trigger cada 5s
3. **ML Distribuido**: Random Forest con Spark MLlib, 54 entrenamientos en paralelo
4. **Escalabilidad**: Diseñado para escalar horizontalmente (más particiones, más ejecutores)
5. **Observabilidad**: Prometheus + Grafana, métricas en tiempo real
