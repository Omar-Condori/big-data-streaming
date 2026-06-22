# Métricas del Pipeline

## Rendimiento

| Métrica | Valor |
|---------|-------|
| Eventos por batch | ~23 |
| Throughput (entrada) | ~7.66 eventos/s |
| Throughput (procesado) | ~35.42 eventos/s |
| Latencia total | ~649ms por batch |
| Latencia TickDB → Kafka | ~50-200ms |
| Mensajes en tópico Kafka | 10,281+ |
| Lag de offsets | 0 |

## Resultados de Pruebas Controladas

| Prueba | Trigger | Watermark | Throughput (evt/s) | Lag | Latencia (ms) |
|--------|---------|------------|--------------------|-----|---------------|
| 1 | 5s | 10 min | 7.66 | 0 | 649 |
| 2 | 3s | 10 min | 8.12 | 0 | 580 |
| 3 | 10s | 10 min | 7.20 | 0 | 800 |

## Desglose de Latencia por Componente

| Componente | Duración (ms) |
|------------|---------------|
| addBatch | 574 |
| commitOffsets | 17 |
| latestOffset | 8 |
| queryPlanning | 14 |
| walCommit | 35 |
| **Total (triggerExecution)** | **649** |

## ML

| Métrica | Valor |
|---------|-------|
| MAE | $12.02 |
| RMSE | $18.41 |
| R² | >0.99 |
| Precisión | 99.98% |
| Feature más importante | prev_price (78.6%) |

## Umbrales de Alerta

| Métrica | Verde | Amarillo | Rojo |
|---------|-------|----------|------|
| Latencia (ms) | < 500 | 500-1000 | > 1000 |
| Throughput (evt/s) | > 5 | 2-5 | < 2 |
| Errores | 0 | 1-5 | > 5 |
| Backpressure | 0 | 1-10 | > 10 |
