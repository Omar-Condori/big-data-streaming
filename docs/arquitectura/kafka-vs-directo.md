# Kafka vs Directo

## Comparación de Arquitecturas

| Aspecto | Con Kafka | Sin Kafka (Directo) |
|---------|-----------|---------------------|
| Flujo | TickDB → Producer → Kafka → Spark | TickDB → TCP Socket → Spark |
| **Ventajas** | Replay de eventos, múltiples consumidores, buffer de carga | Menos componentes, menor latencia, simplicidad |
| **Desventajas** | Componente extra, ~100ms latencia adicional | Sin replay, sin buffer, un solo consumidor |
| **Latencia** | ~150-300ms | ~50-100ms |
| **Resiliencia** | Alta (Kafka persiste) | Baja (sin persistencia) |
| **Escalabilidad** | Múltiples consumidores | Un solo consumidor |

## ¿Cuándo usar cada uno?

### Con Kafka
- Necesitas reprocesar eventos históricos
- Múltiples consumidores (streaming + batch + ML)
- Tolerancia a fallos del consumidor
- Curso/entrega académica

### Sin Kafka
- Prototipado rápido
- Un solo consumidor
- Mínima latencia
- Simplicidad operativa
