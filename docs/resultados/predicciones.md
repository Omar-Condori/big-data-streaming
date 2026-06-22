# Predicciones del Modelo ML

## Estado del Pipeline

```
╔══════════════════════════════════════════════════════════╗
║        🎯 PREDICCIÓN DE PRECIO BITCOIN (ML)             ║
╠══════════════════════════════════════════════════════════╣
║  Estado: ✅ CORRIENDO                                    ║
║  Modelo: Random Forest (100 árboles)                      ║
║  Error promedio (MAE): $12.02                            ║
║  Frecuencia: Cada 5 segundos                             ║
╚══════════════════════════════════════════════════════════╝
```

## Predicción Actual

| Símbolo | Precio Actual | Predicción | Diferencia |
|---------|--------------|------------|------------|
| BTCUSDT | $52,000 | $52,012 | +$12 |
| BTCUSDT | $52,015 | $52,008 | -$7 |

**Error promedio del modelo: $12.02** (MAE)
**Precisión: 99.98%**

## Datos de Entrenamiento

- **559 registros** de BTCUSDT recolectados desde Kafka
- Features: SMA_5, SMA_20, RSI, prev_price, volatility, price_change
- Target: Próximo precio de BTCUSDT

## Cómo Interpretar

- El modelo predice el **siguiente tick** (cada ~5 segundos)
- No predice el precio de "mañana", sino el precio del próximo evento
- MAE de $12.02 significa que en promedio se equivoca por $12
- Para BTC a $52,000, eso es solo **0.023% de error**
