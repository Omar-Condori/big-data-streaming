# Feature Engineering con Spark

## Ventanas Deslizantes Nativas

En lugar de Pandas, usamos `Window.partitionBy().orderBy().rowsBetween()` para calcular features de series temporales de forma distribuida.

```python
from pyspark.sql.window import Window

w = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween

df_features = df \
    .withColumn("prev_price", lag("last_price", 1).over(Window.partitionBy("symbol").orderBy("timestamp"))) \
    .withColumn("price_change", col("last_price") - col("prev_price")) \
    .withColumn("SMA_5", avg("last_price").over(w(-4, 0))) \
    .withColumn("SMA_10", avg("last_price").over(w(-9, 0))) \
    .withColumn("SMA_20", avg("last_price").over(w(-19, 0))) \
    .withColumn("volatility", stddev("last_price").over(w(-19, 0)))
```

## Features Generadas

| Feature | Descripción | Ventana |
|---------|-------------|---------|
| prev_price | Precio anterior | 1 periodo |
| price_change | Diferencia precio actual vs anterior | — |
| SMA_5 | Media móvil simple | 5 periodos |
| SMA_10 | Media móvil simple | 10 periodos |
| SMA_20 | Media móvil simple | 20 periodos |
| volatility | Desviación estándar móvil | 20 periodos |
