# Inferencia en Streaming

## Pipeline ML Completo

```
1. STREAMS: Kafka → Spark (pipeline streaming)
2. COLECTA: 5-10 min de datos históricos en micro-batches
3. FEATURES: SMA, RSI, Volatilidad con ventanas deslizantes
4. MODELO: Random Forest (100 árboles) entrenado con Spark MLlib
5. PREDICCIÓN: En tiempo real dentro del streaming
```

## Carga del Modelo

```python
from pyspark.ml import PipelineModel

model = PipelineModel.load("/home/jovyan/work/models/btc_rf_model")
```

El modelo se carga una vez al inicio y se reusa en cada micro-batch.

## Feature Engineering en Streaming

```python
from pyspark.sql.window import Window

w = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween

df_features = df_valid \
    .withColumn("prev_price", lag("last_price", 1)
                .over(Window.partitionBy("symbol").orderBy("timestamp"))) \
    .withColumn("price_change", col("last_price") - col("prev_price")) \
    .withColumn("SMA_5", avg("last_price").over(w(-4, 0))) \
    .withColumn("SMA_10", avg("last_price").over(w(-9, 0))) \
    .withColumn("SMA_20", avg("last_price").over(w(-19, 0))) \
    .withColumn("volatility", stddev("last_price").over(w(-19, 0)))
```

### Features Generadas

| Feature | Descripción | Ventana |
|---------|-------------|---------|
| prev_price | Precio anterior | 1 periodo |
| price_change | Diferencia precio actual vs anterior | — |
| SMA_5 | Media móvil simple | 5 periodos |
| SMA_10 | Media móvil simple | 10 periodos |
| SMA_20 | Media móvil simple | 20 periodos |
| volatility | Desviación estándar móvil | 20 periodos |

## Inferencia con foreachBatch

```python
def predict_prices(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    features_df = batch_df \
        .withColumn("prev_price", lag("last_price", 1)
                    .over(Window.partitionBy("symbol").orderBy("timestamp"))) \
        .withColumn("SMA_5", avg("last_price")
                    .over(w(-4, 0)))
    
    preds = model.transform(features_df)
    preds.select("symbol", "last_price", "prediction", "timestamp").show()

query = df_valid \
    .writeStream \
    .foreachBatch(predict_prices) \
    .trigger(processingTime="10 seconds") \
    .start()
```

## Resultados de Predicción

| Símbolo | Precio Actual | Predicción | Error |
|---------|--------------|------------|-------|
| BTCUSDT | $76,517 | $76,529 | +$12 |

## Métricas del Modelo

| Métrica | Valor |
|---------|-------|
| MAE (Error Absoluto Medio) | **$12.02** |
| RMSE (Raíz del Error Cuadrático Medio) | **$18.41** |
| Precisión | **99.98%** |
| Feature más importante | prev_price (78.6%) |

## Almacenamiento del Modelo

1. **MLflow**: Artefacto del experimento (`http://localhost:45000`)
2. **Local**: `/home/jovyan/work/models/btc_rf_model`
3. **HDFS**: `hdfs://hdfs-namenode:9000/tickdb/models/btc_rf_model`
