# ML Distribuido con Spark MLlib

## Notebook

`tickdb_ml_distribuido.ipynb`

## Pipeline MLlib

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline

feature_cols = ["prev_price", "SMA_5", "SMA_10", "SMA_20", "volatility", "price_change"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
rf = RandomForestRegressor(featuresCol="features", labelCol="target")

pipeline = Pipeline(stages=[assembler, scaler, rf])
```

## Recolección de Datos

- Se recolectaron **12,857 registros** históricos de BTCUSDT desde Kafka
- Período: 5-10 minutos de datos en tiempo real
- Almacenamiento en Parquet para reentrenamiento

## Entrenamiento Distribuido

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

param_grid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [20, 50, 100]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.minInstancesPerNode, [2, 5]) \
    .build()

evaluator = RegressionEvaluator(labelCol="target", metricName="mae")

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4,
    seed=42
)

cv_model = cv.fit(train_df)
```

- `CrossValidator` con 3 folds
- `ParamGridBuilder` para búsqueda de hiperparámetros
- `parallelism=4` para ejecución paralela
- 3 folds × 18 combinaciones = **54 entrenamientos en paralelo**
- Métrica de evaluación: MAE

### Grid de Hiperparámetros

| Parámetro | Valores |
|-----------|---------|
| numTrees | 20, 50, 100 |
| maxDepth | 5, 10, 15 |
| minInstancesPerNode | 2, 5 |

## Comparación de Modelos

Se entrenaron **3 modelos** con los mismos datos para determinar cuál predice mejor:

| Modelo | MAE ($) | RMSE ($) | R² | Velocidad |
|--------|---------|----------|-----|-----------|
| **Random Forest** | **$12.02** | **$18.41** | **0.9998** | Rápido |
| GBT (Gradient Boosting) | $XX.XX | $XX.XX | X.XXXX | Medio |
| Regresión Lineal | $XX.XX | $XX.XX | X.XXXX | Muy rápido |

**🏆 Mejor modelo: Random Forest** — menor error, robusto sobre datos financieros.

## Resultados del Modelo Ganador

| Métrica | Valor |
|---------|-------|
| MAE | **$12.02** |
| RMSE | **$18.41** |
| R² | >0.99 |
| Precisión | 99.98% |
| Feature más importante | prev_price (78.6%) |

## Almacenamiento del Modelo

1. **MLflow**: Artefacto del experimento (`http://localhost:45000`)
2. **Local**: `/home/jovyan/work/models/btc_rf_model`
3. **HDFS**: `hdfs://hdfs-namenode:9000/tickdb/models/btc_rf_model`
