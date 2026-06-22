# Experimentación Distribuida

## CrossValidator

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

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

El `CrossValidator` ejecuta 3 folds × 18 combinaciones = 54 entrenamientos en paralelo.

## MLflow Tracking

Cada experimento registra:

### Parámetros
- `numTrees`, `maxDepth`, `minInstancesPerNode`
- `featureCols`, `model_type`, `target`

### Métricas
- `mae`, `rmse`, `r2`
- `training_samples`

### Artefactos
- Modelo completo (PipelineModel)
- Notebook de entrenamiento

## Ver Resultados

Abrir MLflow UI en `http://localhost:45000`
