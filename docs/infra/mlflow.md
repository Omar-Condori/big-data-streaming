# MLflow

## Acceso

- **URL**: http://localhost:45000

## Configuración

| Parámetro | Valor |
|-----------|-------|
| Backend Store | file:///mlflow |
| Artifact Root | file:///mlflow/artifacts |
| Puerto | 5000 (mapeado a 45000) |

## Experimentos

### Experimento: `tickdb-price-prediction`

Parámetros registrados:
- `numTrees` — Número de árboles
- `maxDepth` — Profundidad máxima
- `minInstancesPerNode` — Mínimo de instancias por nodo
- `featureCols` — Columnas de features
- `model_type` — Tipo de modelo
- `target` — Símbolo objetivo

Métricas registradas:
- `mae` — Error absoluto medio
- `rmse` — Error cuadrático medio
- `r2` — Coeficiente de determinación
- `training_samples` — Muestras de entrenamiento

## Modelos

Los modelos entrenados se guardan:
1. En MLflow (artifact store)
2. En el filesystem local (`/home/jovyan/work/models/`)
3. En HDFS (`hdfs://hdfs-namenode:9000/tickdb/models/`)
