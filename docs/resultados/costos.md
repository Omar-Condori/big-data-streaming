# Estimación de Costos

## Infraestructura Local (Docker)

| Recurso | Costo |
|---------|-------|
| Docker Desktop | Gratuito |
| Imágenes Docker | Gratuitas (Open Source) |
| API TickDB | Gratuita (plan básico) |

## Proyección Cloud (AWS)

| Servicio | Equivalente | Costo Mensual Estimado |
|----------|-------------|----------------------|
| Kafka | MSK (broker t3.small) | ~$75/mes |
| Spark | EMR (3 nodos m5.xlarge) | ~$350/mes |
| HDFS | S3 (1TB) | ~$23/mes |
| Monitoreo | Prometheus/Grafana (EC2 t3.medium) | ~$35/mes |
| MLflow | EC2 t3.medium + S3 | ~$40/mes |
| **Total** | | **~$523/mes** |

## Optimizaciones

- Usar **Kafka Serverless** para reducir costos fijos
- **Spot instances** para Spark (hasta 70% descuento)
- **S3 Intelligent Tiering** para almacenamiento
- **Grafana Cloud** free tier para equipos pequeños
