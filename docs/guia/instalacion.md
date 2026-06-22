# Instalación

## 1. Clonar el repositorio

```bash
git clone https://github.com/anomalyco/tickdb-streaming.git
cd tickdb-streaming
```

## 2. Levantar infraestructura

```bash
cd 01-docker
docker compose up -d
```

Esto levanta:
- Zookeeper + Kafka + Schema Registry + Kafka UI
- HDFS (NameNode + DataNode)
- Prometheus + Node Exporter + Pushgateway
- Grafana (dashboard precargado)
- MLflow

## 3. Verificar servicios

```bash
docker compose ps
```

Todos los servicios deben mostrar `Up` o `healthy`.

## 4. Configurar API Key

```bash
export TICKDB_API_KEY="tu-api-key"
```

O créala en un archivo `.env`:

```bash
echo "TICKDB_API_KEY=tu-api-key" > .env
```

## 5. Iniciar el productor

```bash
cd 03-producer
pip install -r requirements.txt
python tickdb_producer.py
```

## 6. Iniciar Jupyter/Spark

```bash
cd 04-spark
docker compose up -d
```

Abrir `http://localhost:8888` con token `tickdb`.
