FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir websocket-client kafka-python requests

COPY *.py .

CMD ["python", "tickdb_producer_rest.py"]