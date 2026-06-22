#!/usr/bin/env python3
"""TickDB Producer — WebSocket + REST -> Kafka (Avro) + Prometheus"""
import json
import os
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from kafka import KafkaProducer
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tickdb-producer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.environ.get("TICKDB_API_KEY", "")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = "tickdb-market-data"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
MODE = os.environ.get("TICKDB_MODE", "rest")  # "rest" | "websocket"

# ---------------------------------------------------------------------------
# Schema (Avro)
# ---------------------------------------------------------------------------
AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "MarketTick",
    "namespace": "com.tickdb",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "last_price", "type": "double"},
        {"name": "volume_24h", "type": "double", "default": 0.0},
        {"name": "high_24h", "type": "double", "default": 0.0},
        {"name": "low_24h", "type": "double", "default": 0.0},
        {"name": "price_change_24h", "type": "double", "default": 0.0},
        {"name": "price_change_percent_24h", "type": "double", "default": 0.0},
        {"name": "timestamp", "type": "long", "default": 0},
        {"name": "event_timestamp", "type": "long"},
        {"name": "market", "type": "string"},
    ],
})


def register_schema() -> Optional[int]:
    """Register the Avro schema in Schema Registry. Returns schema_id or None."""
    if not SCHEMA_REGISTRY_URL:
        return None
    try:
        url = f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC}-value/versions"
        resp = requests.post(url, json={"schema": AVRO_SCHEMA}, timeout=5)
        if resp.status_code in (200, 201):
            schema_id = resp.json().get("id")
            log.info("Schema registered — id=%s", schema_id)
            return schema_id
        if resp.status_code == 409:
            log.info("Schema already exists")
            return None
        log.warning("Schema registry error: %s %s", resp.status_code, resp.text)
    except requests.RequestException as exc:
        log.warning("Cannot reach schema registry: %s", exc)
    return None

# ---------------------------------------------------------------------------
# Symbol catalogue
# ---------------------------------------------------------------------------
SYMBOLS: dict[str, list[str]] = {
    "forex": ["XAUUSD", "GBPUSD", "EURUSD", "USDJPY"],
    "indices": ["SPX", "NDX", "HSI"],
    "us_stocks": ["AAPL.US", "TSLA.US", "MSFT.US", "NVDA.US", "GOOGL.US", "AMZN.US", "META.US"],
    "hk_stocks": ["700.HK", "9988.HK", "3690.HK"],
    "crypto": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
}

ALL_SYMBOLS = [s for syms in SYMBOLS.values() for s in syms]

# Build reverse lookup once
_SYMBOL_TO_MARKET: dict[str, str] = {}
for market, syms in SYMBOLS.items():
    for sym in syms:
        _SYMBOL_TO_MARKET[sym] = market


def categorize_symbol(symbol: str) -> str:
    return _SYMBOL_TO_MARKET.get(symbol, "unknown")

# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------
registry = CollectorRegistry()
price_gauge = Gauge("last_price", "Last market price", ["symbol", "market"], registry=registry)
events_counter = Counter("kafka_events_total", "Events sent to Kafka", ["market"], registry=registry)
producer_health = Gauge("producer_healthy", "Producer health (1=ok)", registry=registry)


def push_metrics() -> None:
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="tickdb-producer", registry=registry)
    except Exception as exc:
        log.warning("Prometheus push error: %s", exc)

# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------
producer: KafkaProducer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
    max_in_flight_requests_per_connection=5,
)

# ---------------------------------------------------------------------------
# Event builder
# ---------------------------------------------------------------------------
def build_event(ticker: dict[str, Any]) -> dict[str, Any]:
    symbol = ticker.get("symbol", "UNKNOWN")
    return {
        "symbol": symbol,
        "last_price": float(ticker.get("last_price", 0)),
        "volume_24h": float(ticker.get("volume_24h", 0)),
        "high_24h": float(ticker.get("high_24h", 0)),
        "low_24h": float(ticker.get("low_24h", 0)),
        "price_change_24h": float(ticker.get("price_change_24h", 0)),
        "price_change_percent_24h": float(ticker.get("price_change_percent_24h", 0)),
        "timestamp": ticker.get("timestamp") or int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "event_timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "market": categorize_symbol(symbol),
    }


def send_event(event: dict[str, Any]) -> None:
    producer.send(TOPIC, value=event)
    market = event["market"]
    price_gauge.labels(symbol=event["symbol"], market=market).set(event["last_price"])
    events_counter.labels(market=market).inc()
    log.info("  %-12s $%8.2f  (%+.2f%%)", event["symbol"], event["last_price"],
             event["price_change_percent_24h"])

# ---------------------------------------------------------------------------
# REST polling mode
# ---------------------------------------------------------------------------
def fetch_over_rest() -> None:
    url = f"https://api.tickdb.ai/v1/market/ticker?symbols={','.join(ALL_SYMBOLS)}"
    headers = {"X-API-Key": API_KEY}
    log.info("REST polling %d symbols...", len(ALL_SYMBOLS))
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        log.warning("API code != 0: %s", data)
        return
    tickers = data.get("data", [])
    log.info("Got %d tickers — sending to Kafka", len(tickers))
    for ticker in tickers:
        send_event(build_event(ticker))
    producer.flush()
    push_metrics()


def rest_loop() -> None:
    register_schema()
    producer_health.set(1)
    while True:
        try:
            fetch_over_rest()
        except requests.RequestException as exc:
            log.error("REST error: %s", exc)
            producer_health.set(0)
        except Exception as exc:
            log.error("Unexpected error: %s", exc)
            producer_health.set(0)
        log.info("Sleeping %ds...", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)

# ---------------------------------------------------------------------------
# WebSocket mode (true real-time)
# ---------------------------------------------------------------------------
def ws_loop() -> None:
    import websocket  # type: ignore[import]

    register_schema()
    producer_health.set(1)
    ws_url = "wss://api.tickdb.ai/v1/ws/market"

    def on_message(_ws: Any, message: str) -> None:
        try:
            data = json.loads(message)
            for ticker in data.get("data", [data]):
                send_event(build_event(ticker))
            producer.flush()
            push_metrics()
        except Exception as exc:
            log.error("WS message error: %s", exc)

    def on_error(_ws: Any, error: Any) -> None:
        log.error("WS error: %s", error)
        producer_health.set(0)

    def on_close(_ws: Any, close_status_code: int, close_msg: str) -> None:
        log.warning("WS closed (%s): %s", close_status_code, close_msg)
        producer_health.set(0)

    def on_open(ws: Any) -> None:
        log.info("WS connected — subscribing...")
        ws.send(json.dumps({
            "action": "subscribe",
            "symbols": ALL_SYMBOLS,
            "api_key": API_KEY,
        }))

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            log.error("WS connection error: %s", exc)
        log.info("Reconnecting in 5s...")
        time.sleep(5)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  TICKDB STREAMING PRODUCER")
    print(f"  Kafka:    {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topic:    {TOPIC}")
    print(f"  Mode:     {MODE}")
    print(f"  Symbols:  {len(ALL_SYMBOLS)}")
    print(f"  API Key:  {'✓' if API_KEY else '✗ MISSING!'}")
    print("=" * 60)

    if MODE == "websocket":
        ws_loop()
    else:
        rest_loop()
