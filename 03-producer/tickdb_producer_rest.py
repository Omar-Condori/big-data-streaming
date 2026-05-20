#!/usr/bin/env python3
import json
import time
import threading
import requests
from datetime import datetime
from kafka import KafkaProducer
import os

API_KEY = "xs524g6UkSwLKRUv9jQL-JZpaKcOPXu1"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "tickdb-market-data"
INTERVAL = 5

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

symbols = {
    "forex": ["XAUUSD", "GBPUSD", "EURUSD", "USDJPY"],
    "indices": ["SPX", "NDX", "HSI", "DJIA", "FTSE", "DAX"],
    "us_stocks": ["AAPL.US", "TSLA.US", "MSFT.US", "NVDA.US", "GOOGL.US", "AMZN.US", "META.US"],
    "hk_stocks": ["700.HK", "9988.HK", "3690.HK"],
    "crypto": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
}

all_symbols = []
for category in symbols.values():
    all_symbols.extend(category)

def categorize_symbol(symbol):
    for market, syms in symbols.items():
        if symbol in syms:
            return market
    return "unknown"

def fetch_and_send():
    url = f"https://api.tickdb.ai/v1/market/ticker?symbols={','.join(all_symbols)}"
    headers = {"X-API-Key": API_KEY}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0:
                tickers = data.get("data", [])
                for ticker in tickers:
                    event = {
                        "symbol": ticker.get("symbol"),
                        "last_price": float(ticker.get("last_price", 0)),
                        "volume_24h": float(ticker.get("volume_24h", 0)),
                        "high_24h": float(ticker.get("high_24h", 0)),
                        "low_24h": float(ticker.get("low_24h", 0)),
                        "price_change_24h": float(ticker.get("price_change_24h", 0)),
                        "price_change_percent_24h": float(ticker.get("price_change_percent_24h", 0)),
                        "timestamp": ticker.get("timestamp"),
                        "event_timestamp": int(datetime.now().timestamp() * 1000),
                        "market": categorize_symbol(ticker.get("symbol"))
                    }
                    producer.send(TOPIC, value=event)
                    print(f"📊 {event['symbol']}: ${event['last_price']} ({event['price_change_percent_24h']:+.2f}%)")
        else:
            print(f"Error API: {resp.status_code}")
    except Exception as e:
        print(f"Error: {e}")

def run_polling():
    while True:
        fetch_and_send()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    print("=" * 60)
    print("  TICKDB STREAMING PRODUCER (REST API)")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Tópico: {TOPIC}")
    print(f"  Intervalo: {INTERVAL}s")
    print("=" * 60)
    
    while True:
        try:
            fetch_and_send()
            print(f"✅ Cycle complete. Waiting {INTERVAL}s...")
        except Exception as e:
            print(f"❌ Error en ciclo: {e}")
        
        time.sleep(INTERVAL)