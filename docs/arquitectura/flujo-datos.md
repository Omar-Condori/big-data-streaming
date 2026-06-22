# Flujo de Datos

## Esquema del Evento

Todos los eventos del pipeline siguen este esquema (Avro):

| Campo | Tipo | Descripción |
|-------|------|-------------|
| symbol | string | Símbolo del instrumento (ej: BTCUSDT) |
| last_price | double | Precio actual |
| volume_24h | double | Volumen en 24h |
| high_24h | double | Máximo en 24h |
| low_24h | double | Mínimo en 24h |
| price_change_24h | double | Cambio absoluto en 24h |
| price_change_percent_24h | double | Cambio porcentual en 24h |
| timestamp | long | Timestamp del exchange (ms) |
| event_timestamp | long | Timestamp de ingesta (ms) |
| market | string | Categoría del mercado |

## Ejemplo

```json
{
  "symbol": "BTCUSDT",
  "last_price": 76517.32,
  "volume_24h": 34474022,
  "high_24h": 77000.00,
  "low_24h": 76000.00,
  "price_change_24h": 39.32,
  "price_change_percent_24h": 0.05,
  "timestamp": 1779134400000,
  "event_timestamp": 1779134450000,
  "market": "crypto"
}
```

## Contrato Completo del Evento

| Campo | Tipo | Nullable | Descripción | Ejemplo |
|-------|------|-----------|-------------|---------|
| symbol | STRING | No | Identificador único del activo | "BTCUSDT" |
| last_price | DOUBLE | No | Precio de cierre actual | 76517.32 |
| volume_24h | DOUBLE | Sí | Volumen transado en 24h | 13430.51 |
| high_24h | DOUBLE | Sí | Precio máximo del día | 76850.00 |
| low_24h | DOUBLE | Sí | Precio mínimo del día | 76200.00 |
| price_change_24h | DOUBLE | Sí | Diferencia de precio | 39.32 |
| price_change_percent_24h | DOUBLE | Sí | Porcentaje de cambio | 0.05 |
| timestamp | BIGINT | No | Timestamp origen (epoch ms) | 1779201261000 |
| event_timestamp | BIGINT | No | Timestamp procesamiento | 1779201261839 |
| market | STRING | No | Clasificación de mercado | "crypto" |

## Mercados Soportados

| Mercado | Símbolos |
|---------|----------|
| Forex | XAUUSD, GBPUSD, EURUSD, USDJPY |
| Índices | SPX, NDX, HSI |
| US Stocks | AAPL.US, TSLA.US, MSFT.US, NVDA.US, GOOGL.US, AMZN.US, META.US |
| HK Stocks | 700.HK, 9988.HK, 3690.HK |
| Crypto | BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT |
