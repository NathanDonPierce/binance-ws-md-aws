KAFKA_TOPIC = "market-data"

BINANCE_WS_HOST = "wss://stream.binance.com:9443"

STREAMS = {
    "trade":    {"binance_name": "trade",    "key_prefix": "t",  "id_field": "t"},
    "depth":    {"binance_name": "depth",    "key_prefix": "d",  "id_field": "u"},
    "aggtrade": {"binance_name": "aggTrade", "key_prefix": "aT", "id_field": "a"},
}