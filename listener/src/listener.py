"""Binance WebSocket listener → Kafka producer.
 
Runs as one pod per (stream_type, listener node).
Opens a single-stream WebSocket connection to Binance, produces every event to the market-data Kafka topic
 
Environment variables required:
  STREAM_TYPE       one of 'trade' | 'depth' | 'aggtrade' (case-insensitive)
  SYMBOL            Binance symbol in lowercase, e.g. 'btcusdt'
  SOURCE_ID         unique identifier for this source (the Kubernetes node
                    name, supplied via the downward API)
  KAFKA_BOOTSTRAP   Kafka bootstrap server, e.g.
                    'market-data-kafka-bootstrap.kafka.svc.cluster.local:9092'
"""
 
from __future__ import annotations
 
import asyncio
import json
import logging
import os
import signal
import sys
 
import websockets
from confluent_kafka import Producer, KafkaException
 
from config import BINANCE_WS_HOST, KAFKA_TOPIC, STREAMS
 
 
 
def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("listener")
 
 
def _read_env():
    """Read and validate required environment variables. STREAM_TYPE is
    normalised to lowercase so downstream code has one canonical form."""
    required = ["STREAM_TYPE", "SYMBOL", "SOURCE_ID", "KAFKA_BOOTSTRAP"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing required env vars: {missing}")
 
    stream_type = os.environ["STREAM_TYPE"].lower()
    if stream_type not in STREAMS:
        raise SystemExit(
            f"STREAM_TYPE={os.environ['STREAM_TYPE']!r} not supported. "
            f"Must be one of {list(STREAMS)} (case-insensitive)."
        )
 
    return {
        "stream_type": stream_type,
        "symbol": os.environ["SYMBOL"].lower(),
        "source_id": os.environ["SOURCE_ID"],
        "kafka_bootstrap": os.environ["KAFKA_BOOTSTRAP"],
    }
 
 
def _build_producer(bootstrap: str, log):
    """Build a Kafka producer configured for durable, idempotent writes.
 
    acks=all + enable.idempotence=true gives exactly-once semantics per
    producer session as long as the ISR (in-sync replicas) invariant holds.
    """
    log.info("Connecting to Kafka bootstrap %s", bootstrap)
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
    })
 
 
def _delivery_callback(err, msg, log):
    """Called after each produce attempt completes.
 
    Only logs on failure — success is inferred by absence of errors and
    tracked by producer.flush() at shutdown.
    """
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
 
 
 
def _build_ws_url(stream_type: str, symbol: str):
    """Build the Binance single-stream URL for the given stream type."""
    binance_name = STREAMS[stream_type]["binance_name"]
    return f"{BINANCE_WS_HOST}/ws/{symbol}@{binance_name}"
 
 
def _build_message_key(stream_type: str, event: dict):
    """Build the Kafka message key: '<prefix>:<event_id>'.
 
    Returns None if the event doesn't have the expected ID field.
    """
    cfg = STREAMS[stream_type]
    event_id = event.get(cfg["id_field"])
    if event_id is None:
        return None
    return f"{cfg['key_prefix']}:{event_id}"
 
 
async def _consume_and_produce(
    url: str,
    stream_type: str,
    source_id: str,
    producer: Producer,
    log,
    stop_event: asyncio.Event,
):
    """Open the WebSocket and forward every event to Kafka until stop_event fires."""
    log.info("Connecting to %s", url)
 
    def _cb(err, msg):
        _delivery_callback(err, msg, log)
 
    async for ws in websockets.connect(url, ping_interval=20, ping_timeout=20):
        log.info("WebSocket connected")
        try:
            async for raw in ws:
                if stop_event.is_set():
                    return
 
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as e:
                    log.warning("Failed to parse event as JSON (%s), dropping: %s", e, raw[:200])
                    continue
 
                key = _build_message_key(stream_type, event)
                if key is None:
                    log.warning("Event missing ID field, dropping: %s", raw[:200])
                    continue
 
                # confluent-kafka's produce() is non-blocking; delivery reports
                # are surfaced via periodic poll() calls.
                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=key.encode("utf-8"),
                        value=raw if isinstance(raw, bytes) else raw.encode("utf-8"),
                        headers=[
                            ("source_id", source_id.encode("utf-8")),
                            ("stream_type", stream_type.encode("utf-8")),
                        ],
                        callback=_cb,
                    )
                except BufferError:
                    # Producer queue full — poll to drain, then try again.
                    log.warning("Producer queue full, draining and retrying")
                    producer.poll(1.0)
                    continue
 
                producer.poll(0)
 
        except websockets.ConnectionClosed as e:
            log.warning("WebSocket connection closed (%s), will reconnect", e)
            if stop_event.is_set():
                return
            continue
 
 
async def main():
    log = _configure_logging()
    env = _read_env()
    log.info(
        "Starting listener: stream=%s symbol=%s source_id=%s topic=%s",
        env["stream_type"], env["symbol"], env["source_id"], KAFKA_TOPIC,
    )
 
    producer = _build_producer(env["kafka_bootstrap"], log)
    url = _build_ws_url(env["stream_type"], env["symbol"])
    stop_event = asyncio.Event()
 
    def _handle_signal(signame: str):
        log.info("Received %s, shutting down gracefully", signame)
        stop_event.set()
 
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        loop.add_signal_handler(getattr(signal, signame), _handle_signal, signame)
 
    try:
        await _consume_and_produce(
            url=url,
            stream_type=env["stream_type"],
            source_id=env["source_id"],
            producer=producer,
            log=log,
            stop_event=stop_event,
        )
    finally:
        log.info("Flushing Kafka producer")
        try:
            remaining = producer.flush(timeout=10)
            if remaining > 0:
                log.warning("%d messages still in queue at shutdown", remaining)
        except KafkaException as e:
            log.error("Error flushing producer on shutdown: %s", e)
        log.info("Shutdown complete")
 
 
if __name__ == "__main__":
    asyncio.run(main())
 
