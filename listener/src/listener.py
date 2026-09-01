"""Binance WebSocket listener → Kafka producer.

Runs as one pod per (stream_type, listener node). Opens a single-stream
WebSocket connection to Binance and produces every event to the raw market data
topic.

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
import time

import websockets
from confluent_kafka import Producer, KafkaException

from config import BINANCE_WS_HOST, KAFKA_TOPIC, STREAMS, TOPIC_PARTITIONS
from partitioning import message_key, partition_for



def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("listener")


def _read_env():
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

    log.info("Connecting to Kafka bootstrap %s", bootstrap)
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
    })


# --- Core loop --------------------------------------------------------------

def _build_ws_url(stream_type: str, symbol: str):
    binance_name = STREAMS[stream_type]["binance_name"]
    return f"{BINANCE_WS_HOST}/ws/{symbol}@{binance_name}?timeUnit=MICROSECOND"


def _build_message_key(stream_type: str, event: dict):
    """Build the Kafka message key, or None if the event lacks its id field."""
    cfg = STREAMS[stream_type]
    event_id = event.get(cfg["id_field"])
    if event_id is None:
        return None
    return message_key(cfg["key_prefix"], event_id)


async def _consume_and_produce(
    url: str,
    stream_type: str,
    symbol: str,
    source_id: str,
    producer: Producer,
    partition: int,
    log,
    stop_event: asyncio.Event,
):
    """Forward every WebSocket event to Kafka until stop_event is set."""
    log.info("Connecting to %s", url)

    def _on_delivery(err, msg):
        # Only failures are reported. Successes are implied by their absence
        # and confirmed by the flush at shutdown.
        if err is not None:
            log.error("Delivery failed for key=%s: %s", msg.key(), err)

    async for ws in websockets.connect(url, ping_interval=20, ping_timeout=20):
        log.info("WebSocket connected")
        try:
            async for raw in ws:
                if stop_event.is_set():
                    return

                try:
                    event = json.loads(raw)
                except json.JSONDecodeError as e:
                    log.warning("Event is not valid JSON (%s), dropping: %s", e, raw[:200])
                    continue

                key = _build_message_key(stream_type, event)
                if key is None:
                    log.warning("Event missing its id field, dropping: %s", raw[:200])
                    continue

                if stream_type in ("trade", "aggTrade"):
                    binance_ts_us = int(event["T"])
                else:  # depth
                    binance_ts_us = int(event["E"])
                binance_ts_ns = binance_ts_us * 1000

                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        partition=partition,
                        key=key.encode("utf-8"),
                        value=raw if isinstance(raw, bytes) else raw.encode("utf-8"),
                        headers=[
                            ("source_id", source_id.encode("utf-8")),
                            ("stream_type", stream_type.encode("utf-8")),
                            ("symbol", symbol.encode("utf-8")),
                            ("listener_ts_ns", str(time.time_ns()).encode("utf-8")),
                            ("binance_ts_ns", str(binance_ts_ns).encode("utf-8")),
                        ],
                        callback=_on_delivery,
                    )
                except BufferError:
                    # The producer's local queue is bounded. Drain it and drop
                    # this event rather than blocking the WebSocket read, which
                    # would build back-pressure all the way to Binance.
                    log.warning("Producer queue full, draining and dropping one event")
                    producer.poll(1.0)
                    continue

                # Non-blocking; surfaces delivery callbacks promptly.
                producer.poll(0)

        except websockets.ConnectionClosed as e:
            log.warning("WebSocket closed (%s), reconnecting", e)
            if stop_event.is_set():
                return
            continue


async def main():
    log = _configure_logging()
    env = _read_env()

    # Fixed for the life of the process: this pod serves exactly one stream and
    # one symbol, so its partition never changes.
    partition = partition_for(env["stream_type"], env["symbol"], TOPIC_PARTITIONS)

    log.info(
        "Starting listener: stream=%s symbol=%s source_id=%s topic=%s partition=%d",
        env["stream_type"], env["symbol"], env["source_id"], KAFKA_TOPIC, partition,
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
            symbol=env["symbol"],
            source_id=env["source_id"],
            producer=producer,
            partition=partition,
            log=log,
            stop_event=stop_event,
        )
    finally:
        log.info("Flushing Kafka producer")
        try:
            remaining = producer.flush(timeout=10)
            if remaining > 0:
                log.warning("%d messages still queued at shutdown", remaining)
        except KafkaException as e:
            log.error("Error flushing producer on shutdown: %s", e)
        log.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())