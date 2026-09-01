"""Arbitrator entry point — Kafka consumers, producer, and the main loop.

Three responsibilities run inside one process, but the interesting decisions
have all been made and tested elsewhere. This file is glue.

    Raw ingest        Consume market-data-raw. For every message, route it to
                      the (stream_type, symbol) state machine and act on the
                      returned Outcome — forward the deduplicated event to
                      market-data-arb if it was a first sighting, and publish
                      any audit records the state machine produced.

    Kill confirmations Consume arbitration-audit as a separate consumer group,
                      filter for kill_confirmed messages, and pass them to
                      the matching state machine so its ignore list moves
                      from 'named for removal' to 'confirmed gone'. Every
                      other type on that topic (this arbitrator's own
                      tallies, verdicts, and stall warnings) is skipped.

    Window timer      Every second, call check_window on every state machine.
                      This is not optional — quiet streams never close their
                      windows without it, and a stall where the condemned
                      source is the last publisher is only ever reported
                      from check_window.

State machines are created lazily the first time a (stream_type, symbol) pair
is seen on the raw topic. That means restart recovery is naturally lazy too:
whichever pairs actually have live traffic get their state rebuilt, and no
effort is spent on pairs that no longer produce.

Everything in-memory is lost on restart. Every stream re-establishes its
quorum from cold, so verdicts pause for a few minutes after any restart. This
is correct behaviour — a fresh arbitrator has no basis to name a slowest
source until it has watched the fleet for a while — but worth knowing.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from typing import Dict, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

import config
from messages import KillConfirmed, decode, encode
from metrics import ArbitratorMetrics, serve as serve_metrics
from partitioning import audit_partition_for, partition_for
from state_machine import StreamArbitrator, StreamConfig

log = logging.getLogger("arbitrator")

TIMER_INTERVAL_SECONDS = 1.0


# --- Kafka setup -----------------------------------------------------------

def _build_raw_consumer(bootstrap: str) -> Consumer:

    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 60000,
    })


def _build_audit_consumer(bootstrap: str) -> Consumer:

    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"{config.CONSUMER_GROUP}-audit",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 60000,
    })


def _build_producer(bootstrap: str) -> Producer:

    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
    })


# --- State machine registry -----------------------------------------------

class StateMachineRegistry:


    def __init__(self, settings: dict):
        self._settings = settings
        self._machines: Dict[Tuple[str, str], StreamArbitrator] = {}
        self._lock = threading.Lock()

    def get_or_create(self, stream_type: str, symbol: str, now: float) -> StreamArbitrator:
        key = (stream_type, symbol)
        with self._lock:
            machine = self._machines.get(key)
            if machine is None:
                sc = StreamConfig(
                    stream_type=stream_type,
                    symbol=symbol,
                    expected_sources=self._settings["sources_per_stream"],
                    window_seconds=self._settings["window_seconds"],
                    dedup_lag_threshold=self._settings["dedup_lag"].get(
                        stream_type, 1000
                    ),
                    minimum_events=self._settings["minimum_events"],
                    stall_warn_seconds=self._settings["stall_warn_seconds"],
                    emit_tally_every=self._settings["emit_tally_every"],
                )
                machine = StreamArbitrator(sc, now=now)
                self._machines[key] = machine
                log.info(
                    "Created state machine for stream=%s symbol=%s",
                    stream_type, symbol,
                )
            return machine

    def get(self, stream_type: str, symbol: str) -> StreamArbitrator | None:
        with self._lock:
            return self._machines.get((stream_type, symbol))

    def all_machines(self):
        with self._lock:
            return list(self._machines.items())



def _header_value(headers, name: str) -> str | None:
    """Kafka headers arrive as [(name, bytes), ...] or None."""
    if not headers:
        return None
    for h_name, h_value in headers:
        if h_name == name:
            return h_value.decode("utf-8") if h_value else None
    return None


def _publish_arbitrated(producer: Producer, msg, stream_type: str, symbol: str):

    partition = partition_for(stream_type, symbol, config.ARBITRATED_PARTITIONS)
    producer.produce(
        topic=config.TOPIC_ARBITRATED,
        partition=partition,
        key=msg.key(),
        value=msg.value(),
        headers=msg.headers() or [],
    )


def _publish_audit(producer: Producer, audit_message, metrics: ArbitratorMetrics):

    payload = encode(audit_message)
    partition = audit_partition_for(
        audit_message.stream_type, config.AUDIT_PARTITIONS
    )
    producer.produce(
        topic=config.TOPIC_AUDIT,
        partition=partition,
        key=audit_message.stream_type.encode("utf-8"),
        value=payload,
    )

    # Count by concrete type. Deferred import avoids a top-level cycle if
    # messages ever ends up importing something from metrics.
    from messages import GateStalled, Tally, VerdictMessage

    labels = (audit_message.stream_type, audit_message.symbol)
    if isinstance(audit_message, VerdictMessage):
        metrics.verdicts_emitted.labels(
            stream_type=audit_message.stream_type,
            symbol=audit_message.symbol,
            actionable=str(audit_message.is_actionable).lower(),
        ).inc()
    elif isinstance(audit_message, Tally):
        metrics.tallies_emitted.labels(*labels).inc()
    elif isinstance(audit_message, GateStalled):
        metrics.stall_warnings_emitted.labels(*labels).inc()


def _handle_raw_message(
    msg,
    registry: StateMachineRegistry,
    producer: Producer,
    metrics: ArbitratorMetrics,
):
    stream_type = _header_value(msg.headers(), "stream_type")
    symbol = _header_value(msg.headers(), "symbol")
    source_id = _header_value(msg.headers(), "source_id")
    listener_ts_ns_raw = _header_value(msg.headers(), "listener_ts_ns")
    binance_ts_ns_raw = _header_value(msg.headers(), "binance_ts_ns")

    if not (stream_type and symbol and source_id):
        log.warning(
            "Raw message missing required header — dropping. partition=%d offset=%d",
            msg.partition(), msg.offset(),
        )
        metrics.raw_messages_dropped.labels(reason="missing_header").inc()
        return
    key = msg.key().decode("utf-8") if msg.key() else None

    try:
        listener_ts_ns = int(listener_ts_ns_raw)
    except (ValueError, TypeError):
        log.warning(
            "Raw message has invalid listener_ts_ns header — dropping. partition=%d offset=%d value=%r",
            msg.partition(), msg.offset(), listener_ts_ns_raw,
        )
        metrics.raw_messages_dropped.labels(reason="invalid_ts_header").inc()
        return

    try:
        binance_ts_ns = int(binance_ts_ns_raw)
    except (ValueError, TypeError):
        log.warning(
            "Raw message has invalid binance_ts_ns header — dropping. partition=%d offset=%d value=%r",
            msg.partition(), msg.offset(), binance_ts_ns_raw,
        )
        metrics.raw_messages_dropped.labels(reason="invalid_binance_ts_header").inc()
        return

    if not key:
        log.warning(
            "Raw message missing key — dropping. partition=%d offset=%d",
            msg.partition(), msg.offset(),
        )
        metrics.raw_messages_dropped.labels(reason="missing_key").inc()
        return

    metrics.events_consumed.labels(stream_type, symbol).inc()

    now = time.time()
    machine = registry.get_or_create(stream_type, symbol, now=now)


    was_ignored = source_id in machine.ignored_sources

    outcome = machine.handle_message(key=key, source_id=source_id, listener_timestamp_ns=listener_ts_ns, binance_timestamp_ns=binance_ts_ns, now=now)

    if was_ignored:
        metrics.messages_ignored.labels(stream_type, symbol).inc()
    elif outcome.forward:
        _publish_arbitrated(producer, msg, stream_type, symbol)
        metrics.events_forwarded.labels(stream_type, symbol).inc()
    else:
        metrics.duplicates_dropped.labels(stream_type, symbol).inc()

    for audit in outcome.audit:
        _publish_audit(producer, audit, metrics)

    for eviction in outcome.evictions:
        latency_ns = eviction.winner_timestamp_ns - eviction.winner_timestamp_binance_ns
        if latency_ns >= 0:
            metrics.winning_latency.labels(stream_type, symbol).observe(latency_ns / 1e3)

# --- Main loops -----------------------------------------------------------

def _commit(consumer: Consumer, msg):

    try:
        consumer.commit(message=msg, asynchronous=True)
    except KafkaException as e:
        log.error("Failed to commit offset: %s", e)


def _raw_consumer_loop(
    consumer: Consumer,
    registry: StateMachineRegistry,
    producer: Producer,
    metrics: ArbitratorMetrics,
    stop_event: threading.Event,
):
    """Consume market-data-raw and drive the state machines."""
    consumer.subscribe([config.TOPIC_RAW])
    log.info("Subscribed to %s", config.TOPIC_RAW)

    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            producer.poll(0)
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("Raw consumer error: %s", msg.error())
            continue

        try:
            _handle_raw_message(msg, registry, producer, metrics)
        except Exception:
            # A single bad message must not take the loop down. Log with
            # enough detail to reproduce, then move on and commit past it —
            # otherwise the same message is re-polled forever.
            log.exception(
                "Failed to handle raw message partition=%d offset=%d",
                msg.partition(), msg.offset(),
            )

        # Commit after processing so a crash re-reads whatever was in flight.
        # See _commit for why the try/except is largely defensive.
        _commit(consumer, msg)

        producer.poll(0)

    log.info("Raw consumer loop stopping")


def _audit_consumer_loop(
    consumer: Consumer,
    registry: StateMachineRegistry,
    metrics: ArbitratorMetrics,
    stop_event: threading.Event,
):
    consumer.subscribe([config.TOPIC_AUDIT])
    log.info("Subscribed to %s (filtering for kill_confirmed)", config.TOPIC_AUDIT)

    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("Audit consumer error: %s", msg.error())
            continue

        try:
            decoded = decode(msg.value())
        except ValueError as e:
            log.warning(
                "Malformed audit message partition=%d offset=%d: %s",
                msg.partition(), msg.offset(), e,
            )
            _commit(consumer, msg)
            continue

        if decoded is None:
            # Unknown type or schema — skip and record we've seen it.
            _commit(consumer, msg)
            continue

        if isinstance(decoded, KillConfirmed):
            metrics.kill_confirmations_received.labels(
                decoded.stream_type, decoded.symbol,
            ).inc()
            machine = registry.get(decoded.stream_type, decoded.symbol)
            if machine is not None:
                machine.confirm_kill(decoded.source_id)
                log.info(
                    "Confirmed kill: stream=%s symbol=%s source=%s instance=%s",
                    decoded.stream_type, decoded.symbol,
                    decoded.source_id, decoded.instance_id,
                )
            else:
                # Confirmation for a stream we've never seen — reaper acted
                # before we ever handled a message for it. Nothing to update,
                # but worth noting.
                log.info(
                    "Kill confirmation for unknown state machine: "
                    "stream=%s symbol=%s source=%s",
                    decoded.stream_type, decoded.symbol, decoded.source_id,
                )

        _commit(consumer, msg)

    log.info("Audit consumer loop stopping")


def _timer_loop(
    registry: StateMachineRegistry,
    producer: Producer,
    metrics: ArbitratorMetrics,
    stop_event: threading.Event,
):
    log.info("Timer loop running every %.1fs", TIMER_INTERVAL_SECONDS)
    while not stop_event.is_set():
        now = time.time()
        for (stream_type, symbol), machine in registry.all_machines():
            try:
                outcome = machine.check_window(now)
                for audit in outcome.audit:
                    _publish_audit(producer, audit, metrics)
                if outcome.kill_target is not None:
                    log.info(
                        "VERDICT stream=%s symbol=%s slowest=%s",
                        stream_type, symbol, outcome.kill_target,
                    )
            except Exception:
                log.exception(
                    "Timer loop failed for stream=%s symbol=%s",
                    stream_type, symbol,
                )
        producer.poll(0)
        stop_event.wait(TIMER_INTERVAL_SECONDS)

    log.info("Timer loop stopping")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    settings = config.load()
    log.info(
        "Starting arbitrator: bootstrap=%s sources_per_stream=%d "
        "window_seconds=%.0f minimum_events=%d emit_tally_every=%d",
        settings["kafka_bootstrap"], settings["sources_per_stream"],
        settings["window_seconds"], settings["minimum_events"],
        settings["emit_tally_every"],
    )

    producer = _build_producer(settings["kafka_bootstrap"])
    raw_consumer = _build_raw_consumer(settings["kafka_bootstrap"])
    audit_consumer = _build_audit_consumer(settings["kafka_bootstrap"])
    registry = StateMachineRegistry(settings)

    metrics = ArbitratorMetrics()
    metrics.register_state_collector(registry)
    serve_metrics(metrics, settings["metrics_port"])

    stop_event = threading.Event()

    def _handle_signal(signame: str):
        log.info("Received %s, shutting down", signame)
        stop_event.set()

    for signame in ("SIGINT", "SIGTERM"):
        signal.signal(
            getattr(signal, signame),
            lambda *_a, s=signame: _handle_signal(s),
        )

    threads = [
        threading.Thread(
            target=_raw_consumer_loop,
            args=(raw_consumer, registry, producer, metrics, stop_event),
            name="raw-consumer",
        ),
        threading.Thread(
            target=_audit_consumer_loop,
            args=(audit_consumer, registry, metrics, stop_event),
            name="audit-consumer",
        ),
        threading.Thread(
            target=_timer_loop,
            args=(registry, producer, metrics, stop_event),
            name="timer",
        ),
    ]

    for t in threads:
        t.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    finally:
        stop_event.set()

        for t in threads:
            t.join(timeout=15)
            if t.is_alive():
                log.warning("Thread %s did not stop within timeout", t.name)

        log.info("Flushing producer")
        try:
            remaining = producer.flush(timeout=10)
            if remaining > 0:
                log.warning("%d messages queued at shutdown", remaining)
        except KafkaException as e:
            log.error("Producer flush error: %s", e)

        raw_consumer.close()
        audit_consumer.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()