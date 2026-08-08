from __future__ import annotations

import os

TOPIC_RAW = "market-data-raw"
TOPIC_ARBITRATED = "market-data-arb"
TOPIC_AUDIT = "arbitration-audit"

RAW_PARTITIONS = 6
ARBITRATED_PARTITIONS = 6
AUDIT_PARTITIONS = 3

CONSUMER_GROUP = "arbitrator"

_DEFAULT_DEDUP_LAG = 1000

STREAMS = ("trade", "depth", "aggtrade")


def _int(name: str, default: int):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name}={raw!r} is not an integer")


def _float(name: str, default: float):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{name}={raw!r} is not a number")


def dedup_lag_for(stream_type: str):
    specific = f"DEDUP_LAG_{stream_type.upper()}"
    return _int(specific, _int("DEDUP_LAG", _DEFAULT_DEDUP_LAG))


def load():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise SystemExit("KAFKA_BOOTSTRAP is required")

    sources_per_stream = _int("SOURCES_PER_STREAM", 0)
    if sources_per_stream < 1:
        raise SystemExit("SOURCES_PER_STREAM is required and must be at least 1")

    settings = {
        "kafka_bootstrap": bootstrap,
        "sources_per_stream": sources_per_stream,
        "window_seconds": _float("WINDOW_SECONDS", 900.0),
        "minimum_events": _int("MINIMUM_EVENTS", 100),
        "stall_warn_seconds": _float("GATE_STALL_WARN_SECONDS", 600.0),
        "emit_tally_every": _int("EMIT_TALLY_EVERY", 1),
        "metrics_port": _int("METRICS_PORT", 8080),
        "dedup_lag": {s: dedup_lag_for(s) for s in STREAMS},
    }

    if settings["window_seconds"] <= 0:
        raise SystemExit("WINDOW_SECONDS must be positive")
    if settings["emit_tally_every"] < 1:
        raise SystemExit("EMIT_TALLY_EVERY must be at least 1")

    return settings