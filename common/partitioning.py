"""Partition assignment for the market data and audit topics.

Both the listener and the arbitrator produce to topics partitioned by
(stream_type, symbol). They must agree exactly on which partition a given pair
maps to, so the computation lives here and is imported by both rather than
being reimplemented on each side.

Why this pairing rather than partitioning by the message key:

    Every source node publishes its own copy of the same real market event.
    All of those copies share a stream type and a symbol, so partitioning on
    that pair puts them on the same Kafka partition. Within a partition Kafka
    assigns strictly increasing offsets in append order, which makes "who
    arrived first" a property the broker.  Initially this was the only way to
    arbitrate between copies of the same event.
"""

from __future__ import annotations

import hashlib

AUDIT_STREAM_ORDER = ("trade", "depth", "aggtrade")

def partition_for(stream_type: str, symbol: str, num_partitions: int):
    """Returns the partition index for a (stream_type, symbol) pair.

    Deterministic across processes, restarts and Python versions.

    Args:
        stream_type: lowercase stream identifier, e.g. 'trade'
        symbol: lowercase Binance symbol, e.g. 'btcusdt'
        num_partitions: partition count of the target topic
    """
    if num_partitions < 1:
        raise ValueError("num_partitions must be at least 1")
    if not stream_type:
        raise ValueError("stream_type must not be empty")
    if not symbol:
        raise ValueError("symbol must not be empty")

    material = f"{stream_type.lower()}\0{symbol.lower()}".encode("utf-8")
    digest = hashlib.sha256(material).digest()
    return int.from_bytes(digest[:8], "big") % num_partitions


def audit_partition_for(stream_type: str, num_partitions: int):
    """Return the partition index for an audit message.

    The audit topic is partitioned by stream type alone, not by (stream_type,
    symbol) like the market data topics. Every message about one stream —
    tallies, verdicts, and the reaper's kill confirmations — therefore lands on
    one partition and stays strictly ordered relative to the others. That
    ordering is load-bearing: a consumer must never see a kill confirmation
    before the verdict that caused it, and Kafka only guarantees ordering
    within a partition.

    Symbol is deliberately excluded. Including it would spread one stream's
    audit trail across partitions and break that guarantee, for the sake of
    parallelism the audit topic does not need.

    Assignment is by position in AUDIT_STREAM_ORDER rather than by hashing.
    The stream set is small, fixed and known, and hashing three values into
    three partitions collides more often than not — leaving one partition idle
    while two streams share another. Positional assignment gives each stream
    its own partition with no collisions and no surprises.

    New streams must be appended to AUDIT_STREAM_ORDER, never inserted:
    inserting would shift every later stream onto a different partition, and
    messages already on the old partitions would be read out of order relative
    to the new ones.

    An unrecognised stream falls back to hashing, so an unexpected value is
    still placed somewhere valid rather than raising in the produce path.
    """
    if num_partitions < 1:
        raise ValueError("num_partitions must be at least 1")
    if not stream_type:
        raise ValueError("stream_type must not be empty")

    normalised = stream_type.lower()
    if normalised in AUDIT_STREAM_ORDER:
        return AUDIT_STREAM_ORDER.index(normalised) % num_partitions

    digest = hashlib.sha256(normalised.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % num_partitions


def message_key(key_prefix: str, event_id):
    # Build the Kafka message key identifying a specific market event.
    
    return f"{key_prefix}:{event_id}"


def parse_message_key(key: str):
    """Split a message key back into (prefix, event_id).

    Returns the event_id as a string; callers that need it numeric convert it
    themselves, since not every stream is guaranteed to use integer ids.

    Raises:
        ValueError: if the key is not in '<prefix>:<event_id>' form.
    """
    prefix, separator, event_id = key.partition(":")
    if not separator or not prefix or not event_id:
        raise ValueError(f"malformed message key: {key!r}")
    return prefix, event_id
