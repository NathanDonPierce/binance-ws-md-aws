from __future__ import annotations

import hashlib


AUDIT_STREAM_ORDER = ("trade", "depth", "aggtrade")


def partition_for(stream_type: str, symbol: str, num_partitions: int):
    if num_partitions < 1:
        raise ValueError("num_partitions must be at least 1")
    if not stream_type:
        raise ValueError("stream_type must not be empty")
    if not symbol:
        raise ValueError("symbol must not be empty")

    # A NUL separator cannot appear in either component, so pairs like
    # ('agg', 'trade') and ('aggtrade', '') can never collide.
    material = f"{stream_type.lower()}\0{symbol.lower()}".encode("utf-8")
    digest = hashlib.sha256(material).digest()
    return int.from_bytes(digest[:8], "big") % num_partitions


def audit_partition_for(stream_type: str, num_partitions: int):
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
    return f"{key_prefix}:{event_id}"


def parse_message_key(key: str):
    prefix, separator, event_id = key.partition(":")
    if not separator or not prefix or not event_id:
        raise ValueError(f"malformed message key: {key!r}")
    return prefix, event_id