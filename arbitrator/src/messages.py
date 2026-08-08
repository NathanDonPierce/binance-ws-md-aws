from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

TYPE_TALLY = "tally"
TYPE_VERDICT = "verdict"
TYPE_GATE_STALLED = "gate_stalled"
TYPE_KILL_CONFIRMED = "kill_confirmed"

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Tally:
    """Running win counts for an open window.

    Emitted as wins accrue. High volume by design — one per arbitrated event —
    so it carries only what is needed to reconstruct how a verdict was reached.
    """
    stream_type: str
    symbol: str
    window_start: float
    emitted_at: float
    counts: dict[str, int]
    total_events: int
    correlation_id: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "counts", dict(self.counts))

    def to_dict(self):
        return {
            "type": TYPE_TALLY,
            "schema_version": SCHEMA_VERSION,
            "stream_type": self.stream_type,
            "symbol": self.symbol,
            "window_start": self.window_start,
            "emitted_at": self.emitted_at,
            "counts": dict(self.counts),
            "total_events": self.total_events,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            stream_type=d["stream_type"],
            symbol=d["symbol"],
            window_start=d["window_start"],
            emitted_at=d["emitted_at"],
            counts=dict(d["counts"]),
            total_events=d["total_events"],
            correlation_id=d.get("correlation_id"),
        )


@dataclass(frozen=True)
class VerdictMessage:
    stream_type: str
    symbol: str
    window_start: float
    window_end: float
    slowest_source_id: str | None
    counts: dict[str, int]
    total_events: int
    sources_seen: int
    reason: str | None = None
    correlation_id: str | None = None

    def __post_init__(self):
        # See Tally.__post_init__ — frozen does not make the dict immutable.
        object.__setattr__(self, "counts", dict(self.counts))

    @property
    def is_actionable(self):
        """True if this verdict names a source for the reaper to remove."""
        return self.slowest_source_id is not None

    def to_dict(self):
        return {
            "type": TYPE_VERDICT,
            "schema_version": SCHEMA_VERSION,
            "stream_type": self.stream_type,
            "symbol": self.symbol,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "slowest_source_id": self.slowest_source_id,
            "counts": dict(self.counts),
            "total_events": self.total_events,
            "sources_seen": self.sources_seen,
            "reason": self.reason,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            stream_type=d["stream_type"],
            symbol=d["symbol"],
            window_start=d["window_start"],
            window_end=d["window_end"],
            slowest_source_id=d["slowest_source_id"],
            counts=dict(d["counts"]),
            total_events=d["total_events"],
            sources_seen=d["sources_seen"],
            reason=d.get("reason"),
            correlation_id=d.get("correlation_id"),
        )


@dataclass(frozen=True)
class GateStalled:
    stream_type: str
    symbol: str
    sources_confirmed: int
    sources_expected: int
    stalled_for_seconds: float
    ignored_sources: tuple[str, ...]
    emitted_at: float

    def to_dict(self):
        return {
            "type": TYPE_GATE_STALLED,
            "schema_version": SCHEMA_VERSION,
            "stream_type": self.stream_type,
            "symbol": self.symbol,
            "sources_confirmed": self.sources_confirmed,
            "sources_expected": self.sources_expected,
            "stalled_for_seconds": self.stalled_for_seconds,
            "ignored_sources": list(self.ignored_sources),
            "emitted_at": self.emitted_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            stream_type=d["stream_type"],
            symbol=d["symbol"],
            sources_confirmed=d["sources_confirmed"],
            sources_expected=d["sources_expected"],
            stalled_for_seconds=d["stalled_for_seconds"],
            ignored_sources=tuple(d["ignored_sources"]),
            emitted_at=d["emitted_at"],
        )


@dataclass(frozen=True)
class KillConfirmed:
    stream_type: str
    symbol: str
    source_id: str
    instance_id: str | None
    terminated_at: float
    correlation_id: str | None = None

    def to_dict(self):
        return {
            "type": TYPE_KILL_CONFIRMED,
            "schema_version": SCHEMA_VERSION,
            "stream_type": self.stream_type,
            "symbol": self.symbol,
            "source_id": self.source_id,
            "instance_id": self.instance_id,
            "terminated_at": self.terminated_at,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]):
        return cls(
            stream_type=d["stream_type"],
            symbol=d["symbol"],
            source_id=d["source_id"],
            instance_id=d.get("instance_id"),
            terminated_at=d["terminated_at"],
            correlation_id=d.get("correlation_id"),
        )


_BY_TYPE = {
    TYPE_TALLY: Tally,
    TYPE_VERDICT: VerdictMessage,
    TYPE_GATE_STALLED: GateStalled,
    TYPE_KILL_CONFIRMED: KillConfirmed,
}


def encode(message):
    return json.dumps(message.to_dict(), separators=(",", ":")).encode("utf-8")


def decode(raw):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"audit message is not valid JSON: {e}") from e

    if not isinstance(payload, dict):
        raise ValueError("audit message must be a JSON object")

    message_type = payload.get("type")
    if message_type is None:
        raise ValueError("audit message has no 'type' field")

    if payload.get("schema_version", SCHEMA_VERSION) > SCHEMA_VERSION:
        return None

    cls = _BY_TYPE.get(message_type)
    if cls is None:
        return None

    try:
        return cls.from_dict(payload)
    except KeyError as e:
        raise ValueError(f"{message_type} message missing field {e}") from e