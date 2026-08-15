"""Message envelopes for the arbitration-audit topic.

Four message types share one topic, distinguished by a `type` field:

    tally           emitted as wins accrue; the running state of an open window
    verdict         emitted at window close; names the slowest source
    gate_stalled    emitted while measurement is held shut waiting for a fleet
    kill_confirmed  emitted by the reaper once a termination has completed

The reaper consumes only `verdict`. The arbitrator consumes only
`kill_confirmed`. Everything else on the topic is observational — a single
ordered record of what the arbitration loop did and why, which is what makes
a verdict auditable after the fact.

These definitions are the contract between the arbitrator and the reaper, so
they live in one module that both import rather than each side parsing the
other's JSON by hand.

The message types are frozen dataclasses and compare by value, which is what
round-trip tests rely on. They are not hashable, because two of them carry a
dict of per-source counts — so they can go in a list but not a set.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

TYPE_TALLY = "tally"
TYPE_VERDICT = "verdict"
TYPE_GATE_STALLED = "gate_stalled"
TYPE_KILL_CONFIRMED = "kill_confirmed"

# Bumped when a field changes meaning or is removed. Consumers can refuse to
# act on a version they do not understand rather than misreading a field.
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
        # frozen=True stops the field being rebound but not the dict being
        # mutated in place. Copying on construction means a caller that keeps
        # a reference to the dict it passed in cannot alter a message that has
        # already been handed off for publishing.
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
    """The outcome of a closed window. The only type the reaper acts on.

    `slowest_source_id` is None when the window reached no conclusion — too few
    sources contributed, too few events were arbitrated, or two sources tied on
    the lowest count. `reason` explains which. A verdict with no source is still
    published, because "this window decided nothing, and here is why" is
    information worth having in the audit record.
    """
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
    """Measurement has been held shut longer than the warning interval.

    The gate never proceeds on a short fleet, so a stall lasts as long as the
    fleet stays short — a replacement that crash-loops and never publishes
    would hold a stream shut indefinitely. This message exists so that state is
    visible rather than silent, and repeats at the warning interval for as long
    as the condition holds.
    """
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
    """The reaper has terminated a source. Published by the reaper.

    Closes the loop opened by a verdict. The arbitrator ignores a source from
    the moment it names it, so this does not change gating; it records the
    difference between 'named for removal' and 'confirmed gone'. A source that
    stays named but never confirmed means the reaper did not finish its work.

    `instance_id` is the EC2 instance identifier rather than only the node
    name. Node names derive from private IP addresses, which AWS recycles, so
    the instance id is the durable identity of what was actually terminated.
    """
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
    """Serialise any audit message to UTF-8 JSON bytes."""
    return json.dumps(message.to_dict(), separators=(",", ":")).encode("utf-8")


def decode(raw):
    """Deserialise an audit message.

    Returns None for a message this build does not understand — an unknown
    `type`, or a newer `schema_version`. Returning None rather than raising
    lets a consumer skip messages meant for a different component, which is
    the normal case on a shared topic: the reaper sees three types it has no
    interest in for every verdict it acts on.

    Raises:
        ValueError: if the payload is not valid JSON, is not an object, has no
            `type` field, or is missing a field its type requires. These are
            malformed messages rather than unrecognised ones, and are worth
            surfacing.
    """
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
