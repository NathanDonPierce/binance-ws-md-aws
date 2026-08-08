from __future__ import annotations

from dataclasses import dataclass, field

from dedup import DedupCache
from gate import FleetGate
from messages import GateStalled, Tally, VerdictMessage
from tally import WindowTally


@dataclass
class StreamConfig:
    stream_type: str
    symbol: str
    expected_sources: int
    window_seconds: float
    dedup_lag_threshold: int = 1000
    minimum_events: int = 100
    stall_warn_seconds: float = 600.0
    emit_tally_every: int = 500

    def __post_init__(self):
        if self.expected_sources < 1:
            raise ValueError("expected_sources must be at least 1")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.emit_tally_every < 1:
            raise ValueError("emit_tally_every must be at least 1")


@dataclass
class Outcome:
    forward: bool = False
    audit: list = field(default_factory=list)
    kill_target: str | None = None


class StreamArbitrator:

    def __init__(self, config: StreamConfig, now: float):
        self.config = config

        self._dedup = DedupCache(
            expected_copies=config.expected_sources,
            lag_threshold=config.dedup_lag_threshold,
        )
        self._tally = WindowTally()
        self._gate = FleetGate(
            expected_sources=config.expected_sources,
            stall_warn_seconds=config.stall_warn_seconds,
        )
        self._gate.start(now)

        self._window_start: float | None = None
        self._events_since_tally = 0


    @property
    def is_measuring(self):
        return self._gate.is_open

    @property
    def window_start(self):
        return self._window_start

    @property
    def dedup_size(self):
        return len(self._dedup)

    @property
    def counts(self):
        return dict(self._tally.counts)

    @property
    def ignored_sources(self):
        return self._gate.ignored_sources

    # -- main entry points --------------------------------------------------

    def handle_message(self, key: str, source_id: str, now: float):
        outcome = Outcome()

        if self._gate.is_ignored(source_id):
            return outcome

        gate_opened = self._gate.observe(source_id, now)
        if gate_opened:
            self._open_window(now)

        forward, evictions = self._dedup.observe(key, source_id)
        outcome.forward = forward

        if self._gate.is_open:
            self._tally.register_source(source_id)

            for eviction in evictions:
                self._tally.credit(eviction.winner)
                self._events_since_tally += 1

            if self._events_since_tally >= self.config.emit_tally_every:
                outcome.audit.append(self._build_tally(now))
                self._events_since_tally = 0

        stall = self._gate.check_stall(now)
        if stall is not None:
            outcome.audit.append(self._build_stall(stall, now))

        return outcome

    def check_window(self, now: float):
        outcome = Outcome()

        if not self._gate.is_open or self._window_start is None:
            stall = self._gate.check_stall(now)
            if stall is not None:
                outcome.audit.append(self._build_stall(stall, now))
            return outcome

        if now - self._window_start < self.config.window_seconds:
            return outcome

        verdict = self._close_window(now)
        outcome.audit.append(verdict)

        if verdict.is_actionable:
            outcome.kill_target = verdict.slowest_source_id
            self._gate.close_for_kill(verdict.slowest_source_id, now)
        else:
            self._open_window(now)

        return outcome

    def confirm_kill(self, source_id: str):
        self._gate.confirm_kill(source_id)

    # -- internals ----------------------------------------------------------

    def _open_window(self, now: float):
        self._window_start = now
        self._tally.reset()
        self._events_since_tally = 0

        for source_id in self._gate.confirmed_sources:
            self._tally.register_source(source_id)

    def _close_window(self, now: float):
        verdict = self._tally.verdict(minimum_events=self.config.minimum_events)
        message = VerdictMessage(
            stream_type=self.config.stream_type,
            symbol=self.config.symbol,
            window_start=self._window_start,
            window_end=now,
            slowest_source_id=verdict.slowest_source_id,
            counts=verdict.counts,
            total_events=verdict.total_events,
            sources_seen=verdict.sources_seen,
            reason=verdict.reason,
        )
        self._window_start = None
        return message

    def _build_tally(self, now: float):
        return Tally(
            stream_type=self.config.stream_type,
            symbol=self.config.symbol,
            window_start=self._window_start,
            emitted_at=now,
            counts=dict(self._tally.counts),
            total_events=self._tally.total_events,
        )

    def _build_stall(self, stall, now: float):
        return GateStalled(
            stream_type=self.config.stream_type,
            symbol=self.config.symbol,
            sources_confirmed=stall.sources_confirmed,
            sources_expected=stall.sources_expected,
            stalled_for_seconds=stall.stalled_for_seconds,
            ignored_sources=stall.ignored_sources,
            emitted_at=now,
        )