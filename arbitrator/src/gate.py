"""Fleet gate: decides when measurement is allowed to run.

Deduplication runs continuously and is never gated — the arbitrated feed is the
useful downstream product and should not stall because the fleet is briefly
short a node. Measurement is different. Ranking sources against each other only
means anything when the full fleet is present and publishing, so the tally and
the verdict are held until that is true.

The gate closes whenever a source is removed, and reopens once the expected
number of distinct, non-ignored sources have each been observed publishing at
least once. That single mechanism covers both cases that matter:

  - A reaper-driven kill. The verdict names a source, the arbitrator ignores it
    from that moment, and measurement pauses until the ASG-launched replacement
    starts producing.

  - An unplanned failure. A node crashes and the ASG replaces it. The fleet
    drops below the expected count in exactly the same way, and the gate
    behaves identically. No separate handling needed.

If two sources go down at once the gate waits for the full count rather than
proceeding at one short. A delayed verdict costs nothing; a verdict computed
against a degraded fleet could name a healthy source.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StallWarning:
    """Emitted when the gate has been closed longer than the warning interval.

    The gate never proceeds on a short fleet, so a stall is indefinite by
    design — a replacement that crash-loops and never publishes would hold
    measurement forever. This warning exists so that the stall is visible in
    the audit topic rather than silent.
    """
    sources_confirmed: int
    sources_expected: int
    stalled_for_seconds: float
    ignored_sources: tuple[str, ...]


@dataclass
class FleetGate:

    expected_sources: int
    stall_warn_seconds: float = 600.0

    _confirmed: set[str] = field(default_factory=set)
    _ignored: set[str] = field(default_factory=set)
    _confirmed_dead: set[str] = field(default_factory=set)

    _open: bool = False
    _closed_at: float | None = None
    _last_warned_at: float | None = None

    def start(self, now: float):
        if self._closed_at is None:
            self._closed_at = now

    @property
    def is_open(self):
        return self._open

    @property
    def confirmed_sources(self):
        return frozenset(self._confirmed)

    @property
    def ignored_sources(self):
        return frozenset(self._ignored)

    @property
    def confirmed_dead(self):
        return frozenset(self._confirmed_dead)

    def is_ignored(self, source_id: str):
        return source_id in self._ignored

    def observe(self, source_id: str, now: float):
        if source_id in self._ignored:
            return False

        self._confirmed.add(source_id)

        if not self._open and len(self._confirmed) >= self.expected_sources:
            self._open = True
            self._closed_at = None
            self._last_warned_at = None
            return True

        return False

    def close_for_kill(self, source_id: str, now: float):
        self._ignored.add(source_id)
        self._confirmed.discard(source_id)
        self._open = False
        self._closed_at = now
        self._last_warned_at = None

    def confirm_kill(self, source_id: str):
        self._ignored.add(source_id)
        self._confirmed_dead.add(source_id)
        self._confirmed.discard(source_id)

    def check_stall(self, now: float):
        if self._open or self._closed_at is None:
            return None

        stalled_for = now - self._closed_at
        if stalled_for < self.stall_warn_seconds:
            return None

        if self._last_warned_at is not None:
            if now - self._last_warned_at < self.stall_warn_seconds:
                return None

        self._last_warned_at = now
        return StallWarning(
            sources_confirmed=len(self._confirmed),
            sources_expected=self.expected_sources,
            stalled_for_seconds=stalled_for,
            ignored_sources=tuple(sorted(self._ignored)),
        )