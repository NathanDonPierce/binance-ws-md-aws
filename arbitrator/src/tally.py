from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Verdict:
    slowest_source_id: str | None
    counts: dict[str, int]
    total_events: int
    sources_seen: int
    reason: str | None = None


@dataclass
class WindowTally:

    counts: dict[str, int] = field(default_factory=dict)
    total_events: int = 0

    def register_source(self, source_id: str):
        """Note that a source is present, without crediting it a win."""
        self.counts.setdefault(source_id, 0)

    def credit(self, source_id: str):
        """Credit one win to a source."""
        self.counts[source_id] = self.counts.get(source_id, 0) + 1
        self.total_events += 1

    @property
    def sources_seen(self):
        return len(self.counts)

    def verdict(self, minimum_events: int = 1):
        if self.sources_seen < 2:
            return Verdict(
                slowest_source_id=None,
                counts=dict(self.counts),
                total_events=self.total_events,
                sources_seen=self.sources_seen,
                reason="fewer than two sources contributed",
            )

        if self.total_events < minimum_events:
            return Verdict(
                slowest_source_id=None,
                counts=dict(self.counts),
                total_events=self.total_events,
                sources_seen=self.sources_seen,
                reason=f"only {self.total_events} events, need {minimum_events}",
            )

        lowest = min(self.counts.values())
        laggards = [s for s, c in self.counts.items() if c == lowest]

        if len(laggards) > 1:
            return Verdict(
                slowest_source_id=None,
                counts=dict(self.counts),
                total_events=self.total_events,
                sources_seen=self.sources_seen,
                reason=f"tie between {len(laggards)} sources on {lowest} wins",
            )

        return Verdict(
            slowest_source_id=laggards[0],
            counts=dict(self.counts),
            total_events=self.total_events,
            sources_seen=self.sources_seen,
        )

    def reset(self):
        """Clear all state, ready for the next window."""
        self.counts = {}
        self.total_events = 0