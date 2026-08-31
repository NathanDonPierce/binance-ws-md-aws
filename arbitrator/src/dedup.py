from __future__ import annotations
 
from collections import OrderedDict
from dataclasses import dataclass
 
 
@dataclass(frozen=True)
class Eviction:
    key: str
    winner: str
    winner_timestamp: str
    copies_seen: int
    complete: bool
 
 
@dataclass
class _Entry:
    winner: str
    winner_timestamp_source: str     # source with smallest ts seen so far
    winner_timestamp_ns: int         # smallest timestamp
    sources: set[str]
    admitted_at_sequence: int
 
 
class DedupCache:
    def __init__(self, expected_copies: int, lag_threshold: int = 1000):
        if expected_copies < 1:
            raise ValueError("expected_copies must be at least 1")
        if lag_threshold < 1:
            raise ValueError("lag_threshold must be at least 1")
 
        self.expected_copies = expected_copies
        self.lag_threshold = lag_threshold
 
        # Insertion-ordered so the oldest live key is always the first item.
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        # Monotonic counter of admitted keys; used to measure staleness.
        self._sequence = 0
 
    def __len__(self):
        return len(self._entries)
 
    @property
    def sequence(self):
        """Number of distinct keys admitted over this cache's lifetime."""
        return self._sequence
 
    def observe(self, key: str, source_id: str, listener_timestamp_ns: int):
        evictions: list[Eviction] = []
 
        existing = self._entries.get(key)
 
        if existing is None:
            # First copy of this event. Admit it and forward downstream.
            self._sequence += 1
            self._entries[key] = _Entry(
                winner=source_id,
                winner_timestamp_source=source_id,
                winner_timestamp_ns=listener_timestamp_ns,
                sources={source_id},
                admitted_at_sequence=self._sequence,
            )
            is_first_copy = True
 
            # A single expected copy means the entry is complete on arrival.
            if self.expected_copies == 1:
                evictions.append(self._evict(key, complete=True))
        else:
            # A copy of an event already seen. Never forwarded downstream.
            # Only a source not yet counted moves the entry toward completion.
            is_first_copy = False
 
            if source_id not in existing.sources:
                existing.sources.add(source_id)
                if listener_timestamp_ns < existing.winner_timestamp_ns:
                    existing.winner_timestamp_source = source_id
                    existing.winner_timestamp_ns = listener_timestamp_ns
                if len(existing.sources) >= self.expected_copies:
                    evictions.append(self._evict(key, complete=True))
 
        evictions.extend(self._evict_stale())
        return is_first_copy, evictions
 
    def _evict(self, key: str, complete: bool):
        entry = self._entries.pop(key)
        return Eviction(
            key=key,
            winner=entry.winner,
            winner_timestamp=entry.winner_timestamp_source,
            copies_seen=len(entry.sources),
            complete=complete,
        )
 
    def _evict_stale(self):
        evictions = []
        while self._entries:
            oldest_key = next(iter(self._entries))
            oldest = self._entries[oldest_key]
            lag = self._sequence - oldest.admitted_at_sequence
            if lag < self.lag_threshold:
                break
            evictions.append(self._evict(oldest_key, complete=False))
        return evictions
 
