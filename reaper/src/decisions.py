"""Which verdicts to act on, and which to let pass.

The reaper consumes every message on the audit topic — its own kill
confirmations, the arbitrator's running tallies, stall warnings, and verdicts
that reached no conclusion. Only a small fraction are instructions to do
something. Filtering them is pure logic and lives here, apart from the code
that performs the destructive work, so the question 'should this have been
acted on?' can be answered without a cluster.

The other job here is not acting twice on the same verdict. Kafka redelivers:
a consumer-group rebalance, a restart before the offset commit lands, or a
retry after a transient error can all present a verdict the reaper has
already handled. Cordon, drain and terminate are individually idempotent, so
a repeat is survivable — but it wastes a cordon/drain cycle, produces a
second kill_confirmed, and muddies the audit trail. Recognising the repeat is
cheaper than tolerating it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from messages import VerdictMessage


@dataclass(frozen=True)
class Decision:
    """Whether to act on a verdict, and the reason either way.

    The reason is populated on every decision, not only refusals. A verdict
    the reaper declined to act on is as interesting operationally as one it
    acted on — 'nothing happened' is only reassuring when you can see why.
    """
    act: bool
    reason: str


@dataclass
class VerdictTracker:
    """Remembers which sources have already been acted on.

    State is per-process and lost on restart, which is the right trade. A
    reaper that comes back up has no idea whether the kill it was midway
    through completed, and the safe response to that uncertainty is to repeat
    the sequence — every step is idempotent, so repeating costs a little time
    and nothing else. Persisting the tracker would trade that simplicity for
    a durability guarantee the operations do not need.

    Entries are keyed by (stream_type, symbol, source_id) rather than
    source_id alone. A node name is unique across the fleet in practice, but
    the key that matters is the one the arbitrator reasons about, and its
    state machines are per (stream_type, symbol).
    """

    _handled: set[tuple[str, str, str]] = field(default_factory=set)

    def __len__(self):
        return len(self._handled)

    def key_for(self, verdict: VerdictMessage):
        return (verdict.stream_type, verdict.symbol, verdict.slowest_source_id)

    def already_handled(self, verdict: VerdictMessage):
        return self.key_for(verdict) in self._handled

    def mark_handled(self, verdict: VerdictMessage):
        """Record that this verdict's target has been acted on.

        Called after the action sequence completes, not before. Marking first
        would mean a crash mid-kill left the target recorded as handled while
        the node sat cordoned and drained but never terminated, with nothing
        to retry it.
        """
        self._handled.add(self.key_for(verdict))


def should_act(message, tracker: VerdictTracker):
    """Decide what to do with one decoded audit message.

    Args:
        message: anything `messages.decode` returned — a Tally, a
            GateStalled, a KillConfirmed, a VerdictMessage, or None for a
            type this build does not recognise
        tracker: the in-flight record, consulted but not modified

    Returns a Decision. Only an actionable verdict for a source not already
    handled produces act=True.
    """
    if message is None:
        return Decision(False, "unrecognised message type or schema version")

    if not isinstance(message, VerdictMessage):
        return Decision(
            False, f"not a verdict ({type(message).__name__})",
        )

    if not message.is_actionable:
        # The arbitrator declines to name a source when the window was a tie,
        # saw too few events, or had too few sources contributing. Those are
        # real outcomes worth recording, not failures.
        return Decision(
            False,
            f"verdict reached no conclusion: {message.reason or 'no reason given'}",
        )

    if tracker.already_handled(message):
        return Decision(
            False,
            f"already acted on {message.slowest_source_id} "
            f"for {message.stream_type}/{message.symbol}",
        )

    return Decision(
        True,
        f"{message.slowest_source_id} was slowest over "
        f"{message.total_events} events from {message.sources_seen} sources",
    )
