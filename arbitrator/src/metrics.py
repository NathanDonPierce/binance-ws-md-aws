"""Prometheus metrics for the arbitrator.

Two shapes of instrumentation, mixed on purpose:

    Counters   Volume signals — events consumed, forwarded, dropped, verdicts
               emitted. Incremented on the fly as the arbitrator does its
               work, so the rate reflects real activity even at sub-scrape
               resolution.

    Gauges     State signals — is a stream currently measuring, how big is
               its dedup cache, what are the current per-source win counts.
               Sampled on scrape by reading the state machines' introspection
               properties, so a Prometheus rate() calculation doesn't need to
               remember whether a gauge was set moments before it was reset.

An HTTP server on a dedicated port serves /metrics in the standard Prometheus
text format. It runs on its own thread — separate from the arbitrator's raw,
audit and timer threads — so a slow scrape can't stall message processing.

Labels are always (stream_type, symbol) on stream-scoped metrics. Source_id
is added as a third label on per-source gauges, which does mean the label
cardinality grows with fleet churn — a killed source's win count lingers in
the metric until the arbitrator restarts. At project scale this is a handful
of extra label combinations per session, which is fine. A production system
would clear the gauge for dead sources on kill_confirmed.
"""

from __future__ import annotations

import logging

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    start_http_server,
)
from prometheus_client.core import GaugeMetricFamily

log = logging.getLogger("arbitrator.metrics")


class ArbitratorMetrics:


    def __init__(self, registry: CollectorRegistry | None = None):
        # Own our registry unless the caller supplied one. Tests want isolation.
        self.registry = registry or CollectorRegistry()

        # --- Counters: incremented as work happens --------------------------

        self.events_consumed = Counter(
            "arbitrator_events_consumed_total",
            "Raw messages consumed from market-data-raw",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.events_forwarded = Counter(
            "arbitrator_events_forwarded_total",
            "Messages written to market-data-arb (first sighting of an event)",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.duplicates_dropped = Counter(
            "arbitrator_duplicates_dropped_total",
            "Duplicate copies discarded (event already seen)",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.messages_ignored = Counter(
            "arbitrator_messages_ignored_total",
            "Messages from sources on the ignore list, dropped entirely",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.verdicts_emitted = Counter(
            "arbitrator_verdicts_emitted_total",
            "Verdicts published to arbitration-audit",
            # 'actionable' separates verdicts that named a source from ones
            # that declined (ties, too few events, too few sources) — both
            # are real outcomes and both worth tracking.
            labelnames=("stream_type", "symbol", "actionable"),
            registry=self.registry,
        )
        self.tallies_emitted = Counter(
            "arbitrator_tallies_emitted_total",
            "Running tally messages published to arbitration-audit",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.stall_warnings_emitted = Counter(
            "arbitrator_stall_warnings_emitted_total",
            "Gate stall warnings published to arbitration-audit",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.kill_confirmations_received = Counter(
            "arbitrator_kill_confirmations_received_total",
            "kill_confirmed messages consumed from arbitration-audit",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.raw_messages_dropped = Counter(
            "arbitrator_raw_messages_dropped_total",
            "Raw messages dropped for being malformed (missing header or key)",
            labelnames=("reason",),
            registry=self.registry,
        )

        # --- Gauges: sampled from state machines on every scrape ------------
        #
        # These are exposed via the StateSnapshotCollector below rather than
        # set-and-forget gauges, so a scrape always sees the state at scrape
        # time rather than whenever the arbitrator last touched the gauge.

    def register_state_collector(self, registry_of_machines):
        """Attach a collector that samples state on every scrape.

        Separated from __init__ so the metrics can be constructed before the
        state machine registry exists.
        """
        self.registry.register(StateSnapshotCollector(registry_of_machines))


class StateSnapshotCollector:


    def __init__(self, machines_registry):
        self._machines = machines_registry

    def collect(self):
        gate_open = GaugeMetricFamily(
            "arbitrator_gate_open",
            "1 if the fleet gate is open and measurement is running, else 0",
            labels=["stream_type", "symbol"],
        )
        dedup_size = GaugeMetricFamily(
            "arbitrator_dedup_cache_size",
            "Number of live entries in the dedup cache",
            labels=["stream_type", "symbol"],
        )
        source_wins = GaugeMetricFamily(
            "arbitrator_source_wins",
            "Wins credited to each source",
            labels=["stream_type", "symbol", "source_id", "mode"],
        )
        ignored = GaugeMetricFamily(
            "arbitrator_sources_ignored",
            "Number of sources currently on the ignore list",
            labels=["stream_type", "symbol"],
        )
        window_start = GaugeMetricFamily(
            "arbitrator_window_start_timestamp",
            "Unix timestamp when the current window opened, or 0 if none open",
            labels=["stream_type", "symbol"],
        )

        for (stream_type, symbol), machine in self._machines.all_machines():
            labels = [stream_type, symbol]

            # Each of these takes the machine's lock briefly. Doing them
            # separately (rather than in one atomic snapshot) is fine because
            # they are independent readings for a dashboard, not a critical
            # invariant that must be consistent across metrics.
            gate_open.add_metric(labels, 1.0 if machine.is_measuring else 0.0)
            dedup_size.add_metric(labels, float(machine.dedup_size))
            window_start.add_metric(labels, float(machine.window_start or 0))
            ignored.add_metric(labels, float(len(machine.ignored_sources)))

            for source_id, wins in machine.counts.items():
                source_wins.add_metric(
                    [stream_type, symbol, source_id, "offset"], float(wins),
                )
            for source_id, wins in machine.counts_timestamp.items():
                source_wins.add_metric(
                    [stream_type, symbol, source_id, "timestamp"], float(wins),
                )

        yield gate_open
        yield dedup_size
        yield window_start
        yield ignored
        yield source_wins


def serve(metrics: ArbitratorMetrics, port: int):
    start_http_server(port, registry=metrics.registry)
    log.info("Metrics endpoint listening on :%d/metrics", port)
