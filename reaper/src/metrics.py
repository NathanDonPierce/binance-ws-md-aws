"""Prometheus metrics for the reaper.

Counters for what happened, histograms for how long the slow parts took, and
a gauge for when the last kill landed.

Every `.labels()` call in this codebase uses keyword arguments only. Mixing
positional and keyword raises ValueError, and in Phase 4 that exception was
swallowed by a surrounding try/except — the Kafka side worked perfectly while
the counter sat silently at zero, which took a long time to unpick. Keyword
arguments throughout make the failure mode impossible rather than merely
unlikely.
"""

from __future__ import annotations

import logging

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

log = logging.getLogger("reaper.metrics")


class ReaperMetrics:
    """Counters, histograms and gauges for the reaper.

    An instance rather than module-level globals so tests can build a fresh
    set without polluting a shared registry.
    """

    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()

        self.audit_messages_consumed = Counter(
            "reaper_audit_messages_consumed_total",
            "Messages read from the audit topic, whatever their type",
            labelnames=("message_type",),
            registry=self.registry,
        )
        self.verdicts_received = Counter(
            "reaper_verdicts_received_total",
            "Verdicts seen, split by whether they named a source",
            labelnames=("stream_type", "symbol", "actionable"),
            registry=self.registry,
        )
        self.kills_attempted = Counter(
            "reaper_kills_attempted_total",
            "Removal sequences started",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.kills_succeeded = Counter(
            "reaper_kills_succeeded_total",
            "Removal sequences that completed and published a confirmation",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.kills_failed = Counter(
            "reaper_kills_failed_total",
            "Removal sequences that did not complete",
            labelnames=("stream_type", "symbol", "stage"),
            registry=self.registry,
        )
        self.kills_refused = Counter(
            "reaper_kills_refused_total",
            # A safety rail firing is a signal worth alerting on, not a
            # silence. Counting refusals by reason makes 'the reaper declined
            # to act' visible rather than indistinguishable from 'no verdicts
            # arrived'.
            "Verdicts the reaper declined to act on, by reason",
            labelnames=("reason",),
            registry=self.registry,
        )
        self.kills_skipped_dry_run = Counter(
            "reaper_kills_skipped_dry_run_total",
            "Actionable verdicts not acted on because DRY_RUN is set",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.confirmations_published = Counter(
            "reaper_confirmations_published_total",
            "kill_confirmed messages written back to the audit topic",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )

        self.cordon_duration = Histogram(
            "reaper_cordon_duration_seconds",
            "Time taken to cordon a node",
            registry=self.registry,
        )
        self.drain_duration = Histogram(
            "reaper_drain_duration_seconds",
            "Time taken to drain a node, including waiting for pods to go",
            # The default buckets top out at 10s, but a drain waits for a
            # pod's termination grace period, so anything under a minute is
            # ordinary and the interesting question is whether it approached
            # the timeout.
            buckets=(1, 5, 10, 20, 30, 45, 60, 90, 120, 180, float("inf")),
            registry=self.registry,
        )
        self.terminate_duration = Histogram(
            "reaper_terminate_duration_seconds",
            "Time taken for the EC2 termination call to return",
            registry=self.registry,
        )

        self.last_kill_timestamp = Gauge(
            "reaper_last_kill_timestamp",
            "Unix timestamp of the last completed removal",
            labelnames=("stream_type", "symbol"),
            registry=self.registry,
        )
        self.dry_run = Gauge(
            "reaper_dry_run",
            "1 if the reaper is running in dry-run mode and will not act",
            registry=self.registry,
        )
        self.terminate_enabled = Gauge(
            "reaper_terminate_enabled",
            "1 if EC2 termination is armed (Phase 5b), 0 if cordon/drain only",
            registry=self.registry,
        )


def serve(metrics: ReaperMetrics, port: int):
    """Start the Prometheus HTTP endpoint in a background thread.

    prometheus_client's start_http_server owns its own thread and returns
    immediately, so this is fire-and-forget.
    """
    start_http_server(port, registry=metrics.registry)
    log.info("Metrics endpoint listening on :%d/metrics", port)
