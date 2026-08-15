"""Reaper entry point — consume verdicts, remove the named node, confirm.

One loop, one thread of control. The reaper reaches its targets through the
Kubernetes and AWS APIs rather than running on them, so there is no leader
election to get wrong and no possibility of two instances racing on the same
verdict.

The sequence for an actionable verdict:

    1. Look the node up and check it carries the required label. A verdict
       should only ever name a listener — the taints on the Kafka and
       orchestrator nodes make it impossible for a listener pod to run
       anywhere else — but this is the guard against that assumption being
       wrong.
    2. Cordon, so nothing new lands there while it is being emptied.
    3. Drain, so the listener's SIGTERM handler runs and flushes whatever the
       Kafka producer had accepted but not yet delivered.
    4. Terminate the instance, if that is armed. Phase 5a leaves it disabled
       and stops here, at which point the whole sequence is undone by a
       single `kubectl uncordon`.
    5. Publish kill_confirmed so the arbitrator can move the source from
       'named for removal' to 'confirmed gone'.
    6. Only then commit the offset.

Committing last is what makes a crash mid-kill recoverable. Every step is
idempotent — cordoning a cordoned node, draining a drained one, terminating a
terminated instance are all no-ops or handled errors — so a restart re-reads
the verdict and repeats the sequence harmlessly. Committing first would leave
a node cordoned and drained but never terminated, with nothing to retry it.

Known gap: refused verdicts do not close the loop
-------------------------------------------------
When the reaper refuses a verdict — the node was not found, its label was
wrong, its EC2 instance could not be identified — no confirmation is
published, so the arbitrator never learns and keeps the source on its ignore
list. If the source keeps publishing (in the label-mismatch case, for
instance) the arbitrator will silently drop its messages until it is
restarted.

Fixing this properly means a kill_refused audit message and handling for it
on the arbitrator side, which is a coordinated change across both components
and out of scope for Phase 5. Operationally the workaround is to restart the
arbitrator, which clears every ignore list at once. Watch
`reaper_kills_refused_total` and act on any increment.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

import config
from aws import Ec2Client, InstanceNotFound, TerminationRefused
from decisions import VerdictTracker, should_act
from kube import KubeClient
from messages import KillConfirmed, decode, encode
from metrics import ReaperMetrics, serve as serve_metrics
from partitioning import audit_partition_for
from targeting import instance_id_for, is_eligible_target

log = logging.getLogger("reaper")


def _build_consumer(bootstrap: str):
    """Consumer for the audit topic.
    """
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 600000,
    })


def _build_producer(bootstrap: str):
    """Producer for kill confirmations.

    Same durability settings as the listener and the arbitrator: a
    confirmation that gets lost would leave the arbitrator holding a source
    as pending forever.
    """
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 5,
    })


def _publish_confirmation(producer: Producer, confirmation: KillConfirmed,
                          metrics: ReaperMetrics):
    """Write kill_confirmed back to the audit topic.

    Partitioned by stream type alone, exactly as the arbitrator partitions
    its verdicts, so the confirmation lands on the same partition as the
    verdict that caused it and is read in order behind it. A consumer must
    never see the confirmation before the verdict.
    """
    producer.produce(
        topic=config.TOPIC_AUDIT,
        partition=audit_partition_for(
            confirmation.stream_type, config.AUDIT_PARTITIONS,
        ),
        key=confirmation.stream_type.encode("utf-8"),
        value=encode(confirmation),
    )
    producer.poll(0)
    metrics.confirmations_published.labels(
        stream_type=confirmation.stream_type,
        symbol=confirmation.symbol,
    ).inc()


def _execute_kill(verdict, settings, kube: KubeClient, ec2: Ec2Client | None,
                  producer: Producer, metrics: ReaperMetrics):
    """Run the removal sequence for one verdict.

    Returns True if the sequence completed and a confirmation was published.
    Returns False if it was refused or failed partway, in which case the
    reason has already been logged and counted.
    """
    node_name = verdict.slowest_source_id
    stream_type = verdict.stream_type
    symbol = verdict.symbol

    node = kube.get_node(node_name)
    if node is None:
        log.warning(
            "REFUSED to act on %s for %s/%s: node not found in the cluster",
            node_name, stream_type, symbol,
        )
        metrics.kills_refused.labels(reason="node_not_found").inc()
        return False

    eligibility = is_eligible_target(
        node_labels=node.labels,
        required_label=settings["required_node_label"],
        required_value=settings["required_node_value"],
    )
    if not eligibility.allowed:
        log.warning(
            "REFUSED to act on %s for %s/%s: %s",
            node_name, stream_type, symbol, eligibility.reason,
        )
        # 'ineligible_label' rather than the earlier catch-all
        # 'ineligible_node': a real safety rail firing here means the taints
        # let something through that shouldn't have been reached, and that is
        # a very different alert from the target simply having disappeared.
        metrics.kills_refused.labels(reason="ineligible_label").inc()
        return False

    instance_id, private_ip = instance_id_for(
        provider_id=node.provider_id, node_name=node.name,
    )
    if settings["terminate_enabled"] and instance_id is None and private_ip is None:
        # Refuse before doing anything destructive. Cordoning and draining a
        # node we then cannot terminate would leave it drained but alive,
        # which is a worse state than not having started.
        log.warning(
            "REFUSED to act on %s: cannot determine its EC2 instance from "
            "providerID (%r) or node name",
            node_name, node.provider_id,
        )
        metrics.kills_refused.labels(reason="unidentifiable_instance").inc()
        return False

    metrics.kills_attempted.labels(
        stream_type=stream_type, symbol=symbol,
    ).inc()
    log.info(
        "Removing %s for %s/%s (instance_id=%s private_ip=%s)",
        node_name, stream_type, symbol, instance_id, private_ip,
    )

    # -- cordon ------------------------------------------------------------
    try:
        with metrics.cordon_duration.time():
            kube.cordon(node_name)
    except Exception:
        log.exception("Cordon of %s failed", node_name)
        metrics.kills_failed.labels(
            stream_type=stream_type, symbol=symbol, stage="cordon",
        ).inc()
        return False

    # -- drain -------------------------------------------------------------
    try:
        with metrics.drain_duration.time():
            result = kube.drain(
                node_name,
                timeout_seconds=settings["drain_timeout_seconds"],
            )
        if result.timed_out:
            # The instance is about to go anyway, so a lingering pod is not a
            # reason to abandon the removal — but it means something did not
            # shut down in its grace period and is worth seeing.
            log.warning(
                "Drain of %s timed out with pods still present; continuing",
                node_name,
            )
    except Exception:
        log.exception("Drain of %s failed", node_name)
        metrics.kills_failed.labels(
            stream_type=stream_type, symbol=symbol, stage="drain",
        ).inc()
        return False

    # -- terminate ---------------------------------------------------------
    if not settings["terminate_enabled"]:
        # Phase 5a stops here. The node is cordoned and emptied but alive,
        # and `kubectl uncordon` puts it straight back into service.
        log.info(
            "Termination disabled; %s is cordoned and drained but not "
            "terminated. Reverse with: kubectl uncordon %s",
            node_name, node_name,
        )
        metrics.kills_succeeded.labels(
            stream_type=stream_type, symbol=symbol,
        ).inc()
        metrics.last_kill_timestamp.labels(
            stream_type=stream_type, symbol=symbol,
        ).set(time.time())
        return True

    try:
        with metrics.terminate_duration.time():
            if instance_id is None:
                instance_id = ec2.instance_id_by_private_ip(private_ip)
            ec2.terminate(instance_id)
    except (InstanceNotFound, TerminationRefused) as e:
        log.error("Termination of %s refused or impossible: %s", node_name, e)
        metrics.kills_failed.labels(
            stream_type=stream_type, symbol=symbol, stage="terminate",
        ).inc()
        return False
    except Exception:
        log.exception("Termination of %s failed", node_name)
        metrics.kills_failed.labels(
            stream_type=stream_type, symbol=symbol, stage="terminate",
        ).inc()
        return False

    # -- confirm -----------------------------------------------------------
    _publish_confirmation(
        producer,
        KillConfirmed(
            stream_type=stream_type,
            symbol=symbol,
            source_id=node_name,
            instance_id=instance_id,
            terminated_at=time.time(),
            correlation_id=verdict.correlation_id,
        ),
        metrics,
    )

    metrics.kills_succeeded.labels(
        stream_type=stream_type, symbol=symbol,
    ).inc()
    metrics.last_kill_timestamp.labels(
        stream_type=stream_type, symbol=symbol,
    ).set(time.time())
    log.info("Removed %s (instance %s)", node_name, instance_id)
    return True


def _handle_message(msg, settings, tracker: VerdictTracker, kube: KubeClient,
                    ec2: Ec2Client | None, producer: Producer,
                    metrics: ReaperMetrics):
    """Decode one audit message and act on it if it warrants action."""
    try:
        message = decode(msg.value())
    except ValueError as e:
        log.warning(
            "Malformed audit message partition=%d offset=%d: %s",
            msg.partition(), msg.offset(), e,
        )
        metrics.audit_messages_consumed.labels(message_type="malformed").inc()
        return

    metrics.audit_messages_consumed.labels(
        message_type=type(message).__name__ if message else "unknown",
    ).inc()

    decision = should_act(message, tracker)

    # Count verdicts whether or not they were actionable — a stream producing
    # only inconclusive verdicts is a meaningfully different situation from
    # one producing none, and only this counter distinguishes them.
    from messages import VerdictMessage
    if isinstance(message, VerdictMessage):
        metrics.verdicts_received.labels(
            stream_type=message.stream_type,
            symbol=message.symbol,
            actionable=str(message.is_actionable).lower(),
        ).inc()

    if not decision.act:
        if isinstance(message, VerdictMessage):
            # Only log refusals of things that were at least verdicts; the
            # topic carries a great deal of routine traffic that is not
            # addressed to the reaper at all.
            log.debug("Not acting: %s", decision.reason)
        return

    log.info(
        "Verdict for %s/%s: %s",
        message.stream_type, message.symbol, decision.reason,
    )

    if settings["dry_run"]:
        log.info(
            "DRY RUN — would cordon, drain%s %s. Set DRY_RUN=false to act.",
            " and terminate" if settings["terminate_enabled"] else "",
            message.slowest_source_id,
        )
        metrics.kills_skipped_dry_run.labels(
            stream_type=message.stream_type, symbol=message.symbol,
        ).inc()
        # Deliberately not marked as handled: a dry run has changed nothing,
        # so the next verdict naming the same source should be considered
        # afresh rather than suppressed as a repeat.
        return

    if _execute_kill(message, settings, kube, ec2, producer, metrics):
        # Marked only after the sequence completes. Marking earlier would
        # mean a crash partway left the target recorded as done while the
        # node sat cordoned and drained but never terminated.
        tracker.mark_handled(message)


def _consume_loop(consumer: Consumer, settings, tracker, kube, ec2, producer,
                  metrics, stop_event: threading.Event):
    consumer.subscribe([config.TOPIC_AUDIT])
    log.info("Subscribed to %s", config.TOPIC_AUDIT)

    while not stop_event.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            producer.poll(0)
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error("Consumer error: %s", msg.error())
            continue

        try:
            _handle_message(
                msg, settings, tracker, kube, ec2, producer, metrics,
            )
        except Exception:
            # One bad message must not stop the loop. Log enough to
            # reproduce, then commit past it — otherwise the same message is
            # re-polled forever and no later verdict is ever reached.
            log.exception(
                "Failed to handle audit message partition=%d offset=%d",
                msg.partition(), msg.offset(),
            )

        try:
            consumer.commit(message=msg, asynchronous=True)
        except KafkaException as e:
            log.error("Failed to commit offset: %s", e)

        producer.poll(0)

    log.info("Consumer loop stopping")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    settings = config.load()

    log.info(
        "Starting reaper: bootstrap=%s dry_run=%s terminate_enabled=%s "
        "required_label=%s=%s drain_timeout=%.0fs",
        settings["kafka_bootstrap"], settings["dry_run"],
        settings["terminate_enabled"], settings["required_node_label"],
        settings["required_node_value"], settings["drain_timeout_seconds"],
    )
    if settings["dry_run"]:
        log.info("DRY RUN mode: decisions will be logged, nothing will change")

    metrics = ReaperMetrics()
    metrics.dry_run.set(1 if settings["dry_run"] else 0)
    metrics.terminate_enabled.set(1 if settings["terminate_enabled"] else 0)
    serve_metrics(metrics, settings["metrics_port"])

    kube = KubeClient()
    ec2 = None
    if settings["terminate_enabled"]:
        # Built only when armed, so a Phase 5a deployment does not need EC2
        # permissions at all and cannot terminate anything even by accident.
        ec2 = Ec2Client(region=config.discover_region())

    consumer = _build_consumer(settings["kafka_bootstrap"])
    producer = _build_producer(settings["kafka_bootstrap"])
    tracker = VerdictTracker()

    stop_event = threading.Event()

    def _handle_signal(signame: str):
        log.info("Received %s, shutting down", signame)
        stop_event.set()

    for signame in ("SIGINT", "SIGTERM"):
        signal.signal(
            getattr(signal, signame),
            lambda *_a, s=signame: _handle_signal(s),
        )

    worker = threading.Thread(
        target=_consume_loop,
        args=(consumer, settings, tracker, kube, ec2, producer, metrics,
              stop_event),
        name="audit-consumer",
    )
    worker.start()

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    finally:
        # Set, wait for the loop to notice and return, then close. Closing a
        # Consumer while another thread is inside poll() is undefined
        # behaviour in librdkafka.
        stop_event.set()
        worker.join(timeout=30)
        if worker.is_alive():
            log.warning("Consumer thread did not stop within timeout")

        log.info("Flushing producer")
        try:
            remaining = producer.flush(timeout=10)
            if remaining > 0:
                log.warning("%d messages queued at shutdown", remaining)
        except KafkaException as e:
            log.error("Producer flush error: %s", e)

        consumer.close()
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()
