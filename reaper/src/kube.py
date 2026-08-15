"""Kubernetes operations: inspect, cordon, drain, uncordon.

Thin wrappers over the official client, kept apart from the decision logic so
that everything requiring a live API server sits in one place and everything
testable without one sits elsewhere.

A note on drain, because our workload makes it subtler than it first looks.

The Eviction API deliberately refuses to evict a pod managed by a DaemonSet.
The reasoning is sound: the DaemonSet controller would immediately recreate
the pod on the same node, so evicting it achieves nothing. This is why
`kubectl drain` requires an explicit --ignore-daemonsets flag.

Our listener pods are DaemonSet-managed. So the standard drain does not
remove them, and a reaper that only evicts would cordon the node, evict
nothing, and move on believing it had drained.

What actually matters here is not the eviction mechanism but the graceful
shutdown it normally provides. A listener holds an open WebSocket to Binance
and a Kafka producer with messages accepted but not yet acknowledged; killing
the host outright discards them. Deleting the pod directly still sends
SIGTERM and still honours terminationGracePeriodSeconds, so the listener's
handler runs, flushes the producer, and closes the socket. The graceful
property is preserved; only the PodDisruptionBudget protection is lost, and
the listener DaemonSets have no PDB for it to lose.

So: evict what can be evicted, delete DaemonSet-managed pods directly, and
wait for both to actually go before reporting the node drained.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

log = logging.getLogger("reaper.kube")

# Pods owned by these kinds are recreated on the same node if evicted, so the
# Eviction API refuses them and they must be deleted directly instead.
_NODE_BOUND_OWNERS = {"DaemonSet"}

# Static pods are managed by the kubelet from an on-disk manifest. Nothing the
# API server does can remove them; they are skipped rather than fought with.
_MIRROR_POD_ANNOTATION = "kubernetes.io/config.mirror"


def _http_status(exc):
    """Return an exception's HTTP status, or None if it does not carry one.

    The kubernetes client raises ApiException with a `status` attribute.
    Reading it by duck typing rather than catching the concrete class keeps
    this module importable without the dependency, which is what lets the
    drain logic — the subtlest part of the reaper — be tested against a fake
    API instead of a live cluster.
    """
    return getattr(exc, "status", None)


@dataclass
class NodeInfo:
    """What the reaper needs to know about a node before acting on it."""
    name: str
    labels: dict[str, str] = field(default_factory=dict)
    provider_id: str | None = None
    unschedulable: bool = False


@dataclass(frozen=True)
class DrainResult:
    """Outcome of draining one node."""
    evicted: tuple[str, ...]
    deleted: tuple[str, ...]
    skipped: tuple[str, ...]
    timed_out: bool

    @property
    def removed_count(self):
        return len(self.evicted) + len(self.deleted)


class KubeClient:
    """Wraps the Kubernetes API for the operations the reaper performs."""

    def __init__(self, core_v1=None):
        """
        Args:
            core_v1: an existing CoreV1Api, or None to build one from the
                in-cluster ServiceAccount. Injectable so tests can pass a
                fake without needing a cluster or patching imports.

        The kubernetes package is imported here rather than at module scope
        so that this file can be imported — and its logic tested against a
        fake API — without the dependency installed.
        """
        if core_v1 is None:
            from kubernetes import client, config as kube_config

            kube_config.load_incluster_config()
            core_v1 = client.CoreV1Api()
        self._api = core_v1

    # -- inspection ---------------------------------------------------------

    def get_node(self, name: str):
        """Fetch a node, or None if it does not exist.

        A missing node is an ordinary outcome rather than an error: the
        verdict may name a source whose instance has already gone for
        unrelated reasons. The caller treats None as 'ineligible', which is
        the safe response to an unverifiable target.
        """
        try:
            node = self._api.read_node(name=name)
        except Exception as e:
            if _http_status(e) == 404:
                return None
            raise

        return NodeInfo(
            name=node.metadata.name,
            labels=dict(node.metadata.labels or {}),
            provider_id=node.spec.provider_id,
            unschedulable=bool(node.spec.unschedulable),
        )

    def list_pods_on_node(self, node_name: str):
        """Every pod currently scheduled on a node, across all namespaces."""
        return self._api.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name}",
        ).items

    # -- scheduling ---------------------------------------------------------

    def cordon(self, node_name: str):
        """Mark a node unschedulable.

        Idempotent: cordoning an already-cordoned node is a no-op that
        reports success, which matters because the reaper may replay a
        verdict after a restart.

        Returns True if this call changed anything, False if the node was
        already cordoned.
        """
        node = self.get_node(node_name)
        if node is None:
            raise ValueError(f"cannot cordon unknown node {node_name!r}")

        if node.unschedulable:
            log.info("Node %s already cordoned", node_name)
            return False

        self._api.patch_node(
            name=node_name,
            body={"spec": {"unschedulable": True}},
        )
        log.info("Cordoned %s", node_name)
        return True

    def uncordon(self, node_name: str):
        """Make a node schedulable again.

        The reverse of cordon, and the reason Phase 5a is genuinely
        reversible: a node that was cordoned and drained but not terminated
        returns to service with this one call, and its DaemonSet pods are
        recreated automatically.
        """
        node = self.get_node(node_name)
        if node is None:
            raise ValueError(f"cannot uncordon unknown node {node_name!r}")

        if not node.unschedulable:
            return False

        self._api.patch_node(
            name=node_name,
            body={"spec": {"unschedulable": False}},
        )
        log.info("Uncordoned %s", node_name)
        return True

    # -- draining -----------------------------------------------------------

    def drain(self, node_name: str, timeout_seconds: float = 120.0,
              poll_interval: float = 2.0):
        """Remove the pods running on a node, gracefully.

        Evicts what the Eviction API will accept and deletes the rest
        directly — see the module docstring for why DaemonSet pods need the
        second path. Both routes send SIGTERM and honour the pod's
        terminationGracePeriodSeconds, so a listener gets its chance to flush
        the Kafka producer and close its WebSocket either way.

        Returns a DrainResult. `timed_out` being True means pods were still
        present when the deadline passed; the caller decides whether to
        proceed regardless. For a node about to be terminated the answer is
        usually yes, since the instance is going away in a moment anyway.
        """
        pods = self.list_pods_on_node(node_name)

        evicted: list[str] = []
        deleted: list[str] = []
        skipped: list[str] = []

        for pod in pods:
            name = pod.metadata.name
            namespace = pod.metadata.namespace
            qualified = f"{namespace}/{name}"

            if self._is_mirror_pod(pod):
                # Kubelet-managed from an on-disk manifest; the API server
                # cannot remove it.
                skipped.append(qualified)
                continue

            if self._is_node_bound(pod):
                if self._delete_pod(namespace, name):
                    deleted.append(qualified)
                else:
                    skipped.append(qualified)
                continue

            if self._evict_pod(namespace, name):
                evicted.append(qualified)
            else:
                skipped.append(qualified)

        timed_out = not self._wait_for_pods_gone(
            node_name=node_name,
            expected_gone=set(evicted) | set(deleted),
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )

        result = DrainResult(
            evicted=tuple(evicted),
            deleted=tuple(deleted),
            skipped=tuple(skipped),
            timed_out=timed_out,
        )
        log.info(
            "Drained %s: %d evicted, %d deleted, %d skipped%s",
            node_name, len(evicted), len(deleted), len(skipped),
            " (timed out waiting)" if timed_out else "",
        )
        return result

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _is_mirror_pod(pod):
        annotations = pod.metadata.annotations or {}
        return _MIRROR_POD_ANNOTATION in annotations

    @staticmethod
    def _is_node_bound(pod):
        """True if the pod's controller would recreate it on the same node."""
        for owner in pod.metadata.owner_references or []:
            if owner.kind in _NODE_BOUND_OWNERS:
                return True
        return False

    def _evict_pod(self, namespace: str, name: str):
        """Request eviction. Returns False if the API declined."""
        # A plain dict rather than client.V1Eviction: the API serialises it
        # identically and it avoids needing the kubernetes package imported
        # here, which keeps this path testable against a fake.
        body = {
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": name, "namespace": namespace},
        }
        try:
            self._api.create_namespaced_pod_eviction(
                name=name, namespace=namespace, body=body,
            )
            return True
        except Exception as e:
            status = _http_status(e)
            if status == 404:
                # Already gone between listing and evicting.
                return True
            if status == 429:
                # A PodDisruptionBudget refused the eviction. The listener
                # DaemonSets have no PDB, so this would mean an unexpected
                # workload on a listener node — worth reporting rather than
                # forcing.
                log.warning(
                    "Eviction of %s/%s refused by a disruption budget",
                    namespace, name,
                )
                return False
            log.warning("Eviction of %s/%s failed: %s", namespace, name, e)
            return False

    def _delete_pod(self, namespace: str, name: str):
        """Delete a pod directly, preserving its termination grace period."""
        try:
            self._api.delete_namespaced_pod(name=name, namespace=namespace)
            return True
        except Exception as e:
            if _http_status(e) == 404:
                return True
            log.warning("Delete of %s/%s failed: %s", namespace, name, e)
            return False

    def _wait_for_pods_gone(self, node_name: str, expected_gone: set[str],
                            timeout_seconds: float, poll_interval: float):
        """Poll until the named pods have left the node, or the deadline passes.

        Waiting is the point of draining. Returning as soon as the eviction
        and delete calls are accepted would mean the instance could be
        terminated while a listener was still flushing.
        """
        if not expected_gone:
            return True

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            remaining = {
                f"{p.metadata.namespace}/{p.metadata.name}"
                for p in self.list_pods_on_node(node_name)
            } & expected_gone

            if not remaining:
                return True

            time.sleep(poll_interval)

        return False
