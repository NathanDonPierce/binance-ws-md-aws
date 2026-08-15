"""Target identification and eligibility.

A verdict names a source_id, which is the Kubernetes node name that the
listener pod inherited from its host via the downward API — something like
'ip-172-31-39-170.ap-northeast-1.compute.internal'. Turning that into
something the reaper can act on takes two steps, and both belong here rather
than in the action path, because both are decidable without touching any API
and therefore testable exhaustively.

    Eligibility  Is this node one the reaper is allowed to remove? The taints
                 on Kafka and orchestrator nodes already make it impossible
                 for a listener pod to run there, so a verdict should only
                 ever name a listener. This check is the guard against that
                 assumption being wrong — a malformed verdict, a mislabelled
                 node, or a bug upstream.

    Identity     Which EC2 instance is behind that node name? Terminating
                 needs an instance id, and the node object may or may not
                 carry one depending on whether a cloud provider is
                 configured.

Nothing here performs I/O. Everything takes plain data and returns a decision
plus the reason for it, so that a refusal produces a log line explaining
itself rather than a silent no-op.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# k3s and kubelet derive the node name from the private IP for EC2 hosts:
#   ip-172-31-39-170.ap-northeast-1.compute.internal
# The dashes are the octet separators. Anchored at the start so a name that
# merely contains something IP-shaped further along does not match.
_NODE_NAME_IP = re.compile(r"^ip-(\d{1,3})-(\d{1,3})-(\d{1,3})-(\d{1,3})(?:\.|$)")

# providerID, when a cloud provider is configured:
#   aws:///ap-northeast-1a/i-0a81d486025e6ae27
# The availability zone segment is present in practice but the spec permits
# it to be empty, so it is matched loosely and discarded.
_PROVIDER_ID = re.compile(r"^aws://[^/]*/[^/]*/(i-[0-9a-f]+)$")


@dataclass(frozen=True)
class Eligibility:
    """Whether a node may be acted on, and why not if it may not.

    The reason is always populated on refusal and always reaches the log, so
    that a safety rail firing is a visible event rather than silence. A rail
    nobody has watched fire is a rail nobody knows works.
    """
    allowed: bool
    reason: str | None = None


def is_eligible_target(
    node_labels: dict[str, str] | None,
    required_label: str = "role",
    required_value: str = "listener",
):
    """Decide whether a node may be cordoned, drained and terminated.

    Args:
        node_labels: the node's labels, or None if the node was not found
        required_label: label key that marks a node as removable
        required_value: the value that key must have

    A missing node refuses rather than passing. If the reaper cannot see the
    node it cannot verify what it is, and acting on an unverifiable target is
    exactly what this function exists to prevent.
    """
    if node_labels is None:
        return Eligibility(False, "node not found in the cluster")

    actual = node_labels.get(required_label)
    if actual is None:
        return Eligibility(
            False,
            f"node has no {required_label!r} label; "
            f"only nodes labelled {required_label}={required_value} may be removed",
        )

    if actual != required_value:
        return Eligibility(
            False,
            f"node is {required_label}={actual!r}, not {required_value!r}",
        )

    return Eligibility(True)


def parse_provider_id(provider_id: str | None):
    """Extract the EC2 instance id from a Kubernetes providerID.

    Returns None rather than raising when the value is absent or in an
    unexpected shape, so the caller can fall back to the private-IP lookup
    instead of the reaper dying on a node object it did not expect.

    k3s does not populate providerID unless started with a cloud provider, so
    None is a perfectly ordinary answer here rather than an error condition.
    """
    if not provider_id:
        return None

    match = _PROVIDER_ID.match(provider_id)
    if match is None:
        return None
    return match.group(1)


def private_ip_from_node_name(node_name: str | None):
    """Recover the private IP that a kubelet-derived node name encodes.

    Used only when providerID is absent: the IP is fed to
    ec2:DescribeInstances with a private-ip-address filter to find the
    instance.

    Returns None for anything that is not in the expected shape, including a
    name whose octets are out of range — 'ip-172-31-999-1' parses
    structurally but is not an address, and passing it to the EC2 API would
    produce a confusing error a long way from the cause.
    """
    if not node_name:
        return None

    match = _NODE_NAME_IP.match(node_name)
    if match is None:
        return None

    octets = [int(g) for g in match.groups()]
    if any(o > 255 for o in octets):
        return None

    return ".".join(str(o) for o in octets)


def instance_id_for(provider_id: str | None, node_name: str | None):
    """Work out how to identify the instance behind a node.

    Returns a tuple of (instance_id, private_ip). Exactly one is populated on
    success:

        instance_id set  providerID was present and parseable; the caller can
                         terminate directly with no extra API call
        private_ip set   providerID was absent; the caller must look the
                         instance up by IP first
        both None        neither route worked; the node cannot be safely
                         identified and must not be terminated

    Keeping the fallback decision here rather than in the action path means
    the 'which strategy applies' question has one answer, arrived at the same
    way every time, and can be tested without an AWS account.
    """
    instance_id = parse_provider_id(provider_id)
    if instance_id is not None:
        return instance_id, None

    return None, private_ip_from_node_name(node_name)
