"""EC2 operations: find an instance, terminate it.

Only reached in Phase 5b. Phase 5a runs the whole control loop with these
calls disabled, so that everything upstream — Kafka consumption, verdict
filtering, safety rails, cordon, drain, confirmation publishing — is proven
while the worst available bug is an unnecessarily drained node that
`kubectl uncordon` puts back.

Credentials come from the orchestrator's instance profile via the metadata
service, which boto3 finds without configuration. Nothing here handles keys.

The real protection against terminating the wrong thing is not in this file.
The instance profile's policy scopes ec2:TerminateInstances with a condition
on aws:ResourceTag/Role equalling 'listener', so a reaper that somehow
decided to remove a Kafka broker would be refused by AWS regardless of what
this code asked for. The checks here are the ones that produce a clear error
early; the IAM condition is the one that cannot be defeated by a bug.
"""

from __future__ import annotations

import logging

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger("reaper.aws")


class InstanceNotFound(Exception):
    """No running instance matched the lookup."""


class TerminationRefused(Exception):
    """AWS declined the termination.

    Most likely the IAM condition on the Role tag: the target was not tagged
    as a listener. That is the safety rail working, so it is raised as a
    distinct type rather than folded into a generic error.
    """


class Ec2Client:
    """Wraps the EC2 calls the reaper makes."""

    def __init__(self, region: str, client=None):
        """
        Args:
            region: AWS region, discovered from instance metadata by the
                caller rather than hard-coded, so the same image runs
                anywhere.
            client: an existing boto3 EC2 client, or None to build one.
                Injectable so tests can pass a stub.
        """
        self._client = client or boto3.client("ec2", region_name=region)

    def instance_id_by_private_ip(self, private_ip: str):
        """Find the running instance holding a private IP.

        The fallback path, used when the node object carries no providerID —
        which is the normal case on k3s, since it does not set one unless
        started with a cloud provider.

        Filters on running state deliberately. A terminated instance can
        retain its IP in the API's view briefly, and a stopped one might
        share it with something newer; neither is a valid termination target.
        """
        response = self._client.describe_instances(
            Filters=[
                {"Name": "private-ip-address", "Values": [private_ip]},
                {"Name": "instance-state-name", "Values": ["running"]},
            ],
        )

        instance_ids = [
            instance["InstanceId"]
            for reservation in response.get("Reservations", [])
            for instance in reservation.get("Instances", [])
        ]

        if not instance_ids:
            raise InstanceNotFound(
                f"no running instance with private IP {private_ip}",
            )

        if len(instance_ids) > 1:
            # A private IP is unique within a VPC at any moment, so this
            # means the lookup spanned more than one VPC. Refusing is the
            # only safe answer — there is no way to tell which is the target.
            raise InstanceNotFound(
                f"private IP {private_ip} matched {len(instance_ids)} running "
                f"instances; cannot identify the target unambiguously",
            )

        return instance_ids[0]

    def terminate(self, instance_id: str):
        """Terminate an instance.

        Idempotent from the reaper's point of view: terminating an instance
        that has already gone is reported as success, because the desired
        end state has been reached and a replay after a restart should not
        fail on it.

        Returns the instance's state as AWS reported it.
        """
        try:
            response = self._client.terminate_instances(
                InstanceIds=[instance_id],
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")

            if code == "InvalidInstanceID.NotFound":
                log.info(
                    "Instance %s already gone; treating as terminated",
                    instance_id,
                )
                return "terminated"

            if code in ("UnauthorizedOperation", "AccessDenied"):
                raise TerminationRefused(
                    f"AWS refused termination of {instance_id}. The instance "
                    f"profile only permits terminating instances tagged "
                    f"Role=listener, so this target is either untagged or is "
                    f"not a listener.",
                ) from e

            raise

        for change in response.get("TerminatingInstances", []):
            if change["InstanceId"] == instance_id:
                state = change["CurrentState"]["Name"]
                log.info("Terminating %s (state: %s)", instance_id, state)
                return state

        # A response without our instance in it should not happen, but
        # returning a definite unknown beats pretending it succeeded.
        return "unknown"
