"""Configuration for the reaper, read from the environment.

Deliberately separate from the arbitrator's config rather than shared: the two
components need different settings, and a single module serving both would
accumulate values that only one of them uses.

Topic names and partition counts are constants, because they are coupled to
the arbitrator, the listener, and the playbook that creates the topics — all
four must agree, and a setting that can drift out of step with what it must
match is a liability rather than flexibility.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request

# Shared with the arbitrator and the topic-creation playbook. Must match.
TOPIC_AUDIT = "arbitration-audit"
AUDIT_PARTITIONS = 3

CONSUMER_GROUP = "reaper"

# The instance metadata service. Used to discover the region rather than
# templating it in, so the same image runs in any region without the config
# and the deployment drifting apart.
_IMDS_TOKEN_URL = "http://169.254.169.254/latest/api/token"
_IMDS_REGION_URL = "http://169.254.169.254/latest/meta-data/placement/region"
_IMDS_TIMEOUT_SECONDS = 2.0


def _int(name: str, default: int):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{name}={raw!r} is not an integer")


def _float(name: str, default: float):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{name}={raw!r} is not a number")


def _bool(name: str, default: bool):
    """Parse a boolean environment variable.

    Kubernetes env values are always strings, so 'false' arrives as a
    non-empty string and would be truthy under a naive check — which for
    DRY_RUN would mean a flag set to 'false' still suppressed every action.
    Values are matched explicitly and anything unrecognised is rejected
    rather than guessed at.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default

    lowered = raw.strip().lower()
    if lowered in ("true", "1", "yes", "on"):
        return True
    if lowered in ("false", "0", "no", "off"):
        return False
    raise SystemExit(f"{name}={raw!r} is not a boolean")


def discover_region():
    """Discover the AWS region the reaper is running in.

    Tries the environment first — AWS_REGION is the standard override, and
    honouring it means the same image can run in a test environment without
    the metadata service. Falls back to IMDS when the variable is unset,
    which is the ordinary case on a real EC2 host.

    IMDSv2 requires a token obtained by PUT before any metadata read.
    """
    explicit = os.environ.get("AWS_REGION")
    if explicit:
        return explicit

    try:
        token_request = urllib.request.Request(
            _IMDS_TOKEN_URL,
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        with urllib.request.urlopen(
            token_request, timeout=_IMDS_TIMEOUT_SECONDS,
        ) as response:
            token = response.read().decode("utf-8")

        region_request = urllib.request.Request(
            _IMDS_REGION_URL,
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(
            region_request, timeout=_IMDS_TIMEOUT_SECONDS,
        ) as response:
            return response.read().decode("utf-8").strip()

    except (urllib.error.URLError, OSError, TimeoutError):
        raise SystemExit(
            "could not reach the instance metadata service to discover the "
            "AWS region, and AWS_REGION is not set",
        )


def load():
    """Read and validate the environment.

    Raises SystemExit on anything missing or unusable, so a misconfigured pod
    fails immediately and visibly rather than running in a state nobody
    intended.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise SystemExit("KAFKA_BOOTSTRAP is required")

    settings = {
        "kafka_bootstrap": bootstrap,

        # Phase 5a runs with this True: every decision is logged, nothing is
        # cordoned, drained or terminated. Phase 5b turns it off.
        "dry_run": _bool("DRY_RUN", True),

        # Separate from dry_run so that cordon and drain can run for real
        # while termination stays disabled — which is exactly the 5a state,
        # and the one where a mistake costs an uncordon rather than an
        # instance.
        "terminate_enabled": _bool("TERMINATE_ENABLED", False),

        # The safety rail. Only nodes carrying this label may be removed.
        "required_node_label": os.environ.get("REQUIRED_NODE_LABEL", "role"),
        "required_node_value": os.environ.get("REQUIRED_NODE_VALUE", "listener"),

        "drain_timeout_seconds": _float("DRAIN_TIMEOUT_SECONDS", 120.0),
        "metrics_port": _int("METRICS_PORT", 8080),
    }

    if settings["drain_timeout_seconds"] <= 0:
        raise SystemExit("DRAIN_TIMEOUT_SECONDS must be positive")

    if settings["terminate_enabled"] and settings["dry_run"]:
        # Not fatal, but worth being explicit that dry_run wins, rather than
        # letting someone believe termination is armed when it is not.
        settings["terminate_enabled"] = False

    return settings
