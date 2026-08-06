#!/bin/bash
set -euo pipefail

REGION="ap-northeast-1"
TOKEN_PARAM="/binance-ws/k3s-join-token"
SERVER_IP_PARAM="/binance-ws/k3s-server-ip"

# --- Kernel modules, pinned to the RUNNING kernel ---
# Unversioned kernel-modules-extra installs modules for the LATEST kernel, which
# won't match the running one until reboot; modprobe then fails and (under set -e)
# kills this script. See docs/troubleshooting.md.
dnf install -y "kernel-modules-extra-$(uname -r)"
modprobe overlay
modprobe br_netfilter
modprobe vxlan
cat >/etc/modules-load.d/k3s.conf <<EOF
overlay
br_netfilter
vxlan
EOF

# --- AWS CLI v2 (needed to read the join token from SSM) ---
dnf install -y unzip
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
cd /tmp && unzip -o -q awscliv2.zip && /tmp/aws/install --update
AWS=/usr/local/bin/aws

# --- Poll SSM until the join token exists (zero-touch convergence) ---
until TOKEN=$("$AWS" ssm get-parameter --name "$TOKEN_PARAM" --with-decryption --region "$REGION" --query "Parameter.Value" --output text 2>/dev/null); do
  echo "Join token not yet available in SSM, retrying in 15s..."
  sleep 15
done

SERVER_IP=$("$AWS" ssm get-parameter --name "$SERVER_IP_PARAM" --region "$REGION" --query "Parameter.Value" --output text)

# --- Join as a k3s agent, labelled and tainted for Kafka ---
# The label lets Strimzi target these nodes; the taint keeps listener pods off them.
curl -sfL https://get.k3s.io | \
  K3S_URL="https://${SERVER_IP}:6443" \
  K3S_TOKEN="${TOKEN}" \
  INSTALL_K3S_EXEC="agent --node-label role=kafka --node-taint role=kafka:NoSchedule" \
  sh -
