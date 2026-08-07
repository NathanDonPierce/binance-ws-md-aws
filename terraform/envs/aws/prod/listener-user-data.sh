#!/bin/bash
set -euo pipefail

REGION="ap-northeast-1"
TOKEN_PARAM="/binance-ws/k3s-join-token"
SERVER_IP_PARAM="/binance-ws/k3s-server-ip"


dnf install -y "kernel-modules-extra-$(uname -r)"
modprobe overlay
modprobe br_netfilter
modprobe vxlan
cat >/etc/modules-load.d/k3s.conf <<EOF
overlay
br_netfilter
vxlan
EOF

# --- AWS CLI (needed to read join token from SSM) ---
dnf install -y unzip
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
cd /tmp && unzip -o -q awscliv2.zip && /tmp/aws/install --update
AWS=/usr/local/bin/aws

# --- Poll SSM until the join token exists (zero-touch convergence) ---
until TOKEN=$("$AWS" ssm get-parameter --name "$TOKEN_PARAM" --with-decryption --region "$REGION" --query "Parameter.Value" --output text 2>/dev/null); do
  echo "Join token not yet available, retrying in 15s..."
  sleep 15
done

SERVER_IP=$("$AWS" ssm get-parameter --name "$SERVER_IP_PARAM" --region "$REGION" --query "Parameter.Value" --output text)

# --- Install k3s in agent mode, joining the server ---
curl -sfL https://get.k3s.io | K3S_URL="https://${SERVER_IP}:6443" K3S_TOKEN="${TOKEN}" INSTALL_K3S_EXEC="agent --node-label role=listener" sh -
