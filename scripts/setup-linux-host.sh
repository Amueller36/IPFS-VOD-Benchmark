#!/usr/bin/env bash
set -euo pipefail

# Host networking prerequisites for this benchmark:
# - tc/netem traffic shaping support
# - larger UDP socket buffers for libp2p/quic-go

if ! command -v sysctl >/dev/null 2>&1; then
  echo "error: sysctl not found on this host." >&2
  exit 1
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: this script must run on a native Linux host." >&2
  exit 1
fi

if grep -qiE "microsoft|wsl" /proc/sys/kernel/osrelease 2>/dev/null; then
  echo "error: WSL detected. This benchmark requires a native Linux host for tc/netem traffic shaping." >&2
  echo "       Run this project on Linux (bare metal or a Linux VM with NET_ADMIN support)." >&2
  exit 1
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "error: run as root (e.g. sudo bash scripts/setup-linux-host.sh)." >&2
  exit 1
fi

RBUF=7340032
WBUF=7340032
CONF_FILE="/etc/sysctl.d/99-ipfs-streaming-bench.conf"

echo "Applying UDP socket buffer sysctls..."
sysctl -w net.core.rmem_max="${RBUF}" >/dev/null
sysctl -w net.core.wmem_max="${WBUF}" >/dev/null
sysctl -w net.core.rmem_default="${RBUF}" >/dev/null
sysctl -w net.core.wmem_default="${WBUF}" >/dev/null

cat > "${CONF_FILE}" <<EOF
# Managed by ipfs-streaming-bench/scripts/setup-linux-host.sh
net.core.rmem_max=${RBUF}
net.core.wmem_max=${WBUF}
net.core.rmem_default=${RBUF}
net.core.wmem_default=${WBUF}
EOF

echo "Persisted settings to ${CONF_FILE}"
echo "Current values:"
sysctl net.core.rmem_max net.core.wmem_max net.core.rmem_default net.core.wmem_default

echo
echo "Done. Recreate containers if they are already running:"
echo "  docker compose up -d --force-recreate"
