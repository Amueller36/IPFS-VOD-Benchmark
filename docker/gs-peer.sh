#!/usr/bin/env bash
set -euo pipefail

PROFILE=${SHAPE_PROFILE:-}
if [ "$PROFILE" = "fast" ]; then
  /usr/local/bin/shape_fast.sh
elif [ "$PROFILE" = "slow" ]; then
  /usr/local/bin/shape_slow.sh
fi

if [ -n "${IDENTITY_PATH:-}" ] && [ ! -f "${IDENTITY_PATH}" ]; then
  mkdir -p "$(dirname "${IDENTITY_PATH}")"
  /usr/local/bin/gs-keygen --out "${IDENTITY_PATH}"
fi

# Wait for seeded manifest on first boot.
SEED_DIR_PATH="${SEED_DIR:-/data/seed}"
MANIFEST_PATH="${SEED_DIR_PATH}/manifest.json"
WAIT_POLL_SEC="${WAIT_POLL_SEC:-2}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-0}"

echo "Waiting for seed manifest: ${MANIFEST_PATH}"
start_ts="$(date +%s)"

while [ ! -s "${MANIFEST_PATH}" ]; do
  if [ "${WAIT_TIMEOUT_SEC}" -gt 0 ] && [ $(( "$(date +%s)" - start_ts )) -ge "${WAIT_TIMEOUT_SEC}" ]; then
    echo "Seed manifest not found after ${WAIT_TIMEOUT_SEC}s: ${MANIFEST_PATH}"
    exit 1
  fi
  sleep "${WAIT_POLL_SEC}"
done

echo "Seed manifest found, starting gs-peer"

ARGS=("--store" "${STORE_DIR:-/data}" "--listen" "${LISTEN_ADDR:-/ip4/0.0.0.0/udp/4001/quic-v1}")
if [ -n "${SEED_DIR:-}" ]; then
  ARGS+=("--seed-dir" "${SEED_DIR}")
fi
if [ -n "${IDENTITY_PATH:-}" ]; then
  ARGS+=("--identity" "${IDENTITY_PATH}")
fi
if [ -n "${BOOTSTRAP_ADDRS:-}" ]; then
  ARGS+=("--bootstrap" "${BOOTSTRAP_ADDRS}")
fi

exec /usr/local/bin/gs-peer "${ARGS[@]}"
