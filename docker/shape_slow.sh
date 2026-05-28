#!/bin/sh
set -eu

RATE=${RATE_MBIT:-10}
LATENCY=${LATENCY_MS:-160}
JITTER=${JITTER_MS:-30}

tc qdisc del dev eth0 root 2>/dev/null || true

if grep -qi microsoft /proc/version 2>/dev/null || grep -qi microsoft /proc/sys/kernel/osrelease 2>/dev/null; then
  if ! tc qdisc add dev eth0 root netem delay ${LATENCY}ms ${JITTER}ms rate ${RATE}mbit 2>/tmp/tc_error.log; then
    echo "tc netem not available on WSL, skipping shaping"
    cat /tmp/tc_error.log
  fi
else
  tc qdisc add dev eth0 root netem delay ${LATENCY}ms ${JITTER}ms rate ${RATE}mbit
fi
