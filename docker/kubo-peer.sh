#!/bin/sh
set -eu

VIDEO_PATH=${VIDEO_PATH:-/videos/input.mp4}
DATA_DIR=${DATA_DIR:-/data}
IPFS_PATH=${IPFS_PATH:-/ipfs}
PROFILE=${SHAPE_PROFILE:-}
BITSWAP_CHUNK_KB=${BITSWAP_CHUNK_KB:-1024}

case "${BITSWAP_CHUNK_KB}" in
  ''|*[!0-9]*)
    BITSWAP_CHUNK_KB=1024
    ;;
esac

if [ "${BITSWAP_CHUNK_KB}" -le 0 ]; then
  BITSWAP_CHUNK_KB=1024
elif [ "${BITSWAP_CHUNK_KB}" -le 384 ]; then
  BITSWAP_CHUNK_KB=256
elif [ "${BITSWAP_CHUNK_KB}" -le 768 ]; then
  BITSWAP_CHUNK_KB=512
else
  BITSWAP_CHUNK_KB=1024
fi
BITSWAP_CHUNK_BYTES=$((BITSWAP_CHUNK_KB * 1024))

chmod +x /usr/local/bin/shape_fast.sh /usr/local/bin/shape_slow.sh 2>/dev/null || true
if [ "$PROFILE" = "fast" ] && [ -x /usr/local/bin/shape_fast.sh ]; then
  /usr/local/bin/shape_fast.sh
elif [ "$PROFILE" = "slow" ] && [ -x /usr/local/bin/shape_slow.sh ]; then
  /usr/local/bin/shape_slow.sh
fi

tc qdisc show dev eth0 || true

mkdir -p "${IPFS_PATH}" "${DATA_DIR}"

if [ -f "${IPFS_PATH}/repo.lock" ]; then
  rm -f "${IPFS_PATH}/repo.lock"
fi

DAEMON_STARTED=0
if [ ! -f "${IPFS_PATH}/config" ]; then
  ipfs init --profile=test
fi

API_ADDR=/ip4/127.0.0.1/tcp/5001
ipfs config Addresses.API /ip4/0.0.0.0/tcp/5001
ipfs config Addresses.Gateway /ip4/0.0.0.0/tcp/8080
ipfs config Addresses.Swarm --json '["/ip4/0.0.0.0/udp/4001/quic-v1"]'
if [ -n "${ANNOUNCE_HOST:-}" ]; then
  ipfs config Addresses.Announce --json "[\"/dns4/${ANNOUNCE_HOST}/udp/4001/quic-v1\"]"
fi

if ipfs --api "${API_ADDR}" id >/dev/null 2>&1; then
  echo "Kubo already running, restarting to apply config"
  ipfs shutdown || true
fi

rm -f "${IPFS_PATH}/repo.lock"

START_ATTEMPTS=0
DAEMON_READY=0
while [ $START_ATTEMPTS -lt 6 ]; do
  START_ATTEMPTS=$((START_ATTEMPTS + 1))
  rm -f "${IPFS_PATH}/repo.lock"
  ipfs daemon --migrate=true --enable-gc=false &
  DAEMON_PID=$!
  DAEMON_STARTED=1

  until ipfs --api "${API_ADDR}" id >/dev/null 2>&1; do
    if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  if ipfs --api "${API_ADDR}" id >/dev/null 2>&1; then
    DAEMON_READY=1
    break
  fi

  echo "Daemon failed to start, retrying (${START_ATTEMPTS}/6)"
  ipfs shutdown || true
  rm -f "${IPFS_PATH}/repo.lock"
  sleep 2
done

if [ "${DAEMON_READY}" -ne 1 ]; then
  echo "Daemon failed to start after retries"
  exit 1
fi

if [ -f "${VIDEO_PATH}" ]; then
  CID_FILE="${DATA_DIR}/kubo_cid.txt"
  META_FILE="${DATA_DIR}/kubo_seed.json"
  VIDEO_SIZE=$(stat -c %s "${VIDEO_PATH}")
  VIDEO_MTIME=$(stat -c %Y "${VIDEO_PATH}")
  EXPECTED_CID=""
  if [ -f "${CID_FILE}" ] && [ -s "${CID_FILE}" ] && [ -f "${META_FILE}" ]; then
    CACHED_CID=$(cat "${CID_FILE}")
    CACHED_META_CID=$(jq -r '.root // empty' "${META_FILE}" 2>/dev/null || true)
    CACHED_CHUNK_KB=$(jq -r '.chunkKb // empty' "${META_FILE}" 2>/dev/null || true)
    CACHED_SIZE=$(jq -r '.videoSize // empty' "${META_FILE}" 2>/dev/null || true)
    CACHED_MTIME=$(jq -r '.videoMTime // empty' "${META_FILE}" 2>/dev/null || true)
    if [ "${CACHED_CID}" = "${CACHED_META_CID}" ] && [ "${CACHED_CHUNK_KB}" = "${BITSWAP_CHUNK_KB}" ] && [ "${CACHED_SIZE}" = "${VIDEO_SIZE}" ] && [ "${CACHED_MTIME}" = "${VIDEO_MTIME}" ]; then
      EXPECTED_CID="${CACHED_CID}"
      echo "Kubo CID cache matches import settings: ${EXPECTED_CID}"
    else
      echo "Kubo CID cache missing or stale; reseeding with chunker=size-${BITSWAP_CHUNK_BYTES}"
    fi
  fi

  SEED_ATTEMPTS=0
  while [ $SEED_ATTEMPTS -lt 15 ]; do
    SEED_ATTEMPTS=$((SEED_ATTEMPTS + 1))
    CID=$(ipfs --api "${API_ADDR}" add -Q --cid-version=1 --chunker="size-${BITSWAP_CHUNK_BYTES}" "${VIDEO_PATH}") && ipfs --api "${API_ADDR}" pin add "$CID" >/dev/null
    if [ -n "${CID}" ]; then
      if [ -n "${EXPECTED_CID}" ] && [ "${CID}" != "${EXPECTED_CID}" ]; then
        echo "Kubo CID changed despite matching cache metadata: got ${CID}, cached ${EXPECTED_CID}"
      fi
      echo "$CID" > "${CID_FILE}"
      cat > "${META_FILE}" <<EOF
{
  "root": "${CID}",
  "chunkKb": ${BITSWAP_CHUNK_KB},
  "chunkSize": ${BITSWAP_CHUNK_BYTES},
  "cidVersion": 1,
  "chunker": "size-${BITSWAP_CHUNK_BYTES}",
  "videoPath": "${VIDEO_PATH}",
  "videoSize": ${VIDEO_SIZE},
  "videoMTime": ${VIDEO_MTIME}
}
EOF
      echo "Kubo CID: ${CID}"
      break
    fi
    echo "Seeding failed, retrying (${SEED_ATTEMPTS}/15)"
    sleep 2
  done
fi

if [ -n "${KUBO_PEER_ADDRS:-}" ]; then
  RECONNECT_INTERVAL=${KUBO_PEER_INTERVAL:-10}
  (
    while true; do
      OLD_IFS=$IFS
      IFS=','
      connected_peers=$(ipfs --api "${API_ADDR}" swarm peers 2>/dev/null || true)
      for host in ${KUBO_PEER_ADDRS}; do
        if [ -z "${host}" ]; then
          continue
        fi
        peer_id=$(curl -s -X POST "http://${host}:5001/api/v0/id" | jq -r '.ID // empty')
        if [ -z "${peer_id}" ]; then
          continue
        fi
        if echo "${connected_peers}" | grep -q "/p2p/${peer_id}"; then
          continue
        fi
        addr="/dns4/${host}/udp/4001/quic-v1/p2p/${peer_id}"
        ipfs --api "${API_ADDR}" swarm connect "${addr}" || true
      done
      IFS=$OLD_IFS
      sleep "${RECONNECT_INTERVAL}"
    done
  ) &
fi

if [ "$DAEMON_STARTED" -eq 1 ]; then
  wait ${DAEMON_PID}
else
  tail -f /dev/null
fi
