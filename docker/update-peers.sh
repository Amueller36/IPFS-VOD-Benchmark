#!/usr/bin/env bash
set -euo pipefail

PEERS_JSON="/opt/peers.json"

PROJECT_NAME=${COMPOSE_PROJECT_NAME:-}
if [ -z "${PROJECT_NAME}" ] && [ -n "${HOSTNAME:-}" ] && command -v docker >/dev/null 2>&1; then
  PROJECT_NAME=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}}' "${HOSTNAME}" 2>/dev/null || true)
fi

resolve_container() {
  local service=$1
  local args=(ps -a --filter "label=com.docker.compose.service=${service}")
  if [ -n "${PROJECT_NAME}" ]; then
    args+=(--filter "label=com.docker.compose.project=${PROJECT_NAME}")
  fi
  args+=(--format "{{.Names}}")
  docker "${args[@]}" | awk 'NF {print; exit}'
}

get_peer_id() {
  local service=$1
  if command -v docker >/dev/null 2>&1; then
    local container
    container=$(resolve_container "${service}")
    if [ -z "${container}" ]; then
      return
    fi
    docker logs --tail 200 "${container}" 2>&1 | awk '/GraphSync peer ready:/ {id=$NF} END {if (id) print id}'
    return
  fi
  echo "docker not available" >&2
  return 127
}

fast1=$(get_peer_id gs-peer-fast-1)
fast2=$(get_peer_id gs-peer-fast-2)
slow1=$(get_peer_id gs-peer-slow-1)
slow2=$(get_peer_id gs-peer-slow-2)
slow3=$(get_peer_id gs-peer-slow-3)

if [ -z "${fast1}" ] || [ -z "${fast2}" ] || [ -z "${slow1}" ] || [ -z "${slow2}" ] || [ -z "${slow3}" ]; then
  echo "Failed to read one or more peer IDs from logs. project=${PROJECT_NAME:-unknown}" >&2
  exit 1
fi

cat > "${PEERS_JSON}" <<EOF
{
  "peers": [
    {
      "addr": "/dns4/gs-peer-fast-1/udp/4001/quic-v1/p2p/${fast1}",
      "label": "fast"
    },
    {
      "addr": "/dns4/gs-peer-fast-2/udp/4001/quic-v1/p2p/${fast2}",
      "label": "fast"
    },
    {
      "addr": "/dns4/gs-peer-slow-1/udp/4001/quic-v1/p2p/${slow1}",
      "label": "slow"
    },
    {
      "addr": "/dns4/gs-peer-slow-2/udp/4001/quic-v1/p2p/${slow2}",
      "label": "slow"
    },
    {
      "addr": "/dns4/gs-peer-slow-3/udp/4001/quic-v1/p2p/${slow3}",
      "label": "slow"
    }
  ]
}
EOF

echo "Updated ${PEERS_JSON}"
