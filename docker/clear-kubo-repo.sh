#!/bin/sh
set -eu

if [ $# -lt 1 ]; then
  echo "Usage: clear-kubo-repo.sh <ipfs-path>"
  exit 1
fi

IPFS_PATH=$1

if [ -z "${IPFS_PATH}" ]; then
  echo "IPFS_PATH is required"
  exit 1
fi

rm -f "${IPFS_PATH}/repo.lock"
rm -rf "${IPFS_PATH}/blocks" "${IPFS_PATH}/datastore" "${IPFS_PATH}/repo.lock" "${IPFS_PATH}/keystore"
mkdir -p "${IPFS_PATH}"
