package transport

import (
	"time"

	"ipfs-streaming-bench/pkg/store"
)

type LocalTransport struct {
	Store   store.BlockStore
	Latency time.Duration
	Jitter  time.Duration
}
