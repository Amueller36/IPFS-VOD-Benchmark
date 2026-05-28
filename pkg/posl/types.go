package posl

import "github.com/ipfs/go-cid"

const (
	nodeTypeRoot    = "root"
	nodeTypeSegment = "segment"
	nodeTypeChunk   = "chunk"
)

type Layout struct {
	Root         cid.Cid
	Size         int64
	ChunkSize    int64
	SegmentSize  int64
	SegmentCount int
	ChunkCount   int
	Segments     []cid.Cid
}
