package transport

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

type SelectorFetcher interface {
	Fetch(ctx context.Context, root cid.Cid, selector ipld.Node, onBlock func(blocks.Block)) error
}

type SegmentRequest struct {
	Root     string `json:"root"`
	Selector string `json:"selector"`
}

type SegmentResponse struct {
	Blocks []BlockEnvelope `json:"blocks"`
}

type BlockEnvelope struct {
	CID  string `json:"cid"`
	Data string `json:"data"`
}
