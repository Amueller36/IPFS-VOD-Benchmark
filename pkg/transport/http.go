package transport

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"ipfs-streaming-bench/pkg/posl"
	"ipfs-streaming-bench/pkg/store"
)

type HTTPTransport struct {
	BaseURL string
	Client  *http.Client
	Latency time.Duration
	Jitter  time.Duration
}

type Server struct {
	Store  store.BlockStore
	Logger func(format string, args ...any)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", s.handleFetch)
	return mux
}

func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var request SegmentRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	root, err := cid.Parse(request.Root)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	selector, err := decodeSelector(request.Selector)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	blocksOut := make([]BlockEnvelope, 0)
	ctx := r.Context()
	err = posl.TraverseSelector(ctx, s.Store, root, selector, 0, 0, func(block blocks.Block) {
		blocksOut = append(blocksOut, BlockEnvelope{
			CID:  block.Cid().String(),
			Data: base64.StdEncoding.EncodeToString(block.RawData()),
		})
	})
	if err != nil {
		if s.Logger != nil {
			s.Logger("fetch error: %v", err)
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	payload, err := json.Marshal(SegmentResponse{Blocks: blocksOut})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}

func decodeSelector(encoded string) (ipld.Node, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	node := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(node, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return node.Build(), nil
}
