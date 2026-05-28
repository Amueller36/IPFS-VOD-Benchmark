package posl

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"ipfs-streaming-bench/pkg/store"
)

func BuildLayout(ctx context.Context, data []byte, chunkSize int64, segmentSize int64, store store.BlockStore) (Layout, error) {
	reader := bytes.NewReader(data)
	return BuildLayoutFromReader(ctx, reader, int64(len(data)), chunkSize, segmentSize, store)
}

func BuildLayoutFromReader(ctx context.Context, reader io.Reader, size int64, chunkSize int64, segmentSize int64, store store.BlockStore) (Layout, error) {
	if chunkSize <= 0 {
		return Layout{}, fmt.Errorf("chunk size must be positive")
	}
	if segmentSize <= 0 {
		segmentSize = chunkSize
	}
	if segmentSize%chunkSize != 0 {
		return Layout{}, fmt.Errorf("segment size must be multiple of chunk size")
	}
	if size <= 0 {
		return Layout{}, fmt.Errorf("size must be positive")
	}

	linkSystem := newLinkSystem(store)
	linkPrototype := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagCbor),
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}}

	var segmentLinks []cid.Cid

	segmentChunkCapacity := int(segmentSize / chunkSize)
	chunkCount := 0
	segmentSizeBytes := int64(0)
	segmentNodes := []ipld.Node{}
	bytesRead := int64(0)
	buffer := make([]byte, chunkSize)

	flushSegment := func() error {
		if len(segmentNodes) == 0 {
			return nil
		}
		segmentNode, err := newSegmentNode(segmentNodes, segmentSizeBytes)
		if err != nil {
			return err
		}
		segmentLink, err := storeNode(ctx, linkSystem, linkPrototype, segmentNode)
		if err != nil {
			return err
		}
		segmentLinks = append(segmentLinks, segmentLink)
		segmentNodes = nil
		segmentSizeBytes = 0
		return nil
	}

	for bytesRead < size {
		toRead := chunkSize
		remaining := size - bytesRead
		if remaining < toRead {
			toRead = remaining
		}
		n, err := io.ReadFull(reader, buffer[:toRead])
		if err != nil {
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				return Layout{}, err
			}
		}
		if n == 0 {
			break
		}
		chunkBytes := append([]byte(nil), buffer[:n]...)
		chunkNode, err := newChunkNode(chunkBytes)
		if err != nil {
			return Layout{}, err
		}
		chunkLink, err := storeNode(ctx, linkSystem, linkPrototype, chunkNode)
		if err != nil {
			return Layout{}, err
		}
		segmentNodes = append(segmentNodes, basicnode.NewLink(cidlink.Link{Cid: chunkLink}))
		segmentSizeBytes += int64(len(chunkBytes))
		chunkCount++
		bytesRead += int64(n)
		if len(segmentNodes) >= segmentChunkCapacity {
			if err := flushSegment(); err != nil {
				return Layout{}, err
			}
		}
	}
	if err := flushSegment(); err != nil {
		return Layout{}, err
	}

	rootNode, err := newRootNode(segmentLinks, bytesRead, chunkSize, segmentSize)
	if err != nil {
		return Layout{}, err
	}
	rootLink, err := storeNode(ctx, linkSystem, linkPrototype, rootNode)
	if err != nil {
		return Layout{}, err
	}

	layout := Layout{
		Root:         rootLink,
		Size:         bytesRead,
		ChunkSize:    chunkSize,
		SegmentSize:  segmentSize,
		SegmentCount: len(segmentLinks),
		ChunkCount:   chunkCount,
		Segments:     segmentLinks,
	}
	return layout, nil
}

func storeNode(ctx context.Context, linkSystem linking.LinkSystem, proto cidlink.LinkPrototype, node ipld.Node) (cid.Cid, error) {
	link, err := linkSystem.Store(linking.LinkContext{Ctx: ctx}, proto, node)
	if err != nil {
		return cid.Undef, err
	}
	return link.(cidlink.Link).Cid, nil
}

func newRootNode(segmentLinks []cid.Cid, size int64, chunkSize int64, segmentSize int64) (ipld.Node, error) {
	segmentList, err := linkListNode(segmentLinks)
	if err != nil {
		return nil, err
	}
	return mapNode(map[string]ipld.Node{
		"type":        basicnode.NewString(nodeTypeRoot),
		"size":        basicnode.NewInt(size),
		"chunkSize":   basicnode.NewInt(chunkSize),
		"segmentSize": basicnode.NewInt(segmentSize),
		"segments":    segmentList,
	})
}

func newSegmentNode(chunkLinks []ipld.Node, size int64) (ipld.Node, error) {
	listNode, err := listNode(chunkLinks)
	if err != nil {
		return nil, err
	}
	return mapNode(map[string]ipld.Node{
		"type":   basicnode.NewString(nodeTypeSegment),
		"size":   basicnode.NewInt(size),
		"chunks": listNode,
	})
}

func newChunkNode(data []byte) (ipld.Node, error) {
	return mapNode(map[string]ipld.Node{
		"type": basicnode.NewString(nodeTypeChunk),
		"size": basicnode.NewInt(int64(len(data))),
		"data": basicnode.NewBytes(data),
	})
}

func mapNode(values map[string]ipld.Node) (ipld.Node, error) {
	builder := basicnode.Prototype__Map{}.NewBuilder()
	assembler, err := builder.BeginMap(int64(len(values)))
	if err != nil {
		return nil, err
	}
	for key, value := range values {
		if err := assembler.AssembleKey().AssignString(key); err != nil {
			return nil, err
		}
		if err := assembler.AssembleValue().AssignNode(value); err != nil {
			return nil, err
		}
	}
	if err := assembler.Finish(); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

func listNode(values []ipld.Node) (ipld.Node, error) {
	builder := basicnode.Prototype__List{}.NewBuilder()
	assembler, err := builder.BeginList(int64(len(values)))
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := assembler.AssembleValue().AssignNode(value); err != nil {
			return nil, err
		}
	}
	if err := assembler.Finish(); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

func linkListNode(links []cid.Cid) (ipld.Node, error) {
	values := make([]ipld.Node, 0, len(links))
	for _, link := range links {
		values = append(values, basicnode.NewLink(cidlink.Link{Cid: link}))
	}
	return listNode(values)
}

func newLinkSystem(store store.BlockStore) linking.LinkSystem {
	linkSystem := cidlink.DefaultLinkSystem()
	linkSystem.StorageReadOpener = func(linkingCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
		cidLink, ok := link.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link type: %T", link)
		}
		block, err := store.Get(linkingCtx.Ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(block.RawData()), nil
	}
	linkSystem.StorageWriteOpener = func(linkingCtx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		buffer := &bytes.Buffer{}
		committer := func(link ipld.Link) error {
			cidLink, ok := link.(cidlink.Link)
			if !ok {
				return fmt.Errorf("unsupported link type: %T", link)
			}
			block, err := blocks.NewBlockWithCid(buffer.Bytes(), cidLink.Cid)
			if err != nil {
				return err
			}
			return store.Put(linkingCtx.Ctx, block)
		}
		return buffer, committer, nil
	}
	linkSystem.EncoderChooser = func(ipld.LinkPrototype) (ipld.Encoder, error) {
		return dagcbor.Encode, nil
	}
	linkSystem.DecoderChooser = func(ipld.Link) (ipld.Decoder, error) {
		return dagcbor.Decode, nil
	}
	return linkSystem
}
