package posl

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"ipfs-streaming-bench/pkg/store"
)

func LoadLayout(ctx context.Context, store store.BlockStore, root cid.Cid) (Layout, error) {
	linkSystem := newLinkSystem(store)
	rootNode, err := linkSystem.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	if err != nil {
		return Layout{}, err
	}
	layout, err := decodeRoot(root, rootNode)
	if err != nil {
		return Layout{}, err
	}
	return layout, nil
}

func ReadSegmentWithCallback(ctx context.Context, store store.BlockStore, segment cid.Cid, onChunk func(index int, size int)) ([]byte, error) {
	linkSystem := newLinkSystem(store)
	segmentNode, err := linkSystem.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: segment}, basicnode.Prototype.Any)
	if err != nil {
		return nil, fmt.Errorf("read segment cid=%s: %w", segment, err)
	}
	chunkLinks, err := getLinkList(segmentNode, "chunks")
	if err != nil {
		return nil, fmt.Errorf("read segment cid=%s chunks: %w", segment, err)
	}
	buffer := make([]byte, 0)
	for index, link := range chunkLinks {
		chunkNode, err := linkSystem.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: link}, basicnode.Prototype.Any)
		if err != nil {
			return nil, fmt.Errorf("read segment cid=%s chunkIndex=%d chunkCid=%s: %w", segment, index, link, err)
		}
		data, err := getBytes(chunkNode, "data")
		if err != nil {
			return nil, fmt.Errorf("read segment cid=%s chunkIndex=%d chunkCid=%s data: %w", segment, index, link, err)
		}
		if onChunk != nil {
			onChunk(index, len(data))
		}
		buffer = append(buffer, data...)
	}
	return buffer, nil
}

func decodeRoot(root cid.Cid, node ipld.Node) (Layout, error) {
	if err := ensureType(node, nodeTypeRoot); err != nil {
		return Layout{}, err
	}
	size, err := getInt(node, "size")
	if err != nil {
		return Layout{}, err
	}
	chunkSize, err := getInt(node, "chunkSize")
	if err != nil {
		return Layout{}, err
	}
	segmentSize, err := getInt(node, "segmentSize")
	if err != nil {
		return Layout{}, err
	}
	segmentLinks, err := getLinkList(node, "segments")
	if err != nil {
		return Layout{}, err
	}
	return Layout{
		Root:         root,
		Size:         size,
		ChunkSize:    chunkSize,
		SegmentSize:  segmentSize,
		SegmentCount: len(segmentLinks),
		ChunkCount:   int((size + chunkSize - 1) / chunkSize),
		Segments:     segmentLinks,
	}, nil
}

func ensureType(node ipld.Node, nodeType string) error {
	value, err := getString(node, "type")
	if err != nil {
		return err
	}
	if value != nodeType {
		return fmt.Errorf("unexpected node type: %s", value)
	}
	return nil
}

func getString(node ipld.Node, field string) (string, error) {
	value, err := node.LookupByString(field)
	if err != nil {
		return "", err
	}
	return value.AsString()
}

func getInt(node ipld.Node, field string) (int64, error) {
	value, err := node.LookupByString(field)
	if err != nil {
		return 0, err
	}
	return value.AsInt()
}

func getBytes(node ipld.Node, field string) ([]byte, error) {
	value, err := node.LookupByString(field)
	if err != nil {
		return nil, err
	}
	return value.AsBytes()
}

func getLinkList(node ipld.Node, field string) ([]cid.Cid, error) {
	listNode, err := node.LookupByString(field)
	if err != nil {
		return nil, err
	}
	iterator := listNode.ListIterator()
	links := make([]cid.Cid, 0)
	for !iterator.Done() {
		_, value, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		link, err := value.AsLink()
		if err != nil {
			return nil, err
		}
		cidLink, ok := link.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unexpected link type: %T", link)
		}
		links = append(links, cidLink.Cid)
	}
	return links, nil
}
