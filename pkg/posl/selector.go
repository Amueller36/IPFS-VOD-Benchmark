package posl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"

	"ipfs-streaming-bench/pkg/store"
)

func RootSelector() (ipld.Node, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.Matcher().Node(), nil
}

func SegmentSelector(segmentIndex int) (ipld.Node, error) {
	if segmentIndex < 0 {
		return nil, fmt.Errorf("segment index must be non-negative")
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selectorNode := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("segments", ssb.ExploreIndex(int64(segmentIndex), ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("chunks", ssb.ExploreAll(ssb.Matcher()))
		})))
	}).Node()
	return selectorNode, nil
}

func TraverseSelector(ctx context.Context, store store.BlockStore, root cid.Cid, selectorNode ipld.Node, latency time.Duration, jitter time.Duration, onBlock func(blocks.Block)) error {
	linkSystem := newLinkSystem(store)
	linkSystem.StorageReadOpener = func(linkingCtx linking.LinkContext, link ipld.Link) (io.Reader, error) {
		SleepWithJitter(latency, jitter)
		cidLink, ok := link.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("unsupported link type: %T", link)
		}
		block, err := store.Get(linkingCtx.Ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}
		if onBlock != nil {
			onBlock(block)
		}
		return bytes.NewReader(block.RawData()), nil
	}

	rootNode, err := linkSystem.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: root}, basicnode.Prototype.Any)
	if err != nil {
		return err
	}
	parsedSelector, err := selector.ParseSelector(selectorNode)
	if err != nil {
		return err
	}
	progress := traversal.Progress{Cfg: &traversal.Config{
		LinkSystem: linkSystem,
		LinkTargetNodePrototypeChooser: func(_ ipld.Link, _ ipld.LinkContext) (ipld.NodePrototype, error) {
			return basicnode.Prototype.Any, nil
		},
	}}
	return progress.WalkMatching(rootNode, parsedSelector, func(_ traversal.Progress, _ ipld.Node) error {
		return nil
	})
}

func SleepWithJitter(latency time.Duration, jitter time.Duration) {
	if latency <= 0 && jitter <= 0 {
		return
	}
	jitterValue := time.Duration(0)
	if jitter > 0 {
		jitterValue = time.Duration(rand.Int63n(int64(jitter)))
	}
	time.Sleep(latency + jitterValue)
}
