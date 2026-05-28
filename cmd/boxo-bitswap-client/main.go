package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/client/traceability"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"ipfs-streaming-bench/pkg/buildinfo"
	"ipfs-streaming-bench/pkg/metrics"
)

type repeatedFlag []string

func (f *repeatedFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type providerConfig struct {
	Addr  string
	Label string
	Host  string
}

type staticProviders struct {
	providers []peer.AddrInfo
}

func (s staticProviders) FindProvidersAsync(ctx context.Context, _ cid.Cid, count int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		limit := len(s.providers)
		if count > 0 && count < limit {
			limit = count
		}
		for i := 0; i < limit; i++ {
			select {
			case <-ctx.Done():
				return
			case out <- s.providers[i]:
			}
		}
	}()
	return out
}

type rangeInfo struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type cidRangeIndex map[string][]rangeInfo

type blockReceivedEvent struct {
	Type            string      `json:"type"`
	Protocol        string      `json:"protocol"`
	CID             string      `json:"cid"`
	PeerID          string      `json:"peerId,omitempty"`
	ProviderHost    string      `json:"providerHost,omitempty"`
	ProviderLabel   string      `json:"providerLabel,omitempty"`
	Bytes           int         `json:"bytes"`
	FileOffsetStart *int64      `json:"fileOffsetStart,omitempty"`
	FileOffsetEnd   *int64      `json:"fileOffsetEnd,omitempty"`
	VirtualSegments []int       `json:"virtualSegments,omitempty"`
	Ranges          []rangeInfo `json:"ranges,omitempty"`
	ArrivalTimeNs   int64       `json:"arrivalTimeNs"`
	TimeNs          int64       `json:"timeNs"`
}

type virtualSegmentReadyEvent struct {
	Type              string           `json:"type"`
	Protocol          string           `json:"protocol"`
	Segment           int              `json:"segment"`
	ReadyTimeNs       int64            `json:"readyTimeNs"`
	BytesReady        int64            `json:"bytesReady"`
	SegmentBytes      int64            `json:"segmentBytes"`
	ProviderBreakdown map[string]int64 `json:"providerBreakdown,omitempty"`
	TimeNs            int64            `json:"timeNs"`
}

type fetchSummaryEvent struct {
	Type       string `json:"type"`
	Protocol   string `json:"protocol"`
	Blocks     int    `json:"blocks"`
	Bytes      int64  `json:"bytes"`
	ElapsedNs  int64  `json:"elapsedNs"`
	SegmentCnt int    `json:"virtualSegments"`
	TimeNs     int64  `json:"timeNs"`
}

type bitswapOverheadStatsEvent struct {
	Type                       string  `json:"type"`
	Protocol                   string  `json:"protocol"`
	DataReceivedBytes          uint64  `json:"dataReceivedBytes"`
	DuplicateDataReceivedBytes uint64  `json:"duplicateDataReceivedBytes"`
	DuplicateBlocksReceived    uint64  `json:"duplicateBlocksReceived"`
	MessagesReceived           uint64  `json:"messagesReceived"`
	UsefulDataReceivedBytes    uint64  `json:"usefulDataReceivedBytes"`
	MeasurementScope           string  `json:"measurementScope,omitempty"`
	OverheadPct                float64 `json:"overheadPct"`
	TimeNs                     int64   `json:"timeNs"`
}

func main() {
	var providers repeatedFlag
	var (
		root       = flag.String("root", "", "CID to fetch")
		playbackMS = flag.Int("playback-ms", 0, "virtual playback segment duration in ms for QoE metrics")
		segments   = flag.Int("virtual-segments", 0, "virtual segment count for QoE metrics")
		segmentB   = flag.Int64("virtual-segment-bytes", 0, "virtual segment size in bytes for QoE metrics")
		sessionMS  = flag.Int("session-ms", 0, "playback session duration in ms (0 disables early stop)")
		indexMS    = flag.Int("index-timeout-ms", 15000, "timeout in ms for UnixFS metadata indexing (0 disables)")
		formatOut  = flag.String("format", "json", "output format: json or csv")
	)
	flag.Var(&providers, "provider", "provider as multiaddr,label,host; repeat for each Kubo provider")
	flag.Parse()

	if *root == "" {
		fmt.Println("--root is required")
		os.Exit(1)
	}
	if len(providers) == 0 {
		fmt.Println("at least one --provider is required")
		os.Exit(1)
	}

	ctx := context.Background()
	if *sessionMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*sessionMS)*time.Millisecond)
		defer cancel()
	}

	rootCID, err := cid.Parse(*root)
	if err != nil {
		fmt.Printf("invalid root cid: %v\n", err)
		os.Exit(1)
	}

	providerConfigs, addrInfos, labelByPeer, hostByPeer, err := parseProviders(providers)
	if err != nil {
		fmt.Printf("invalid provider config: %v\n", err)
		os.Exit(1)
	}

	h, err := libp2p.New()
	if err != nil {
		fmt.Printf("failed to create libp2p host: %v\n", err)
		os.Exit(1)
	}
	defer h.Close()

	connectedInfos := make([]peer.AddrInfo, 0, len(addrInfos))
	for _, info := range addrInfos {
		h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstoreTTL)
		err := h.Connect(ctx, info)
		emitJSON(map[string]any{
			"type":          "boxo_bitswap_connect_result",
			"protocol":      "bitswap",
			"peerId":        info.ID.String(),
			"providerHost":  hostByPeer[info.ID.String()],
			"providerLabel": labelByPeer[info.ID.String()],
			"success":       err == nil,
			"error":         errorString(err),
			"timeNs":        time.Now().UnixNano(),
		})
		if err != nil {
			fmt.Printf("provider connect failed peer=%s err=%v\n", info.ID, err)
			continue
		}
		connectedInfos = append(connectedInfos, info)
	}
	if len(connectedInfos) == 0 {
		emitBoxoError("connect", fmt.Errorf("no provider connections succeeded"))
		os.Exit(1)
	}

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	network := bsnet.NewFromIpfsHost(h)
	exchange := bitswap.New(ctx, network, staticProviders{providers: connectedInfos}, bstore,
		bitswap.WithServerEnabled(false),
		bitswap.WithClientOption(bsclient.WithTraceBlock(true)),
	)
	defer exchange.Close()
	for _, info := range connectedInfos {
		exchange.PeerConnected(info.ID)
	}
	bserv := blockservice.New(bstore, exchange)
	dserv := merkledag.NewDAGService(bserv)

	tracker := metrics.NewTracker()
	if *playbackMS > 0 && *segments > 0 && *segmentB > 0 {
		tracker.SetPlaybackModel(*segments, time.Duration(*playbackMS)*time.Millisecond)
	}

	emitJSON(map[string]any{
		"type":                   "boxo_bitswap_client_config",
		"protocol":               "bitswap",
		"root":                   rootCID.String(),
		"providers":              providerConfigs,
		"connectedProviderCount": len(connectedInfos),
		"indexTimeoutMs":         *indexMS,
		"timeNs":                 time.Now().UnixNano(),
	})

	start := time.Now()
	emitJSON(map[string]any{
		"type":     "boxo_bitswap_index_start",
		"protocol": "bitswap",
		"root":     rootCID.String(),
		"timeNs":   time.Now().UnixNano(),
	})
	indexCtx := ctx
	var indexCancel context.CancelFunc
	if *indexMS > 0 {
		indexCtx, indexCancel = context.WithTimeout(ctx, time.Duration(*indexMS)*time.Millisecond)
	} else {
		indexCtx, indexCancel = context.WithCancel(ctx)
	}
	index, mediaCIDs, err := buildUnixFSRangeIndex(indexCtx, rootCID, dserv)
	indexCancel()
	if err != nil {
		emitBoxoError("unixfs_index", err)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			fmt.Println("playback session elapsed during DAG indexing; stopping run")
			outputMetrics(tracker.Summary(), *formatOut)
			return
		}
		fmt.Printf("failed to index unixfs dag: %v\n", err)
		os.Exit(1)
	}
	emitJSON(map[string]any{
		"type":          "boxo_bitswap_index_done",
		"protocol":      "bitswap",
		"mediaCidCount": len(mediaCIDs),
		"rangeCount":    rangeCount(index),
		"elapsedNs":     time.Since(start).Nanoseconds(),
		"timeNs":        time.Now().UnixNano(),
	})

	if len(mediaCIDs) == 0 {
		outputMetrics(tracker.Summary(), *formatOut)
		return
	}

	fetchStatBaseline, fetchStatBaselineErr := exchange.Stat()
	received := newSegmentReadiness(*segments, *segmentB)
	lastBlock := start
	emitJSON(map[string]any{
		"type":          "boxo_bitswap_fetch_start",
		"protocol":      "bitswap",
		"mediaCidCount": len(mediaCIDs),
		"timeNs":        time.Now().UnixNano(),
	})
	blockCh, err := exchange.GetBlocks(ctx, mediaCIDs)
	if err != nil {
		emitBoxoError("fetch_request", err)
		fmt.Printf("failed to request blocks: %v\n", err)
		os.Exit(1)
	}

	totalBytes := int64(0)
	blockCount := 0
	for block := range blockCh {
		now := time.Now()
		blockCount++
		totalBytes += int64(len(block.RawData()))
		peerID := ""
		if traced, ok := block.(traceability.Block); ok && traced.From != "" {
			peerID = traced.From.String()
		}
		label := labelByPeer[peerID]
		hostName := hostByPeer[peerID]
		ranges := index[block.Cid().String()]
		virtualSegments := segmentsForRanges(ranges, *segmentB)
		evt := blockReceivedEvent{
			Type:            "bitswap_block_received",
			Protocol:        "bitswap",
			CID:             block.Cid().String(),
			PeerID:          peerID,
			ProviderHost:    hostName,
			ProviderLabel:   label,
			Bytes:           len(block.RawData()),
			VirtualSegments: virtualSegments,
			Ranges:          ranges,
			ArrivalTimeNs:   now.Sub(start).Nanoseconds(),
			TimeNs:          now.UnixNano(),
		}
		if len(ranges) > 0 {
			startOffset := ranges[0].Start
			endOffset := ranges[len(ranges)-1].End
			evt.FileOffsetStart = &startOffset
			evt.FileOffsetEnd = &endOffset
		}
		emitJSON(evt)
		if *segments > 0 && *segmentB > 0 {
			providerKey := hostName
			if providerKey == "" {
				providerKey = peerID
			}
			if providerKey == "" {
				providerKey = label
			}
			for _, ready := range received.addBlock(ranges, providerKey, now.Sub(start)) {
				tracker.RecordSegmentReady(ready.Segment, ready.ReadyAt)
				emitJSON(virtualSegmentReadyEvent{
					Type:              "bitswap_virtual_segment_ready",
					Protocol:          "bitswap",
					Segment:           ready.Segment,
					ReadyTimeNs:       ready.ReadyAt.Nanoseconds(),
					BytesReady:        ready.BytesReady,
					SegmentBytes:      ready.SegmentBytes,
					ProviderBreakdown: ready.ProviderBreakdown,
					TimeNs:            now.UnixNano(),
				})
			}
		}
		tracker.RecordSegment(now.Sub(lastBlock), len(block.RawData()), false)
		lastBlock = now
	}

	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("playback session elapsed; stopping block fetch")
	}
	emitJSON(fetchSummaryEvent{
		Type:       "boxo_bitswap_fetch_done",
		Protocol:   "bitswap",
		Blocks:     blockCount,
		Bytes:      totalBytes,
		ElapsedNs:  time.Since(start).Nanoseconds(),
		SegmentCnt: *segments,
		TimeNs:     time.Now().UnixNano(),
	})
	if stat, err := exchange.Stat(); err == nil && fetchStatBaselineErr == nil {
		emitJSON(newBitswapOverheadStatsEvent(bitswapStatDelta(fetchStatBaseline, stat), time.Now(), "fetch"))
	} else {
		errorText := ""
		if fetchStatBaselineErr != nil {
			errorText = fetchStatBaselineErr.Error()
		}
		if err != nil {
			errorText = err.Error()
		}
		emitJSON(map[string]any{
			"type":     "bitswap_overhead_stats_error",
			"protocol": "bitswap",
			"error":    errorText,
			"timeNs":   time.Now().UnixNano(),
		})
	}
	outputMetrics(tracker.Summary(), *formatOut)
}

func newBitswapOverheadStatsEvent(stat *bitswap.Stat, now time.Time, scope string) bitswapOverheadStatsEvent {
	if stat == nil {
		stat = &bitswap.Stat{}
	}
	event := bitswapOverheadStatsEvent{
		Type:                       "bitswap_overhead_stats",
		Protocol:                   "bitswap",
		DataReceivedBytes:          stat.DataReceived,
		DuplicateDataReceivedBytes: stat.DupDataReceived,
		DuplicateBlocksReceived:    stat.DupBlksReceived,
		MessagesReceived:           stat.MessagesReceived,
		UsefulDataReceivedBytes:    saturatingUint64Sub(stat.DataReceived, stat.DupDataReceived),
		MeasurementScope:           scope,
		TimeNs:                     now.UnixNano(),
	}
	if stat.DataReceived > 0 {
		event.OverheadPct = 100.0 * float64(stat.DupDataReceived) / float64(stat.DataReceived)
	}
	return event
}

func bitswapStatDelta(before *bitswap.Stat, after *bitswap.Stat) *bitswap.Stat {
	if before == nil {
		before = &bitswap.Stat{}
	}
	if after == nil {
		after = &bitswap.Stat{}
	}
	return &bitswap.Stat{
		BlocksReceived:   saturatingUint64Sub(after.BlocksReceived, before.BlocksReceived),
		DataReceived:     saturatingUint64Sub(after.DataReceived, before.DataReceived),
		DupBlksReceived:  saturatingUint64Sub(after.DupBlksReceived, before.DupBlksReceived),
		DupDataReceived:  saturatingUint64Sub(after.DupDataReceived, before.DupDataReceived),
		MessagesReceived: saturatingUint64Sub(after.MessagesReceived, before.MessagesReceived),
	}
}

func saturatingUint64Sub(after uint64, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

const peerstoreTTL = time.Hour

func parseProviders(values []string) ([]providerConfig, []peer.AddrInfo, map[string]string, map[string]string, error) {
	configs := make([]providerConfig, 0, len(values))
	infos := make([]peer.AddrInfo, 0, len(values))
	labels := make(map[string]string, len(values))
	hosts := make(map[string]string, len(values))
	for _, value := range values {
		parts := strings.Split(value, ",")
		addrText := strings.TrimSpace(parts[0])
		if addrText == "" {
			return nil, nil, nil, nil, fmt.Errorf("empty provider address")
		}
		label := ""
		if len(parts) > 1 {
			label = strings.TrimSpace(parts[1])
		}
		hostName := ""
		if len(parts) > 2 {
			hostName = strings.TrimSpace(parts[2])
		}
		addr, err := ma.NewMultiaddr(addrText)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		configs = append(configs, providerConfig{Addr: addrText, Label: label, Host: hostName})
		infos = append(infos, *info)
		labels[info.ID.String()] = label
		hosts[info.ID.String()] = hostName
	}
	return configs, infos, labels, hosts, nil
}

func buildUnixFSRangeIndex(ctx context.Context, root cid.Cid, dserv format.DAGService) (cidRangeIndex, []cid.Cid, error) {
	index := make(cidRangeIndex)
	orderedLeaves := make([]cid.Cid, 0)
	seen := make(map[string]struct{})
	var walk func(cid.Cid, int64) (int64, error)
	walk = func(c cid.Cid, offset int64) (int64, error) {
		if c.Prefix().Codec == cid.Raw {

			blk, err := dserv.Get(ctx, c)
			if err != nil {
				return 0, err
			}
			size := int64(len(blk.RawData()))
			addRange(index, c, offset, offset+size)
			if _, ok := seen[c.String()]; !ok {
				seen[c.String()] = struct{}{}
				orderedLeaves = append(orderedLeaves, c)
			}
			return size, nil
		}
		node, err := dserv.Get(ctx, c)
		if err != nil {
			return 0, err
		}
		protoNode, ok := node.(*merkledag.ProtoNode)
		if !ok {
			return 0, nil
		}
		data, err := unixfs.FromBytes(protoNode.Data())
		if err != nil {
			return 0, err
		}
		cur := offset
		if len(data.GetData()) > 0 {
			inlineSize := int64(len(data.GetData()))
			addRange(index, c, cur, cur+inlineSize)
			if _, ok := seen[c.String()]; !ok {
				seen[c.String()] = struct{}{}
				orderedLeaves = append(orderedLeaves, c)
			}
			cur += inlineSize
		}
		links := protoNode.Links()
		blockSizes := data.GetBlocksizes()
		for i, link := range links {
			childSize := int64(link.Size)
			if i < len(blockSizes) {
				childSize = int64(blockSizes[i])
			}
			if link.Cid.Prefix().Codec == cid.Raw {
				addRange(index, link.Cid, cur, cur+childSize)
				if _, ok := seen[link.Cid.String()]; !ok {
					seen[link.Cid.String()] = struct{}{}
					orderedLeaves = append(orderedLeaves, link.Cid)
				}
			} else {
				_, err := walk(link.Cid, cur)
				if err != nil {
					return 0, err
				}
			}
			cur += childSize
		}
		return cur - offset, nil
	}
	_, err := walk(root, 0)
	if err != nil {
		return nil, nil, err
	}
	sort.SliceStable(orderedLeaves, func(i, j int) bool {
		left := index[orderedLeaves[i].String()]
		right := index[orderedLeaves[j].String()]
		if len(left) == 0 || len(right) == 0 {
			return orderedLeaves[i].String() < orderedLeaves[j].String()
		}
		return left[0].Start < right[0].Start
	})
	return index, orderedLeaves, nil
}

func addRange(index cidRangeIndex, c cid.Cid, start int64, end int64) {
	if end <= start {
		return
	}
	index[c.String()] = append(index[c.String()], rangeInfo{Start: start, End: end})
}

func segmentsForRanges(ranges []rangeInfo, segmentBytes int64) []int {
	if segmentBytes <= 0 || len(ranges) == 0 {
		return nil
	}
	seen := map[int]struct{}{}
	for _, r := range ranges {
		start := int(r.Start / segmentBytes)
		end := int((r.End - 1) / segmentBytes)
		for segment := start; segment <= end; segment++ {
			seen[segment] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for segment := range seen {
		out = append(out, segment)
	}
	sort.Ints(out)
	return out
}

type segmentReady struct {
	Segment           int
	ReadyAt           time.Duration
	BytesReady        int64
	SegmentBytes      int64
	ProviderBreakdown map[string]int64
}

type segmentReadiness struct {
	segments  []int64
	needed    []int64
	ready     []bool
	breakdown []map[string]int64
	segmentB  int64
}

func newSegmentReadiness(count int, segmentBytes int64) *segmentReadiness {
	if count <= 0 || segmentBytes <= 0 {
		return &segmentReadiness{}
	}
	needed := make([]int64, count)
	for i := range needed {
		needed[i] = segmentBytes
	}
	return &segmentReadiness{
		segments:  make([]int64, count),
		needed:    needed,
		ready:     make([]bool, count),
		breakdown: make([]map[string]int64, count),
		segmentB:  segmentBytes,
	}
}

func (s *segmentReadiness) addBlock(ranges []rangeInfo, providerLabel string, readyAt time.Duration) []segmentReady {
	if s.segmentB <= 0 || len(s.segments) == 0 {
		return nil
	}
	if providerLabel == "" {
		providerLabel = "unknown"
	}
	ready := []segmentReady{}
	for _, r := range ranges {
		startSeg := int(r.Start / s.segmentB)
		endSeg := int((r.End - 1) / s.segmentB)
		for segment := startSeg; segment <= endSeg && segment < len(s.segments); segment++ {
			if segment < 0 || s.ready[segment] {
				continue
			}
			segStart := int64(segment) * s.segmentB
			segEnd := segStart + s.segmentB
			overlap := min64(r.End, segEnd) - max64(r.Start, segStart)
			if overlap <= 0 {
				continue
			}
			s.segments[segment] += overlap
			if s.breakdown[segment] == nil {
				s.breakdown[segment] = make(map[string]int64)
			}
			s.breakdown[segment][providerLabel] += overlap
			if s.segments[segment] >= s.needed[segment] {
				s.ready[segment] = true
				ready = append(ready, segmentReady{
					Segment:           segment,
					ReadyAt:           readyAt,
					BytesReady:        s.segments[segment],
					SegmentBytes:      s.needed[segment],
					ProviderBreakdown: cloneBreakdown(s.breakdown[segment]),
				})
			}
		}
	}
	return ready
}

func cloneBreakdown(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func emitJSON(value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		fmt.Printf("failed to encode event: %v\n", err)
		return
	}
	fmt.Println(string(payload))
}

func emitBoxoError(stage string, err error) {
	emitJSON(map[string]any{
		"type":     "boxo_bitswap_error",
		"protocol": "bitswap",
		"stage":    stage,
		"error":    errorString(err),
		"timeNs":   time.Now().UnixNano(),
	})
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func rangeCount(index cidRangeIndex) int {
	total := 0
	for _, ranges := range index {
		total += len(ranges)
	}
	return total
}

func outputMetrics(summary metrics.Summary, format string) {
	switch format {
	case "csv":
		csvOutput, err := summary.CSV()
		if err != nil {
			fmt.Printf("failed to output csv: %v\n", err)
			return
		}
		fmt.Print(csvOutput)
	default:
		jsonOutput, err := summary.JSON()
		if err != nil {
			fmt.Printf("failed to output json: %v\n", err)
			return
		}
		fmt.Printf("BENCHMARK_BUILD %s\n", buildinfo.String())
		fmt.Printf("SUMMARY_JSON %s\n", jsonOutput)
	}
}

var _ routing.ContentDiscovery = staticProviders{}
var _ blocks.Block = traceability.Block{}
