package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	gs "github.com/ipfs/go-graphsync"
	ipldformat "github.com/ipfs/go-ipld-format"
	"ipfs-streaming-bench/pkg/buildinfo"
	"ipfs-streaming-bench/pkg/discovery"
	"ipfs-streaming-bench/pkg/metrics"
	"ipfs-streaming-bench/pkg/posl"
	"ipfs-streaming-bench/pkg/prefetch"
	boxostore "ipfs-streaming-bench/pkg/store"

	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type peerConfig struct {
	Addr  string `json:"addr"`
	Label string `json:"label"`
}

type config struct {
	Peers []peerConfig `json:"peers"`
}

type peerState struct {
	info    peer.AddrInfo
	profile peerConfig
}

type adaptiveSelector struct {
	peers             []peerState
	segmentSize       int64
	playbackDelay     time.Duration
	urgentWindow      int64
	emaAlpha          float64
	logPeers          bool
	stats             map[peer.ID]*peerStats
	mu                sync.Mutex
	raceSegments      int
	raceFanout        int
	discoveryFanout   int
	probeInterval     time.Duration
	raceTimeout       time.Duration
	discoveryTimeout  time.Duration
	startTime         time.Time
	lastProbe         time.Time
	probeOffset       int
	raceOffset        int
	clientCapacity    float64
	recentTotalRate   float64
	activeRequests    int
	discoveryInflight map[peer.ID]int
	preferReprobeNext bool
}

const minCleanSamplesForFresh = 2
const unqualifiedDiscoveryRetryDelay = 5 * time.Second
const tentativeNormalInflightLimit = 1
const deadlineFitSpeedBandRatio = 0.5
const minNetworkSampleDuration = 20 * time.Millisecond
const minNetworkSampleBytes = 256 * 1024

type inflightAssignment struct {
	Segment         int
	Peer            peer.ID
	Start           time.Time
	PlaybackIndex   int64
	Deadline        time.Duration
	Estimate        time.Duration
	SafetyMargin    time.Duration
	ProjectedFinish time.Duration
	Rescued         bool
	Unsafe          bool
	RescueReason    string
}

func (a *adaptiveSelector) peerLabel(id peer.ID) string {
	for _, p := range a.peers {
		if p.info.ID == id {
			return p.profile.Label
		}
	}
	return ""
}

type peerStats struct {
	bytesTotal                   int64
	timeTotal                    time.Duration
	inflight                     int
	emaThroughput                float64
	lowerBoundThroughput         float64
	safeThroughput               float64
	emaDurationSec               float64
	segments                     int
	cleanSamples                 int
	probeSamples                 int
	contaminatedSamples          int
	unqualifiedDiscoveryAttempts int
	discoveryRetryAfter          time.Time
	lastSample                   time.Time
	lastQualifiedSample          time.Time
	consecutiveFails             int
	cooldownUntil                time.Time
}

type selectionDecision struct {
	Distance               int64
	Urgent                 bool
	Deadline               time.Duration
	Estimate               time.Duration
	ProjectedFinish        time.Duration
	SafetyMargin           time.Duration
	CandidateCount         int
	Reason                 string
	DiscoveryPeerState     string
	SelectedDiscoveryClass string
}

type throughputSample struct {
	Type                         string  `json:"type"`
	Peer                         string  `json:"peer"`
	EMA                          float64 `json:"ema"`
	SafeThroughput               float64 `json:"safeThroughput,omitempty"`
	LowerBoundThroughput         float64 `json:"lowerBoundThroughput,omitempty"`
	ClientCapacity               float64 `json:"clientCapacity,omitempty"`
	RecentTotalRate              float64 `json:"recentTotalRate,omitempty"`
	SampleQuality                string  `json:"sampleQuality,omitempty"`
	Reason                       string  `json:"reason,omitempty"`
	ActiveRequests               int     `json:"activeRequests,omitempty"`
	PeerInflight                 int     `json:"peerInflight,omitempty"`
	CleanSamples                 int     `json:"cleanSamples,omitempty"`
	ProbeSamples                 int     `json:"probeSamples,omitempty"`
	QualifiedSamples             int     `json:"qualifiedSamples,omitempty"`
	ContaminatedSamples          int     `json:"contaminatedSamples,omitempty"`
	UnqualifiedDiscoveryAttempts int     `json:"unqualifiedDiscoveryAttempts,omitempty"`
	LastQualifiedSample          int64   `json:"lastQualifiedSample,omitempty"`
	Bytes                        int     `json:"bytes"`
	Duration                     int64   `json:"durationNs"`
	TimeNS                       int64   `json:"timeNs"`
}

type durationPredictionSample struct {
	Type                string  `json:"type"`
	Segment             int     `json:"segment"`
	PlaybackIndex       int64   `json:"playbackIndex"`
	Distance            int64   `json:"distance"`
	Urgent              bool    `json:"urgent"`
	Method              string  `json:"method"`
	RequestKind         string  `json:"requestKind"`
	PeerID              string  `json:"peer"`
	PeerLabel           string  `json:"peerLabel,omitempty"`
	PredictedDuration   int64   `json:"predictedDurationNs,omitempty"`
	PredictedThroughput float64 `json:"predictedThroughput,omitempty"`
	InflightAtStart     int     `json:"inflightAtStart"`
	ActualDuration      int64   `json:"actualDurationNs,omitempty"`
	ActualThroughput    float64 `json:"actualThroughput,omitempty"`
	Bytes               int     `json:"bytes,omitempty"`
	Success             bool    `json:"success"`
	Outcome             string  `json:"outcome"`
	Reason              string  `json:"reason,omitempty"`
	Error               string  `json:"error,omitempty"`
	StartedTimeNS       int64   `json:"startedTimeNs"`
	TimeNS              int64   `json:"timeNs"`
}

type durationPrediction struct {
	duration       time.Duration
	throughput     float64
	inflight       int
	activeRequests int
}

type schedulerConfigPeer struct {
	PeerID string `json:"peerId"`
	Label  string `json:"label,omitempty"`
}

type schedulerConfigEvent struct {
	Type             string                `json:"type"`
	UrgentWindow     int64                 `json:"urgentWindow"`
	PlaybackDelay    int64                 `json:"playbackDelayNs"`
	RaceFanout       int                   `json:"raceFanout"`
	DiscoveryFanout  int                   `json:"discoveryFanout"`
	RaceSegments     int                   `json:"raceSegments"`
	ProbeInterval    int64                 `json:"probeIntervalNs"`
	DiscoveryTimeout int64                 `json:"discoveryTimeoutNs"`
	PeerCount        int                   `json:"peerCount"`
	Peers            []schedulerConfigPeer `json:"peers,omitempty"`
	TimeNS           int64                 `json:"timeNs"`
}

type schedulerDecisionPeer struct {
	PeerID   string `json:"peerId"`
	Label    string `json:"label,omitempty"`
	Estimate int64  `json:"estimateNs,omitempty"`
}

type schedulerDecisionEvent struct {
	Type                         string                  `json:"type"`
	Segment                      int                     `json:"segment"`
	PlaybackIndex                int64                   `json:"playbackIndex"`
	Distance                     int64                   `json:"distance"`
	Urgent                       bool                    `json:"urgent"`
	Method                       string                  `json:"method"`
	Reason                       string                  `json:"reason,omitempty"`
	Deadline                     int64                   `json:"deadlineNs,omitempty"`
	Estimate                     int64                   `json:"estimateNs,omitempty"`
	ProjectedFinish              int64                   `json:"projectedFinishNs,omitempty"`
	SafetyMargin                 int64                   `json:"safetyMarginNs,omitempty"`
	CandidateCount               int                     `json:"candidateCount,omitempty"`
	RaceFanout                   int                     `json:"raceFanout,omitempty"`
	SelectedPeerID               string                  `json:"selectedPeer,omitempty"`
	SelectedPeerLabel            string                  `json:"selectedLabel,omitempty"`
	SelectedEstimate             int64                   `json:"selectedEstimateNs,omitempty"`
	OriginalPeerID               string                  `json:"originalPeer,omitempty"`
	Candidates                   []schedulerDecisionPeer `json:"candidates,omitempty"`
	UrgentWindowCached           bool                    `json:"urgentWindowCached"`
	UrgentWindowCovered          bool                    `json:"urgentWindowCovered"`
	ThroughputDiscoveryNeeded    bool                    `json:"throughputDiscoveryNeeded"`
	StalePeerCount               int                     `json:"stalePeerCount"`
	DiscoveryDebtCoverage        int                     `json:"discoveryDebtCoverage,omitempty"`
	DiscoveryDebtReprobe         int                     `json:"discoveryDebtReprobe,omitempty"`
	DiscoveryInflight            int                     `json:"discoveryInflight,omitempty"`
	SelectedDiscoveryClass       string                  `json:"selectedDiscoveryClass,omitempty"`
	SelectedQualifiedSamples     int                     `json:"selectedQualifiedSamples,omitempty"`
	SelectedNormalInflight       int                     `json:"selectedNormalInflight,omitempty"`
	UnqualifiedDiscoveryAttempts int                     `json:"unqualifiedDiscoveryAttempts,omitempty"`
	DiscoveryPeerState           string                  `json:"discoveryPeerState,omitempty"`
	DiscoveryTimeout             int64                   `json:"discoveryTimeoutNs,omitempty"`
	DiscoveryFanout              int                     `json:"discoveryFanout,omitempty"`
	TimeNS                       int64                   `json:"timeNs"`
}

type schedulerDiscoverySkipEvent struct {
	Type                         string `json:"type"`
	Segment                      int    `json:"segment"`
	PlaybackIndex                int64  `json:"playbackIndex"`
	Distance                     int64  `json:"distance"`
	Urgent                       bool   `json:"urgent"`
	Reason                       string `json:"reason"`
	UrgentWindowCached           bool   `json:"urgentWindowCached"`
	UrgentWindowCovered          bool   `json:"urgentWindowCovered"`
	ThroughputDiscoveryNeeded    bool   `json:"throughputDiscoveryNeeded"`
	DiscoveryDebtCoverage        int    `json:"discoveryDebtCoverage,omitempty"`
	DiscoveryDebtReprobe         int    `json:"discoveryDebtReprobe,omitempty"`
	DiscoveryInflight            int    `json:"discoveryInflight,omitempty"`
	DiscoveryFanout              int    `json:"discoveryFanout,omitempty"`
	SelectedQualifiedSamples     int    `json:"selectedQualifiedSamples,omitempty"`
	SelectedNormalInflight       int    `json:"selectedNormalInflight,omitempty"`
	UnqualifiedDiscoveryAttempts int    `json:"unqualifiedDiscoveryAttempts,omitempty"`
	TimeNS                       int64  `json:"timeNs"`
}

type schedulerResultEvent struct {
	Type                     string `json:"type"`
	Segment                  int    `json:"segment"`
	PlaybackIndex            int64  `json:"playbackIndex"`
	Distance                 int64  `json:"distance"`
	Urgent                   bool   `json:"urgent"`
	Method                   string `json:"method"`
	Reason                   string `json:"reason,omitempty"`
	PeerID                   string `json:"peer,omitempty"`
	PeerLabel                string `json:"peerLabel,omitempty"`
	Duration                 int64  `json:"durationNs,omitempty"`
	Bytes                    int    `json:"bytes,omitempty"`
	Success                  bool   `json:"success"`
	Error                    string `json:"error,omitempty"`
	RaceWastedBytes          int    `json:"raceWastedBytes"`
	RaceTotalBytes           int    `json:"raceTotalBytes"`
	RaceCompletedCandidates  int    `json:"raceCompletedCandidates,omitempty"`
	RaceCancelledCandidates  int    `json:"raceCancelledCandidates,omitempty"`
	RaceSuccessfulCandidates int    `json:"raceSuccessfulCandidates,omitempty"`
	RaceReturnReason         string `json:"raceReturnReason,omitempty"`
	TimeNS                   int64  `json:"timeNs"`
}

type schedulerRaceOverheadEvent struct {
	Type                     string `json:"type"`
	Segment                  int    `json:"segment"`
	PlaybackIndex            int64  `json:"playbackIndex"`
	Distance                 int64  `json:"distance"`
	Urgent                   bool   `json:"urgent"`
	Reason                   string `json:"reason,omitempty"`
	WinnerPeer               string `json:"winnerPeer,omitempty"`
	WinnerPeerLabel          string `json:"winnerPeerLabel,omitempty"`
	RaceUsefulBytes          int    `json:"raceUsefulBytes"`
	RaceWastedBytes          int    `json:"raceWastedBytes"`
	RaceTotalBytes           int    `json:"raceTotalBytes"`
	RaceStartedCandidates    int    `json:"raceStartedCandidates"`
	RaceCompletedCandidates  int    `json:"raceCompletedCandidates"`
	RaceCancelledCandidates  int    `json:"raceCancelledCandidates"`
	RaceSuccessfulCandidates int    `json:"raceSuccessfulCandidates"`
	RaceMetricsComplete      bool   `json:"raceMetricsComplete"`
	TimeNS                   int64  `json:"timeNs"`
}

func emitStructuredEvent(event interface{}) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	fmt.Println(string(payload))
}

func (g *graphsyncFetcher) emitDiscoverySkip(index int, playbackIndex int64, distance int64, urgent bool, reason string, urgentWindowCached bool, urgentWindowCovered bool, discoveryNeeded bool) {
	if g == nil || g.selector == nil {
		return
	}
	now := time.Now()
	debt := g.selector.discoveryDebt(g.selector.peers, now)
	emitStructuredEvent(schedulerDiscoverySkipEvent{
		Type:                      "scheduler_discovery_skip",
		Segment:                   index,
		PlaybackIndex:             playbackIndex,
		Distance:                  distance,
		Urgent:                    urgent,
		Reason:                    reason,
		UrgentWindowCached:        urgentWindowCached,
		UrgentWindowCovered:       urgentWindowCovered,
		ThroughputDiscoveryNeeded: discoveryNeeded,
		DiscoveryDebtCoverage:     debt.coverage,
		DiscoveryDebtReprobe:      debt.reprobe,
		DiscoveryInflight:         g.selector.activeDiscoveryInflight(now),
		DiscoveryFanout:           g.selector.discoveryFanout,
		TimeNS:                    time.Now().UnixNano(),
	})
}

func (a *adaptiveSelector) selectPeerWithInfo(segmentIndex int, currentIndex int64) (peerState, selectionDecision) {
	distance := int64(segmentIndex) - currentIndex
	decision := selectionDecision{Distance: distance, Urgent: distance <= a.urgentWindow}
	if decision.Urgent {
		peer, estimate, candidateCount, ok := a.fastestBalancedMeasuredPeer()
		if ok {
			decision.Reason = "urgent_balanced_fast_measured"
			decision.Estimate = estimate
			decision.ProjectedFinish = a.projectedFinish(peer, estimate)
			decision.CandidateCount = candidateCount
			return peer, decision
		}
		peer, estimate, candidateCount, ok = a.fastestBalancedUsablePeer()
		if ok {
			decision.Reason = "urgent_tentative_measured_fallback"
			decision.Estimate = estimate
			decision.ProjectedFinish = a.projectedFinish(peer, estimate)
			decision.CandidateCount = candidateCount
			return peer, decision
		}
		decision.Reason = "urgent_unmeasured_fallback"
		return a.peers[0], decision
	}
	deadline := time.Duration(distance) * a.playbackDelay
	decision.Deadline = deadline
	safetyMargin := a.safetyMargin()
	decision.SafetyMargin = safetyMargin
	peer, projectedFinish, candidateCount, ok := a.slowestDeadlineFitPeer(deadline, safetyMargin)
	decision.CandidateCount = candidateCount
	if ok {
		decision.Reason = "deadline_slowest_fit"
		decision.Estimate = a.estimatedTime(peer)
		decision.ProjectedFinish = projectedFinish
		return peer, decision
	}
	peer, ok = a.fastestMeasuredPeerForNormal()
	if ok {
		decision.Reason = "nonurgent_fastest_measured_fallback"
		decision.Estimate = a.estimatedTime(peer)
		decision.ProjectedFinish = a.projectedFinish(peer, decision.Estimate)
		return peer, decision
	}
	decision.Reason = "nonurgent_unmeasured_fallback"
	return a.peers[0], decision
}

func (a *adaptiveSelector) safetyMargin() time.Duration {
	if a.playbackDelay > 0 {
		return a.playbackDelay
	}
	return 0
}

func (a *adaptiveSelector) throughputFreshness() time.Duration {
	if a.probeInterval > 0 {
		return a.probeInterval
	}
	return 60 * time.Second
}

func (a *adaptiveSelector) hasUsableThroughput(id peer.ID, now time.Time) bool {
	stats := a.stats[id]
	if stats == nil || stats.emaThroughput <= 0 || stats.emaDurationSec <= 0 {
		return false
	}
	if !stats.cooldownUntil.IsZero() && now.Before(stats.cooldownUntil) {
		return false
	}
	if stats.segments <= 0 {
		return a.startTime.IsZero()
	}
	if stats.lastSample.IsZero() {
		return a.startTime.IsZero()
	}
	return now.Sub(stats.lastSample) <= a.throughputFreshness()
}

func (a *adaptiveSelector) throughputDiscoveryNeeded(peers []peerState) (bool, int) {
	debt := a.discoveryDebt(peers, time.Now())
	return debt.total() > 0, debt.total()
}

type discoveryDebtCounts struct {
	coverage int
	reprobe  int
}

func (d discoveryDebtCounts) total() int {
	return d.coverage + d.reprobe
}

func discoveryClassForState(state string) string {
	switch state {
	case "unknown", "tentative":
		return "coverage"
	case "stale":
		return "reprobe"
	default:
		return ""
	}
}

func (a *adaptiveSelector) discoveryDebt(peers []peerState, now time.Time) discoveryDebtCounts {
	debt := discoveryDebtCounts{}
	for _, p := range peers {
		if a.isCoolingDown(p.info.ID) {
			continue
		}
		state := a.throughputState(p.info.ID, now)
		switch discoveryClassForState(state) {
		case "coverage":
			debt.coverage++
		case "reprobe":
			debt.reprobe++
		}
	}
	return debt
}

func (a *adaptiveSelector) throughputState(id peer.ID, now time.Time) string {
	stats := a.stats[id]
	if stats != nil && !stats.cooldownUntil.IsZero() && now.Before(stats.cooldownUntil) {
		return "cooldown"
	}
	if stats == nil || stats.emaThroughput <= 0 || stats.emaDurationSec <= 0 || stats.segments <= 0 || stats.lastSample.IsZero() {
		return "unknown"
	}
	qualifiedSamples := qualifiedSamples(stats)
	if qualifiedSamples < minCleanSamplesForFresh {
		return "tentative"
	}
	qualifiedAt := lastQualifiedSample(stats)
	if qualifiedAt.IsZero() || now.Sub(qualifiedAt) > a.throughputFreshness() {
		return "stale"
	}
	return "fresh"
}

func (a *adaptiveSelector) activeDiscoveryInflight(now time.Time) int {
	total, _, _ := a.discoveryInflightCounts(now)
	return total
}

func (a *adaptiveSelector) discoveryInflightCounts(now time.Time) (int, int, int) {
	total := 0
	coverage := 0
	reprobe := 0
	for id, count := range a.discoveryInflight {
		if count <= 0 {
			continue
		}
		total += count
		switch discoveryClassForState(a.throughputState(id, now)) {
		case "coverage":
			coverage += count
		case "reprobe":
			reprobe += count
		default:
			coverage += count
		}
	}
	return total, coverage, reprobe
}

func (a *adaptiveSelector) discoveryPeerWithClass() (peerState, string, string, string, bool) {
	now := time.Now()
	if a.discoveryFanout <= 0 {
		return peerState{}, "", "", "fanout_full", false
	}
	totalInflight, coverageInflight, reprobeInflight := a.discoveryInflightCounts(now)
	if totalInflight >= a.discoveryFanout {
		return peerState{}, "", "", "fanout_full", false
	}
	debt := a.discoveryDebt(a.peers, now)
	if debt.total() == 0 {
		return peerState{}, "", "", "no_candidate", false
	}
	classOrder := a.discoveryClassOrder(debt, coverageInflight, reprobeInflight)
	if len(classOrder) == 0 {
		return peerState{}, "", "", "fanout_full", false
	}
	for _, class := range classOrder {
		if peer, state, ok := a.bestDiscoveryPeerForClass(now, class); ok {
			if a.discoveryFanout == 1 && debt.coverage > 0 && debt.reprobe > 0 {
				a.preferReprobeNext = class == "coverage"
			}
			return peer, state, class, "", true
		}
	}
	if a.blockedDiscoveryCandidateExists(now, classOrder) {
		return peerState{}, "", "", "no_isolated_discovery_candidate", false
	}
	return peerState{}, "", "", "no_candidate", false
}

func (a *adaptiveSelector) discoveryClassOrder(debt discoveryDebtCounts, coverageInflight int, reprobeInflight int) []string {
	if a.discoveryFanout <= 1 && debt.coverage > 0 && debt.reprobe > 0 {
		if a.preferReprobeNext {
			return []string{"reprobe", "coverage"}
		}
		return []string{"coverage", "reprobe"}
	}
	if a.discoveryFanout >= 2 {
		order := make([]string, 0, 2)
		if debt.reprobe > 0 && reprobeInflight < 1 {
			order = append(order, "reprobe")
		}
		if debt.coverage > 0 {
			coverageLimit := a.discoveryFanout
			if debt.reprobe > 0 {
				coverageLimit = a.discoveryFanout - 1
				if coverageLimit < 1 {
					coverageLimit = 1
				}
			}
			if coverageInflight < coverageLimit {
				order = append(order, "coverage")
			}
		}
		return order
	}
	return []string{"coverage", "reprobe"}
}

func (a *adaptiveSelector) blockedDiscoveryCandidateExists(now time.Time, classes []string) bool {
	classSet := make(map[string]bool, len(classes))
	for _, class := range classes {
		classSet[class] = true
	}
	for _, candidate := range a.peers {
		state := a.throughputState(candidate.info.ID, now)
		class := discoveryClassForState(state)
		if !classSet[class] {
			continue
		}
		if a.discoveryInflight[candidate.info.ID] > 0 {
			continue
		}
		stats := a.stats[candidate.info.ID]
		if normalInflight(stats) > 0 {
			return true
		}
		if stats != nil && !stats.discoveryRetryAfter.IsZero() && now.Before(stats.discoveryRetryAfter) {
			return true
		}
	}
	return false
}

func (a *adaptiveSelector) bestDiscoveryPeerForClass(now time.Time, class string) (peerState, string, bool) {
	best := peerState{}
	bestState := ""
	bestDiscoveryInflight := 0
	bestInflight := 0
	bestLastSample := time.Time{}
	bestSegments := 0
	bestQualifiedSamples := 0
	bestSet := false

	for _, candidate := range a.peers {
		state := a.throughputState(candidate.info.ID, now)
		if discoveryClassForState(state) != class {
			continue
		}
		currentDiscoveryInflight := a.discoveryInflight[candidate.info.ID]
		if currentDiscoveryInflight > 0 {
			continue
		}
		stats := a.stats[candidate.info.ID]
		inflight := 0
		lastSample := time.Time{}
		segments := 0
		currentQualifiedSamples := 0
		if stats != nil {
			inflight = stats.inflight
			if inflight > 0 {
				continue
			}
			if !stats.discoveryRetryAfter.IsZero() && now.Before(stats.discoveryRetryAfter) {
				continue
			}
			lastSample = lastQualifiedSample(stats)
			if lastSample.IsZero() {
				lastSample = stats.lastSample
			}
			segments = stats.segments
			currentQualifiedSamples = qualifiedSamples(stats)
		}
		if !bestSet {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			bestSet = true
			continue
		}
		if class == "coverage" {
			stateRank := coverageDiscoveryStateRank(state)
			bestStateRank := coverageDiscoveryStateRank(bestState)
			if stateRank < bestStateRank {
				best = candidate
				bestState = state
				bestDiscoveryInflight = currentDiscoveryInflight
				bestInflight = inflight
				bestLastSample = lastSample
				bestSegments = segments
				bestQualifiedSamples = currentQualifiedSamples
				continue
			}
			if stateRank > bestStateRank {
				continue
			}
		}
		if class == "reprobe" {
			if lastSample.Before(bestLastSample) {
				best = candidate
				bestState = state
				bestDiscoveryInflight = currentDiscoveryInflight
				bestInflight = inflight
				bestLastSample = lastSample
				bestSegments = segments
				bestQualifiedSamples = currentQualifiedSamples
				continue
			}
			if bestLastSample.Before(lastSample) {
				continue
			}
		}
		if currentDiscoveryInflight < bestDiscoveryInflight {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			continue
		}
		if currentDiscoveryInflight > bestDiscoveryInflight {
			continue
		}
		if currentQualifiedSamples < bestQualifiedSamples {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			continue
		}
		if currentQualifiedSamples > bestQualifiedSamples {
			continue
		}
		if inflight < bestInflight {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			continue
		}
		if inflight > bestInflight {
			continue
		}
		if lastSample.Before(bestLastSample) {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			continue
		}
		if bestLastSample.Before(lastSample) {
			continue
		}
		if segments < bestSegments {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
			continue
		}
		if segments > bestSegments {
			continue
		}
		if candidate.info.ID.String() < best.info.ID.String() {
			best = candidate
			bestState = state
			bestDiscoveryInflight = currentDiscoveryInflight
			bestInflight = inflight
			bestLastSample = lastSample
			bestSegments = segments
			bestQualifiedSamples = currentQualifiedSamples
		}
	}
	return best, bestState, bestSet
}

func coverageDiscoveryStateRank(state string) int {
	switch state {
	case "unknown":
		return 0
	case "tentative":
		return 1
	default:
		return 2
	}
}

func (a *adaptiveSelector) projectedFinish(peer peerState, estimate time.Duration) time.Duration {
	latency, transfer, ok := a.peerTiming(peer)
	if !ok {
		return 0
	}
	stats := a.stats[peer.info.ID]
	inflight := 0
	if stats != nil {
		inflight = stats.inflight
	}
	projected := latency + time.Duration(inflight+1)*transfer
	if projected <= 0 && estimate > 0 {
		return estimate
	}
	return projected
}

func (a *adaptiveSelector) projectedBacklog(peer peerState) time.Duration {
	_, transfer, ok := a.peerTiming(peer)
	if !ok {
		return 0
	}
	stats := a.stats[peer.info.ID]
	if stats == nil || stats.inflight <= 0 {
		return 0
	}
	return time.Duration(stats.inflight) * transfer
}

func (a *adaptiveSelector) slowestDeadlineFitPeer(deadline time.Duration, safetyMargin time.Duration) (peerState, time.Duration, int, bool) {
	if deadline <= 0 {
		return peerState{}, 0, 0, false
	}
	budget := deadline - safetyMargin
	if budget <= 0 {
		return peerState{}, 0, 0, false
	}
	type deadlineFitCandidate struct {
		peer        peerState
		estimate    time.Duration
		projected   time.Duration
		utilization float64
		backlog     time.Duration
		segments    int
	}
	candidates := make([]deadlineFitCandidate, 0, len(a.peers))
	maxEstimate := time.Duration(0)
	candidateCount := 0
	now := time.Now()

	for _, candidate := range a.peers {
		if a.isCoolingDown(candidate.info.ID) {
			continue
		}
		if a.shouldThrottleTentativeNormal(candidate.info.ID, now) {
			continue
		}
		estimate := a.estimatedTime(candidate)
		if estimate == 0 {
			continue
		}
		projected := a.projectedFinish(candidate, estimate)
		if projected == 0 || projected > budget {
			continue
		}
		utilization := float64(projected) / float64(budget)
		candidateCount++
		stats := a.stats[candidate.info.ID]
		backlog := a.projectedBacklog(candidate)
		segments := 0
		if stats != nil {
			segments = stats.segments
		}
		candidates = append(candidates, deadlineFitCandidate{
			peer:        candidate,
			estimate:    estimate,
			projected:   projected,
			utilization: utilization,
			backlog:     backlog,
			segments:    segments,
		})
		if estimate > maxEstimate {
			maxEstimate = estimate
		}
	}
	if len(candidates) == 0 {
		return peerState{}, 0, candidateCount, false
	}

	minEstimate := time.Duration(float64(maxEstimate) * deadlineFitSpeedBandRatio)
	best := deadlineFitCandidate{}
	bestSet := false
	for _, candidate := range candidates {
		if minEstimate > 0 && candidate.estimate < minEstimate {
			continue
		}
		if !bestSet {
			best = candidate
			bestSet = true
			continue
		}
		if candidate.backlog < best.backlog {
			best = candidate
			continue
		}
		if candidate.backlog > best.backlog {
			continue
		}
		if utilizationsNearEqual(candidate.utilization, best.utilization) && candidate.segments < best.segments {
			best = candidate
			continue
		}
		if utilizationsNearEqual(candidate.utilization, best.utilization) && candidate.segments > best.segments {
			continue
		}
		if candidate.estimate > best.estimate {
			best = candidate
			continue
		}
		if candidate.estimate < best.estimate {
			continue
		}
		if candidate.segments < best.segments {
			best = candidate
			continue
		}
		if candidate.segments > best.segments {
			continue
		}
		if candidate.peer.info.ID.String() < best.peer.info.ID.String() {
			best = candidate
		}
	}
	return best.peer, best.projected, candidateCount, bestSet
}

func (a *adaptiveSelector) shouldThrottleTentativeNormal(id peer.ID, now time.Time) bool {
	if a.discoveryDebt(a.peers, now).coverage <= 0 {
		return false
	}
	if a.throughputState(id, now) != "tentative" {
		return false
	}
	stats := a.stats[id]
	return normalInflight(stats) >= tentativeNormalInflightLimit
}

func utilizationsNearEqual(a float64, b float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	larger := a
	if b > larger {
		larger = b
	}
	return diff/larger <= 0.15
}

func estimatesNearEqual(a time.Duration, b time.Duration) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	larger := a
	if b > larger {
		larger = b
	}
	return float64(diff)/float64(larger) <= 0.15
}

func (a *adaptiveSelector) estimatedTime(peer peerState) time.Duration {
	latency, transfer, ok := a.peerTiming(peer)
	if !ok {
		return 0
	}
	return latency + transfer
}

func (a *adaptiveSelector) peerTiming(peer peerState) (time.Duration, time.Duration, bool) {
	stats := a.stats[peer.info.ID]
	if stats == nil {
		return 0, 0, false
	}
	if stats != nil && !stats.cooldownUntil.IsZero() && time.Now().Before(stats.cooldownUntil) {
		return 0, 0, false
	}
	if !a.hasUsableThroughput(peer.info.ID, time.Now()) {
		return 0, 0, false
	}
	if stats != nil && stats.emaDurationSec > 0 && stats.emaThroughput > 0 {
		rate := stats.safeThroughput
		if rate <= 0 {
			rate = stats.emaThroughput
		}
		rawTransfer := time.Duration(float64(time.Second) * (float64(a.segmentSize) / stats.emaThroughput))
		transfer := time.Duration(float64(time.Second) * (float64(a.segmentSize) / rate))
		latency := time.Duration(stats.emaDurationSec*float64(time.Second)) - rawTransfer
		if latency < 0 {
			latency = 0
		}
		return latency, transfer, true
	}

	return 0, 0, false
}

func (a *adaptiveSelector) predictionSnapshot(peer peerState) durationPrediction {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[peer.info.ID]
	if stats == nil {
		return durationPrediction{}
	}
	throughput := stats.safeThroughput
	if throughput <= 0 {
		throughput = stats.emaThroughput
	}
	prediction := durationPrediction{
		throughput:     throughput,
		inflight:       stats.inflight,
		activeRequests: a.activeRequests,
	}
	if !stats.cooldownUntil.IsZero() && now.Before(stats.cooldownUntil) {
		return prediction
	}
	if stats.emaThroughput <= 0 || stats.emaDurationSec <= 0 {
		return prediction
	}
	if stats.segments <= 0 || stats.lastSample.IsZero() {
		if !a.startTime.IsZero() {
			return prediction
		}
	} else if now.Sub(stats.lastSample) > a.throughputFreshness() {
		return prediction
	}
	rawTransfer := time.Duration(float64(time.Second) * (float64(a.segmentSize) / stats.emaThroughput))
	transfer := time.Duration(float64(time.Second) * (float64(a.segmentSize) / throughput))
	latency := time.Duration(stats.emaDurationSec*float64(time.Second)) - rawTransfer
	if latency < 0 {
		latency = 0
	}
	prediction.duration = latency + transfer
	return prediction
}

func requestKind(method string, urgent bool) string {
	switch method {
	case "race":
		return "race"
	case "discovery_single":
		return "discovery"
	case "rescue":
		return "rescue"
	}
	if urgent {
		return "urgent"
	}
	return "normal"
}

func predictionOutcome(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if ctx != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "timeout"
		}
		if ctx.Err() == context.Canceled {
			return "cancelled"
		}
	}
	return "failure"
}

func emitDurationPredictionSample(index int, playbackIndex int64, distance int64, urgent bool, method string, peerState peerState, prediction durationPrediction, started time.Time, duration time.Duration, bytes int, err error, ctx context.Context) {
	emitDurationPredictionSampleWithReason(index, playbackIndex, distance, urgent, method, peerState, prediction, started, duration, bytes, err, ctx, "", "")
}

func emitDurationPredictionSampleWithReason(index int, playbackIndex int64, distance int64, urgent bool, method string, peerState peerState, prediction durationPrediction, started time.Time, duration time.Duration, bytes int, err error, ctx context.Context, outcomeOverride string, reason string) {
	actualThroughput := 0.0
	if duration > 0 && bytes > 0 {
		actualThroughput = float64(bytes) / duration.Seconds()
	}
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	outcome := predictionOutcome(ctx, err)
	if outcomeOverride != "" {
		outcome = outcomeOverride
	}
	emitStructuredEvent(durationPredictionSample{
		Type:                "duration_prediction_sample",
		Segment:             index,
		PlaybackIndex:       playbackIndex,
		Distance:            distance,
		Urgent:              urgent,
		Method:              method,
		RequestKind:         requestKind(method, urgent),
		PeerID:              peerState.info.ID.String(),
		PeerLabel:           peerState.profile.Label,
		PredictedDuration:   prediction.duration.Nanoseconds(),
		PredictedThroughput: prediction.throughput,
		InflightAtStart:     prediction.inflight,
		ActualDuration:      duration.Nanoseconds(),
		ActualThroughput:    actualThroughput,
		Bytes:               bytes,
		Success:             err == nil,
		Outcome:             outcome,
		Reason:              reason,
		Error:               errorText,
		StartedTimeNS:       started.UnixNano(),
		TimeNS:              time.Now().UnixNano(),
	})
}

func (a *adaptiveSelector) estimatedTimeByID(id peer.ID) time.Duration {
	for _, peerState := range a.peers {
		if peerState.info.ID == id {
			return a.estimatedTime(peerState)
		}
	}
	return 0
}

func (a *adaptiveSelector) markInflight(id peer.ID, delta int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		stats = &peerStats{}
		a.stats[id] = stats
	}
	stats.inflight += delta
	if stats.inflight < 0 {
		stats.inflight = 0
	}
	a.activeRequests += delta
	if a.activeRequests < 0 {
		a.activeRequests = 0
	}
}

func (a *adaptiveSelector) markDiscoveryInflight(id peer.ID, delta int) {
	if delta == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.discoveryInflight == nil {
		a.discoveryInflight = make(map[peer.ID]int)
	}
	next := a.discoveryInflight[id] + delta
	if next <= 0 {
		delete(a.discoveryInflight, id)
		return
	}
	a.discoveryInflight[id] = next
}

func (a *adaptiveSelector) recordFailure(id peer.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		stats = &peerStats{}
		a.stats[id] = stats
	}
	stats.consecutiveFails++
	backoff := time.Second * time.Duration(1<<minInt(stats.consecutiveFails, 6))
	stats.cooldownUntil = time.Now().Add(backoff)
}

func (a *adaptiveSelector) recordSuccess(id peer.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		stats = &peerStats{}
		a.stats[id] = stats
	}
	stats.consecutiveFails = 0
	stats.cooldownUntil = time.Time{}
}

func (a *adaptiveSelector) isCoolingDown(id peer.ID) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		return false
	}
	if stats.cooldownUntil.IsZero() {
		return false
	}
	return time.Now().Before(stats.cooldownUntil)
}

func (a *adaptiveSelector) updateClientCapacityLocked(totalRateSample float64, activeRequests int) {
	if totalRateSample <= 0 {
		return
	}
	if a.recentTotalRate <= 0 {
		a.recentTotalRate = totalRateSample
	} else {
		a.recentTotalRate = 0.2*totalRateSample + 0.8*a.recentTotalRate
	}
	if a.clientCapacity <= 0 {
		a.clientCapacity = totalRateSample
		return
	}
	if activeRequests < 2 && totalRateSample <= 0.8*a.clientCapacity {
		return
	}
	alpha := 0.03
	if totalRateSample > a.clientCapacity {
		alpha = 0.3
	}
	a.clientCapacity = alpha*totalRateSample + (1-alpha)*a.clientCapacity
}

func qualifiedSamples(stats *peerStats) int {
	if stats == nil {
		return 0
	}
	return stats.cleanSamples + stats.probeSamples
}

func lastQualifiedSample(stats *peerStats) time.Time {
	if stats == nil {
		return time.Time{}
	}
	if !stats.lastQualifiedSample.IsZero() {
		return stats.lastQualifiedSample
	}
	if qualifiedSamples(stats) >= minCleanSamplesForFresh {
		return stats.lastSample
	}
	return time.Time{}
}

func normalInflight(stats *peerStats) int {
	if stats == nil || stats.inflight <= 0 {
		return 0
	}
	return stats.inflight
}

func unqualifiedDiscoveryAttempts(stats *peerStats) int {
	if stats == nil || stats.unqualifiedDiscoveryAttempts <= 0 {
		return 0
	}
	return stats.unqualifiedDiscoveryAttempts
}

func safeThroughput(stats *peerStats) float64 {
	if stats == nil || stats.emaThroughput <= 0 {
		return 0
	}
	confidence := float64(qualifiedSamples(stats)) / 10.0
	if confidence < 0.2 {
		confidence = 0.2
	}
	if confidence > 1 {
		confidence = 1
	}
	estimate := stats.emaThroughput * confidence * 0.7
	lowerBound := stats.lowerBoundThroughput * 0.7
	if lowerBound > estimate {
		return lowerBound
	}
	return estimate
}

func (a *adaptiveSelector) recordSample(id peer.ID, bytes int, duration time.Duration, method string, prediction durationPrediction) {
	if duration <= 0 {
		return
	}
	if bytes >= minNetworkSampleBytes && duration < minNetworkSampleDuration {
		a.recordIgnoredSample(id, bytes, duration, prediction, "duration_below_min_network_sample")
		return
	}
	observedThroughput := float64(bytes) / duration.Seconds()
	observedDurationSec := duration.Seconds()
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		stats = &peerStats{}
		a.stats[id] = stats
	}
	sampleTime := time.Now()
	stats.bytesTotal += int64(bytes)
	stats.timeTotal += duration
	stats.segments++
	stats.lastSample = sampleTime
	stats.consecutiveFails = 0
	stats.cooldownUntil = time.Time{}
	activeRequests := maxInt(a.activeRequests, prediction.activeRequests+1)
	if activeRequests <= 0 {
		activeRequests = 1
	}
	peerInflight := maxInt(stats.inflight, prediction.inflight+1)
	if peerInflight <= 0 {
		peerInflight = 1
	}
	previousClientCapacity := a.clientCapacity
	totalRateSample := observedThroughput * float64(minInt(activeRequests, 2))
	clientSaturated := previousClientCapacity > 0 && totalRateSample >= 0.85*previousClientCapacity
	a.updateClientCapacityLocked(totalRateSample, activeRequests)
	cleanSample := !clientSaturated && peerInflight <= 1 && activeRequests <= 1
	probeSample := method == "discovery_single" && peerInflight <= 1 && !cleanSample
	qualifiedSample := cleanSample || probeSample
	sampleQuality := "clean"
	if probeSample {
		sampleQuality = "probe"
	} else if !cleanSample {
		sampleQuality = "contaminated"
	}
	if stats.emaThroughput <= 0 {
		stats.emaThroughput = observedThroughput
	} else if qualifiedSample {
		stats.emaThroughput = a.emaAlpha*observedThroughput + (1-a.emaAlpha)*stats.emaThroughput
	} else if observedThroughput > stats.emaThroughput {
		alpha := a.emaAlpha * 0.75
		if alpha <= 0 {
			alpha = 0.15
		}
		stats.emaThroughput = alpha*observedThroughput + (1-alpha)*stats.emaThroughput
	} else {
		alpha := 0.02
		stats.emaThroughput = alpha*observedThroughput + (1-alpha)*stats.emaThroughput
	}
	if qualifiedSample {
		if stats.lowerBoundThroughput <= 0 {
			stats.lowerBoundThroughput = observedThroughput
		} else {
			stats.lowerBoundThroughput = a.emaAlpha*observedThroughput + (1-a.emaAlpha)*stats.lowerBoundThroughput
		}
	}
	if stats.emaDurationSec <= 0 {
		stats.emaDurationSec = observedDurationSec
	} else {
		stats.emaDurationSec = a.emaAlpha*observedDurationSec + (1-a.emaAlpha)*stats.emaDurationSec
	}
	if cleanSample {
		stats.cleanSamples++
		stats.lastQualifiedSample = sampleTime
		stats.discoveryRetryAfter = time.Time{}
	} else if probeSample {
		stats.probeSamples++
		stats.lastQualifiedSample = sampleTime
		stats.discoveryRetryAfter = time.Time{}
	} else {
		stats.contaminatedSamples++
		if method == "discovery_single" {
			stats.unqualifiedDiscoveryAttempts++
			stats.discoveryRetryAfter = sampleTime.Add(unqualifiedDiscoveryRetryDelay)
		}
	}
	stats.safeThroughput = safeThroughput(stats)
	if a.logPeers {
		lastQualifiedNS := int64(0)
		if qualifiedAt := lastQualifiedSample(stats); !qualifiedAt.IsZero() {
			lastQualifiedNS = qualifiedAt.UnixNano()
		}
		sample := throughputSample{
			Type:                         "throughput_sample",
			Peer:                         id.String(),
			EMA:                          stats.emaThroughput,
			SafeThroughput:               stats.safeThroughput,
			LowerBoundThroughput:         stats.lowerBoundThroughput,
			ClientCapacity:               a.clientCapacity,
			RecentTotalRate:              a.recentTotalRate,
			SampleQuality:                sampleQuality,
			ActiveRequests:               activeRequests,
			PeerInflight:                 peerInflight,
			CleanSamples:                 stats.cleanSamples,
			ProbeSamples:                 stats.probeSamples,
			QualifiedSamples:             qualifiedSamples(stats),
			ContaminatedSamples:          stats.contaminatedSamples,
			UnqualifiedDiscoveryAttempts: stats.unqualifiedDiscoveryAttempts,
			LastQualifiedSample:          lastQualifiedNS,
			Bytes:                        bytes,
			Duration:                     duration.Nanoseconds(),
			TimeNS:                       time.Now().UnixNano(),
		}
		payload, err := json.Marshal(sample)
		if err == nil {
			fmt.Println(string(payload))
		}
	}
}

func (a *adaptiveSelector) recordIgnoredSample(id peer.ID, bytes int, duration time.Duration, prediction durationPrediction, reason string) {
	if duration <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	stats := a.stats[id]
	if stats == nil {
		stats = &peerStats{}
		a.stats[id] = stats
	}
	activeRequests := maxInt(a.activeRequests, prediction.activeRequests+1)
	if activeRequests <= 0 {
		activeRequests = 1
	}
	peerInflight := maxInt(stats.inflight, prediction.inflight+1)
	if peerInflight <= 0 {
		peerInflight = 1
	}
	if !a.logPeers {
		return
	}
	lastQualifiedNS := int64(0)
	if qualifiedAt := lastQualifiedSample(stats); !qualifiedAt.IsZero() {
		lastQualifiedNS = qualifiedAt.UnixNano()
	}
	sample := throughputSample{
		Type:                         "throughput_sample",
		Peer:                         id.String(),
		EMA:                          stats.emaThroughput,
		SafeThroughput:               stats.safeThroughput,
		LowerBoundThroughput:         stats.lowerBoundThroughput,
		ClientCapacity:               a.clientCapacity,
		RecentTotalRate:              a.recentTotalRate,
		SampleQuality:                "ignored",
		Reason:                       reason,
		ActiveRequests:               activeRequests,
		PeerInflight:                 peerInflight,
		CleanSamples:                 stats.cleanSamples,
		ProbeSamples:                 stats.probeSamples,
		QualifiedSamples:             qualifiedSamples(stats),
		ContaminatedSamples:          stats.contaminatedSamples,
		UnqualifiedDiscoveryAttempts: stats.unqualifiedDiscoveryAttempts,
		LastQualifiedSample:          lastQualifiedNS,
		Bytes:                        bytes,
		Duration:                     duration.Nanoseconds(),
		TimeNS:                       time.Now().UnixNano(),
	}
	payload, err := json.Marshal(sample)
	if err == nil {
		fmt.Println(string(payload))
	}
}

func (a *adaptiveSelector) shouldRace(segmentIndex int) bool {
	if a.raceFanout <= 1 {
		return false
	}
	if segmentIndex < a.raceSegments {
		return true
	}
	if a.probeInterval <= 0 {
		return false
	}
	now := time.Now()
	if a.lastProbe.IsZero() {
		a.lastProbe = a.startTime
	}
	if now.Sub(a.lastProbe) >= a.probeInterval {
		return true
	}
	return false
}

func (a *adaptiveSelector) nextProbePeers() []peerState {
	a.lastProbe = time.Now()
	allPeers := make([]peerState, len(a.peers))
	copy(allPeers, a.peers)
	total := len(allPeers)
	if total == 0 {
		return nil
	}
	fanout := a.raceFanout
	if fanout > total {
		fanout = total
	}
	start := a.probeOffset % total
	out := make([]peerState, 0, fanout)
	for i := 0; i < total && len(out) < fanout; i++ {
		idx := (start + i) % total
		peerState := allPeers[idx]
		if a.isCoolingDown(peerState.info.ID) {
			continue
		}
		out = append(out, peerState)
	}
	if len(out) == 0 {
		for i := 0; i < total && len(out) < fanout; i++ {
			idx := (start + i) % total
			out = append(out, allPeers[idx])
		}
	}
	a.probeOffset = (a.probeOffset + fanout) % total
	return out
}

func (a *adaptiveSelector) rotatePeers(peers []peerState, fanout int) []peerState {
	if len(peers) == 0 {
		return peers
	}
	start := a.raceOffset % len(peers)
	rotated := append(peers[start:], peers[:start]...)
	advance := fanout
	if advance <= 0 {
		advance = len(peers)
	}
	a.raceOffset = (a.raceOffset + advance) % len(peers)
	return rotated
}

func (a *adaptiveSelector) measuredPeers() []peerState {
	measured := make([]peerState, 0, len(a.peers))
	now := time.Now()
	for _, p := range a.peers {
		if a.throughputState(p.info.ID, now) != "fresh" {
			continue
		}
		measured = append(measured, p)
	}
	return measured
}

func (a *adaptiveSelector) usableMeasuredPeers() []peerState {
	measured := make([]peerState, 0, len(a.peers))
	now := time.Now()
	for _, p := range a.peers {
		state := a.throughputState(p.info.ID, now)
		if state != "fresh" && state != "tentative" {
			continue
		}
		if !a.hasUsableThroughput(p.info.ID, now) {
			continue
		}
		measured = append(measured, p)
	}
	return measured
}

func (a *adaptiveSelector) hasMeasuredPeer() bool {
	now := time.Now()
	for _, p := range a.peers {
		if a.hasUsableThroughput(p.info.ID, now) {
			return true
		}
	}
	return false
}

func (a *adaptiveSelector) fastestMeasuredPeer() (peerState, bool) {
	measured := a.measuredPeers()
	if len(measured) == 0 {
		return peerState{}, false
	}
	best := measured[0]
	bestEstimate := a.estimatedTime(best)
	for _, p := range measured[1:] {
		est := a.estimatedTime(p)
		if bestEstimate == 0 || (est > 0 && est < bestEstimate) {
			best = p
			bestEstimate = est
		}
	}
	return best, true
}

func (a *adaptiveSelector) fastestBalancedMeasuredPeer() (peerState, time.Duration, int, bool) {
	return a.fastestBalancedPeer(a.measuredPeers())
}

func (a *adaptiveSelector) fastestBalancedUsablePeer() (peerState, time.Duration, int, bool) {
	return a.fastestBalancedPeer(a.usableMeasuredPeers())
}

func (a *adaptiveSelector) fastestBalancedPeer(measured []peerState) (peerState, time.Duration, int, bool) {
	if len(measured) == 0 {
		return peerState{}, 0, 0, false
	}
	estimates := make(map[peer.ID]time.Duration, len(measured))
	projected := make(map[peer.ID]time.Duration, len(measured))
	fastestProjected := time.Duration(0)
	for _, p := range measured {
		estimate := a.estimatedTime(p)
		if estimate <= 0 {
			continue
		}
		finish := a.projectedFinish(p, estimate)
		if finish <= 0 {
			continue
		}
		estimates[p.info.ID] = estimate
		projected[p.info.ID] = finish
		if fastestProjected == 0 || finish < fastestProjected {
			fastestProjected = finish
		}
	}
	if fastestProjected <= 0 {
		return peerState{}, 0, 0, false
	}

	best := peerState{}
	bestBacklog := time.Duration(0)
	bestSegments := 0
	bestSet := false
	candidateCount := 0
	for _, candidate := range measured {
		finish := projected[candidate.info.ID]
		if finish <= 0 || !estimatesNearEqual(finish, fastestProjected) {
			continue
		}
		candidateCount++
		stats := a.stats[candidate.info.ID]
		backlog := a.projectedBacklog(candidate)
		segments := 0
		if stats != nil {
			segments = stats.segments
		}
		if !bestSet {
			best = candidate
			bestBacklog = backlog
			bestSegments = segments
			bestSet = true
			continue
		}
		if backlog < bestBacklog {
			best = candidate
			bestBacklog = backlog
			bestSegments = segments
			continue
		}
		if backlog > bestBacklog {
			continue
		}
		if segments < bestSegments {
			best = candidate
			bestBacklog = backlog
			bestSegments = segments
			continue
		}
		if segments > bestSegments {
			continue
		}
		if candidate.info.ID.String() < best.info.ID.String() {
			best = candidate
			bestBacklog = backlog
			bestSegments = segments
		}
	}
	if !bestSet {
		return peerState{}, 0, candidateCount, false
	}
	return best, estimates[best.info.ID], candidateCount, true
}

func (a *adaptiveSelector) fastestMeasuredPeerExcluding(excluded peer.ID) (peerState, bool) {
	measured := a.measuredPeers()
	best := peerState{}
	bestEstimate := time.Duration(0)
	bestSet := false
	for _, p := range measured {
		if p.info.ID == excluded {
			continue
		}
		est := a.estimatedTime(p)
		if est <= 0 {
			continue
		}
		if !bestSet || est < bestEstimate {
			best = p
			bestEstimate = est
			bestSet = true
		}
	}
	return best, bestSet
}

func (a *adaptiveSelector) measuredPeersExcluding(excluded peer.ID) []peerState {
	measured := a.measuredPeers()
	filtered := make([]peerState, 0, len(measured))
	for _, p := range measured {
		if p.info.ID == excluded {
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered
}

func (a *adaptiveSelector) fastestMeasuredPeerForNormal() (peerState, bool) {
	now := time.Now()
	measured := a.measuredPeers()
	best := peerState{}
	bestEstimate := time.Duration(0)
	bestSet := false
	for _, p := range measured {
		if a.shouldThrottleTentativeNormal(p.info.ID, now) {
			continue
		}
		est := a.estimatedTime(p)
		if est <= 0 {
			continue
		}
		if !bestSet || est < bestEstimate {
			best = p
			bestEstimate = est
			bestSet = true
		}
	}
	if bestSet {
		return best, true
	}
	return a.fastestMeasuredPeer()
}

type graphsyncFetcher struct {
	graphsync    gs.GraphExchange
	store        blockstore.Blockstore
	root         cid.Cid
	layout       posl.Layout
	selector     *adaptiveSelector
	currentIndex *int64
	logPeers     bool
	raceStores   *raceStoreManager
	prefetcher   *prefetch.Prefetcher

	assignmentsMu sync.Mutex
	assignments   map[int]*inflightAssignment

	raceOverheadWG sync.WaitGroup
}

type raceResult struct {
	peerID      peer.ID
	peerLabel   string
	duration    time.Duration
	bytes       int
	storedBytes int
	err         error
	store       *memoryBlockstore
}

type raceAttempt struct {
	peerID    peer.ID
	peerLabel string
	store     *memoryBlockstore
}

type raceStoreManager struct {
	mu      sync.Mutex
	options map[gs.RequestID]string
}

func newRaceStoreManager(exchange gs.GraphExchange) *raceStoreManager {
	manager := &raceStoreManager{options: make(map[gs.RequestID]string)}
	exchange.RegisterOutgoingRequestHook(func(_ peer.ID, requestData gs.RequestData, hookActions gs.OutgoingRequestHookActions) {
		if requestData.Type() != gs.RequestTypeNew {
			return
		}
		manager.mu.Lock()
		option, ok := manager.options[requestData.ID()]
		manager.mu.Unlock()
		if ok {
			hookActions.UsePersistenceOption(option)
		}
	})
	return manager
}

func (g *graphsyncFetcher) currentPlaybackIndex() int64 {
	if g.currentIndex == nil {
		return 0
	}
	return *g.currentIndex
}

func (g *graphsyncFetcher) urgentWindowBounds(playbackIndex int64) (int, int, bool) {
	if g.prefetcher == nil || g.selector == nil {
		return 0, 0, false
	}
	start := int(playbackIndex)
	if start < 0 {
		start = 0
	}
	end := start + int(g.selector.urgentWindow)
	if end >= g.layout.SegmentCount {
		end = g.layout.SegmentCount - 1
	}
	return start, end, true
}

func (g *graphsyncFetcher) urgentWindowCached(playbackIndex int64) bool {
	start, end, ok := g.urgentWindowBounds(playbackIndex)
	if !ok {
		return false
	}
	return g.prefetcher.CachedRange(start, end)
}

func (g *graphsyncFetcher) urgentWindowCovered(playbackIndex int64) bool {
	start, end, ok := g.urgentWindowBounds(playbackIndex)
	if !ok {
		return false
	}
	return g.prefetcher.CoveredRange(start, end)
}

func raceCollectionPolicy(urgent bool, urgentWindowCovered bool, throughputDiscoveryNeeded bool, deadline time.Duration, playbackDelay time.Duration, raceTimeout time.Duration) (bool, string, time.Duration) {
	if raceTimeout <= 0 {
		raceTimeout = 30 * time.Second
	}
	if urgent {
		return true, "first_success_urgent", raceTimeout
	}
	if !urgentWindowCovered {
		return true, "first_success_fill_buffer", raceTimeout
	}
	if !throughputDiscoveryNeeded {
		return true, "first_success_fresh_throughput", raceTimeout
	}
	return true, "first_success_bootstrap", raceTimeout
}

func (a *adaptiveSelector) discoveryFetchTimeout(deadline time.Duration) (time.Duration, bool) {
	timeout := a.discoveryTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if deadline <= 0 {
		return timeout, true
	}
	budget := deadline - a.safetyMargin()
	if budget <= 0 {
		return 0, false
	}
	if budget < timeout {
		return budget, true
	}
	return timeout, true
}

func (g *graphsyncFetcher) beginAssignment(index int, peerID peer.ID, playbackIndex int64, decision selectionDecision) {
	if g.assignments == nil {
		return
	}
	g.assignmentsMu.Lock()
	g.assignments[index] = &inflightAssignment{
		Segment:         index,
		Peer:            peerID,
		Start:           time.Now(),
		PlaybackIndex:   playbackIndex,
		Deadline:        decision.Deadline,
		Estimate:        decision.Estimate,
		SafetyMargin:    decision.SafetyMargin,
		ProjectedFinish: decision.ProjectedFinish,
	}
	g.assignmentsMu.Unlock()
}

func (g *graphsyncFetcher) finishAssignment(index int, peerID peer.ID) {
	if g.assignments == nil {
		return
	}
	g.assignmentsMu.Lock()
	if assignment, ok := g.assignments[index]; ok && assignment.Peer == peerID {
		delete(g.assignments, index)
	}
	g.assignmentsMu.Unlock()
}

func (g *graphsyncFetcher) revalidateAllAssignments(excludeSegment int) {
	g.revalidateAssignments("", excludeSegment)
}

func (g *graphsyncFetcher) revalidateAssignments(peerFilter peer.ID, excludeSegment int) {
	if g.assignments == nil {
		return
	}

	rescueSegments := make([]int, 0)
	now := time.Now()
	g.assignmentsMu.Lock()
	for segment, assignment := range g.assignments {
		if segment == excludeSegment {
			continue
		}
		if peerFilter != "" && assignment.Peer != peerFilter {
			continue
		}
		if assignment.Rescued {
			continue
		}
		estimate := g.selector.estimatedTimeByID(assignment.Peer)
		if estimate <= 0 {
			continue
		}
		projectedFinish := now.Sub(assignment.Start) + estimate
		assignment.Estimate = estimate
		assignment.ProjectedFinish = projectedFinish
		if !assignmentHasSlack(assignment, projectedFinish) {
			assignment.Unsafe = true
			assignment.RescueReason = "ema_revalidation_rescue"
			rescueSegments = append(rescueSegments, segment)
		}
	}
	g.assignmentsMu.Unlock()

	if g.prefetcher == nil {
		return
	}
	sort.Ints(rescueSegments)
	for _, segment := range rescueSegments {
		if g.prefetcher.RequestRescue(segment) {
			return
		}
	}
}

func assignmentHasSlack(assignment *inflightAssignment, projectedFinish time.Duration) bool {
	if assignment.Deadline <= 0 {
		return true
	}
	budget := assignment.Deadline - assignment.SafetyMargin
	if budget <= 0 {
		return false
	}
	return projectedFinish <= budget
}

func (g *graphsyncFetcher) ShouldRescue(index int) bool {
	if g.assignments == nil {
		return false
	}
	g.assignmentsMu.Lock()
	defer g.assignmentsMu.Unlock()
	assignment, ok := g.assignments[index]
	if !ok || assignment.Rescued {
		return false
	}
	if assignment.Unsafe {
		return true
	}
	distance := int64(index) - g.currentPlaybackIndex()
	if distance <= g.selector.urgentWindow {
		assignment.Unsafe = true
		assignment.RescueReason = "urgent_window_rescue"
		return true
	}
	return false
}

func (g *graphsyncFetcher) markAssignmentRescued(index int) (inflightAssignment, bool) {
	g.assignmentsMu.Lock()
	defer g.assignmentsMu.Unlock()
	assignment, ok := g.assignments[index]
	if !ok || assignment.Rescued {
		return inflightAssignment{}, false
	}
	assignment.Rescued = true
	if assignment.RescueReason == "" {
		assignment.RescueReason = "urgent_window_rescue"
	}
	return *assignment, true
}

func (g *graphsyncFetcher) RescueFetch(ctx context.Context, index int) ([]byte, error) {
	assignment, ok := g.markAssignmentRescued(index)
	if !ok {
		return nil, fmt.Errorf("segment %d has no rescueable assignment", index)
	}
	selector, err := posl.SegmentSelector(index)
	if err != nil {
		return nil, err
	}
	playbackIndex := g.currentPlaybackIndex()
	distance := int64(index) - playbackIndex
	urgent := distance <= g.selector.urgentWindow
	if data, raced, err := g.rescueRaceFetch(ctx, index, selector, assignment, playbackIndex, distance, urgent); raced {
		return data, err
	}
	return g.singleRescueFetch(ctx, index, selector, assignment, playbackIndex, distance, urgent)
}

func (g *graphsyncFetcher) rescueRaceFetch(ctx context.Context, index int, selector ipld.Node, assignment inflightAssignment, playbackIndex int64, distance int64, urgent bool) ([]byte, bool, error) {
	if !g.shouldRaceRescue(assignment) {
		return nil, false, nil
	}
	peers := g.rescueRaceCandidates(assignment.Peer)
	deadline := assignment.Deadline
	if deadline <= 0 {
		deadline = time.Duration(distance) * g.selector.playbackDelay
	}
	data, raced, err := g.executeRaceFetch(ctx, index, selector, playbackIndex, distance, urgent, deadline, peers, "urgent_rescue_race", true, "first_success_urgent_rescue", g.selector.raceTimeout, false)
	if !raced {
		return nil, false, nil
	}
	return data, true, err
}

func (g *graphsyncFetcher) shouldRaceRescue(assignment inflightAssignment) bool {
	if g.selector == nil || g.raceStores == nil || g.selector.raceFanout <= 1 {
		return false
	}
	return len(g.rescueRaceCandidates(assignment.Peer)) >= 2
}

func (g *graphsyncFetcher) rescueRaceCandidates(originalPeer peer.ID) []peerState {
	if g.selector == nil {
		return nil
	}
	peers := g.selector.measuredPeersExcluding(originalPeer)
	if len(peers) == 0 {
		peers = g.selector.measuredPeers()
	}
	sort.Slice(peers, func(i, j int) bool {
		left := g.selector.estimatedTime(peers[i])
		right := g.selector.estimatedTime(peers[j])
		if left == right {
			return peers[i].info.ID.String() < peers[j].info.ID.String()
		}
		if left <= 0 {
			return false
		}
		if right <= 0 {
			return true
		}
		return left < right
	})
	return peers
}

func (g *graphsyncFetcher) singleRescueFetch(ctx context.Context, index int, selector ipld.Node, assignment inflightAssignment, playbackIndex int64, distance int64, urgent bool) ([]byte, error) {
	rescuePeer, ok := g.selector.fastestMeasuredPeerExcluding(assignment.Peer)
	if !ok {
		rescuePeer, ok = g.selector.fastestMeasuredPeer()
	}
	if !ok {
		return nil, fmt.Errorf("segment %d rescue has no measured peer", index)
	}
	decisionEstimate := g.selector.estimatedTime(rescuePeer)
	emitStructuredEvent(schedulerDecisionEvent{
		Type:              "scheduler_decision",
		Segment:           index,
		PlaybackIndex:     playbackIndex,
		Distance:          distance,
		Urgent:            urgent,
		Method:            "rescue",
		Reason:            assignment.RescueReason,
		Deadline:          assignment.Deadline.Nanoseconds(),
		Estimate:          assignment.Estimate.Nanoseconds(),
		ProjectedFinish:   assignment.ProjectedFinish.Nanoseconds(),
		SafetyMargin:      assignment.SafetyMargin.Nanoseconds(),
		SelectedPeerID:    rescuePeer.info.ID.String(),
		SelectedPeerLabel: rescuePeer.profile.Label,
		SelectedEstimate:  decisionEstimate.Nanoseconds(),
		OriginalPeerID:    assignment.Peer.String(),
		TimeNS:            time.Now().UnixNano(),
	})
	return g.fetchRescueFromPeer(ctx, index, selector, rescuePeer, playbackIndex, distance, urgent)
}

func (g *graphsyncFetcher) fetchRescueFromPeer(ctx context.Context, index int, selector ipld.Node, peerState peerState, playbackIndex int64, distance int64, urgent bool) ([]byte, error) {
	prediction := g.selector.predictionSnapshot(peerState)
	g.selector.markInflight(peerState.info.ID, 1)
	defer g.selector.markInflight(peerState.info.ID, -1)

	start := time.Now()
	rescueStore := newMemoryBlockstore()
	if g.raceStores == nil {
		err := fmt.Errorf("rescue stores not configured")
		g.emitFetchFailure(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	requestID := gs.NewRequestID()
	optionName := fmt.Sprintf("rescue-%s", requestID.String())
	rescueLinkSystem := storeutil.LinkSystemForBlockstore(rescueStore)
	if err := g.graphsync.RegisterPersistenceOption(optionName, rescueLinkSystem); err != nil {
		g.emitFetchFailure(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	g.raceStores.track(requestID, optionName)
	defer func() {
		g.raceStores.untrack(requestID)
		_ = g.graphsync.UnregisterPersistenceOption(optionName)
	}()

	reqCtx := context.WithValue(ctx, gs.RequestIDContextKey{}, requestID)
	if err := requestSegment(reqCtx, g.graphsync, peerState.info.ID, g.root, selector); err != nil {
		g.emitFetchFailure(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	segmentBytes, err := g.readSegmentForFetch(ctx, rescueStore, index, "rescue", "rescue", peerState.info.ID, func(chunkIndex int, size int) {
		fmt.Printf("chunk seg=%d idx=%d bytes=%d\n", index, chunkIndex, size)
	})
	if err != nil {
		g.emitFetchFailure(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	if err := mergeBlockstores(ctx, g.store, rescueStore); err != nil {
		err = fmt.Errorf("merge rescue store segment=%d method=rescue store=rescue peer=%s segmentCid=%s: %w", index, peerState.info.ID, g.layout.Segments[index], err)
		g.emitFetchFailure(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, 0, err, ctx)
		return nil, err
	}

	duration := time.Since(start)
	emitDurationPredictionSample(index, playbackIndex, distance, urgent, "rescue", peerState, prediction, start, duration, len(segmentBytes), nil, ctx)
	g.selector.recordSample(peerState.info.ID, len(segmentBytes), duration, "rescue", prediction)
	g.revalidateAllAssignments(index)
	g.selector.recordSuccess(peerState.info.ID)
	emitStructuredEvent(schedulerResultEvent{
		Type:          "scheduler_result",
		Segment:       index,
		PlaybackIndex: playbackIndex,
		Distance:      distance,
		Urgent:        urgent,
		Method:        "rescue",
		PeerID:        peerState.info.ID.String(),
		PeerLabel:     peerState.profile.Label,
		Duration:      duration.Nanoseconds(),
		Bytes:         len(segmentBytes),
		Success:       true,
		TimeNS:        time.Now().UnixNano(),
	})
	fmt.Printf("segment %d fetched peer=%s label=%s\n", index, peerState.info.ID, peerState.profile.Label)
	return segmentBytes, nil
}

func (m *raceStoreManager) track(requestID gs.RequestID, option string) {
	m.mu.Lock()
	m.options[requestID] = option
	m.mu.Unlock()
}

func (m *raceStoreManager) untrack(requestID gs.RequestID) {
	m.mu.Lock()
	delete(m.options, requestID)
	m.mu.Unlock()
}

type memoryBlockstore struct {
	mu     sync.RWMutex
	blocks map[string]blocks.Block
}

func newMemoryBlockstore() *memoryBlockstore {
	return &memoryBlockstore{blocks: make(map[string]blocks.Block)}
}

func (m *memoryBlockstore) DeleteBlock(_ context.Context, cid cid.Cid) error {
	m.mu.Lock()
	delete(m.blocks, cid.String())
	m.mu.Unlock()
	return nil
}

func (m *memoryBlockstore) Has(_ context.Context, cid cid.Cid) (bool, error) {
	m.mu.RLock()
	_, ok := m.blocks[cid.String()]
	m.mu.RUnlock()
	return ok, nil
}

func (m *memoryBlockstore) Get(_ context.Context, cid cid.Cid) (blocks.Block, error) {
	m.mu.RLock()
	block, ok := m.blocks[cid.String()]
	m.mu.RUnlock()
	if !ok {
		return nil, ipldformat.ErrNotFound{Cid: cid}
	}
	return block, nil
}

func (m *memoryBlockstore) GetSize(_ context.Context, cid cid.Cid) (int, error) {
	m.mu.RLock()
	block, ok := m.blocks[cid.String()]
	m.mu.RUnlock()
	if !ok {
		return -1, ipldformat.ErrNotFound{Cid: cid}
	}
	return len(block.RawData()), nil
}

func (m *memoryBlockstore) Put(_ context.Context, block blocks.Block) error {
	if err := validateBlockCID(block); err != nil {
		return err
	}
	m.mu.Lock()
	m.blocks[block.Cid().String()] = block
	m.mu.Unlock()
	return nil
}

func (m *memoryBlockstore) PutMany(ctx context.Context, blocksList []blocks.Block) error {
	for _, block := range blocksList {
		if err := m.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (m *memoryBlockstore) totalStoredBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	total := 0
	for _, block := range m.blocks {
		total += len(block.RawData())
	}
	return total
}

func (m *memoryBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	m.mu.RLock()
	keys := make([]cid.Cid, 0, len(m.blocks))
	for _, block := range m.blocks {
		keys = append(keys, block.Cid())
	}
	m.mu.RUnlock()
	output := make(chan cid.Cid, len(keys))
	go func() {
		defer close(output)
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return
			case output <- key:
			}
		}
	}()
	return output, nil
}

func validateBlockCID(block blocks.Block) error {
	if block == nil {
		return fmt.Errorf("nil block")
	}
	actual, err := block.Cid().Prefix().Sum(block.RawData())
	if err != nil {
		return fmt.Errorf("validate block cid %s: %w", block.Cid(), err)
	}
	if !actual.Equals(block.Cid()) {
		return fmt.Errorf("block cid mismatch actual=%s expected=%s", actual, block.Cid())
	}
	return nil
}

type validatingBlockstore struct {
	blockstore.Blockstore
}

func (v validatingBlockstore) Put(ctx context.Context, block blocks.Block) error {
	if err := validateBlockCID(block); err != nil {
		return err
	}
	return v.Blockstore.Put(ctx, block)
}

func (v validatingBlockstore) PutMany(ctx context.Context, blocksList []blocks.Block) error {
	for _, block := range blocksList {
		if err := v.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (g *graphsyncFetcher) raceFetch(ctx context.Context, index int) ([]byte, error) {
	selector, err := posl.SegmentSelector(index)
	if err != nil {
		return nil, err
	}
	playbackIndex := int64(0)
	if g.currentIndex != nil {
		playbackIndex = *g.currentIndex
	}
	distance := int64(index) - playbackIndex
	urgent := distance <= g.selector.urgentWindow
	deadline := time.Duration(distance) * g.selector.playbackDelay
	fanout := g.selector.raceFanout
	if fanout > len(g.selector.peers) {
		fanout = len(g.selector.peers)
	}
	if fanout <= 1 {
		return g.singleFetch(ctx, index, selector)
	}
	raceTimeout := g.selector.raceTimeout
	if raceTimeout <= 0 {
		raceTimeout = 30 * time.Second
	}
	urgentWindowCached := g.urgentWindowCached(playbackIndex)
	urgentWindowCovered := urgentWindowCached || g.urgentWindowCovered(playbackIndex)

	var peers []peerState
	raceReason := "probe_race"
	if index < g.selector.raceSegments {
		raceReason = "startup_race"
		peers = make([]peerState, len(g.selector.peers))
		copy(peers, g.selector.peers)
	} else if g.selector.shouldRace(index) {
		raceReason = "probe_race"
		peers = g.selector.nextProbePeers()
	} else {
		raceReason = "measured_race"
		peers = g.selector.measuredPeers()
		if len(peers) == 0 {
			raceReason = "fallback_race"
			peers = make([]peerState, len(g.selector.peers))
			copy(peers, g.selector.peers)
		}
	}
	if fanout > len(peers) {
		fanout = len(peers)
	}
	throughputDiscoveryNeeded, _ := g.selector.throughputDiscoveryNeeded(peers)
	firstSuccess, raceReturnReason, collectionTimeout := raceCollectionPolicy(urgent, urgentWindowCovered, throughputDiscoveryNeeded, deadline, g.selector.playbackDelay, raceTimeout)
	data, raced, err := g.executeRaceFetch(ctx, index, selector, playbackIndex, distance, urgent, deadline, peers, raceReason, firstSuccess, raceReturnReason, collectionTimeout, true)
	if !raced {
		return g.singleFetch(ctx, index, selector)
	}
	return data, err
}

func (g *graphsyncFetcher) executeRaceFetch(ctx context.Context, index int, selector ipld.Node, playbackIndex int64, distance int64, urgent bool, deadline time.Duration, peers []peerState, raceReason string, firstSuccess bool, raceReturnReason string, collectionTimeout time.Duration, rotate bool) ([]byte, bool, error) {
	fanout := g.selector.raceFanout
	if fanout > len(peers) {
		fanout = len(peers)
	}
	if fanout <= 1 || len(peers) == 0 {
		return nil, false, nil
	}
	raceTimeout := g.selector.raceTimeout
	if raceTimeout <= 0 {
		raceTimeout = 30 * time.Second
	}
	if collectionTimeout <= 0 {
		collectionTimeout = raceTimeout
	}
	urgentWindowCached := g.urgentWindowCached(playbackIndex)
	urgentWindowCovered := urgentWindowCached || g.urgentWindowCovered(playbackIndex)
	throughputDiscoveryNeeded, stalePeerCount := g.selector.throughputDiscoveryNeeded(peers)
	var ctxRace context.Context
	var cancel context.CancelFunc
	if !firstSuccess {
		ctxRace, cancel = context.WithTimeout(ctx, collectionTimeout)
	} else {
		ctxRace, cancel = context.WithTimeout(ctx, raceTimeout)
	}
	defer cancel()
	var raceComplete atomic.Bool

	results := make(chan raceResult, fanout)
	if raceReason == "measured_race" {
		sort.Slice(peers, func(i, j int) bool {
			return g.selector.estimatedTime(peers[i]) < g.selector.estimatedTime(peers[j])
		})
	}
	if rotate {
		peers = g.selector.rotatePeers(peers, fanout)
	}
	if g.logPeers {
		fmt.Printf("segment %d race peers:", index)
		for _, p := range peers[:fanout] {
			fmt.Printf(" %s", p.info.ID)
		}
		fmt.Println()
	}
	started := 0
	attemptedPeers := make([]peerState, 0, fanout)
	raceAttempts := make([]raceAttempt, 0, fanout)
	for _, p := range peers[:fanout] {
		peerCandidate := p
		if g.selector.isCoolingDown(peerCandidate.info.ID) {
			continue
		}
		prediction := g.selector.predictionSnapshot(peerCandidate)
		started++
		attemptedPeers = append(attemptedPeers, peerCandidate)
		requestID := gs.NewRequestID()
		optionName := fmt.Sprintf("race-%s", requestID.String())
		raceStore := newMemoryBlockstore()
		raceAttempts = append(raceAttempts, raceAttempt{peerID: peerCandidate.info.ID, peerLabel: peerCandidate.profile.Label, store: raceStore})
		g.selector.markInflight(peerCandidate.info.ID, 1)
		go func(peerState peerState, prediction durationPrediction, requestID gs.RequestID, optionName string, raceStore *memoryBlockstore) {
			defer g.selector.markInflight(peerState.info.ID, -1)
			start := time.Now()
			if g.raceStores == nil {
				err := fmt.Errorf("race stores not configured")
				emitDurationPredictionSample(index, playbackIndex, distance, urgent, "race", peerState, prediction, start, time.Since(start), 0, err, ctxRace)
				results <- raceResult{peerID: peerState.info.ID, peerLabel: peerState.profile.Label, err: err}
				return
			}
			raceLinkSystem := storeutil.LinkSystemForBlockstore(raceStore)
			if err := g.graphsync.RegisterPersistenceOption(optionName, raceLinkSystem); err != nil {
				emitDurationPredictionSample(index, playbackIndex, distance, urgent, "race", peerState, prediction, start, time.Since(start), 0, err, ctxRace)
				results <- raceResult{peerID: peerState.info.ID, peerLabel: peerState.profile.Label, err: err, store: raceStore, storedBytes: raceStore.totalStoredBytes()}
				return
			}
			g.raceStores.track(requestID, optionName)
			defer func() {
				g.raceStores.untrack(requestID)
				_ = g.graphsync.UnregisterPersistenceOption(optionName)
			}()
			reqCtx := context.WithValue(ctxRace, gs.RequestIDContextKey{}, requestID)
			err := requestSegment(reqCtx, g.graphsync, peerState.info.ID, g.root, selector)
			if err != nil {
				if !raceComplete.Load() && ctxRace.Err() == nil {
					g.selector.recordFailure(peerState.info.ID)
				}
				emitDurationPredictionSample(index, playbackIndex, distance, urgent, "race", peerState, prediction, start, time.Since(start), 0, err, ctxRace)
				results <- raceResult{peerID: peerState.info.ID, peerLabel: peerState.profile.Label, err: err, store: raceStore, storedBytes: raceStore.totalStoredBytes()}
				return
			}
			segmentBytes, err := g.readSegmentForFetch(ctxRace, raceStore, index, "race", "race", peerState.info.ID, nil)
			if err != nil {
				if !raceComplete.Load() && ctxRace.Err() == nil {
					g.selector.recordFailure(peerState.info.ID)
				}
				emitDurationPredictionSample(index, playbackIndex, distance, urgent, "race", peerState, prediction, start, time.Since(start), 0, err, ctxRace)
				results <- raceResult{peerID: peerState.info.ID, peerLabel: peerState.profile.Label, err: err, store: raceStore, storedBytes: raceStore.totalStoredBytes()}
				return
			}
			duration := time.Since(start)
			emitDurationPredictionSample(index, playbackIndex, distance, urgent, "race", peerState, prediction, start, duration, len(segmentBytes), nil, ctxRace)
			g.selector.recordSample(peerState.info.ID, len(segmentBytes), duration, "race", prediction)
			g.revalidateAllAssignments(-1)
			g.selector.recordSuccess(peerState.info.ID)
			results <- raceResult{peerID: peerState.info.ID, peerLabel: peerState.profile.Label, duration: duration, bytes: len(segmentBytes), store: raceStore, storedBytes: raceStore.totalStoredBytes()}
		}(peerCandidate, prediction, requestID, optionName, raceStore)
	}
	if started == 0 {
		cancel()
		return nil, false, nil
	}

	candidateEvents := make([]schedulerDecisionPeer, 0, len(attemptedPeers))
	for _, peerState := range attemptedPeers {
		candidateEvents = append(candidateEvents, schedulerDecisionPeer{
			PeerID:   peerState.info.ID.String(),
			Label:    peerState.profile.Label,
			Estimate: g.selector.estimatedTime(peerState).Nanoseconds(),
		})
	}
	emitStructuredEvent(schedulerDecisionEvent{
		Type:                      "scheduler_decision",
		Segment:                   index,
		PlaybackIndex:             playbackIndex,
		Distance:                  distance,
		Urgent:                    urgent,
		Method:                    "race",
		Reason:                    raceReason,
		Deadline:                  deadline.Nanoseconds(),
		CandidateCount:            started,
		RaceFanout:                fanout,
		Candidates:                candidateEvents,
		UrgentWindowCached:        urgentWindowCached,
		UrgentWindowCovered:       urgentWindowCovered,
		ThroughputDiscoveryNeeded: throughputDiscoveryNeeded,
		StalePeerCount:            stalePeerCount,
		TimeNS:                    time.Now().UnixNano(),
	})

	winner, winnerSet, allResults, firstErr, raceReturnReason := collectRaceResults(index, results, started, g.logPeers, firstSuccess, raceReturnReason)
	if winnerSet {
		raceComplete.Store(true)
		cancel()
		if g.logPeers {
			fmt.Printf("segment %d race winner: %s duration=%s\n", index, winner.peerID, winner.duration)
			if started > len(allResults) {
				fmt.Printf("segment %d race cancelled %d loser request(s) after first success\n", index, started-len(allResults))
			}
		}
		var readStore blockstore.Blockstore = winner.store
		if readStore == nil {
			readStore = g.store
		}
		segmentBytes, err := g.readSegmentForFetch(ctx, readStore, index, "race", "race", winner.peerID, func(chunkIndex int, size int) {
			fmt.Printf("chunk seg=%d idx=%d bytes=%d\n", index, chunkIndex, size)
		})
		if err != nil {
			return nil, true, err
		}
		if winner.store != nil {
			if err := mergeBlockstores(ctx, g.store, winner.store); err != nil {
				return nil, true, err
			}
		}

		label := g.selector.peerLabel(winner.peerID)
		raceUsefulBytes := 0
		if winner.store != nil {
			raceUsefulBytes = winner.store.totalStoredBytes()
		}
		emitStructuredEvent(schedulerResultEvent{
			Type:                     "scheduler_result",
			Segment:                  index,
			PlaybackIndex:            playbackIndex,
			Distance:                 distance,
			Urgent:                   urgent,
			Method:                   "race",
			Reason:                   raceReason,
			PeerID:                   winner.peerID.String(),
			PeerLabel:                label,
			Duration:                 winner.duration.Nanoseconds(),
			Bytes:                    len(segmentBytes),
			Success:                  true,
			RaceWastedBytes:          0,
			RaceTotalBytes:           raceUsefulBytes,
			RaceCompletedCandidates:  len(allResults),
			RaceCancelledCandidates:  started - len(allResults),
			RaceSuccessfulCandidates: countSuccessfulRaceResults(allResults),
			RaceReturnReason:         raceReturnReason,
			TimeNS:                   time.Now().UnixNano(),
		})
		g.startRaceOverheadAccounting(ctx, results, started, allResults, raceAttempts, winner, schedulerRaceOverheadEvent{
			Type:                  "scheduler_race_overhead",
			Segment:               index,
			PlaybackIndex:         playbackIndex,
			Distance:              distance,
			Urgent:                urgent,
			Reason:                raceReason,
			WinnerPeer:            winner.peerID.String(),
			WinnerPeerLabel:       label,
			RaceStartedCandidates: started,
		}, collectionTimeout)
		fmt.Printf("segment %d fetched peer=%s label=%s\n", index, winner.peerID, label)
		return segmentBytes, true, nil
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("race fetch failed for segment %d", index)
	}
	emitStructuredEvent(schedulerResultEvent{
		Type:                    "scheduler_result",
		Segment:                 index,
		PlaybackIndex:           playbackIndex,
		Distance:                distance,
		Urgent:                  urgent,
		Method:                  "race",
		Reason:                  raceReason,
		Success:                 false,
		Error:                   firstErr.Error(),
		RaceCompletedCandidates: len(allResults),
		RaceCancelledCandidates: started - len(allResults),
		RaceReturnReason:        raceReturnReason,
		TimeNS:                  time.Now().UnixNano(),
	})
	return nil, true, firstErr
}

func (g *graphsyncFetcher) startRaceOverheadAccounting(ctx context.Context, results <-chan raceResult, started int, initial []raceResult, attempts []raceAttempt, winner raceResult, event schedulerRaceOverheadEvent, drainTimeout time.Duration) {
	if drainTimeout <= 0 {
		drainTimeout = 5 * time.Second
	}
	if drainTimeout > 5*time.Second {
		drainTimeout = 5 * time.Second
	}
	g.raceOverheadWG.Add(1)
	go func() {
		defer g.raceOverheadWG.Done()
		allResults := append([]raceResult(nil), initial...)
		remaining := started - len(allResults)
		complete := true
		timer := time.NewTimer(drainTimeout)
		defer timer.Stop()
		for remaining > 0 {
			select {
			case result := <-results:
				allResults = append(allResults, result)
				remaining--
			case <-ctx.Done():
				complete = false
				remaining = 0
			case <-timer.C:
				complete = false
				remaining = 0
			}
		}
		fillRaceOverheadEvent(&event, allResults, attempts, winner.peerID, complete)
		event.TimeNS = time.Now().UnixNano()
		emitStructuredEvent(event)
	}()
}

func (g *graphsyncFetcher) waitRaceOverheadAccounting() {
	g.raceOverheadWG.Wait()
}

func fillRaceOverheadEvent(event *schedulerRaceOverheadEvent, results []raceResult, attempts []raceAttempt, winner peer.ID, complete bool) {
	storesByPeer := make(map[peer.ID]*memoryBlockstore, len(attempts))
	for _, attempt := range attempts {
		if attempt.store != nil {
			storesByPeer[attempt.peerID] = attempt.store
		}
	}
	successful := 0
	cancelled := 0
	for _, result := range results {
		if result.err == nil {
			successful++
		} else {
			cancelled++
		}
		if result.store != nil {
			storesByPeer[result.peerID] = result.store
		}
	}
	useful := 0
	wasted := 0
	for peerID, store := range storesByPeer {
		if store == nil {
			continue
		}
		stored := store.totalStoredBytes()
		if peerID == winner {
			useful += stored
		} else {
			wasted += stored
		}
	}
	event.RaceUsefulBytes = useful
	event.RaceWastedBytes = wasted
	event.RaceTotalBytes = useful + wasted
	event.RaceCompletedCandidates = len(results)
	event.RaceCancelledCandidates = cancelled
	event.RaceSuccessfulCandidates = successful
	event.RaceMetricsComplete = complete && len(results) >= event.RaceStartedCandidates
}

func countSuccessfulRaceResults(results []raceResult) int {
	successful := 0
	for _, result := range results {
		if result.err == nil {
			successful++
		}
	}
	return successful
}

func collectRaceResults(index int, results <-chan raceResult, started int, logPeers bool, firstSuccess bool, firstSuccessReason string) (raceResult, bool, []raceResult, error, string) {
	var firstErr error
	allResults := make([]raceResult, 0, started)
	var winner raceResult
	winnerSet := false
	for i := 0; i < started; i++ {
		result := <-results
		allResults = append(allResults, result)

		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			if logPeers {
				fmt.Printf("segment %d race peer %s error: %v\n", index, result.peerID, result.err)
			}
			continue
		}
		if logPeers {
			fmt.Printf("segment %d race peer %s duration=%s\n", index, result.peerID, result.duration)
		}
		if !winnerSet || result.duration < winner.duration {
			winner = result
			winnerSet = true
		}
		if firstSuccess {
			return winner, true, allResults, firstErr, firstSuccessReason
		}
	}
	if winnerSet {
		reason := "completed_all_candidates"
		if firstSuccessReason != "" {
			reason = firstSuccessReason
		}
		if firstErr != nil {
			reason = "timeout_with_success"
		}
		return winner, true, allResults, firstErr, reason
	}
	return raceResult{}, false, allResults, firstErr, "all_failed"
}

func (g *graphsyncFetcher) fetchFromPeer(ctx context.Context, index int, selector ipld.Node, peerState peerState, playbackIndex int64, distance int64, urgent bool, method string) ([]byte, error) {
	prediction := g.selector.predictionSnapshot(peerState)
	g.selector.markInflight(peerState.info.ID, 1)
	defer g.selector.markInflight(peerState.info.ID, -1)

	start := time.Now()
	if segmentBytes, ok := g.readSegmentFromGlobalCache(ctx, index, method, peerState.info.ID); ok {
		g.emitFetchSuccess(index, playbackIndex, distance, urgent, method, peerState, prediction, start, segmentBytes, "local_store_hit", false, ctx)
		fmt.Printf("segment %d fetched peer=%s label=%s\n", index, peerState.info.ID, peerState.profile.Label)
		return segmentBytes, nil
	}
	if err := requestSegment(ctx, g.graphsync, peerState.info.ID, g.root, selector); err != nil {
		g.emitFetchFailure(index, playbackIndex, distance, urgent, method, peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	segmentBytes, err := g.readSegmentForFetch(ctx, g.store, index, method, "global", peerState.info.ID, func(chunkIndex int, size int) {
		fmt.Printf("chunk seg=%d idx=%d bytes=%d\n", index, chunkIndex, size)
	})
	if err != nil {
		g.emitFetchFailure(index, playbackIndex, distance, urgent, method, peerState, prediction, start, 0, err, ctx)
		return nil, err
	}
	reason := g.successIgnoredReason(ctx, index, peerState.info.ID, method)
	if reason != "" {
		g.emitFetchSuccess(index, playbackIndex, distance, urgent, method, peerState, prediction, start, segmentBytes, reason, false, ctx)
		fmt.Printf("segment %d fetched peer=%s label=%s\n", index, peerState.info.ID, peerState.profile.Label)
		return segmentBytes, nil
	}
	g.emitFetchSuccess(index, playbackIndex, distance, urgent, method, peerState, prediction, start, segmentBytes, "", true, ctx)
	fmt.Printf("segment %d fetched peer=%s label=%s\n", index, peerState.info.ID, peerState.profile.Label)
	return segmentBytes, nil
}

func (g *graphsyncFetcher) readSegmentFromGlobalCache(ctx context.Context, index int, method string, peerID peer.ID) ([]byte, bool) {
	if method != "single" && method != "discovery_single" {
		return nil, false
	}
	segmentBytes, err := g.readSegmentForFetch(ctx, g.store, index, method, "global_cache", peerID, nil)
	if err != nil {
		return nil, false
	}
	return segmentBytes, true
}

func (g *graphsyncFetcher) successIgnoredReason(ctx context.Context, index int, peerID peer.ID, method string) string {
	if ctx.Err() != nil {
		if ctx.Err() == context.Canceled {
			return "stale_after_rescue"
		}
		return "context_done_after_success"
	}
	if method != "single" && method != "discovery_single" {
		return ""
	}
	if g.assignments == nil {
		return ""
	}
	g.assignmentsMu.Lock()
	defer g.assignmentsMu.Unlock()
	assignment, ok := g.assignments[index]
	if !ok {
		return ""
	}
	if assignment.Peer != peerID {
		return "assignment_replaced"
	}
	if assignment.Rescued {
		return "overtaken_by_rescue"
	}
	return ""
}

func (g *graphsyncFetcher) emitFetchSuccess(index int, playbackIndex int64, distance int64, urgent bool, method string, peerState peerState, prediction durationPrediction, start time.Time, segmentBytes []byte, reason string, updateMetrics bool, ctx context.Context) {
	duration := time.Since(start)
	outcomeOverride := ""
	if reason != "" {
		outcomeOverride = reason
	}
	emitDurationPredictionSampleWithReason(index, playbackIndex, distance, urgent, method, peerState, prediction, start, duration, len(segmentBytes), nil, ctx, outcomeOverride, reason)
	if updateMetrics {
		g.selector.recordSample(peerState.info.ID, len(segmentBytes), duration, method, prediction)
		g.revalidateAllAssignments(index)
		g.selector.recordSuccess(peerState.info.ID)
	} else {
		g.selector.recordIgnoredSample(peerState.info.ID, len(segmentBytes), duration, prediction, reason)
	}
	emitStructuredEvent(schedulerResultEvent{
		Type:          "scheduler_result",
		Segment:       index,
		PlaybackIndex: playbackIndex,
		Distance:      distance,
		Urgent:        urgent,
		Method:        method,
		Reason:        reason,
		PeerID:        peerState.info.ID.String(),
		PeerLabel:     peerState.profile.Label,
		Duration:      duration.Nanoseconds(),
		Bytes:         len(segmentBytes),
		Success:       true,
		TimeNS:        time.Now().UnixNano(),
	})
}

func (g *graphsyncFetcher) emitFetchFailure(index int, playbackIndex int64, distance int64, urgent bool, method string, peerState peerState, prediction durationPrediction, start time.Time, bytes int, err error, ctx context.Context) {
	g.selector.recordFailure(peerState.info.ID)
	duration := time.Since(start)
	emitDurationPredictionSample(index, playbackIndex, distance, urgent, method, peerState, prediction, start, duration, bytes, err, ctx)
	reason := ""
	if method == "discovery_single" && ctx.Err() == context.DeadlineExceeded {
		reason = "discovery_timeout"
	}
	emitStructuredEvent(schedulerResultEvent{
		Type:          "scheduler_result",
		Segment:       index,
		PlaybackIndex: playbackIndex,
		Distance:      distance,
		Urgent:        urgent,
		Method:        method,
		Reason:        reason,
		PeerID:        peerState.info.ID.String(),
		PeerLabel:     peerState.profile.Label,
		Duration:      duration.Nanoseconds(),
		Success:       false,
		Error:         err.Error(),
		TimeNS:        time.Now().UnixNano(),
	})
}

func (g *graphsyncFetcher) readSegmentForFetch(ctx context.Context, store blockstore.Blockstore, index int, method string, storeName string, peerID peer.ID, onChunk func(index int, size int)) ([]byte, error) {
	segmentCID := g.layout.Segments[index]
	segmentBytes, err := posl.ReadSegmentWithCallback(ctx, store, segmentCID, onChunk)
	if err != nil {
		return nil, fmt.Errorf("read fetch segment=%d method=%s store=%s peer=%s segmentCid=%s: %w", index, method, storeName, peerID, segmentCID, err)
	}
	return segmentBytes, nil
}

func (g *graphsyncFetcher) discoveryFetch(ctx context.Context, index int, selector ipld.Node) ([]byte, error) {
	playbackIndex := g.currentPlaybackIndex()
	distance := int64(index) - playbackIndex
	deadline := time.Duration(distance) * g.selector.playbackDelay
	urgentWindowCached := g.urgentWindowCached(playbackIndex)
	urgentWindowCovered := urgentWindowCached || g.urgentWindowCovered(playbackIndex)
	discoveryNeeded, stalePeerCount := g.selector.throughputDiscoveryNeeded(g.selector.peers)
	debt := g.selector.discoveryDebt(g.selector.peers, time.Now())
	peerState, peerStateReason, discoveryClass, skipReason, ok := g.selector.discoveryPeerWithClass()
	if !ok {
		g.emitDiscoverySkip(index, playbackIndex, distance, false, skipReason, urgentWindowCached, urgentWindowCovered, discoveryNeeded)
		return g.singleFetch(ctx, index, selector)
	}
	discoveryTimeout, ok := g.selector.discoveryFetchTimeout(deadline)
	if !ok {
		g.emitDiscoverySkip(index, playbackIndex, distance, false, "deadline_budget_too_small", urgentWindowCached, urgentWindowCovered, discoveryNeeded)
		return g.singleFetch(ctx, index, selector)
	}
	decision := selectionDecision{
		Distance:               distance,
		Urgent:                 false,
		Deadline:               deadline,
		Estimate:               g.selector.estimatedTime(peerState),
		SafetyMargin:           g.selector.safetyMargin(),
		CandidateCount:         stalePeerCount,
		Reason:                 "throughput_discovery_assignment",
		DiscoveryPeerState:     peerStateReason,
		SelectedDiscoveryClass: discoveryClass,
	}
	if decision.Estimate > 0 {
		decision.ProjectedFinish = g.selector.projectedFinish(peerState, decision.Estimate)
	}
	if g.logPeers {
		fmt.Printf("segment %d discovery -> peer %s state=%s timeout=%s\n", index, peerState.info.ID, peerStateReason, discoveryTimeout)
	}
	g.selector.markDiscoveryInflight(peerState.info.ID, 1)
	defer g.selector.markDiscoveryInflight(peerState.info.ID, -1)
	selectedStats := g.selector.stats[peerState.info.ID]
	emitStructuredEvent(schedulerDecisionEvent{
		Type:                         "scheduler_decision",
		Segment:                      index,
		PlaybackIndex:                playbackIndex,
		Distance:                     distance,
		Urgent:                       false,
		Method:                       "discovery_single",
		Reason:                       decision.Reason,
		Deadline:                     deadline.Nanoseconds(),
		Estimate:                     decision.Estimate.Nanoseconds(),
		ProjectedFinish:              decision.ProjectedFinish.Nanoseconds(),
		SafetyMargin:                 decision.SafetyMargin.Nanoseconds(),
		CandidateCount:               decision.CandidateCount,
		SelectedPeerID:               peerState.info.ID.String(),
		SelectedPeerLabel:            peerState.profile.Label,
		SelectedEstimate:             decision.Estimate.Nanoseconds(),
		UrgentWindowCached:           urgentWindowCached,
		UrgentWindowCovered:          urgentWindowCovered,
		ThroughputDiscoveryNeeded:    discoveryNeeded,
		StalePeerCount:               stalePeerCount,
		DiscoveryDebtCoverage:        debt.coverage,
		DiscoveryDebtReprobe:         debt.reprobe,
		DiscoveryInflight:            g.selector.activeDiscoveryInflight(time.Now()),
		SelectedDiscoveryClass:       discoveryClass,
		SelectedQualifiedSamples:     qualifiedSamples(selectedStats),
		SelectedNormalInflight:       normalInflight(selectedStats),
		UnqualifiedDiscoveryAttempts: unqualifiedDiscoveryAttempts(selectedStats),
		DiscoveryPeerState:           peerStateReason,
		DiscoveryTimeout:             discoveryTimeout.Nanoseconds(),
		DiscoveryFanout:              g.selector.discoveryFanout,
		TimeNS:                       time.Now().UnixNano(),
	})
	g.beginAssignment(index, peerState.info.ID, playbackIndex, decision)
	fetchCtx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	data, err := g.fetchFromPeer(fetchCtx, index, selector, peerState, playbackIndex, distance, false, "discovery_single")
	cancel()
	g.finishAssignment(index, peerState.info.ID)
	if err != nil {
		return g.singleFetch(ctx, index, selector)
	}
	return data, nil
}

func (g *graphsyncFetcher) singleFetch(ctx context.Context, index int, selector ipld.Node) ([]byte, error) {
	playbackIndex := g.currentPlaybackIndex()
	peerState, decision := g.selector.selectPeerWithInfo(index, playbackIndex)
	urgentWindowCached := g.urgentWindowCached(playbackIndex)
	urgentWindowCovered := urgentWindowCached || g.urgentWindowCovered(playbackIndex)
	discoveryNeeded, stalePeerCount := g.selector.throughputDiscoveryNeeded(g.selector.peers)
	if g.logPeers {
		fmt.Printf("segment %d network -> peer %s (distance=%d, est=%s)\n", index, peerState.info.ID, decision.Distance, decision.Estimate)
	}
	emitStructuredEvent(schedulerDecisionEvent{
		Type:                      "scheduler_decision",
		Segment:                   index,
		PlaybackIndex:             playbackIndex,
		Distance:                  decision.Distance,
		Urgent:                    decision.Urgent,
		Method:                    "single",
		Reason:                    decision.Reason,
		Deadline:                  decision.Deadline.Nanoseconds(),
		Estimate:                  decision.Estimate.Nanoseconds(),
		ProjectedFinish:           decision.ProjectedFinish.Nanoseconds(),
		SafetyMargin:              decision.SafetyMargin.Nanoseconds(),
		CandidateCount:            decision.CandidateCount,
		SelectedPeerID:            peerState.info.ID.String(),
		SelectedPeerLabel:         peerState.profile.Label,
		SelectedEstimate:          decision.Estimate.Nanoseconds(),
		UrgentWindowCached:        urgentWindowCached,
		UrgentWindowCovered:       urgentWindowCovered,
		ThroughputDiscoveryNeeded: discoveryNeeded,
		StalePeerCount:            stalePeerCount,
		TimeNS:                    time.Now().UnixNano(),
	})
	g.beginAssignment(index, peerState.info.ID, playbackIndex, decision)
	defer g.finishAssignment(index, peerState.info.ID)
	return g.fetchFromPeer(ctx, index, selector, peerState, playbackIndex, decision.Distance, decision.Urgent, "single")
}

func (g *graphsyncFetcher) Fetch(ctx context.Context, index int) ([]byte, error) {
	if index < 0 || index >= g.layout.SegmentCount {
		return nil, fmt.Errorf("segment %d out of range", index)
	}
	playbackIndex := g.currentPlaybackIndex()
	distance := int64(index) - playbackIndex
	urgent := distance <= g.selector.urgentWindow
	discoveryNeeded, _ := g.selector.throughputDiscoveryNeeded(g.selector.peers)
	if g.shouldBootstrapRace() {
		return g.raceFetch(ctx, index)
	}
	if g.shouldDeferFutureFetch(playbackIndex, distance, urgent) {
		if err := g.deferFutureFetch(ctx); err != nil {
			return nil, err
		}
		playbackIndex = g.currentPlaybackIndex()
		distance = int64(index) - playbackIndex
		urgent = distance <= g.selector.urgentWindow
		discoveryNeeded, _ = g.selector.throughputDiscoveryNeeded(g.selector.peers)
	}
	selector, err := posl.SegmentSelector(index)
	if err != nil {
		return nil, err
	}
	urgentWindowCached := g.urgentWindowCached(playbackIndex)
	urgentWindowCovered := urgentWindowCached || g.urgentWindowCovered(playbackIndex)
	if shouldUseDiscoveryAssignment(urgent, urgentWindowCovered, discoveryNeeded) {
		return g.discoveryFetch(ctx, index, selector)
	}
	if discoveryNeeded {
		reason := "urgent_window_not_covered"
		if urgent {
			reason = "urgent_segment"
		} else if urgentWindowCovered {
			reason = "no_candidate"
		}
		g.emitDiscoverySkip(index, playbackIndex, distance, urgent, reason, urgentWindowCached, urgentWindowCovered, discoveryNeeded)
	}
	return g.singleFetch(ctx, index, selector)
}

func (g *graphsyncFetcher) shouldDeferFutureFetch(playbackIndex int64, distance int64, urgent bool) bool {
	if g.prefetcher == nil || g.selector == nil {
		return false
	}
	if urgent || distance <= g.selector.urgentWindow {
		return false
	}
	return !g.urgentWindowCovered(playbackIndex)
}

func (g *graphsyncFetcher) deferFutureFetch(ctx context.Context) error {
	delay := 100 * time.Millisecond
	if g.selector != nil && g.selector.playbackDelay > 0 {
		delay = g.selector.playbackDelay / 4
		if delay <= 0 {
			delay = time.Millisecond
		}
		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (g *graphsyncFetcher) shouldBootstrapRace() bool {
	return g.selector != nil && !g.selector.hasMeasuredPeer()
}

func shouldUseDiscoveryAssignment(urgent bool, urgentWindowCovered bool, discoveryNeeded bool) bool {
	return !urgent && urgentWindowCovered && discoveryNeeded
}

func main() {
	var (
		peerConfigPath     = flag.String("peers", "", "JSON file with peer configurations (optional if using DHT)")
		rootFlag           = flag.String("root", "", "root CID")
		prefetchCount      = flag.Int("prefetch-segments", 6, "segments to prefetch (-1 means prefetch as many remaining segments as possible)")
		workers            = flag.Int("prefetch-workers", 0, "prefetch worker count (<=0 defaults to connected peer count)")
		playbackMS         = flag.Int("playback-ms", 40, "simulated playback delay in ms")
		format             = flag.String("format", "json", "output format: json or csv")
		useDHT             = flag.Bool("use-dht", true, "discover peers via DHT when peers.json is not provided")
		logPeers           = flag.Bool("log-peers", false, "log peer selection and connections")
		urgentWindow       = flag.Int("urgent-window", 10, "segments considered urgent")
		emaAlpha           = flag.Float64("ema-alpha", 0.2, "EMA alpha for throughput updates")
		bootstrap          = flag.String("bootstrap", "", "comma-separated bootstrap multiaddrs (optional)")
		maxProviders       = flag.Int("dht-providers", 5, "maximum providers to use from DHT")
		providerTimeoutMS  = flag.Int("dht-timeout-ms", 5000, "timeout in ms for DHT provider discovery")
		storeDir           = flag.String("store", "", "blockstore directory for GraphSync client")
		raceFanout         = flag.Int("race-fanout", 0, "max peers per race (-1 means all connected peers; 0 is a legacy alias)")
		discoveryFanout    = flag.Int("discovery-fanout", 1, "max concurrent nonurgent throughput discovery peer assignments (<=0 disables discovery)")
		raceRoundsPerPeer  = flag.Int("race-rounds-per-peer", 4, "initial race segments per peer")
		probeIntervalSec   = flag.Int("probe-interval-sec", 60, "seconds between probe races")
		raceTimeoutMS      = flag.Int("race-timeout-ms", 30000, "timeout in ms for race requests")
		discoveryTimeoutMS = flag.Int("discovery-timeout-ms", 10000, "timeout in ms for nonurgent throughput discovery fetches")
	)
	flag.Parse()

	if *rootFlag == "" {
		fmt.Println("--root is required")
		os.Exit(1)
	}

	root, err := cid.Parse(*rootFlag)
	if err != nil {
		fmt.Printf("invalid root cid: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	p2pHost, err := libp2p.New()
	if err != nil {
		fmt.Printf("failed to create host: %v\n", err)
		os.Exit(1)
	}
	defer p2pHost.Close()

	peers, err := resolvePeers(ctx, p2pHost, *peerConfigPath, *useDHT, root, *bootstrap, *maxProviders, time.Duration(*providerTimeoutMS)*time.Millisecond)
	if err != nil {
		fmt.Printf("failed to resolve peers: %v\n", err)
		os.Exit(1)
	}
	if *logPeers {
		fmt.Printf("Connected peers: %d\n", len(peers))
		for _, peerState := range peers {
			fmt.Printf("- %s %s\n", peerState.info.ID, peerState.profile.Label)
			for _, addr := range peerState.info.Addrs {
				fmt.Printf("  %s\n", addr.String())
			}
		}
	}

	bs := createBlockstore(*storeDir)
	linkSystem := storeutil.LinkSystemForBlockstore(bs)
	gs := gsimpl.New(ctx, gsnet.NewFromLibp2pHost(p2pHost), linkSystem)
	raceStores := newRaceStoreManager(gs)

	layout, err := fetchLayout(ctx, gs, bs, peers[0].info.ID, root)
	if err != nil {
		fmt.Printf("failed to fetch layout: %v\n", err)
		os.Exit(1)
	}

	segmentSize := int64(layout.SegmentSize)
	raceFanoutValue := *raceFanout
	if raceFanoutValue <= 0 {
		raceFanoutValue = len(peers)
	}
	if raceFanoutValue > len(peers) {
		raceFanoutValue = len(peers)
	}
	if raceFanoutValue <= 0 {
		raceFanoutValue = 1
	}
	discoveryFanoutValue := *discoveryFanout
	if discoveryFanoutValue > len(peers) {
		discoveryFanoutValue = len(peers)
	}
	playbackDelay := time.Duration(*playbackMS) * time.Millisecond
	selector := &adaptiveSelector{
		peers:             peers,
		segmentSize:       segmentSize,
		playbackDelay:     playbackDelay,
		urgentWindow:      int64(*urgentWindow),
		emaAlpha:          *emaAlpha,
		logPeers:          *logPeers,
		stats:             make(map[peer.ID]*peerStats),
		discoveryInflight: make(map[peer.ID]int),
		raceSegments:      len(peers) * *raceRoundsPerPeer,
		raceFanout:        raceFanoutValue,
		discoveryFanout:   discoveryFanoutValue,
		probeInterval:     time.Duration(*probeIntervalSec) * time.Second,
		raceTimeout:       time.Duration(*raceTimeoutMS) * time.Millisecond,
		discoveryTimeout:  time.Duration(*discoveryTimeoutMS) * time.Millisecond,
		startTime:         time.Now(),
		lastProbe:         time.Now(),
	}
	currentIndex := int64(0)
	fetcher := &graphsyncFetcher{
		graphsync:    gs,
		store:        bs,
		root:         root,
		layout:       layout,
		selector:     selector,
		currentIndex: &currentIndex,
		logPeers:     *logPeers,
		raceStores:   raceStores,
		assignments:  make(map[int]*inflightAssignment),
	}

	configPeers := make([]schedulerConfigPeer, 0, len(peers))
	for _, peerState := range peers {
		configPeers = append(configPeers, schedulerConfigPeer{
			PeerID: peerState.info.ID.String(),
			Label:  peerState.profile.Label,
		})
	}
	emitStructuredEvent(schedulerConfigEvent{
		Type:             "scheduler_config",
		UrgentWindow:     selector.urgentWindow,
		PlaybackDelay:    selector.playbackDelay.Nanoseconds(),
		RaceFanout:       selector.raceFanout,
		DiscoveryFanout:  selector.discoveryFanout,
		RaceSegments:     selector.raceSegments,
		ProbeInterval:    selector.probeInterval.Nanoseconds(),
		DiscoveryTimeout: selector.discoveryTimeout.Nanoseconds(),
		PeerCount:        len(peers),
		Peers:            configPeers,
		TimeNS:           time.Now().UnixNano(),
	})

	prefetchCountValue := *prefetchCount
	if prefetchCountValue < 0 {
		prefetchCountValue = layout.SegmentCount
	}
	workersValue := *workers
	if workersValue <= 0 {
		workersValue = len(peers)
		if workersValue <= 0 {
			workersValue = 1
		}
	}
	prefetcher := prefetch.NewWithWorkers(fetcher, prefetchCountValue, workersValue)
	fetcher.prefetcher = prefetcher
	prefetcher.SetMaxIndex(layout.SegmentCount - 1)
	prefetcher.Start()

	tracker := metrics.NewTracker()
	if playbackDelay > 0 {
		tracker.SetPlaybackModel(layout.SegmentCount, playbackDelay)
	}
	runStart := time.Now()
	for segmentIndex := 0; segmentIndex < layout.SegmentCount; segmentIndex++ {
		currentIndex = int64(segmentIndex)
		if selector.urgentWindow >= 0 {
			urgentEnd := segmentIndex + int(selector.urgentWindow)
			if urgentEnd >= layout.SegmentCount {
				urgentEnd = layout.SegmentCount - 1
			}
			rescued := prefetcher.RescueInflightRange(segmentIndex, urgentEnd)
			if *logPeers && rescued > 0 {
				fmt.Printf("playback urgent window queued %d rescue request(s) for segments %d..%d\n", rescued, segmentIndex, urgentEnd)
			}
		}
		start := time.Now()
		data, cacheHit, err := prefetcher.Get(ctx, segmentIndex)
		if err != nil {
			fmt.Printf("segment %d error: %v\n", segmentIndex, err)
			os.Exit(1)
		}
		duration := time.Since(start)
		if *logPeers {
			fmt.Printf("segment %d playback fetched in %s (prefetchCache=%v)\n", segmentIndex, duration, cacheHit)
		}
		tracker.RecordSegment(duration, len(data), cacheHit)
		if playbackDelay > 0 {
			tracker.RecordSegmentReady(segmentIndex, time.Since(runStart))
			if segmentIndex < layout.SegmentCount-1 {
				time.Sleep(playbackDelay)
			}
		}
	}
	if *logPeers {
		fmt.Println("Peer summary:")
		selector.mu.Lock()
		for id, stats := range selector.stats {
			cooldownLeft := time.Duration(0)
			if !stats.cooldownUntil.IsZero() {
				cooldownLeft = time.Until(stats.cooldownUntil)
				if cooldownLeft < 0 {
					cooldownLeft = 0
				}
			}
			avgDuration := time.Duration(0)
			if stats.segments > 0 {
				avgDuration = stats.timeTotal / time.Duration(stats.segments)
			}
			fmt.Printf("- %s segments=%d inflight=%d bytes=%d ema=%.2fB/s avgNet=%s fails=%d cooldown=%s\n", id, stats.segments, stats.inflight, stats.bytesTotal, stats.emaThroughput, avgDuration, stats.consecutiveFails, cooldownLeft)
		}
		selector.mu.Unlock()
	}

	prefetcher.Stop()
	fetcher.waitRaceOverheadAccounting()
	outputMetrics(tracker.Summary(), *format)
}

func loadConfig(path string) (config, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}
	var cfg config
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return config{}, err
	}
	if len(cfg.Peers) == 0 {
		return config{}, fmt.Errorf("no peers configured")
	}
	return cfg, nil
}

func resolvePeers(ctx context.Context, host host.Host, peerConfigPath string, useDHT bool, root cid.Cid, bootstrap string, maxProviders int, timeout time.Duration) ([]peerState, error) {
	if peerConfigPath != "" {
		cfg, err := loadConfig(peerConfigPath)
		if err != nil {
			return nil, err
		}
		peers, err := buildPeers(cfg.Peers)
		if err != nil {
			return nil, err
		}
		return connectPeers(ctx, host, peers)
	}
	if !useDHT {
		return nil, fmt.Errorf("no peers provided and DHT disabled")
	}
	bootstrapAddrs := parseBootstrap(bootstrap)
	providers, err := discoverProviders(ctx, host, root, bootstrapAddrs, maxProviders, timeout)
	if err != nil {
		return nil, err
	}
	peers := make([]peerState, 0, len(providers))
	for _, info := range providers {
		peers = append(peers, peerState{
			info:    info,
			profile: peerConfig{Label: "dht"},
		})
	}
	return connectPeers(ctx, host, peers)
}

func buildPeers(peers []peerConfig) ([]peerState, error) {
	states := make([]peerState, 0, len(peers))
	for _, peerCfg := range peers {
		addr, err := multiaddr.NewMultiaddr(peerCfg.Addr)
		if err != nil {
			return nil, err
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, err
		}
		states = append(states, peerState{
			info:    *info,
			profile: peerCfg,
		})
	}
	return states, nil
}

func connectPeers(ctx context.Context, host host.Host, peers []peerState) ([]peerState, error) {
	connected := make([]peerState, 0, len(peers))
	for _, peerState := range peers {
		if err := host.Connect(ctx, peerState.info); err != nil {
			fmt.Printf("failed to connect to %s: %v\n", peerState.info.ID, err)
			continue
		}
		connected = append(connected, peerState)
	}
	if len(connected) == 0 {
		return nil, fmt.Errorf("no peers could be connected")
	}
	return connected, nil
}

func filterAddrInfo(info peer.AddrInfo) peer.AddrInfo {
	filtered := make([]multiaddr.Multiaddr, 0, len(info.Addrs))
	for _, addr := range info.Addrs {
		if isPublicAddr(addr) {
			filtered = append(filtered, addr)
		}
	}
	info.Addrs = filtered
	return info
}

func isPublicAddr(addr multiaddr.Multiaddr) bool {
	ip4, err := addr.ValueForProtocol(multiaddr.P_IP4)
	if err == nil {
		ip := net.ParseIP(ip4)
		return isPublicIP(ip)
	}
	ip6, err := addr.ValueForProtocol(multiaddr.P_IP6)
	if err == nil {
		ip := net.ParseIP(ip6)
		return isPublicIP(ip)
	}
	return true
}

func isPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.IsPrivate() {
		return false
	}
	return true
}

func parseBootstrap(value string) []string {
	if strings.TrimSpace(value) == "" {
		return discovery.DefaultBootstrapAddrs
	}
	parts := strings.Split(value, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			addrs = append(addrs, trimmed)
		}
	}
	return addrs
}

func discoverProviders(ctx context.Context, host host.Host, root cid.Cid, bootstrapAddrs []string, maxProviders int, timeout time.Duration) ([]peer.AddrInfo, error) {
	dhtNode, err := dht.New(ctx, host, dht.Mode(dht.ModeAuto))
	if err != nil {
		return nil, err
	}
	if err := dhtNode.Bootstrap(ctx); err != nil {
		return nil, err
	}
	if err := connectBootstrap(ctx, host, bootstrapAddrs); err != nil {
		return nil, err
	}
	providerCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	providerCh := dhtNode.FindProvidersAsync(providerCtx, root, maxProviders)
	providers := make([]peer.AddrInfo, 0, maxProviders)
	for provider := range providerCh {
		filtered := filterAddrInfo(provider)
		if len(filtered.Addrs) == 0 {
			continue
		}
		providers = append(providers, filtered)
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for %s", root.String())
	}
	return providers, nil
}

func connectBootstrap(ctx context.Context, host host.Host, addrs []string) error {
	for _, addr := range addrs {
		multiaddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return err
		}
		info, err := peer.AddrInfoFromP2pAddr(multiaddr)
		if err != nil {
			return err
		}
		if err := host.Connect(ctx, *info); err != nil {
			continue
		}
	}
	return nil
}

func fetchLayout(ctx context.Context, gs gs.GraphExchange, bs blockstore.Blockstore, peerID peer.ID, root cid.Cid) (posl.Layout, error) {
	selector, err := posl.RootSelector()
	if err != nil {
		return posl.Layout{}, err
	}
	if err := requestSegment(ctx, gs, peerID, root, selector); err != nil {
		return posl.Layout{}, err
	}
	return posl.LoadLayout(ctx, bs, root)
}

func requestSegment(ctx context.Context, gs gs.GraphExchange, peerID peer.ID, root cid.Cid, selector ipld.Node) error {
	progressCh, errCh := gs.Request(ctx, peerID, cidlink.Link{Cid: root}, selector)
	for progressCh != nil || errCh != nil {
		select {
		case _, ok := <-progressCh:
			if !ok {
				progressCh = nil
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func outputMetrics(summary metrics.Summary, format string) {
	switch strings.ToLower(format) {
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

func mergeBlockstores(ctx context.Context, dst blockstore.Blockstore, src blockstore.Blockstore) error {
	if src == nil {
		return nil
	}
	keys, err := src.AllKeysChan(ctx)
	if err != nil {
		return err
	}
	for key := range keys {
		block, err := src.Get(ctx, key)
		if err != nil {
			return err
		}
		if err := dst.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func createBlockstore(storeDir string) blockstore.Blockstore {
	if strings.TrimSpace(storeDir) == "" {
		return validatingBlockstore{Blockstore: blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))}
	}
	return validatingBlockstore{Blockstore: boxostore.NewFileBlockstore(storeDir)}
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
