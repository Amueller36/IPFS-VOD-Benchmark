package main

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type runRequest struct {
	Mode            string                 `json:"mode"`
	Root            string                 `json:"root"`
	Prefetch        int                    `json:"prefetch"`
	Workers         int                    `json:"workers"`
	RaceFanout      int                    `json:"raceFanout"`
	DiscoveryFanout int                    `json:"discoveryFanout"`
	PlaybackMS      int                    `json:"playbackMs"`
	PlaybackSpeed   float64                `json:"playbackSpeed"`
	LogPeers        bool                   `json:"logPeers"`
	UrgentWindow    int                    `json:"urgentWindow"`
	EMAAlpha        float64                `json:"emaAlpha"`
	ChunkBytes      int                    `json:"chunkBytes"`
	ScenarioPreset  string                 `json:"scenarioPreset"`
	NetworkPreset   string                 `json:"networkPreset"`
	NetworkProfile  string                 `json:"networkProfile"`
	PeerNetwork     []peerNetworkCondition `json:"peerNetwork"`
	QueueID         string                 `json:"queueId"`
	QueueItemID     string                 `json:"queueItemId"`
	QueuePosition   int                    `json:"queuePosition"`
	RepeatCount     int                    `json:"repeatCount"`
	RepeatIndex     int                    `json:"repeatIndex"`
	InterRunDelay   int                    `json:"interRunDelayMs"`
}

type queueStartRequest struct {
	Queue []queueRequestItem `json:"queue"`
}

type queueRequestItem struct {
	ID              string     `json:"id"`
	Position        int        `json:"position"`
	Preset          string     `json:"preset"`
	Label           string     `json:"label"`
	Payload         runRequest `json:"payload"`
	RepeatCount     int        `json:"repeatCount"`
	InterRunDelayMS int        `json:"interRunDelayMs"`
}

type peerNetworkCondition struct {
	Slot      string  `json:"slot"`
	Label     string  `json:"label"`
	RateMbit  int     `json:"rateMbit"`
	LatencyMS int     `json:"latencyMs"`
	JitterMS  int     `json:"jitterMs"`
	LossPct   float64 `json:"lossPct"`
}

type seedRequest struct {
	SegmentKB int `json:"segmentKb"`
	ChunkKB   int `json:"chunkKb"`
}

type bitswapSeedRequest struct {
	ChunkKB int `json:"chunkKb"`
}

type seedResponse struct {
	Root   string `json:"root"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

type seedInfoResponse struct {
	Root        string  `json:"root"`
	Size        int64   `json:"size"`
	SegmentSize int64   `json:"segmentSize"`
	ChunkSize   int64   `json:"chunkSize"`
	Segments    int     `json:"segments"`
	DurationS   float64 `json:"durationSec"`
}

type videoInfoResponse struct {
	Exists            bool    `json:"exists"`
	DurationSec       float64 `json:"durationSec"`
	Quality           string  `json:"quality"`
	BitrateMbitPerSec float64 `json:"bitrateMbitPerSec"`
	SizeMB            float64 `json:"sizeMb"`
	Error             string  `json:"error,omitempty"`
}

type bitswapSeedResponse struct {
	Root   string `json:"root"`
	Output string `json:"output"`
	Error  string `json:"error"`
}

type bitswapSeedInfoResponse struct {
	Root string `json:"root"`
}

type bitswapSeedMetadata struct {
	Root       string `json:"root"`
	ChunkKB    int    `json:"chunkKb"`
	ChunkSize  int64  `json:"chunkSize"`
	CIDVersion int    `json:"cidVersion"`
	Chunker    string `json:"chunker"`
	VideoPath  string `json:"videoPath"`
	VideoSize  int64  `json:"videoSize"`
	VideoMTime int64  `json:"videoMTime"`
}

type seedMetadata struct {
	Root        string `json:"root,omitempty"`
	Size        int64  `json:"size"`
	SegmentSize int64  `json:"segmentSize"`
	ChunkSize   int64  `json:"chunkSize"`
	Segments    int    `json:"segments"`
}

type updatePeersResponse struct {
	Output string `json:"output"`
	Error  string `json:"error"`
}

type timelineGenerateResponse struct {
	Generated int      `json:"generated"`
	Skipped   int      `json:"skipped"`
	Errors    []string `json:"errors"`
}

type peerCountResponse struct {
	Count int `json:"count"`
}

type runStatus struct {
	ID        string                 `json:"id"`
	State     string                 `json:"state"`
	Output    string                 `json:"output"`
	StartedAt string                 `json:"startedAt"`
	EndedAt   string                 `json:"endedAt"`
	Error     string                 `json:"error"`
	Params    map[string]interface{} `json:"params"`
}

type queueStatusItem struct {
	ID              string   `json:"id"`
	Position        int      `json:"position"`
	Preset          string   `json:"preset"`
	Label           string   `json:"label"`
	State           string   `json:"state"`
	RunIDs          []string `json:"runIds"`
	RepeatCount     int      `json:"repeatCount"`
	InterRunDelayMS int      `json:"interRunDelayMs"`
}

type queueStatusResponse struct {
	ID           string            `json:"id"`
	State        string            `json:"state"`
	StartedAt    string            `json:"startedAt"`
	EndedAt      string            `json:"endedAt"`
	Error        string            `json:"error"`
	CurrentRunID string            `json:"currentRunId"`
	CurrentRun   *runStatus        `json:"currentRun,omitempty"`
	Items        []queueStatusItem `json:"items"`
}

type httpError struct {
	status  int
	message string
}

type runSummary struct {
	TTFB                  int64     `json:"ttfb"`
	StallCount            int       `json:"stallCount"`
	PlaybackStallCount    int       `json:"playbackStallCount"`
	StallDurations        []int64   `json:"stallDurations"`
	StallDurationsSec     []float64 `json:"stallDurationsSec"`
	AvgSegmentFetch       int64     `json:"avgSegmentFetch"`
	CacheHitRate          float64   `json:"cacheHitRate"`
	TotalTime             int64     `json:"totalTime"`
	TotalBytes            int64     `json:"totalBytes"`
	ThroughputBytesPerS   float64   `json:"throughputBytesPerSec"`
	StartupDelay          int64     `json:"startupDelay"`
	TotalStallTime        int64     `json:"totalStallTime"`
	StallRatio            float64   `json:"stallRatio"`
	PlaybackOverheadRatio float64   `json:"playbackOverheadRatio"`
	DeadlineMissRate      float64   `json:"deadlineMissRate"`
	SegmentReadyP50       int64     `json:"segmentReadyP50"`
	SegmentReadyP95       int64     `json:"segmentReadyP95"`
	SegmentLatenessP50    int64     `json:"segmentLatenessP50"`
	SegmentLatenessP95    int64     `json:"segmentLatenessP95"`
	PlaybackDuration      int64     `json:"playbackDuration"`
	PlaybackCompletion    int64     `json:"playbackCompletion"`
}

type peerSummary struct {
	PeerID    string  `json:"peerId"`
	Segments  int     `json:"segments"`
	Bytes     int64   `json:"bytes"`
	EMA       float64 `json:"ema"`
	AvgNetMS  float64 `json:"avgNetMs"`
	Failures  int     `json:"failures"`
	CooldownS int64   `json:"cooldownSec"`
	Label     string  `json:"label"`
}

type graphsyncPeerEntry struct {
	Service string
	PeerID  string
	Label   string
}

type peerSlot struct {
	Slot         string
	DefaultLabel string
	Graphsync    string
	Kubo         string
}

type runExport struct {
	ID            string                 `json:"id"`
	Mode          string                 `json:"mode"`
	Root          string                 `json:"root"`
	Params        map[string]interface{} `json:"params"`
	StartedAt     string                 `json:"startedAt"`
	EndedAt       string                 `json:"endedAt"`
	State         string                 `json:"state,omitempty"`
	Error         string                 `json:"error,omitempty"`
	Outcome       string                 `json:"outcome,omitempty"`
	OutcomeReason string                 `json:"outcomeReason,omitempty"`
	Summary       *runSummary            `json:"summary,omitempty"`
	Peers         []peerSummary          `json:"peers,omitempty"`
	Seed          *seedMetadata          `json:"seed,omitempty"`
	Output        string                 `json:"output"`
}

type server struct {
	mu            sync.Mutex
	peersMu       sync.Mutex
	runs          map[string]*runStatus
	runCancels    map[string]context.CancelFunc
	cancelRequest map[string]bool
	batch         *queueStatusResponse
	batchCancel   context.CancelFunc
	startRunFunc  func(context.Context, runRequest) (*runStatus, *httpError)
	gsBin         string
	bitswapBin    string
	seedBin       string
	plotsAPI      string
	uiDistDir     string
}

const defaultGraphsyncStore = "/tmp/gs-client-store"
const defaultPeersPath = "/opt/peers.json"
const graphsyncPeerReadinessAttempts = 3
const graphsyncPeerReadinessDelay = 10 * time.Second
const graphsyncPeerRecoveryTimeout = 60 * time.Second

var seedManifestPath = "/graphsync-data/seed/manifest.json"
var seedOutputDir = "/graphsync-data/seed"
var runsRootDir = "/data/runs"

const graphsyncPeerRecoveryPoll = 10 * time.Second

var graphsyncPeerServices = []string{"gs-peer-fast-1", "gs-peer-fast-2", "gs-peer-slow-1", "gs-peer-slow-2", "gs-peer-slow-3"}

var peerSlots = []peerSlot{
	{Slot: "peer-1", DefaultLabel: "fast", Graphsync: "gs-peer-fast-1", Kubo: "kubo-fast-1"},
	{Slot: "peer-2", DefaultLabel: "fast", Graphsync: "gs-peer-fast-2", Kubo: "kubo-fast-2"},
	{Slot: "peer-3", DefaultLabel: "slow", Graphsync: "gs-peer-slow-1", Kubo: "kubo-slow-1"},
	{Slot: "peer-4", DefaultLabel: "slow", Graphsync: "gs-peer-slow-2", Kubo: "kubo-slow-2"},
	{Slot: "peer-5", DefaultLabel: "slow", Graphsync: "gs-peer-slow-3", Kubo: "kubo-slow-3"},
}

var defaultPeerNetwork = []peerNetworkCondition{
	{Slot: "peer-1", Label: "fast", RateMbit: 120, LatencyMS: 30, JitterMS: 8, LossPct: 0},
	{Slot: "peer-2", Label: "fast", RateMbit: 120, LatencyMS: 30, JitterMS: 8, LossPct: 0},
	{Slot: "peer-3", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
	{Slot: "peer-4", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
	{Slot: "peer-5", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
}

var stressedPeerNetwork = []peerNetworkCondition{
	{Slot: "peer-1", Label: "fast", RateMbit: 30, LatencyMS: 50, JitterMS: 10, LossPct: 0},
	{Slot: "peer-2", Label: "fast", RateMbit: 30, LatencyMS: 50, JitterMS: 10, LossPct: 0},
	{Slot: "peer-3", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
	{Slot: "peer-4", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
	{Slot: "peer-5", Label: "slow", RateMbit: 10, LatencyMS: 160, JitterMS: 30, LossPct: 0},
}

func clonePeerNetwork(input []peerNetworkCondition) []peerNetworkCondition {
	out := make([]peerNetworkCondition, len(input))
	copy(out, input)
	return out
}

func peerSlotByName(slot string) (peerSlot, bool) {
	slot = strings.TrimSpace(slot)
	for _, item := range peerSlots {
		if item.Slot == slot {
			return item, true
		}
	}
	return peerSlot{}, false
}

func peerSlotForGraphsyncService(service string) (peerSlot, bool) {
	service = strings.TrimSpace(service)
	for _, item := range peerSlots {
		if item.Graphsync == service {
			return item, true
		}
	}
	return peerSlot{}, false
}

func peerSlotForKuboHost(host string) (peerSlot, bool) {
	host = strings.TrimSpace(host)
	for _, item := range peerSlots {
		if item.Kubo == host {
			return item, true
		}
	}
	return peerSlot{}, false
}

func peerNetworkBySlot(conditions []peerNetworkCondition) map[string]peerNetworkCondition {
	bySlot := make(map[string]peerNetworkCondition, len(conditions))
	for _, condition := range conditions {
		bySlot[condition.Slot] = condition
	}
	return bySlot
}

func peerNetworkByGraphsyncService(conditions []peerNetworkCondition) map[string]peerNetworkCondition {
	bySlot := peerNetworkBySlot(conditions)
	byService := make(map[string]peerNetworkCondition, len(conditions))
	for _, slot := range peerSlots {
		if condition, ok := bySlot[slot.Slot]; ok {
			byService[slot.Graphsync] = condition
		}
	}
	return byService
}

func peerNetworkByKuboHost(conditions []peerNetworkCondition) map[string]peerNetworkCondition {
	bySlot := peerNetworkBySlot(conditions)
	byHost := make(map[string]peerNetworkCondition, len(conditions))
	for _, slot := range peerSlots {
		if condition, ok := bySlot[slot.Slot]; ok {
			byHost[slot.Kubo] = condition
		}
	}
	return byHost
}

func peerNetworkFromLegacyProfile(profile string) ([]peerNetworkCondition, error) {
	switch strings.TrimSpace(profile) {
	case "", "default":
		return clonePeerNetwork(defaultPeerNetwork), nil
	case "stressed":
		return clonePeerNetwork(stressedPeerNetwork), nil
	default:
		return nil, fmt.Errorf("unknown network profile: %s", profile)
	}
}

func normalizePeerNetwork(input []peerNetworkCondition, legacyProfile string) ([]peerNetworkCondition, error) {
	if len(input) == 0 {
		return peerNetworkFromLegacyProfile(legacyProfile)
	}
	seen := make(map[string]peerNetworkCondition, len(input))
	for _, raw := range input {
		condition := peerNetworkCondition{
			Slot:      strings.TrimSpace(raw.Slot),
			Label:     strings.TrimSpace(raw.Label),
			RateMbit:  raw.RateMbit,
			LatencyMS: raw.LatencyMS,
			JitterMS:  raw.JitterMS,
			LossPct:   raw.LossPct,
		}
		if _, ok := peerSlotByName(condition.Slot); !ok {
			return nil, fmt.Errorf("unknown peer slot: %s", condition.Slot)
		}
		if condition.Label == "" {
			return nil, fmt.Errorf("%s label is required", condition.Slot)
		}
		if condition.RateMbit <= 0 {
			return nil, fmt.Errorf("%s rateMbit must be > 0", condition.Slot)
		}
		if condition.LatencyMS < 0 {
			return nil, fmt.Errorf("%s latencyMs must be >= 0", condition.Slot)
		}
		if condition.JitterMS < 0 {
			return nil, fmt.Errorf("%s jitterMs must be >= 0", condition.Slot)
		}
		if condition.LossPct < 0 || condition.LossPct > 100 {
			return nil, fmt.Errorf("%s lossPct must be between 0 and 100", condition.Slot)
		}
		if _, exists := seen[condition.Slot]; exists {
			return nil, fmt.Errorf("duplicate peer slot: %s", condition.Slot)
		}
		seen[condition.Slot] = condition
	}
	if len(seen) != len(peerSlots) {
		return nil, fmt.Errorf("peerNetwork must contain exactly %d peer slots", len(peerSlots))
	}
	normalized := make([]peerNetworkCondition, 0, len(peerSlots))
	for _, slot := range peerSlots {
		condition, ok := seen[slot.Slot]
		if !ok {
			return nil, fmt.Errorf("missing peer slot: %s", slot.Slot)
		}
		normalized = append(normalized, condition)
	}
	return normalized, nil
}

func samePeerNetwork(left, right []peerNetworkCondition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func main() {
	gsBin := getenv("GS_CLIENT_BIN", "./gs-client")
	bitswapBin := getenv("BITSWAP_CLIENT_BIN", "./boxo-bitswap-client")
	seedBin := getenv("SEED_BIN", "./seed")
	addr := getenv("UI_ADDR", ":8080")
	plotsAPI := strings.TrimSpace(getenv("PLOTS_API_URL", "http://plots:8090"))
	uiDistDir := strings.TrimSpace(getenv("BENCHMARK_UI_DIST_DIR", ""))
	if uiDistDir == "" {
		uiDistDir = strings.TrimSpace(getenv("GS_UI_DIST_DIR", ""))
	}
	publicURL := strings.TrimSpace(getenv("UI_PUBLIC_URL", ""))
	if publicURL == "" {
		publicURL = uiDisplayURL(addr)
	}

	srv := &server{runs: make(map[string]*runStatus), runCancels: make(map[string]context.CancelFunc), cancelRequest: make(map[string]bool), gsBin: gsBin, bitswapBin: bitswapBin, seedBin: seedBin, plotsAPI: plotsAPI, uiDistDir: uiDistDir}
	srv.autoSeedGraphsyncLayout(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/assets/", srv.handleFrontendAsset)
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/run", srv.handleRun)
	mux.HandleFunc("/status", srv.handleStatus)
	mux.HandleFunc("/cancel", srv.handleCancel)
	mux.HandleFunc("/queue/start", srv.handleQueueStart)
	mux.HandleFunc("/queue/status", srv.handleQueueStatus)
	mux.HandleFunc("/queue/cancel", srv.handleQueueCancel)
	mux.HandleFunc("/seed", srv.handleSeed)
	mux.HandleFunc("/seed-info", srv.handleSeedInfo)
	mux.HandleFunc("/video-info", srv.handleVideoInfo)
	mux.HandleFunc("/peer-count", srv.handlePeerCount)
	mux.HandleFunc("/seed-bitswap", srv.handleBitswapSeed)
	mux.HandleFunc("/seed-bitswap-info", srv.handleBitswapSeedInfo)
	mux.HandleFunc("/update-peers", srv.handleUpdatePeers)
	mux.HandleFunc("/runs/download", srv.handleDownloadRuns)
	mux.HandleFunc("/runs/timelines/generate", srv.handleGenerateTimelines)
	mux.HandleFunc("/runs/timelines/download", srv.handleDownloadTimelines)
	mux.HandleFunc("/runs/clear", srv.handleClearRuns)
	mux.HandleFunc("/plots/download", srv.handleDownloadPlots)
	mux.HandleFunc("/reshape", srv.handleReshape)

	log.Printf("UI listening on %s", addr)
	log.Printf("UI available at %s", publicURL)
	log.Printf("Plots service at %s", plotsAPI)
	if dir, ok := srv.frontendDistDir(); ok {
		log.Printf("Serving Vue frontend from %s", dir)
	} else if uiDistDir != "" {
		log.Printf("BENCHMARK_UI_DIST_DIR=%s ist nicht verfügbar; liefere Frontend-Hinweisseite aus", uiDistDir)
	} else {
		log.Printf("BENCHMARK_UI_DIST_DIR ist nicht gesetzt; liefere Frontend-Hinweisseite aus")
	}
	log.Fatal(http.ListenAndServe(addr, mux))
}

func uiDisplayURL(addr string) string {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return "http://localhost:8080"
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}
	if strings.HasPrefix(trimmed, ":") {
		return "http://localhost" + trimmed
	}
	host, port, err := net.SplitHostPort(trimmed)
	if err == nil {
		host = strings.Trim(host, "[]")
		switch host {
		case "", "0.0.0.0", "127.0.0.1", "::", "::1":
			host = "localhost"
		}
		return "http://" + host + ":" + port
	}
	if _, err := strconv.Atoi(trimmed); err == nil {
		return "http://localhost:" + trimmed
	}
	return "http://localhost:8080"
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if dir, ok := s.frontendDistDir(); ok {
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(frontendUnavailableHTML))
}

func (s *server) frontendDistDir() (string, bool) {
	dir := strings.TrimSpace(s.uiDistDir)
	if dir == "" {
		return "", false
	}
	info, err := os.Stat(filepath.Join(dir, "index.html"))
	if err != nil || info.IsDir() {
		return "", false
	}
	assetsDir := filepath.Join(dir, "assets")
	assetsInfo, err := os.Stat(assetsDir)
	if err != nil || !assetsInfo.IsDir() {
		return "", false
	}
	return dir, true
}

func (s *server) handleFrontendAsset(w http.ResponseWriter, r *http.Request) {
	dir, ok := s.frontendDistDir()
	if !ok {
		http.NotFound(w, r)
		return
	}
	http.StripPrefix("/assets/", http.FileServer(http.Dir(filepath.Join(dir, "assets")))).ServeHTTP(w, r)
}

func normalizeRunRequest(req runRequest) (runRequest, *httpError) {
	if req.Root == "" {
		return req, &httpError{status: http.StatusBadRequest, message: "root is required"}
	}
	if req.Mode == "" {
		return req, &httpError{status: http.StatusBadRequest, message: "mode is required"}
	}
	switch req.Mode {
	case "graphsync", "bitswap":
	default:
		return req, &httpError{status: http.StatusBadRequest, message: fmt.Sprintf("invalid mode: %s", req.Mode)}
	}
	req.QueueID = strings.TrimSpace(req.QueueID)
	req.QueueItemID = strings.TrimSpace(req.QueueItemID)
	req.ScenarioPreset = strings.TrimSpace(req.ScenarioPreset)
	if req.ScenarioPreset == "" {
		req.ScenarioPreset = "custom"
	}
	req.NetworkPreset = strings.TrimSpace(req.NetworkPreset)
	if req.NetworkPreset == "" {
		req.NetworkPreset = "custom"
	}
	req.NetworkProfile = strings.TrimSpace(req.NetworkProfile)
	if req.NetworkProfile == "" {
		req.NetworkProfile = "default"
	}
	peerNetwork, err := normalizePeerNetwork(req.PeerNetwork, req.NetworkProfile)
	if err != nil {
		return req, &httpError{status: http.StatusBadRequest, message: err.Error()}
	}
	req.PeerNetwork = peerNetwork
	return req, nil
}

func (s *server) prepareRunRequest(ctx context.Context, req runRequest) (runRequest, string, *httpError) {
	req, httpErr := normalizeRunRequest(req)
	if httpErr != nil {
		return req, "", httpErr
	}
	preflightOutput := ""
	if req.Mode == "graphsync" {
		peerOutput, err := s.ensurePeersJSON(ctx, req.PeerNetwork)
		if strings.TrimSpace(peerOutput) != "" {
			preflightOutput += peerOutput
		}
		if err != nil {
			labels := loadPeerLabels("")
			if len(labels) == 0 {
				message := strings.TrimSpace(peerOutput)
				if message == "" {
					message = err.Error()
				}
				return req, preflightOutput, &httpError{status: http.StatusInternalServerError, message: fmt.Sprintf("failed to prepare peers.json: %s", message)}
			}
			preflightOutput += fmt.Sprintf("WARNING: peers.json refresh failed, continuing with existing peers.json: %v\n", err)
		}
	}
	return req, preflightOutput, nil
}

func cloneRunStatus(status *runStatus) *runStatus {
	if status == nil {
		return nil
	}
	copyStatus := *status
	copyStatus.Params = cloneParams(status.Params)
	return &copyStatus
}

func cloneParams(params map[string]interface{}) map[string]interface{} {
	if params == nil {
		return nil
	}
	copyParams := make(map[string]interface{}, len(params))
	for key, value := range params {
		copyParams[key] = value
	}
	return copyParams
}

func runParams(req runRequest) map[string]interface{} {
	return map[string]interface{}{
		"mode":            req.Mode,
		"prefetch":        req.Prefetch,
		"workers":         req.Workers,
		"raceFanout":      req.RaceFanout,
		"discoveryFanout": req.DiscoveryFanout,
		"playbackMs":      req.PlaybackMS,
		"playbackSpeed":   req.PlaybackSpeed,
		"urgentWindow":    req.UrgentWindow,
		"emaAlpha":        req.EMAAlpha,
		"logPeers":        req.LogPeers,
		"chunkBytes":      req.ChunkBytes,
		"cachePolicy":     "always-clear",
		"scenarioPreset":  req.ScenarioPreset,
		"networkPreset":   req.NetworkPreset,
		"networkProfile":  req.NetworkProfile,
		"peerNetwork":     req.PeerNetwork,
		"queueId":         req.QueueID,
		"queueItemId":     req.QueueItemID,
		"queuePosition":   req.QueuePosition,
		"repeatCount":     req.RepeatCount,
		"repeatIndex":     req.RepeatIndex,
		"interRunDelayMs": req.InterRunDelay,
	}
}

func (s *server) startRun(ctx context.Context, req runRequest) (*runStatus, *httpError) {
	req, preflightOutput, httpErr := s.prepareRunRequest(ctx, req)
	if httpErr != nil {
		return nil, httpErr
	}
	id := fmt.Sprintf("run-%d", time.Now().UnixNano())
	status := &runStatus{ID: id, State: "running", StartedAt: time.Now().Format(time.RFC3339), Output: preflightOutput, Params: runParams(req)}

	s.mu.Lock()
	s.runs[id] = status
	s.mu.Unlock()

	go s.execute(id, req)
	return cloneRunStatus(status), nil
}

func (s *server) startQueueRun(ctx context.Context, req runRequest) (*runStatus, *httpError) {
	if s.startRunFunc != nil {
		return s.startRunFunc(ctx, req)
	}
	return s.startRun(ctx, req)
}

func (s *server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	status, httpErr := s.startRun(r.Context(), req)
	if httpErr != nil {
		writeError(w, httpErr.status, httpErr.message)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	status, ok := s.runs[id]
	statusCopy := cloneRunStatus(status)
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(statusCopy)
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	s.mu.Lock()
	status, ok := s.runs[id]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	cancelFn, running := s.runCancels[id]
	if running {
		s.cancelRequest[id] = true
		status.Output += "Cancellation requested...\n"
	}
	resp := map[string]interface{}{"id": id, "running": running, "state": status.State}
	s.mu.Unlock()

	if running {
		cancelFn()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleQueueStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req queueStartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	items, httpErr := normalizeQueueRequest(req)
	if httpErr != nil {
		writeError(w, httpErr.status, httpErr.message)
		return
	}

	batchID := fmt.Sprintf("batch-%d", time.Now().UnixNano())
	statusItems := make([]queueStatusItem, 0, len(items))
	for _, item := range items {
		statusItems = append(statusItems, queueStatusItem{
			ID:              item.ID,
			Position:        item.Position,
			Preset:          item.Preset,
			Label:           item.Label,
			State:           "pending",
			RunIDs:          []string{},
			RepeatCount:     item.RepeatCount,
			InterRunDelayMS: item.InterRunDelayMS,
		})
	}
	ctx, cancel := context.WithCancel(context.Background())

	s.mu.Lock()
	if s.batch != nil && s.batch.State == "running" {
		s.mu.Unlock()
		cancel()
		writeError(w, http.StatusConflict, "queue already running")
		return
	}
	s.batch = &queueStatusResponse{
		ID:        batchID,
		State:     "running",
		StartedAt: time.Now().Format(time.RFC3339),
		Items:     statusItems,
	}
	s.batchCancel = cancel
	resp := s.cloneQueueStatusLocked()
	s.mu.Unlock()

	go s.executeQueue(ctx, batchID, items)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleQueueStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	resp := s.cloneQueueStatusLocked()
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleQueueCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var batchCancel context.CancelFunc
	var runCancel context.CancelFunc
	s.mu.Lock()
	if s.batch != nil && s.batch.State == "running" {
		s.batch.State = "cancelled"
		s.batch.Error = "queue cancelled"
		s.batch.EndedAt = time.Now().Format(time.RFC3339)
		for i := range s.batch.Items {
			if s.batch.Items[i].State == "running" || s.batch.Items[i].State == "pending" {
				s.batch.Items[i].State = "cancelled"
			}
		}
		batchCancel = s.batchCancel
		if s.batch.CurrentRunID != "" {
			if status := s.runs[s.batch.CurrentRunID]; status != nil && status.State == "running" {
				status.Output += "Cancellation requested...\n"
				s.cancelRequest[s.batch.CurrentRunID] = true
			}
			runCancel = s.runCancels[s.batch.CurrentRunID]
		}
	}
	resp := s.cloneQueueStatusLocked()
	s.mu.Unlock()

	if batchCancel != nil {
		batchCancel()
	}
	if runCancel != nil {
		runCancel()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func normalizeQueueRequest(req queueStartRequest) ([]queueRequestItem, *httpError) {
	if len(req.Queue) == 0 {
		return nil, &httpError{status: http.StatusBadRequest, message: "queue must contain at least one item"}
	}
	items := make([]queueRequestItem, 0, len(req.Queue))
	for i, item := range req.Queue {
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			item.ID = fmt.Sprintf("queue-%d", i+1)
		}
		if item.Position <= 0 {
			item.Position = i + 1
		}
		item.Preset = strings.TrimSpace(item.Preset)
		item.Label = strings.TrimSpace(item.Label)
		if item.Label == "" {
			item.Label = item.Preset
		}
		repeatCount := item.RepeatCount
		if repeatCount <= 0 {
			repeatCount = item.Payload.RepeatCount
		}
		if repeatCount <= 0 {
			repeatCount = 1
		}
		item.RepeatCount = repeatCount
		if item.InterRunDelayMS <= 0 {
			item.InterRunDelayMS = item.Payload.InterRunDelay
		}
		if item.InterRunDelayMS < 0 {
			item.InterRunDelayMS = 0
		}
		payload, httpErr := normalizeRunRequest(item.Payload)
		if httpErr != nil {
			httpErr.message = fmt.Sprintf("queue item %d: %s", i+1, httpErr.message)
			return nil, httpErr
		}
		payload.RepeatCount = item.RepeatCount
		payload.InterRunDelay = item.InterRunDelayMS
		item.Payload = payload
		items = append(items, item)
	}
	return items, nil
}

func (s *server) cloneQueueStatusLocked() queueStatusResponse {
	if s.batch == nil {
		return queueStatusResponse{State: "idle", Items: []queueStatusItem{}}
	}
	resp := *s.batch
	resp.Items = make([]queueStatusItem, len(s.batch.Items))
	for i, item := range s.batch.Items {
		resp.Items[i] = item
		resp.Items[i].RunIDs = make([]string, len(item.RunIDs))
		copy(resp.Items[i].RunIDs, item.RunIDs)
	}
	resp.CurrentRun = nil
	if resp.CurrentRunID != "" {
		resp.CurrentRun = cloneRunStatus(s.runs[resp.CurrentRunID])
	}
	return resp
}

func (s *server) executeQueue(ctx context.Context, batchID string, items []queueRequestItem) {
	defer func() {
		s.mu.Lock()
		if s.batch != nil && s.batch.ID == batchID {
			s.batchCancel = nil
			if s.batch.State == "running" {
				s.batch.State = "done"
				s.batch.EndedAt = time.Now().Format(time.RFC3339)
			}
		}
		s.mu.Unlock()
	}()

	for itemIndex, item := range items {
		if ctx.Err() != nil {
			s.finishQueue(batchID, "cancelled", "queue cancelled")
			return
		}
		s.updateQueueItemState(batchID, itemIndex, "running")

		for repeat := 0; repeat < item.RepeatCount; repeat++ {
			if ctx.Err() != nil {
				s.updateQueueItemState(batchID, itemIndex, "cancelled")
				s.finishQueue(batchID, "cancelled", "queue cancelled")
				return
			}
			payload := item.Payload
			payload.QueueID = batchID
			payload.QueueItemID = item.ID
			payload.QueuePosition = item.Position
			payload.RepeatCount = item.RepeatCount
			payload.RepeatIndex = repeat + 1
			payload.InterRunDelay = item.InterRunDelayMS

			run, httpErr := s.startQueueRun(ctx, payload)
			if httpErr != nil {
				s.updateQueueItemState(batchID, itemIndex, "error")
				s.finishQueue(batchID, "error", httpErr.message)
				return
			}
			s.attachQueueRun(batchID, itemIndex, run.ID)
			finalStatus := s.waitRunDone(ctx, run.ID)
			if ctx.Err() != nil {
				s.updateQueueItemState(batchID, itemIndex, "cancelled")
				s.finishQueue(batchID, "cancelled", "queue cancelled")
				return
			}
			if finalStatus == nil {
				s.updateQueueItemState(batchID, itemIndex, "error")
				s.finishQueue(batchID, "error", "run status missing")
				return
			}
			switch finalStatus.State {
			case "done":
			case "cancelled":
				s.updateQueueItemState(batchID, itemIndex, "cancelled")
				s.finishQueue(batchID, "cancelled", "queue cancelled")
				return
			default:
				s.updateQueueItemState(batchID, itemIndex, "error")
				errorText := strings.TrimSpace(finalStatus.Error)
				if errorText == "" {
					errorText = "run failed"
				}
				s.finishQueue(batchID, "error", errorText)
				return
			}

			hasMoreRuns := repeat < item.RepeatCount-1 || itemIndex < len(items)-1
			if hasMoreRuns && item.InterRunDelayMS > 0 {
				timer := time.NewTimer(time.Duration(item.InterRunDelayMS) * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					s.updateQueueItemState(batchID, itemIndex, "cancelled")
					s.finishQueue(batchID, "cancelled", "queue cancelled")
					return
				}
			}
		}

		s.updateQueueItemState(batchID, itemIndex, "done")
	}
	s.finishQueue(batchID, "done", "")
}

func (s *server) waitRunDone(ctx context.Context, id string) *runStatus {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		status := cloneRunStatus(s.runs[id])
		s.mu.Unlock()
		if status == nil || status.State != "running" {
			return status
		}
		select {
		case <-ctx.Done():
			return status
		case <-ticker.C:
		}
	}
}

func (s *server) updateQueueItemState(batchID string, itemIndex int, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batch == nil || s.batch.ID != batchID || itemIndex < 0 || itemIndex >= len(s.batch.Items) {
		return
	}
	s.batch.Items[itemIndex].State = state
}

func (s *server) attachQueueRun(batchID string, itemIndex int, runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batch == nil || s.batch.ID != batchID || itemIndex < 0 || itemIndex >= len(s.batch.Items) {
		return
	}
	s.batch.CurrentRunID = runID
	s.batch.Items[itemIndex].RunIDs = append(s.batch.Items[itemIndex].RunIDs, runID)
}

func (s *server) finishQueue(batchID string, state string, errorText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batch == nil || s.batch.ID != batchID || s.batch.State != "running" {
		return
	}
	s.batch.State = state
	s.batch.Error = errorText
	s.batch.EndedAt = time.Now().Format(time.RFC3339)
}

func (s *server) handlePeerCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	entries, _ := currentGraphsyncPeerEntriesHTTP(r.Context())
	resp := peerCountResponse{Count: len(entries)}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleSeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req seedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	seedAdjustNote := ""
	req.SegmentKB, req.ChunkKB, seedAdjustNote = normalizeSeedLayoutSizes(req.SegmentKB, req.ChunkKB)
	resp := seedResponse{}

	stopOutput, stopErr := s.stopGraphsyncPeers(r.Context())
	output := stopOutput
	if seedAdjustNote != "" {
		output += seedAdjustNote + "\n"
	}
	if stopErr != nil {
		output += fmt.Sprintf("WARNING: failed to stop GraphSync peers: %v\n", stopErr)
	}
	seedOutput, root, err := s.seedGraphsyncLayout(r.Context(), req)
	output += seedOutput
	if err != nil {
		startOutput, startErr := s.startGraphsyncPeers(r.Context())
		if startOutput != "" {
			output += startOutput
		}
		if startErr != nil {
			output += fmt.Sprintf("WARNING: failed to start GraphSync peers: %v\n", startErr)
		}
		resp.Error = err.Error()
		resp.Output = output
	} else {
		startOutput, startErr := s.startGraphsyncPeers(r.Context())
		if startOutput != "" {
			output += startOutput
		}
		if startErr != nil {
			output += fmt.Sprintf("WARNING: failed to start GraphSync peers: %v\n", startErr)
		}
		if peerOutput, peerErr := s.ensurePeersJSON(r.Context(), defaultPeerNetwork); peerOutput != "" || peerErr != nil {
			output += peerOutput
			if peerErr != nil {
				output += fmt.Sprintf("WARNING: failed to refresh peers.json: %v\n", peerErr)
			}
		}
		resp.Root = root
		resp.Output = output
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleSeedInfo(w http.ResponseWriter, r *http.Request) {
	data, err := readSeedMetadataFromManifest(seedManifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	durationSec, _ := probeVideoDuration("/videos/input.mp4")
	resp := seedInfoResponse{Root: data.Root, Size: data.Size, SegmentSize: data.SegmentSize, ChunkSize: data.ChunkSize, Segments: data.Segments, DurationS: durationSec}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleVideoInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	resp, err := probeVideoInfo("/videos/input.mp4")
	if err != nil {
		resp.Error = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func readSeedRoot() (string, error) {
	payload, err := os.ReadFile(seedManifestPath)
	if err != nil {
		return "", err
	}
	var data struct {
		Root string `json:"root"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return "", err
	}
	return data.Root, nil
}

func readBitswapSeedRoot() (string, error) {
	path := "/data/kubo_cid.txt"
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	root := strings.TrimSpace(string(payload))
	if root == "" {
		return "", fmt.Errorf("bitswap provider seed cid file is empty")
	}
	return root, nil
}

func readSeedMetadataFromManifest(seedPath string) (seedMetadata, error) {
	payload, err := os.ReadFile(seedPath)
	if err != nil {
		return seedMetadata{}, err
	}
	var data seedMetadata
	if err := json.Unmarshal(payload, &data); err != nil {
		return seedMetadata{}, err
	}
	return data, nil
}

type seedPlaybackModel struct {
	Segments    int
	SegmentSize int64
}

func readSeedPlaybackModel() (seedPlaybackModel, error) {
	data, err := readSeedMetadataFromManifest(seedManifestPath)
	if err != nil {
		return seedPlaybackModel{}, err
	}
	return seedPlaybackModel{Segments: data.Segments, SegmentSize: data.SegmentSize}, nil
}

func (s *server) handleBitswapSeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req bitswapSeedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	chunkKB, adjustNote := normalizeBitswapChunkKB(req.ChunkKB)
	resp := bitswapSeedResponse{}
	output := ""
	if adjustNote != "" {
		output += adjustNote + "\n"
	}
	seedOutput, root, err := s.seedBitswapProviders(r.Context(), chunkKB)
	output += seedOutput
	if err != nil {
		resp.Error = err.Error()
		resp.Output = output
	} else {
		resp.Root = root
		resp.Output = output
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleBitswapSeedInfo(w http.ResponseWriter, r *http.Request) {
	root, err := readBitswapSeedRoot()
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	resp := bitswapSeedInfoResponse{Root: root}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleUpdatePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	out, err := s.forceRefreshPeersJSON(r.Context())
	resp := updatePeersResponse{Output: string(out)}
	if err != nil {
		resp.Error = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) ensurePeersJSON(ctx context.Context, peerNetwork []peerNetworkCondition) (string, error) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	httpEntries, readinessOutput, err := s.waitForGraphsyncPeers(ctx)
	if err != nil {
		return readinessOutput, err
	}
	httpEntries = applyGraphsyncPeerLabels(httpEntries, peerNetwork)
	if peerConfigMatchesEntries("", httpEntries) {
		return readinessOutput + "peers.json is up to date (from status endpoints).\n", nil
	}
	if err := writePeersJSON(defaultPeersPath, httpEntries); err != nil {
		return readinessOutput, fmt.Errorf("failed to write peers.json from status endpoints: %w", err)
	}
	updated := loadPeerLabels("")
	if !peerLabelsMatchEntries(updated, httpEntries) {
		return readinessOutput, fmt.Errorf("peers.json mismatch after status-endpoint refresh")
	}
	return readinessOutput + "Refreshed peers.json from GraphSync status endpoints.\n", nil
}

type graphsyncPeerReadinessOptions struct {
	attempts        int
	retryDelay      time.Duration
	recoveryTimeout time.Duration
	recoveryPoll    time.Duration
	probe           func(context.Context) ([]graphsyncPeerEntry, string)
	restart         func(context.Context, []string) (string, error)
	sleep           func(context.Context, time.Duration) error
}

func (s *server) waitForGraphsyncPeers(ctx context.Context) ([]graphsyncPeerEntry, string, error) {
	return s.waitForGraphsyncPeersWithOptions(ctx, graphsyncPeerReadinessOptions{})
}

func (s *server) waitForGraphsyncPeersWithOptions(ctx context.Context, options graphsyncPeerReadinessOptions) ([]graphsyncPeerEntry, string, error) {
	if options.attempts <= 0 {
		options.attempts = graphsyncPeerReadinessAttempts
	}
	if options.retryDelay <= 0 {
		options.retryDelay = graphsyncPeerReadinessDelay
	}
	if options.recoveryTimeout <= 0 {
		options.recoveryTimeout = graphsyncPeerRecoveryTimeout
	}
	if options.recoveryPoll <= 0 {
		options.recoveryPoll = graphsyncPeerRecoveryPoll
	}
	if options.probe == nil {
		options.probe = currentGraphsyncPeerEntriesHTTP
	}
	if options.restart == nil {
		options.restart = s.restartGraphsyncPeerServices
	}
	if options.sleep == nil {
		options.sleep = sleepContext
	}

	var output strings.Builder
	var entries []graphsyncPeerEntry
	var missing []string
	for attempt := 1; attempt <= options.attempts; attempt++ {
		output.WriteString(fmt.Sprintf("GraphSync peer readiness attempt %d/%d\n", attempt, options.attempts))
		probedEntries, probeOutput := options.probe(ctx)
		output.WriteString(probeOutput)
		entries = probedEntries
		missing = missingGraphsyncPeerServices(entries)
		if len(missing) == 0 {
			return entries, output.String(), nil
		}
		output.WriteString(fmt.Sprintf("Missing GraphSync peer services: %s\n", strings.Join(missing, ", ")))
		if attempt < options.attempts {
			output.WriteString(fmt.Sprintf("Retrying GraphSync peer readiness in %s...\n", options.retryDelay))
			if err := options.sleep(ctx, options.retryDelay); err != nil {
				return entries, output.String(), err
			}
		}
	}

	output.WriteString(fmt.Sprintf("Restarting unavailable GraphSync peer containers: %s\n", strings.Join(missing, ", ")))
	restartOutput, err := options.restart(ctx, missing)
	output.WriteString(restartOutput)
	if err != nil {
		return entries, output.String(), fmt.Errorf("failed to restart unavailable graphsync peer containers (%s): %w", strings.Join(missing, ", "), err)
	}

	recoveryAttempts := int(options.recoveryTimeout / options.recoveryPoll)
	if recoveryAttempts < 1 {
		recoveryAttempts = 1
	}
	for attempt := 1; attempt <= recoveryAttempts; attempt++ {
		output.WriteString(fmt.Sprintf("Waiting %s before GraphSync peer recovery check %d/%d...\n", options.recoveryPoll, attempt, recoveryAttempts))
		if err := options.sleep(ctx, options.recoveryPoll); err != nil {
			return entries, output.String(), err
		}
		output.WriteString(fmt.Sprintf("GraphSync peer recovery check %d/%d\n", attempt, recoveryAttempts))
		probedEntries, probeOutput := options.probe(ctx)
		output.WriteString(probeOutput)
		entries = probedEntries
		missing = missingGraphsyncPeerServices(entries)
		if len(missing) == 0 {
			return entries, output.String(), nil
		}
		output.WriteString(fmt.Sprintf("Missing GraphSync peer services: %s\n", strings.Join(missing, ", ")))
	}

	return entries, output.String(), fmt.Errorf("graphsync peer readiness failed: missing services after retry and restart: %s", strings.Join(missing, ", "))
}

func missingGraphsyncPeerServices(entries []graphsyncPeerEntry) []string {
	available := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		service := strings.TrimSpace(entry.Service)
		peerID := strings.TrimSpace(entry.PeerID)
		if service == "" || peerID == "" {
			continue
		}
		available[service] = struct{}{}
	}
	missing := make([]string, 0)
	for _, service := range graphsyncPeerServices {
		if _, ok := available[service]; !ok {
			missing = append(missing, service)
		}
	}
	return missing
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *server) forceRefreshPeersJSON(ctx context.Context) (string, error) {
	s.peersMu.Lock()
	defer s.peersMu.Unlock()
	return runUpdatePeersScript(ctx)
}

func runUpdatePeersScript(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "/usr/local/bin/update-peers.sh")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func applyGraphsyncPeerLabels(entries []graphsyncPeerEntry, peerNetwork []peerNetworkCondition) []graphsyncPeerEntry {
	byService := peerNetworkByGraphsyncService(peerNetwork)
	out := make([]graphsyncPeerEntry, len(entries))
	for i, entry := range entries {
		out[i] = entry
		if condition, ok := byService[entry.Service]; ok && strings.TrimSpace(condition.Label) != "" {
			out[i].Label = strings.TrimSpace(condition.Label)
		}
	}
	return out
}

func peerLabelsMatchEntries(labels map[string]string, entries []graphsyncPeerEntry) bool {
	if len(labels) == 0 || len(entries) == 0 || len(labels) != len(entries) {
		return false
	}
	for _, entry := range entries {
		id := strings.TrimSpace(entry.PeerID)
		if id == "" || labels[id] != strings.TrimSpace(entry.Label) {
			return false
		}
	}
	return true
}

func peerConfigMatchesEntries(peersPath string, entries []graphsyncPeerEntry) bool {
	peerIDs := make([]string, 0, len(entries))
	expectedLabels := make(map[string]string, len(entries))
	for _, entry := range entries {
		id := strings.TrimSpace(entry.PeerID)
		if id == "" {
			return false
		}
		peerIDs = append(peerIDs, id)
		expectedLabels[id] = strings.TrimSpace(entry.Label)
	}
	if !peerConfigMatches(peersPath, peerIDs) {
		return false
	}
	return peerLabelsMatchEntries(loadPeerLabels(peersPath), entries) && len(expectedLabels) == len(entries)
}

func peerConfigMatches(peersPath string, peerIDs []string) bool {
	path := peersPath
	if path == "" {
		path = defaultPeersPath
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var data struct {
		Peers []map[string]any `json:"peers"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return false
	}
	if len(data.Peers) == 0 || len(data.Peers) != len(peerIDs) {
		return false
	}
	expectedIDs := make(map[string]struct{}, len(peerIDs))
	for _, id := range peerIDs {
		expectedIDs[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(data.Peers))
	for _, peer := range data.Peers {
		addr, _ := peer["addr"].(string)
		parts := strings.Split(addr, "/p2p/")
		if len(parts) != 2 {
			return false
		}
		id := parts[1]
		if _, ok := expectedIDs[id]; !ok {
			return false
		}
		seen[id] = struct{}{}
		if _, ok := peer["maxConcurrent"]; ok {
			return false
		}
		if _, ok := peer["bandwidthMbps"]; ok {
			return false
		}
		if _, ok := peer["latencyMs"]; ok {
			return false
		}
	}
	return len(seen) == len(expectedIDs)
}

func (s *server) handleDownloadRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if _, err := os.Stat(runsRootDir); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=bench-runs.zip")
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()
	_ = zipRunFiles(zipWriter, runsRootDir, func(rel string) bool {
		return strings.HasSuffix(rel, ".json") || strings.HasSuffix(rel, ".log")
	})
}

func (s *server) handleGenerateTimelines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	resp := generateTimelineVideos(r.Context(), runsRootDir, filepath.Join(runsRootDir, "timelines"), seedManifestPath)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleDownloadTimelines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	timelineDir := filepath.Join(runsRootDir, "timelines")
	if _, err := os.Stat(timelineDir); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=bench-timelines.zip")
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()
	_ = zipRunFiles(zipWriter, timelineDir, func(rel string) bool {
		return strings.HasSuffix(rel, ".mp4")
	})
}

func (s *server) handleClearRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	entries, err := os.ReadDir(runsRootDir)
	if err != nil {
		if os.IsNotExist(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"deleted": 0})
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read runs dir: %v", err))
		return
	}
	deleted := 0
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(runsRootDir, entry.Name())); err == nil {
			deleted++
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"deleted": deleted})
}

func zipRunFiles(zipWriter *zip.Writer, root string, include func(string) bool) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !include(rel) {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		fileWriter, err := zipWriter.Create(rel)
		if err != nil {
			return nil
		}
		_, _ = fileWriter.Write(content)
		return nil
	})
}

func (s *server) handleDownloadPlots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.plotsAPI == "" {
		writeError(w, http.StatusServiceUnavailable, "plots service is not configured")
		return
	}

	endpoint := strings.TrimRight(s.plotsAPI, "/") + "/plots/download"
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to build plots request: %v", err))
		return
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("plots service request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		msg := strings.TrimSpace(string(payload))
		if msg == "" {
			msg = resp.Status
		}
		writeError(w, http.StatusBadGateway, fmt.Sprintf("plots service error: %s", msg))
		return
	}

	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition == "" {
		contentDisposition = "attachment; filename=bench-plots.zip"
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", contentDisposition)
	_, _ = io.Copy(w, resp.Body)
}

func (s *server) execute(id string, req runRequest) {
	defer func() {
		s.mu.Lock()
		status := s.runs[id]
		if status != nil && status.EndedAt == "" {
			status.EndedAt = time.Now().Format(time.RFC3339)
			if status.State == "running" {
				status.State = "error"
				if strings.TrimSpace(status.Error) == "" {
					status.Error = "run ended unexpectedly"
				}
			}
		}
		var output string
		if status != nil {
			output = status.Output
		}
		s.mu.Unlock()
		if status == nil {
			return
		}
		s.saveRun(id, req, status, output)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.mu.Lock()
	s.runCancels[id] = cancel
	s.cancelRequest[id] = false
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.runCancels, id)
		delete(s.cancelRequest, id)
		s.mu.Unlock()
	}()

	if req.Mode == "graphsync" {
		graphsyncStore := graphsyncStoreForRun(id)
		defer os.RemoveAll(graphsyncStore)
		s.mu.Lock()
		status := s.runs[id]
		status.Output += "Clearing GraphSync cache (always-clear policy)...\n"
		s.mu.Unlock()
		if err := clearGraphsyncStore(graphsyncStore); err != nil {
			s.mu.Lock()
			status := s.runs[id]
			if errors.Is(err, context.Canceled) {
				status.State = "cancelled"
				status.Error = "run cancelled"
			} else {
				status.State = "error"
				status.Error = fmt.Sprintf("failed to clear cache: %v", err)
			}
			status.EndedAt = time.Now().Format(time.RFC3339)
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		status = s.runs[id]
		status.Output += "GraphSync cache cleared.\n"
		s.mu.Unlock()
	}

	s.mu.Lock()
	status := s.runs[id]
	status.Output += "Applying peer network conditions...\n"
	s.mu.Unlock()
	reshapeOut, reshapeErr := s.reshapeNetwork(ctx, req.PeerNetwork)
	s.mu.Lock()
	status = s.runs[id]
	if reshapeOut != "" {
		status.Output += reshapeOut
	}
	if reshapeErr != nil {
		status.Output += fmt.Sprintf("WARNING: network reshape failed: %v\n", reshapeErr)
	} else {
		status.Output += "Peer network conditions applied successfully.\n"
	}
	s.mu.Unlock()

	var cmd *exec.Cmd
	s.mu.Lock()
	status = s.runs[id]
	status.Output += "Starting run...\n"
	s.mu.Unlock()
	if req.Mode == "graphsync" {
		if seedRoot, err := readSeedRoot(); err == nil && seedRoot != "" && seedRoot != req.Root {
			s.mu.Lock()
			status := s.runs[id]
			status.Output += fmt.Sprintf("WARNING: root overridden from seed manifest: %s\n", seedRoot)
			s.mu.Unlock()
			req.Root = seedRoot
		}
	}
	if req.Mode == "bitswap" {
		if bitswapRoot, err := readBitswapSeedRoot(); err == nil && bitswapRoot != "" && bitswapRoot != req.Root {
			s.mu.Lock()
			status := s.runs[id]
			status.Output += fmt.Sprintf("WARNING: bitswap root overridden from provider seed CID: %s\n", bitswapRoot)
			s.mu.Unlock()
			req.Root = bitswapRoot
		}
	}
	switch req.Mode {
	case "graphsync":
		cmd = s.buildGraphsyncCommand(ctx, req, graphsyncStoreForRun(id))
	case "bitswap":
		s.mu.Lock()
		status := s.runs[id]
		status.Output += "Bitswap backend: Boxo Bitswap trace baseline\n"
		s.mu.Unlock()
		cmd = s.buildBoxoBitswapCommand(ctx, req)
	default:
		cmd = s.buildGraphsyncCommand(ctx, req, graphsyncStoreForRun(id))
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.mu.Lock()
		status := s.runs[id]
		status.State = "error"
		status.Error = err.Error()
		status.EndedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.mu.Lock()
		status := s.runs[id]
		status.State = "error"
		status.Error = err.Error()
		status.EndedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		return
	}
	if err := cmd.Start(); err != nil {
		s.mu.Lock()
		status := s.runs[id]
		status.State = "error"
		status.Error = err.Error()
		status.EndedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		return
	}

	var wg sync.WaitGroup
	readStream := func(reader io.Reader) {
		defer wg.Done()
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			s.mu.Lock()
			status := s.runs[id]
			status.Output += line + "\n"
			s.mu.Unlock()
		}
	}
	wg.Add(2)
	go readStream(stdout)
	go readStream(stderr)
	wg.Wait()

	err = cmd.Wait()
	s.mu.Lock()
	status = s.runs[id]
	cancelled := s.cancelRequest[id] || errors.Is(err, context.Canceled)
	status.EndedAt = time.Now().Format(time.RFC3339)
	if cancelled {
		status.State = "cancelled"
		status.Error = "run cancelled"
	} else if err != nil {
		status.State = "error"
		status.Error = err.Error()
	} else {
		status.State = "done"
	}
	s.mu.Unlock()

	if !samePeerNetwork(req.PeerNetwork, defaultPeerNetwork) {
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer restoreCancel()
		restoreOut, restoreErr := s.reshapeNetwork(restoreCtx, defaultPeerNetwork)
		s.mu.Lock()
		status = s.runs[id]
		status.Output += "Restoring default peer network conditions...\n"
		if restoreOut != "" {
			status.Output += restoreOut
		}
		if restoreErr != nil {
			status.Output += fmt.Sprintf("WARNING: failed to restore default network: %v\n", restoreErr)
		} else {
			status.Output += "Peer network conditions restored to default.\n"
		}
		s.mu.Unlock()
	}

}

func (s *server) buildGraphsyncCommand(ctx context.Context, req runRequest, storePath string) *exec.Cmd {
	args := []string{req.gsBinPath(s.gsBin), "--root", req.Root, "--store", storePath}
	if req.Prefetch > 0 || req.Prefetch == -1 {
		args = append(args, "--prefetch-segments", fmt.Sprintf("%d", req.Prefetch))
	}
	if req.Workers > 0 {
		args = append(args, "--prefetch-workers", fmt.Sprintf("%d", req.Workers))
	}
	if req.PlaybackMS >= 0 {
		args = append(args, "--playback-ms", fmt.Sprintf("%d", req.PlaybackMS))
	}
	if req.UrgentWindow >= 0 {
		args = append(args, "--urgent-window", fmt.Sprintf("%d", req.UrgentWindow))
	}
	if req.EMAAlpha > 0 {
		args = append(args, "--ema-alpha", fmt.Sprintf("%.3f", req.EMAAlpha))
	}
	if req.RaceFanout > 0 || req.RaceFanout == -1 {
		args = append(args, "--race-fanout", fmt.Sprintf("%d", req.RaceFanout))
	}
	args = append(args, "--discovery-fanout", fmt.Sprintf("%d", req.DiscoveryFanout))
	args = append(args, "--peers", defaultPeersPath, "--use-dht=false")
	if req.LogPeers {
		args = append(args, "--log-peers")
	}
	return exec.CommandContext(ctx, args[0], args[1:]...)
}

func (s *server) buildBoxoBitswapCommand(ctx context.Context, req runRequest) *exec.Cmd {
	args := []string{s.bitswapBin, "--root", req.Root}
	args = append(args, kuboProviderArgs(req.PeerNetwork)...)
	if req.PlaybackMS > 0 {
		args = append(args, "--playback-ms", fmt.Sprintf("%d", req.PlaybackMS))
		if playbackModel, err := readSeedPlaybackModel(); err == nil {
			if playbackModel.Segments > 0 && playbackModel.SegmentSize > 0 {
				args = append(args,
					"--virtual-segments", fmt.Sprintf("%d", playbackModel.Segments),
					"--virtual-segment-bytes", fmt.Sprintf("%d", playbackModel.SegmentSize),
				)
			}
		}
	}
	args = append(args, "--index-timeout-ms", "15000")
	args = append(args, "--format", "json")
	return exec.CommandContext(ctx, args[0], args[1:]...)
}

func kuboProviderArgs(peerNetwork []peerNetworkCondition) []string {
	return kuboProviderArgsForHosts(peerNetwork, bitswapProviderHosts(), kuboPeerID)
}

func kuboProviderArgsForHosts(peerNetwork []peerNetworkCondition, hosts []string, peerIDForHost func(string) (string, error)) []string {
	args := make([]string, 0, len(hosts)*2)
	for _, host := range hosts {
		if host == "" {
			continue
		}
		peerID, err := peerIDForHost(host)
		if err != nil || strings.TrimSpace(peerID) == "" {
			continue
		}
		addr := fmt.Sprintf("/dns4/%s/udp/4001/quic-v1/p2p/%s", host, strings.TrimSpace(peerID))
		label := kuboProviderLabel(host, peerNetwork)
		args = append(args, "--provider", fmt.Sprintf("%s,%s,%s", addr, label, host))
	}
	return args
}

func kuboProviderLabel(host string, peerNetwork []peerNetworkCondition) string {
	labels := peerNetworkByKuboHost(peerNetwork)
	if condition, ok := labels[host]; ok && strings.TrimSpace(condition.Label) != "" {
		return strings.TrimSpace(condition.Label)
	}
	return bitswapHostLabel(host)
}

var seedSegmentSizeChoicesKB = []int{256, 512, 1024, 2048, 4096, 8192}
var seedChunkSizeChoicesKB = []int{256, 512, 1024}
var bitswapChunkSizeChoicesKB = []int{256, 512, 1024}

const bitswapDefaultChunkKB = 1024

func normalizeSeedLayoutSizes(segmentKB int, chunkKB int) (int, int, string) {
	origSegment := segmentKB
	origChunk := chunkKB
	segmentKB = clampSegmentKB(segmentKB)
	chunkKB = clampChunkKB(chunkKB)
	if chunkKB > segmentKB || segmentKB%chunkKB != 0 {
		chunkKB = bestSeedChunkKB(segmentKB)
	}
	notes := make([]string, 0, 2)
	if segmentKB != origSegment {
		notes = append(notes, fmt.Sprintf("Adjusted segment KB from %d to %d.", origSegment, segmentKB))
	}
	if chunkKB != origChunk {
		notes = append(notes, fmt.Sprintf("Adjusted chunk KB from %d to %d (segment must be a multiple of chunk).", origChunk, chunkKB))
	}
	return segmentKB, chunkKB, strings.Join(notes, " ")
}

func normalizeBitswapChunkKB(chunkKB int) (int, string) {
	origChunk := chunkKB
	if chunkKB <= 0 {
		chunkKB = bitswapDefaultChunkKB
	} else if chunkKB > bitswapDefaultChunkKB {
		chunkKB = bitswapDefaultChunkKB
	} else {
		chunkKB = nearestSeedChoiceKB(chunkKB, bitswapChunkSizeChoicesKB)
	}
	if chunkKB != origChunk {
		return chunkKB, fmt.Sprintf("Adjusted Bitswap chunk KB from %d to %d.", origChunk, chunkKB)
	}
	return chunkKB, ""
}

func bestSeedChunkKB(segmentKB int) int {
	for i := len(seedChunkSizeChoicesKB) - 1; i >= 0; i-- {
		candidate := seedChunkSizeChoicesKB[i]
		if candidate <= segmentKB && segmentKB%candidate == 0 {
			return candidate
		}
	}
	return 256
}

func nearestSeedChoiceKB(value int, choices []int) int {
	if len(choices) == 0 {
		return value
	}
	best := choices[0]
	bestDistance := absInt(value - best)
	for _, choice := range choices[1:] {
		distance := absInt(value - choice)
		if distance < bestDistance {
			best = choice
			bestDistance = distance
		}
	}
	return best
}

func clampChunkKB(value int) int {
	if value <= 0 {
		return 256
	}
	if value > 1024 {
		value = 1024
	}
	return nearestSeedChoiceKB(value, seedChunkSizeChoicesKB)
}

func clampSegmentKB(value int) int {
	if value <= 0 {
		return 1024
	}
	return nearestSeedChoiceKB(value, seedSegmentSizeChoicesKB)
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func kuboAPIURL(host string) string {
	trimmed := strings.TrimSpace(host)
	trimmed = strings.TrimPrefix(trimmed, "http://")
	trimmed = strings.TrimPrefix(trimmed, "https://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	trimmed = strings.Split(trimmed, "/")[0]
	return "http://" + trimmed + ":5001"
}

func (s *server) autoSeedGraphsyncLayout(ctx context.Context) {
	if _, err := os.Stat(seedManifestPath); err == nil {
		log.Printf("GraphSync auto-seed skipped; seed manifest already exists at %s", seedManifestPath)
		return
	} else if !os.IsNotExist(err) {
		log.Printf("GraphSync auto-seed skipped; failed to inspect seed manifest %s: %v", seedManifestPath, err)
		return
	}
	if _, err := os.Stat("/videos/input.mp4"); err != nil {
		log.Printf("GraphSync auto-seed skipped; missing input video at /videos/input.mp4: %v", err)
		return
	}

	log.Printf("GraphSync auto-seed starting; manifest missing at %s", seedManifestPath)
	output, root, err := s.seedGraphsyncLayout(ctx, seedRequest{SegmentKB: 1024, ChunkKB: 256})
	if output != "" {
		log.Printf("GraphSync auto-seed output:\n%s", strings.TrimSpace(output))
	}
	if err != nil {
		log.Printf("GraphSync auto-seed failed: %v", err)
		return
	}
	log.Printf("GraphSync auto-seed completed; Layout CID: %s", root)
}

func (s *server) seedGraphsyncLayout(ctx context.Context, req seedRequest) (string, string, error) {
	blocksDir := filepath.Join(seedOutputDir, "blocks")
	if _, err := os.Stat("/videos/input.mp4"); err != nil {
		return "", "", fmt.Errorf("missing input video at /videos/input.mp4 (mount ./videos and add input.mp4): %w", err)
	}
	if err := os.Remove(seedManifestPath); err != nil && !os.IsNotExist(err) {
		return "", "", fmt.Errorf("failed to remove old seed manifest: %w", err)
	}
	if err := os.Remove(seedManifestPath + ".tmp"); err != nil && !os.IsNotExist(err) {
		return "", "", fmt.Errorf("failed to remove old temporary seed manifest: %w", err)
	}
	if err := os.RemoveAll(blocksDir); err != nil {
		return "", "", fmt.Errorf("failed to clear seed blocks: %w", err)
	}
	if err := os.MkdirAll(blocksDir, 0o755); err != nil {
		return "", "", fmt.Errorf("failed to create seed blocks dir: %w", err)
	}
	seedArgs := []string{s.seedBin, "--file", "/videos/input.mp4", "--chunk-kb", fmt.Sprintf("%d", req.ChunkKB), "--segment-kb", fmt.Sprintf("%d", req.SegmentKB), "--out", seedOutputDir}
	output := fmt.Sprintf("Seed command: %s\n", strings.Join(seedArgs, " "))
	seedCmd := exec.CommandContext(ctx, seedArgs[0], seedArgs[1:]...)
	seedOut, err := seedCmd.CombinedOutput()
	output += string(seedOut)
	if err != nil {
		return output, "", err
	}
	root := parseRootCID(output)
	if root == "" {
		return output, "", fmt.Errorf("root cid not found in seed output")
	}

	output += fmt.Sprintf("GraphSync seed dir: %s\n", seedOutputDir)
	output += "GraphSync peers read POSL blocks directly from the shared seed directory; Kubo import is skipped.\n"
	return output, root, nil
}

func (s *server) seedBitswapProviders(ctx context.Context, chunkKB int) (string, string, error) {
	targetHosts := bitswapProviderHosts()
	chunkBytes := chunkKB * 1024
	output := fmt.Sprintf("Bitswap chunk size: %dKB (%d bytes)\n", chunkKB, chunkBytes)
	canonicalRoot := ""
	for _, targetHost := range targetHosts {
		addURL := kuboAddURL(targetHost, chunkBytes)
		output += fmt.Sprintf("Bitswap provider add file on %s: %s\n", targetHost, addURL)
		addOut, root, err := callKuboAddFile(ctx, addURL, "/videos/input.mp4")
		output += addOut
		if err != nil {
			return output, "", fmt.Errorf("bitswap provider add failed on %s: %w", targetHost, err)
		}
		if root == "" {
			return output, "", fmt.Errorf("root cid not found in bitswap provider add output for %s", targetHost)
		}
		if canonicalRoot == "" {
			canonicalRoot = root
			continue
		}
		if root != canonicalRoot {
			return output, "", fmt.Errorf("bitswap provider add produced mismatched cid on %s: got %s expected %s", targetHost, root, canonicalRoot)
		}
	}

	if canonicalRoot == "" {
		return output, "", fmt.Errorf("bitswap provider add did not produce a cid")
	}
	if err := os.WriteFile("/data/kubo_cid.txt", []byte(canonicalRoot+"\n"), 0o644); err != nil {
		return output, canonicalRoot, err
	}
	if err := writeBitswapSeedMetadata("/data/kubo_seed.json", canonicalRoot, chunkKB, chunkBytes, "/videos/input.mp4"); err != nil {
		return output, canonicalRoot, err
	}
	return output, canonicalRoot, nil
}

func kuboAddURL(host string, chunkBytes int) string {
	return fmt.Sprintf("http://%s:5001/api/v0/add?pin=true&cid-version=1&chunker=size-%d", host, chunkBytes)
}

func writeBitswapSeedMetadata(path string, root string, chunkKB int, chunkBytes int, videoPath string) error {
	info, err := os.Stat(videoPath)
	if err != nil {
		return err
	}
	metadata := bitswapSeedMetadata{
		Root:       root,
		ChunkKB:    chunkKB,
		ChunkSize:  int64(chunkBytes),
		CIDVersion: 1,
		Chunker:    fmt.Sprintf("size-%d", chunkBytes),
		VideoPath:  videoPath,
		VideoSize:  info.Size(),
		VideoMTime: info.ModTime().Unix(),
	}
	payload, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return os.WriteFile(path, payload, 0o644)
}

func (s *server) restartGraphsyncPeerServices(ctx context.Context, services []string) (string, error) {
	return s.runGraphsyncPeerServicesCommand(ctx, "restart", services, true)
}

func (s *server) stopGraphsyncPeers(ctx context.Context) (string, error) {
	return s.runGraphsyncPeersCommand(ctx, "stop")
}

func (s *server) startGraphsyncPeers(ctx context.Context) (string, error) {
	return s.runGraphsyncPeersCommand(ctx, "start")
}

func (s *server) runGraphsyncPeersCommand(ctx context.Context, action string) (string, error) {
	return s.runGraphsyncPeerServicesCommand(ctx, action, graphsyncPeerServices, false)
}

func (s *server) runGraphsyncPeerServicesCommand(ctx context.Context, action string, services []string, requireContainers bool) (string, error) {
	containers, resolveOutput, err := findGraphsyncPeerContainersForServices(ctx, services)
	if err != nil {
		if isDockerClientTooOldError(resolveOutput + "\n" + err.Error()) {
			return resolveOutput + "WARNING: docker client inside UI container is older than daemon API; skipping GraphSync peer " + action + " command.\n", nil
		}
		return resolveOutput, err
	}
	if len(containers) == 0 {
		output := resolveOutput + "No GraphSync peer containers found by compose labels.\n"
		if requireContainers {
			return output, fmt.Errorf("no graphsync peer containers found for services: %s", strings.Join(services, ", "))
		}
		return output, nil
	}
	args := append([]string{action}, containers...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	combined := resolveOutput + string(output)
	if err != nil {
		if isDockerClientTooOldError(combined + "\n" + err.Error()) {
			return combined + "WARNING: docker client inside UI container is older than daemon API; skipping GraphSync peer " + action + " command.\n", nil
		}
		return combined, err
	}
	return combined, nil
}

func findGraphsyncPeerContainersForServices(ctx context.Context, services []string) ([]string, string, error) {
	projectName := detectComposeProject(ctx)
	var output strings.Builder
	if projectName != "" {
		output.WriteString(fmt.Sprintf("Compose project: %s\n", projectName))
	}
	containers := make([]string, 0, len(services))
	for _, service := range services {
		args := []string{"ps", "-a", "--filter", "label=com.docker.compose.service=" + service}
		if projectName != "" {
			args = append(args, "--filter", "label=com.docker.compose.project="+projectName)
		}
		args = append(args, "--format", "{{.Names}}")
		cmd := exec.CommandContext(ctx, "docker", args...)
		listing, err := cmd.CombinedOutput()
		if err != nil {
			return nil, output.String() + string(listing), err
		}
		names := strings.TrimSpace(string(listing))
		if names == "" {
			output.WriteString(fmt.Sprintf("No container found for service %s\n", service))
			continue
		}
		for _, line := range strings.Split(names, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			containers = append(containers, line)
		}
	}
	return containers, output.String(), nil
}

func findPeerSlotContainers(ctx context.Context) (map[string][]string, string, error) {
	projectName := detectComposeProject(ctx)
	var out strings.Builder
	resolve := func(service string) ([]string, error) {
		args := []string{"ps", "-a", "--filter", "label=com.docker.compose.service=" + service}
		if projectName != "" {
			args = append(args, "--filter", "label=com.docker.compose.project="+projectName)
		}
		args = append(args, "--format", "{{.Names}}")
		cmd := exec.CommandContext(ctx, "docker", args...)
		listing, cmdErr := cmd.CombinedOutput()
		if cmdErr != nil {
			return nil, cmdErr
		}
		containers := make([]string, 0)
		for _, line := range strings.Split(strings.TrimSpace(string(listing)), "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				containers = append(containers, line)
			}
		}
		return containers, nil
	}

	containers := make(map[string][]string, len(peerSlots))
	total := 0
	for _, slot := range peerSlots {
		for _, service := range []string{slot.Graphsync, slot.Kubo} {
			resolved, err := resolve(service)
			if err != nil {
				return containers, out.String(), err
			}
			containers[slot.Slot] = append(containers[slot.Slot], resolved...)
			total += len(resolved)
		}
	}
	out.WriteString(fmt.Sprintf("Found %d peer containers across %d logical peer slots\n", total, len(peerSlots)))
	return containers, out.String(), nil
}

func formatLossPct(value float64) string {
	text := strconv.FormatFloat(value, 'f', 3, 64)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if text == "" {
		return "0"
	}
	return text
}

func netemArgs(condition peerNetworkCondition) []string {
	args := []string{
		"tc", "qdisc", "add", "dev", "eth0", "root", "netem",
		"delay", fmt.Sprintf("%dms", condition.LatencyMS), fmt.Sprintf("%dms", condition.JitterMS),
	}
	if condition.LossPct > 0 {
		args = append(args, "loss", formatLossPct(condition.LossPct)+"%")
	}
	args = append(args, "rate", fmt.Sprintf("%dmbit", condition.RateMbit))
	return args
}

func reshapeTCOnContainer(ctx context.Context, container string, condition peerNetworkCondition) error {

	delCmd := exec.CommandContext(ctx, "docker", "exec", container, "tc", "qdisc", "del", "dev", "eth0", "root")
	_ = delCmd.Run()

	args := append([]string{"exec", container}, netemArgs(condition)...)
	addCmd := exec.CommandContext(ctx, "docker", args...)
	output, err := addCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("tc on %s: %v: %s", container, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (s *server) reshapeNetwork(ctx context.Context, conditions []peerNetworkCondition) (string, error) {
	normalized, err := normalizePeerNetwork(conditions, "default")
	if err != nil {
		return "", err
	}
	containersBySlot, discoverOut, err := findPeerSlotContainers(ctx)
	if err != nil {
		return discoverOut, fmt.Errorf("container discovery failed: %w", err)
	}

	var out strings.Builder
	out.WriteString(discoverOut)

	var errors []string
	for _, condition := range normalized {
		containers := containersBySlot[condition.Slot]
		if len(containers) == 0 {
			out.WriteString(fmt.Sprintf("  WARN %s (%s): no containers found\n", condition.Slot, condition.Label))
			continue
		}
		for _, c := range containers {
			if err := reshapeTCOnContainer(ctx, c, condition); err != nil {
				errors = append(errors, err.Error())
				out.WriteString(fmt.Sprintf("  FAIL %s: %v\n", c, err))
			} else {
				lossText := ""
				if condition.LossPct > 0 {
					lossText = fmt.Sprintf(", loss %s%%", formatLossPct(condition.LossPct))
				}
				out.WriteString(fmt.Sprintf("  OK %s [%s/%s]: %d mbit, %d ms (+/-%d ms)%s\n", c, condition.Slot, condition.Label, condition.RateMbit, condition.LatencyMS, condition.JitterMS, lossText))
			}
		}
	}

	if len(errors) > 0 {
		return out.String(), fmt.Errorf("%d container(s) failed reshaping", len(errors))
	}
	return out.String(), nil
}

func (s *server) handleReshape(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	profile := r.URL.Query().Get("profile")
	if profile == "" {
		profile = "default"
	}
	conditions, err := peerNetworkFromLegacyProfile(profile)
	if err == nil && r.Body != nil {
		var body struct {
			PeerNetwork []peerNetworkCondition `json:"peerNetwork"`
		}
		if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr == nil && len(body.PeerNetwork) > 0 {
			conditions, err = normalizePeerNetwork(body.PeerNetwork, profile)
		}
	}
	output := ""
	if err == nil {
		output, err = s.reshapeNetwork(r.Context(), conditions)
	}
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]interface{}{
		"profile":     profile,
		"peerNetwork": conditions,
		"output":      output,
		"ok":          err == nil,
	}
	if err != nil {
		resp["error"] = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func currentGraphsyncPeerEntriesHTTP(ctx context.Context) ([]graphsyncPeerEntry, string) {
	client := &http.Client{Timeout: 2 * time.Second}
	seen := make(map[string]struct{})
	entries := make([]graphsyncPeerEntry, 0, len(graphsyncPeerServices))
	var output strings.Builder
	output.WriteString("GraphSync peer ID discovery via HTTP status endpoints:\n")
	for _, service := range graphsyncPeerServices {
		url := fmt.Sprintf("http://%s:7001/id", service)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			output.WriteString(fmt.Sprintf("- %s request error: %v\n", service, err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			output.WriteString(fmt.Sprintf("- %s unavailable (%v)\n", service, err))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			output.WriteString(fmt.Sprintf("- %s status %s\n", service, resp.Status))
			continue
		}
		var payload struct {
			PeerID string `json:"peerId"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			output.WriteString(fmt.Sprintf("- %s invalid payload\n", service))
			continue
		}
		id := strings.TrimSpace(payload.PeerID)
		if id == "" {
			output.WriteString(fmt.Sprintf("- %s missing peerId\n", service))
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		label := "slow"
		if slot, ok := peerSlotForGraphsyncService(service); ok {
			label = slot.DefaultLabel
		}
		entries = append(entries, graphsyncPeerEntry{Service: service, PeerID: id, Label: label})
		output.WriteString(fmt.Sprintf("- %s peerId=%s\n", service, id))
	}
	if len(entries) == 0 {
		output.WriteString("No peer IDs discovered via HTTP endpoints.\n")
	}
	return entries, output.String()
}

func detectComposeProject(ctx context.Context) string {
	if value := strings.TrimSpace(os.Getenv("COMPOSE_PROJECT_NAME")); value != "" {
		return value
	}
	self := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if self == "" {
		return ""
	}
	cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"com.docker.compose.project\"}}", self)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func newMultipartFileRequest(ctx context.Context, apiURL, filePath string) (*http.Request, <-chan error, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	reader, writerPipe := io.Pipe()
	writer := multipart.NewWriter(writerPipe)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, reader)
	if err != nil {
		_ = reader.Close()
		_ = writerPipe.Close()
		_ = file.Close()
		return nil, nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	writeErrCh := make(chan error, 1)
	go func() {
		defer close(writeErrCh)
		defer file.Close()

		part, err := writer.CreateFormFile("file", filepath.Base(filePath))
		if err != nil {
			_ = writerPipe.CloseWithError(err)
			writeErrCh <- err
			return
		}
		if _, err := io.Copy(part, file); err != nil {
			_ = writerPipe.CloseWithError(err)
			writeErrCh <- err
			return
		}
		if err := writer.Close(); err != nil {
			_ = writerPipe.CloseWithError(err)
			writeErrCh <- err
			return
		}
		if err := writerPipe.Close(); err != nil {
			writeErrCh <- err
			return
		}
		writeErrCh <- nil
	}()

	return req, writeErrCh, nil
}

func callKuboAddFile(ctx context.Context, apiURL, filePath string) (string, string, error) {
	req, writeErrCh, err := newMultipartFileRequest(ctx, apiURL, filePath)
	if err != nil {
		return "", "", err
	}

	client := &http.Client{Timeout: 3 * time.Minute}
	resp, err := client.Do(req)
	writeErr := <-writeErrCh
	if err != nil {
		if writeErr != nil {
			return "", "", writeErr
		}
		return "", "", err
	}
	defer resp.Body.Close()
	if writeErr != nil {
		return "", "", writeErr
	}
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return string(payload), "", fmt.Errorf("kubo add error: %s %s", resp.Status, string(payload))
	}
	root := ""
	lines := strings.Split(string(payload), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var data struct {
			Hash string `json:"Hash"`
		}
		if err := json.Unmarshal([]byte(line), &data); err == nil {
			if data.Hash != "" {
				root = data.Hash
			}
		}
	}
	return string(payload), root, nil
}

func parseRootCID(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "RootCID:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "RootCID:"))
		}
	}
	return ""
}

func probeVideoDuration(path string) (float64, error) {
	cmd := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(string(output))
	if value == "" {
		return 0, fmt.Errorf("ffprobe returned empty duration")
	}
	return strconv.ParseFloat(value, 64)
}

func probeVideoInfo(path string) (videoInfoResponse, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return videoInfoResponse{Exists: false}, nil
		}
		return videoInfoResponse{Exists: false}, err
	}
	cmd := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,avg_frame_rate:format=duration,bit_rate", "-of", "json", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return videoInfoResponse{Exists: true}, fmt.Errorf("ffprobe video info failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return parseVideoInfoProbeOutput(output, stat.Size())
}

func parseVideoInfoProbeOutput(payload []byte, fileSize int64) (videoInfoResponse, error) {
	var raw struct {
		Streams []struct {
			Width        int    `json:"width"`
			Height       int    `json:"height"`
			AvgFrameRate string `json:"avg_frame_rate"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
			BitRate  string `json:"bit_rate"`
		} `json:"format"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return videoInfoResponse{Exists: true}, err
	}
	durationSec, _ := strconv.ParseFloat(strings.TrimSpace(raw.Format.Duration), 64)
	bitrateBitsPerSec, _ := strconv.ParseFloat(strings.TrimSpace(raw.Format.BitRate), 64)
	if bitrateBitsPerSec <= 0 && fileSize > 0 && durationSec > 0 {
		bitrateBitsPerSec = float64(fileSize) * 8 / durationSec
	}
	quality := ""
	if len(raw.Streams) > 0 {
		quality = videoQualityLabel(raw.Streams[0].Width, raw.Streams[0].Height, raw.Streams[0].AvgFrameRate)
	}
	return videoInfoResponse{
		Exists:            true,
		DurationSec:       durationSec,
		Quality:           quality,
		BitrateMbitPerSec: bitrateBitsPerSec / 1_000_000,
		SizeMB:            float64(fileSize) / 1_000_000,
	}, nil
}

func videoQualityLabel(width int, height int, avgFrameRate string) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	label := fmt.Sprintf("%dx%d", width, height)
	fps := parseFrameRate(avgFrameRate)
	if fps > 0 {
		label += fmt.Sprintf(" @ %.2f fps", fps)
	}
	return label
}

func parseFrameRate(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" || value == "0/0" {
		return 0
	}
	parts := strings.Split(value, "/")
	if len(parts) == 2 {
		num, errNum := strconv.ParseFloat(parts[0], 64)
		den, errDen := strconv.ParseFloat(parts[1], 64)
		if errNum != nil || errDen != nil || den == 0 {
			return 0
		}
		return num / den
	}
	fps, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return fps
}

func bitswapProviderHosts() []string {
	hosts := []string{"kubo-fast-1", "kubo-fast-2", "kubo-slow-1", "kubo-slow-2", "kubo-slow-3"}
	seen := make(map[string]struct{}, len(hosts))
	ordered := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		ordered = append(ordered, host)
	}
	return ordered
}

func bitswapHostLabel(host string) string {
	if slot, ok := peerSlotForKuboHost(host); ok {
		return slot.DefaultLabel
	}
	return "unknown"
}

func (r runRequest) gsBinPath(defaultPath string) string {
	if filepath.IsAbs(defaultPath) {
		return defaultPath
	}
	return defaultPath
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func graphsyncStoreForRun(runID string) string {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID = fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return filepath.Join(defaultGraphsyncStore, filepath.Base(runID))
}

func clearGraphsyncStore(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("graphsync store path is required")
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

func kuboPeerID(host string) (string, error) {
	baseURL := kuboAPIURL(host)
	idURL := fmt.Sprintf("%s/api/v0/id", baseURL)
	payload, err := callKuboAPIWithResponse(idURL)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"ID"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil {
		return "", err
	}
	if strings.TrimSpace(resp.ID) == "" {
		return "", fmt.Errorf("missing peer ID")
	}
	return strings.TrimSpace(resp.ID), nil
}

func callKuboAPIWithResponse(rawURL string) ([]byte, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	req, err := http.NewRequest(http.MethodPost, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kubo api error: %s %s", resp.Status, string(body))
	}
	return body, nil
}

func isDockerClientTooOldError(text string) bool {
	value := strings.ToLower(text)
	return strings.Contains(value, "client version") && strings.Contains(value, "too old") && strings.Contains(value, "minimum supported api version")
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (s *server) saveRun(id string, req runRequest, status *runStatus, output string) {
	summary := parseSummary(output)
	peers := parsePeerSummary(output)
	outcome, outcomeReason := classifyRunOutcome(status, summary, output)
	var seed *seedMetadata
	if metadata, err := readSeedMetadataFromManifest(seedManifestPath); err == nil {
		seed = &metadata
	}
	if req.Mode == "graphsync" {
		labels := parsePeerLabelsFromOutput(output)
		if len(labels) == 0 {
			labels = loadPeerLabels("")
		}
		for i := range peers {
			peers[i].Label = labels[peers[i].PeerID]
		}
	}
	payload := runExport{
		ID:            id,
		Mode:          req.Mode,
		Root:          req.Root,
		Params:        runParams(req),
		StartedAt:     status.StartedAt,
		EndedAt:       status.EndedAt,
		State:         status.State,
		Error:         status.Error,
		Outcome:       outcome,
		OutcomeReason: outcomeReason,
		Summary:       summary,
		Peers:         peers,
		Seed:          seed,
		Output:        output,
	}
	outputDir := filepath.Join(runsRootDir, safeRunGroup(req.NetworkPreset))
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return
	}
	timestamp := time.Now().Format("02-01-2006-15-04-05")
	baseName := fmt.Sprintf("%s-%s", req.Mode, timestamp)
	filePath := filepath.Join(outputDir, baseName+".json")
	file, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(payload)
	_ = os.WriteFile(filepath.Join(outputDir, baseName+".log"), []byte(output), 0o644)
}

func safeRunGroup(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "custom"
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		valid := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if valid {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "._-")
	if out == "" || out == "." || out == ".." {
		return "custom"
	}
	return out
}

func classifyRunOutcome(status *runStatus, summary *runSummary, output string) (string, string) {
	if status == nil {
		return "unknown", "missing run status"
	}
	state := strings.ToLower(strings.TrimSpace(status.State))
	errText := strings.TrimSpace(status.Error)
	outputText := strings.ToLower(output)

	if state == "cancelled" {
		if errText == "" {
			errText = "run cancelled"
		}
		return "cancelled", errText
	}

	if strings.Contains(outputText, "playback session elapsed before first byte") {
		if summary != nil && summary.TotalBytes > 0 {
			return "success", ""
		}
		return "timeout_before_first_byte", "playback session elapsed before first byte"
	}

	if state == "error" {
		if errText == "" {
			errText = "run failed"
		}
		return "error", errText
	}

	if summary == nil {
		if state == "done" {
			return "missing_summary", "summary was not captured"
		}
		if state != "" {
			return state, errText
		}
		return "unknown", "summary was not captured"
	}

	if summary.TotalBytes <= 0 {
		if state == "done" {
			return "zero_bytes", "run completed with zero transferred bytes"
		}
		if state == "" {
			return "zero_bytes", "no bytes transferred"
		}
		if errText != "" {
			return state, errText
		}
		return state, "no bytes transferred"
	}

	if state == "done" || state == "" {
		return "success", ""
	}
	if errText != "" {
		return state, errText
	}
	return state, ""
}

func parseSummary(output string) *runSummary {
	const summaryPrefix = "SUMMARY_JSON "
	lines := strings.Split(output, "\n")
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if !strings.HasPrefix(line, summaryPrefix) {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, summaryPrefix))
		if line == "" {
			continue
		}
		var summary runSummary
		if err := json.Unmarshal([]byte(line), &summary); err == nil {
			return &summary
		}
	}
	return nil
}

func parsePeerSummary(output string) []peerSummary {
	lines := strings.Split(output, "\n")
	peers := make([]peerSummary, 0)
	inPeerSummary := false
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "Peer summary:" {
			inPeerSummary = true
			continue
		}
		if !inPeerSummary {
			continue
		}
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "-") {
			break
		}
		if !strings.Contains(line, "segments=") || !strings.Contains(line, "bytes=") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "-"))
		if len(fields) == 0 {
			continue
		}
		peerID := fields[0]
		ps := peerSummary{PeerID: peerID}
		for _, field := range fields[1:] {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) != 2 {
				continue
			}
			switch parts[0] {
			case "segments":
				ps.Segments = atoiDefault(parts[1])
			case "bytes":
				ps.Bytes = atoi64Default(parts[1])
			case "ema":
				ps.EMA = atofDefault(strings.TrimSuffix(parts[1], "B/s"))
			case "avgNet":
				ps.AvgNetMS = durationToMillis(parts[1])
			case "fails":
				ps.Failures = atoiDefault(parts[1])
			case "cooldown":
				ps.CooldownS = durationToSeconds(parts[1])
			}
		}
		peers = append(peers, ps)
	}
	return peers
}

func parsePeerLabelsFromOutput(output string) map[string]string {
	labels := parsePeerLabelsFromSchedulerConfig(output)
	if len(labels) > 0 {
		return labels
	}
	return parsePeerLabelsFromConnectedPeers(output)
}

func parsePeerLabelsFromSchedulerConfig(output string) map[string]string {
	labels := make(map[string]string)
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if !strings.HasPrefix(line, "{") || !strings.Contains(line, `"type":"scheduler_config"`) {
			continue
		}
		var event struct {
			Type  string `json:"type"`
			Peers []struct {
				PeerID string `json:"peerId"`
				Label  string `json:"label"`
			} `json:"peers"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil || event.Type != "scheduler_config" {
			continue
		}
		for _, peer := range event.Peers {
			peerID := strings.TrimSpace(peer.PeerID)
			label := strings.TrimSpace(peer.Label)
			if peerID != "" && label != "" {
				labels[peerID] = label
			}
		}
		if len(labels) > 0 {
			return labels
		}
	}
	return labels
}

func parsePeerLabelsFromConnectedPeers(output string) map[string]string {
	labels := make(map[string]string)
	inConnectedPeers := false
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if strings.HasPrefix(line, "Connected peers:") {
			inConnectedPeers = true
			continue
		}
		if !inConnectedPeers {
			continue
		}
		if strings.HasPrefix(line, "/") {
			continue
		}
		if strings.HasPrefix(line, "-") {
			fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "-")))
			if len(fields) >= 2 {
				labels[fields[0]] = fields[1]
			}
			continue
		}
		if len(labels) > 0 {
			break
		}
	}
	return labels
}

func writePeersJSON(path string, peers []graphsyncPeerEntry) error {
	if strings.TrimSpace(path) == "" {
		path = defaultPeersPath
	}
	if len(peers) == 0 {
		return fmt.Errorf("no peers to write")
	}
	type peerConfig struct {
		Addr  string `json:"addr"`
		Label string `json:"label"`
	}
	payload := struct {
		Peers []peerConfig `json:"peers"`
	}{
		Peers: make([]peerConfig, 0, len(peers)),
	}
	for _, entry := range peers {
		peerID := strings.TrimSpace(entry.PeerID)
		service := strings.TrimSpace(entry.Service)
		if peerID == "" || service == "" {
			continue
		}
		label := strings.TrimSpace(entry.Label)
		if label == "" {
			label = "slow"
		}
		payload.Peers = append(payload.Peers, peerConfig{
			Addr:  fmt.Sprintf("/dns4/%s/udp/4001/quic-v1/p2p/%s", service, peerID),
			Label: label,
		})
	}
	if len(payload.Peers) == 0 {
		return fmt.Errorf("no valid peers to write")
	}
	bytesPayload, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, bytesPayload, 0o644)
}

func loadPeerLabels(peersPath string) map[string]string {
	path := peersPath
	if path == "" {
		path = defaultPeersPath
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return map[string]string{}
	}
	var data struct {
		Peers []struct {
			Addr  string `json:"addr"`
			Label string `json:"label"`
		}
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return map[string]string{}
	}
	labels := make(map[string]string)
	for _, peer := range data.Peers {
		parts := strings.Split(peer.Addr, "/p2p/")
		if len(parts) != 2 {
			continue
		}
		labels[parts[1]] = peer.Label
	}
	return labels
}

func atoiDefault(value string) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func atoi64Default(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func atofDefault(value string) float64 {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func durationToMillis(value string) float64 {
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return float64(duration) / float64(time.Millisecond)
}

func durationToSeconds(value string) int64 {
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return int64(duration.Seconds())
}

const frontendUnavailableHTML = `<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <title>benchmark-ui-Frontend nicht verfügbar</title>
  <style>
    body { font-family: "IBM Plex Mono", monospace; background: #f6f4ef; color: #1f1f1f; margin: 24px; }
    main { max-width: 760px; background: #fffdf8; border: 1px solid #d8d2c7; border-radius: 8px; padding: 18px 20px; }
    h1 { margin-top: 0; font-size: 22px; }
    p, code { font-size: 14px; line-height: 1.5; }
    code { background: #f1ece4; padding: 2px 5px; border-radius: 4px; }
  </style>
</head>
<body>
  <main>
    <h1>benchmark-ui-Frontend nicht verfügbar</h1>
    <p>Das Go-Backend läuft, aber unter dieser Adresse ist kein gebautes Vue-Frontend verfügbar.</p>
    <p>Baue die Vue-App in <code>ui/</code> und starte <code>benchmark-ui</code> mit <code>BENCHMARK_UI_DIST_DIR</code>, das auf das erzeugte <code>dist</code>-Verzeichnis zeigt.</p>
  </main>
</body>
</html>`
