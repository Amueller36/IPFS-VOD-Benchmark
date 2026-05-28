package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const timelineFPS = 30

type timelineModel struct {
	Mode             string
	Segments         int
	ChunksPerSegment int
	PlaybackMS       int
	PlaybackSpeed    float64
	UrgentWindow     int
	ReadyTimes       map[int]int64
	InFlightStarts   map[int]int64
	UrgentDecisions  map[int]int64
	SegmentPeer      map[int]string
	SegmentPeerBytes map[int]map[string]int64
	SegmentPeerOrder map[int][]string
	PeerLabels       map[string]string
	PeerColors       map[string]color.RGBA
	PeerOrder        []string
	DurationNs       int64
}

type timelineLegendItem struct {
	key    string
	label  string
	color  color.RGBA
	accent bool
	band   bool
}

var timelineColors = struct {
	pending        color.RGBA
	fetching       color.RGBA
	prefetched     color.RGBA
	prefetchAccent color.RGBA
	played         color.RGBA
	urgent         color.RGBA
	urgentDecision color.RGBA
	border         color.RGBA
}{
	pending:        color.RGBA{R: 0xe6, G: 0xe2, B: 0xdc, A: 0xff},
	fetching:       color.RGBA{R: 0x8b, G: 0x5c, B: 0xf6, A: 0xff},
	prefetched:     color.RGBA{R: 0x4a, G: 0x90, B: 0xe2, A: 0xff},
	prefetchAccent: color.RGBA{R: 0x2f, G: 0x48, B: 0x58, A: 0xff},
	played:         color.RGBA{R: 0x7e, G: 0xb7, B: 0x7f, A: 0xff},
	urgent:         color.RGBA{R: 220, G: 53, B: 69, A: 64},
	urgentDecision: color.RGBA{R: 0x9f, G: 0x2d, B: 0x2d, A: 0xff},
	border:         color.RGBA{R: 0xb7, G: 0xb0, B: 0xa7, A: 0xff},
}

var timelinePeerPalettes = map[string][]color.RGBA{
	"fast": {
		{R: 0x1f, G: 0x77, B: 0xb4, A: 0xff},
		{R: 0x4a, G: 0x90, B: 0xe2, A: 0xff},
	},
	"slow": {
		{R: 0xff, G: 0x7f, B: 0x0e, A: 0xff},
		{R: 0xff, G: 0xa6, B: 0x4d, A: 0xff},
		{R: 0xff, G: 0xbf, B: 0x80, A: 0xff},
	},
}

var timelineCustomPeerColors = []color.RGBA{
	{R: 0x2c, G: 0xa0, B: 0x2c, A: 0xff},
	{R: 0x94, G: 0x67, B: 0xbd, A: 0xff},
	{R: 0x8c, G: 0x56, B: 0x4b, A: 0xff},
	{R: 0x17, G: 0xbe, B: 0xcf, A: 0xff},
	{R: 0xbc, G: 0xbd, B: 0x22, A: 0xff},
}

type timelineRunCandidate struct {
	rel        string
	experiment string
	mode       string
	score      int64
	run        runExport
}

func generateTimelineVideos(ctx context.Context, runsDir string, timelineDir string, fallbackManifest string) timelineGenerateResponse {
	resp := timelineGenerateResponse{}
	if err := os.MkdirAll(timelineDir, 0o755); err != nil {
		resp.Errors = append(resp.Errors, fmt.Sprintf("failed to create timeline dir: %v", err))
		return resp
	}
	clearGeneratedTimelineVideos(timelineDir)

	var fallback *seedMetadata
	if metadata, err := readSeedMetadataFromManifest(fallbackManifest); err == nil && validSeedMetadata(metadata) {
		fallback = &metadata
	}

	candidates := collectTimelineRunCandidates(runsDir, &resp)
	selected := selectMedianTimelineRunCandidates(candidates)

	for _, candidate := range selected {
		run := candidate.run
		seed := run.Seed
		if seed == nil || !validSeedMetadata(*seed) {
			seed = fallback
		}
		if seed == nil || !validSeedMetadata(*seed) {
			resp.Skipped++
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: missing seed metadata", filepath.Base(candidate.rel)))
			continue
		}
		model, err := buildTimelineModel(run, *seed)
		if err != nil {
			resp.Skipped++
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: %v", filepath.Base(candidate.rel), err))
			continue
		}
		rel := strings.TrimSuffix(candidate.rel, filepath.Ext(candidate.rel)) + "-timeline.mp4"
		outputPath := filepath.Join(timelineDir, rel)
		if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
			resp.Skipped++
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: output dir failed: %v", filepath.Base(candidate.rel), err))
			continue
		}
		if err := renderTimelineMP4(ctx, model, outputPath); err != nil {
			resp.Skipped++
			resp.Errors = append(resp.Errors, fmt.Sprintf("%s: render failed: %v", filepath.Base(candidate.rel), err))
			continue
		}
		resp.Generated++
	}
	return resp
}

func collectTimelineRunCandidates(runsDir string, resp *timelineGenerateResponse) []timelineRunCandidate {
	candidates := make([]timelineRunCandidate, 0)
	if err := filepath.WalkDir(runsDir, func(runPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if resp != nil {
				resp.Skipped++
				resp.Errors = append(resp.Errors, fmt.Sprintf("%s: read failed: %v", runPath, walkErr))
			}
			return nil
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil
		}
		rel, err := filepath.Rel(runsDir, runPath)
		if err != nil {
			rel = entry.Name()
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) > 0 && parts[0] == "timelines" {
			return nil
		}
		payload, err := os.ReadFile(runPath)
		if err != nil {
			if resp != nil {
				resp.Skipped++
				resp.Errors = append(resp.Errors, fmt.Sprintf("%s: read failed: %v", entry.Name(), err))
			}
			return nil
		}
		var run runExport
		if err := json.Unmarshal(payload, &run); err != nil {
			if resp != nil {
				resp.Skipped++
				resp.Errors = append(resp.Errors, fmt.Sprintf("%s: invalid JSON: %v", entry.Name(), err))
			}
			return nil
		}
		mode := strings.ToLower(strings.TrimSpace(run.Mode))
		if mode != "graphsync" && mode != "bitswap" {
			return nil
		}
		experiment := "."
		if len(parts) > 1 {
			experiment = parts[0]
		}
		candidates = append(candidates, timelineRunCandidate{
			rel:        filepath.ToSlash(rel),
			experiment: experiment,
			mode:       mode,
			score:      timelineSelectionScore(run.Summary),
			run:        run,
		})
		return nil
	}); err != nil {
		if resp != nil {
			resp.Errors = append(resp.Errors, fmt.Sprintf("failed to read runs dir: %v", err))
		}
	}
	return candidates
}

func timelineSelectionScore(summary *runSummary) int64 {
	if summary == nil {
		return 0
	}
	if summary.PlaybackCompletion > 0 {
		return summary.PlaybackCompletion
	}
	return summary.TotalTime
}

func selectMedianTimelineRunCandidates(candidates []timelineRunCandidate) []timelineRunCandidate {
	grouped := make(map[string][]timelineRunCandidate)
	for _, candidate := range candidates {
		key := candidate.experiment + "\x00" + candidate.mode
		grouped[key] = append(grouped[key], candidate)
	}
	selected := make([]timelineRunCandidate, 0, len(grouped))
	for _, group := range grouped {
		sort.Slice(group, func(i, j int) bool {
			if group[i].score != group[j].score {
				return group[i].score < group[j].score
			}
			return group[i].rel < group[j].rel
		})
		selected = append(selected, group[(len(group)-1)/2])
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].experiment != selected[j].experiment {
			return selected[i].experiment < selected[j].experiment
		}
		if selected[i].mode != selected[j].mode {
			return selected[i].mode < selected[j].mode
		}
		return selected[i].rel < selected[j].rel
	})
	return selected
}

func clearGeneratedTimelineVideos(timelineDir string) {
	_ = filepath.WalkDir(timelineDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".mp4") {
			return nil
		}
		_ = os.Remove(path)
		return nil
	})
}

func validSeedMetadata(seed seedMetadata) bool {
	return seed.Segments > 0 && seed.SegmentSize > 0 && seed.ChunkSize > 0
}

func buildTimelineModel(run runExport, seed seedMetadata) (timelineModel, error) {
	if strings.TrimSpace(run.Output) == "" {
		return timelineModel{}, fmt.Errorf("missing run output")
	}
	chunksPerSegment := int((seed.SegmentSize + seed.ChunkSize - 1) / seed.ChunkSize)
	if chunksPerSegment <= 0 {
		chunksPerSegment = 1
	}
	model := timelineModel{
		Mode:             strings.TrimSpace(run.Mode),
		Segments:         seed.Segments,
		ChunksPerSegment: chunksPerSegment,
		PlaybackMS:       intParam(run.Params, "playbackMs", 40),
		PlaybackSpeed:    floatParam(run.Params, "playbackSpeed", 1),
		UrgentWindow:     intParam(run.Params, "urgentWindow", 0),
		ReadyTimes:       make(map[int]int64),
		InFlightStarts:   make(map[int]int64),
		UrgentDecisions:  make(map[int]int64),
		SegmentPeer:      make(map[int]string),
		SegmentPeerBytes: make(map[int]map[string]int64),
		SegmentPeerOrder: make(map[int][]string),
		PeerLabels:       make(map[string]string),
		PeerColors:       make(map[string]color.RGBA),
	}
	if model.Mode == "bitswap" {
		model.UrgentWindow = 0
	}
	if model.PlaybackMS <= 0 {
		model.PlaybackMS = 40
	}
	if model.PlaybackSpeed <= 0 {
		model.PlaybackSpeed = 1
	}

	parseTimelineOutput(run.Output, &model)
	finalizeTimelineSegmentPeers(&model)
	if len(model.ReadyTimes) == 0 {
		return timelineModel{}, fmt.Errorf("no timeline readiness events")
	}
	model.DurationNs = timelineDurationNs(run.Summary, model)
	return model, nil
}

func intParam(params map[string]interface{}, key string, fallback int) int {
	if params == nil {
		return fallback
	}
	switch value := params[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		if parsed, err := value.Int64(); err == nil {
			return int(parsed)
		}
	}
	return fallback
}

func floatParam(params map[string]interface{}, key string, fallback float64) float64 {
	if params == nil {
		return fallback
	}
	switch value := params[key].(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case json.Number:
		if parsed, err := value.Float64(); err == nil {
			return parsed
		}
	}
	return fallback
}

func parseTimelineOutput(output string, model *timelineModel) {
	ignoreLegacySegments := strings.EqualFold(strings.TrimSpace(model.Mode), "graphsync") && hasStructuredGraphsyncTimelineEvents(output)
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	var graphsyncStartNs *int64
	var graphsyncFallbackStartNs *int64
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "segment ") {
			if !ignoreLegacySegments {
				parseLegacyTimelineSegment(line, model)
			}
			continue
		}
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var sample struct {
			Type            string `json:"type"`
			Protocol        string `json:"protocol"`
			Segment         int    `json:"segment"`
			PlaybackIndex   int    `json:"playbackIndex"`
			TimeNs          int64  `json:"timeNs"`
			ReadyTimeNs     int64  `json:"readyTimeNs"`
			Peer            string `json:"peer"`
			PeerLabel       string `json:"peerLabel"`
			SelectedPeer    string `json:"selectedPeer"`
			SelectedLabel   string `json:"selectedLabel"`
			Success         *bool  `json:"success"`
			Urgent          bool   `json:"urgent"`
			ProviderHost    string `json:"providerHost"`
			ProviderPeerID  string `json:"providerPeerId"`
			ProviderLabel   string `json:"providerLabel"`
			PeerID          string `json:"peerId"`
			Bytes           int64  `json:"bytes"`
			VirtualSegments []int  `json:"virtualSegments"`
			Peers           []struct {
				PeerID string `json:"peerId"`
				Label  string `json:"label"`
			} `json:"peers"`
		}
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			continue
		}
		if graphsyncFallbackStartNs == nil && sample.TimeNs > 0 && isGraphsyncTimelineEvent(sample.Type) {
			startNs := sample.TimeNs
			graphsyncFallbackStartNs = &startNs
		}
		switch sample.Type {
		case "scheduler_config":
			if sample.TimeNs > 0 {
				startNs := sample.TimeNs
				graphsyncStartNs = &startNs
			}
			for _, peer := range sample.Peers {
				assignTimelinePeerColor(model, peer.PeerID, peer.Label)
			}
		case "scheduler_decision":
			if !validSegment(sample.Segment, model.Segments) {
				continue
			}
			eventTime := graphsyncRelativeTime(sample.TimeNs, graphsyncStartNs, graphsyncFallbackStartNs)
			setMinTime(model.InFlightStarts, sample.Segment, eventTime)
			if sample.Urgent {
				setMinTime(model.UrgentDecisions, sample.Segment, eventTime)
			}
			if sample.SelectedPeer != "" {
				assignTimelinePeerColor(model, sample.SelectedPeer, sample.SelectedLabel)
			}
		case "scheduler_result":
			if !validSegment(sample.Segment, model.Segments) || (sample.Success != nil && !*sample.Success) {
				continue
			}
			readyTime := graphsyncRelativeTime(sample.TimeNs, graphsyncStartNs, graphsyncFallbackStartNs)
			setMinTime(model.ReadyTimes, sample.Segment, readyTime)
			if sample.Peer != "" {
				model.SegmentPeer[sample.Segment] = sample.Peer
				assignTimelinePeerColor(model, sample.Peer, sample.PeerLabel)
			}
		case "bitswap_virtual_segment_ready":
			if sample.Protocol != "bitswap" || !validSegment(sample.Segment, model.Segments) {
				continue
			}
			setMinTime(model.ReadyTimes, sample.Segment, sample.ReadyTimeNs)
		case "bitswap_block_received":
			peerKey := sample.ProviderPeerID
			if peerKey == "" {
				peerKey = sample.PeerID
			}
			if peerKey == "" {
				peerKey = sample.ProviderHost
			}
			if peerKey == "" {
				continue
			}
			assignTimelinePeerColor(model, peerKey, sample.ProviderLabel)
			addTimelineBitswapSegmentPeerBytes(model, sample.VirtualSegments, peerKey, sample.Bytes)
		}
	}
}

func addTimelineBitswapSegmentPeerBytes(model *timelineModel, segments []int, peerID string, bytes int64) {
	if peerID == "" || len(segments) == 0 {
		return
	}
	share := bytes
	if share <= 0 {
		share = 1
	}
	if len(segments) > 1 {
		share = maxInt64(1, share/int64(len(segments)))
	}
	for _, segment := range segments {
		if !validSegment(segment, model.Segments) {
			continue
		}
		if model.SegmentPeerBytes[segment] == nil {
			model.SegmentPeerBytes[segment] = make(map[string]int64)
		}
		if _, ok := model.SegmentPeerBytes[segment][peerID]; !ok {
			model.SegmentPeerOrder[segment] = append(model.SegmentPeerOrder[segment], peerID)
		}
		model.SegmentPeerBytes[segment][peerID] += share
	}
}

func finalizeTimelineSegmentPeers(model *timelineModel) {
	for segment, peerBytes := range model.SegmentPeerBytes {
		if len(peerBytes) == 0 {
			continue
		}
		bestPeer := ""
		var bestBytes int64
		for _, peerID := range model.SegmentPeerOrder[segment] {
			bytes := peerBytes[peerID]
			if bestPeer == "" || bytes > bestBytes {
				bestPeer = peerID
				bestBytes = bytes
			}
		}
		if bestPeer != "" {
			model.SegmentPeer[segment] = bestPeer
		}
	}
}

func hasStructuredGraphsyncTimelineEvents(output string) bool {
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var sample struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			continue
		}
		if isGraphsyncTimelineEvent(sample.Type) {
			return true
		}
	}
	return false
}

func isGraphsyncTimelineEvent(eventType string) bool {
	switch eventType {
	case "scheduler_config", "scheduler_decision", "scheduler_result":
		return true
	default:
		return false
	}
}

func parseLegacyTimelineSegment(line string, model *timelineModel) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return
	}
	segment, err := strconvAtoi(fields[1])
	if err != nil || !validSegment(segment, model.Segments) || !strings.Contains(line, "fetched") {
		return
	}
	setMinTime(model.ReadyTimes, segment, 0)
	var peerID, label string
	for _, field := range fields {
		if strings.HasPrefix(field, "peer=") {
			peerID = strings.TrimPrefix(field, "peer=")
		}
		if strings.HasPrefix(field, "label=") {
			label = strings.TrimPrefix(field, "label=")
		}
	}
	if peerID != "" {
		model.SegmentPeer[segment] = peerID
		assignTimelinePeerColor(model, peerID, label)
	}
}

func strconvAtoi(value string) (int, error) {
	var parsed int
	_, err := fmt.Sscanf(value, "%d", &parsed)
	return parsed, err
}

func validSegment(segment int, segmentCount int) bool {
	return segment >= 0 && segment < segmentCount
}

func setMinTime(values map[int]int64, key int, value int64) {
	if value < 0 {
		value = 0
	}
	if current, ok := values[key]; !ok || value < current {
		values[key] = value
	}
}

func graphsyncRelativeTime(timeNs int64, startNs *int64, fallbackStartNs *int64) int64 {
	if timeNs <= 0 {
		return 0
	}
	if startNs == nil {
		startNs = fallbackStartNs
	}
	if startNs == nil {
		return 0
	}
	return timeNs - *startNs
}

func assignTimelinePeerColor(model *timelineModel, peerID string, label string) color.RGBA {
	if peerID == "" {
		return timelineColors.prefetched
	}
	if existing, ok := model.PeerColors[peerID]; ok {
		return existing
	}
	normalizedLabel := strings.TrimSpace(label)
	if normalizedLabel == "" {
		normalizedLabel = "slow"
	}
	model.PeerLabels[peerID] = normalizedLabel
	model.PeerOrder = append(model.PeerOrder, peerID)
	group := timelinePeerColorGroup(normalizedLabel)
	palette := timelinePeerPalettes[group]
	usedForGroup := 0
	for peer, existingLabel := range model.PeerLabels {
		if peer != peerID && timelinePeerColorGroup(existingLabel) == group {
			usedForGroup++
		}
	}
	if len(palette) == 0 {
		palette = timelineCustomPeerColors
	}
	colorValue := palette[usedForGroup%len(palette)]
	model.PeerColors[peerID] = colorValue
	return colorValue
}

func timelinePeerColorGroup(label string) string {
	normalizedLabel := strings.TrimSpace(strings.ToLower(label))
	if normalizedLabel == "fast" {
		return "fast"
	}
	if normalizedLabel == "slow" || strings.HasPrefix(normalizedLabel, "slow-") {
		return "slow"
	}
	return ""
}

func timelinePeerDisplayLabel(peerID string, label string) string {
	if peerID == "" {
		return strings.TrimSpace(label)
	}
	normalizedLabel := strings.TrimSpace(label)
	parts := strings.Split(peerID, "-")
	if len(parts) >= 3 && parts[0] == "kubo" && (parts[1] == "fast" || parts[1] == "slow") {
		return parts[1] + "-" + strings.Join(parts[2:], "-")
	}
	if normalizedLabel == "" {
		return peerID
	}
	suffix := peerID
	if len(suffix) > 6 {
		suffix = suffix[len(suffix)-6:]
	}
	return normalizedLabel + " " + suffix
}

func timelineDurationNs(summary *runSummary, model timelineModel) int64 {
	if summary != nil {
		if summary.PlaybackCompletion > 0 {
			return summary.PlaybackCompletion
		}
		if summary.TotalTime > 0 {
			return summary.TotalTime
		}
	}
	var latest int64
	for _, value := range model.ReadyTimes {
		if value > latest {
			latest = value
		}
	}
	interval := playbackIntervalNs(model)
	if interval > 0 {
		latest += interval
	}
	if latest <= 0 {
		latest = int64(model.PlaybackMS) * int64(model.Segments) * int64(timeMillisecondNs)
	}
	return latest
}

const timeMillisecondNs = 1_000_000

func playbackIntervalNs(model timelineModel) int64 {
	speed := model.PlaybackSpeed
	if speed <= 0 {
		speed = 1
	}
	return int64((float64(model.PlaybackMS) * float64(timeMillisecondNs)) / speed)
}

func renderTimelineMP4(ctx context.Context, model timelineModel, outputPath string) error {
	if model.DurationNs <= 0 {
		return fmt.Errorf("invalid timeline duration")
	}
	frameDir, err := os.MkdirTemp("", "timeline-frames-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(frameDir)

	frameCount := int(math.Ceil((float64(model.DurationNs)/1e9)*timelineFPS)) + 1
	if frameCount < 1 {
		frameCount = 1
	}
	for frame := 1; frame <= frameCount; frame++ {
		timeNs := int64((float64(frame-1) / timelineFPS) * 1e9)
		img := drawTimelineFrame(model, timeNs)
		framePath := filepath.Join(frameDir, fmt.Sprintf("frame-%06d.png", frame))
		file, err := os.Create(framePath)
		if err != nil {
			return err
		}
		if err := png.Encode(file, img); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	args := []string{
		"-y",
		"-framerate", fmt.Sprintf("%d", timelineFPS),
		"-i", filepath.Join(frameDir, "frame-%06d.png"),
		"-c:v", "libx264",
		"-pix_fmt", "yuv420p",
		"-movflags", "+faststart",
		outputPath,
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func drawTimelineFrame(model timelineModel, timeNs int64) image.Image {
	const cellWidth = 4
	const baseCellHeight = 12
	width := maxInt(model.Segments*cellWidth, 320)
	bodyHeight := maxInt(model.ChunksPerSegment*baseCellHeight, 260)
	legendHeight := timelineLegendHeight(model, width)
	height := legendHeight + bodyHeight
	if width%2 != 0 {
		width++
	}
	if height%2 != 0 {
		height++
	}
	cellHeight := float64(bodyHeight) / float64(model.ChunksPerSegment)
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	fillRect(img, image.Rect(0, 0, width, height), color.RGBA{R: 0xf5, G: 0xf2, B: 0xea, A: 0xff})
	drawTimelineLegend(img, model, width, legendHeight)

	projection := computeTimelinePlaybackProjection(model.ReadyTimes, model.Segments, playbackIntervalNs(model), timeNs)
	for seg := 0; seg < model.Segments; seg++ {
		isPlayed := seg < projection.index
		readyAt, fetched := model.ReadyTimes[seg]
		fetched = fetched && readyAt <= timeNs
		fetchStart, inFlight := model.InFlightStarts[seg]
		inFlight = inFlight && fetchStart <= timeNs && !fetched
		peerColor := timelineColors.prefetched
		if peerID := model.SegmentPeer[seg]; peerID != "" {
			if colorValue, ok := model.PeerColors[peerID]; ok {
				peerColor = colorValue
			}
		}
		for row := 0; row < model.ChunksPerSegment; row++ {
			y0 := legendHeight + int(math.Round(float64(row)*cellHeight))
			y1 := legendHeight + int(math.Round(float64(row+1)*cellHeight))
			if y1 <= y0 {
				y1 = y0 + 1
			}
			rect := image.Rect(seg*cellWidth, y0, seg*cellWidth+cellWidth, y1)
			fill := timelineColors.pending
			isPrefetched := false
			if fetched {
				fill = peerColor
				isPrefetched = !isPlayed
			} else if inFlight {
				fill = timelineColors.fetching
			}
			fillRect(img, rect, fill)
			if isPrefetched {
				accentHeight := clampInt(int(math.Round(float64(y1-y0)/5)), 1, 2)
				fillRect(img, image.Rect(rect.Min.X, rect.Min.Y, rect.Max.X, rect.Min.Y+accentHeight), timelineColors.prefetchAccent)
				fillRect(img, image.Rect(rect.Min.X, rect.Max.Y-accentHeight, rect.Max.X, rect.Max.Y), timelineColors.prefetchAccent)
			}
			if isPlayed {
				bandHeight := clampInt(int(math.Round(float64(y1-y0)/3)), 2, 4)
				bandY := y0 + maxInt(0, ((y1-y0)-bandHeight)/2)
				fillRect(img, image.Rect(rect.Min.X, bandY, rect.Max.X, bandY+bandHeight), timelineColors.played)
			}
		}
	}

	for segment, decisionTime := range model.UrgentDecisions {
		if decisionTime > timeNs || !validSegment(segment, model.Segments) {
			continue
		}
		x := segment * cellWidth
		for row := 0; row < model.ChunksPerSegment; row++ {
			y0 := legendHeight + int(math.Round(float64(row)*cellHeight))
			y1 := legendHeight + int(math.Round(float64(row+1)*cellHeight))
			markerHeight := 2
			markerY := y0 + maxInt(markerHeight, int(math.Floor(float64(y1-y0)*0.72)))
			if markerY+markerHeight > y1 {
				markerY = y1 - markerHeight
			}
			fillRect(img, image.Rect(x, markerY, x+cellWidth, markerY+markerHeight), timelineColors.urgentDecision)
		}
	}

	if model.UrgentWindow > 0 {
		start := projection.index
		end := minInt(model.Segments, projection.index+model.UrgentWindow)
		if end > start {
			fillRectAlpha(img, image.Rect(start*cellWidth, legendHeight, end*cellWidth, legendHeight+bodyHeight), timelineColors.urgent)
		}
	}

	playbackX := int(math.Round(clampFloat(projection.position, 0, float64(model.Segments)) * cellWidth))
	fillRect(img, image.Rect(playbackX, legendHeight, playbackX+2, legendHeight+bodyHeight), timelineColors.played)
	drawRectBorder(img, image.Rect(0, legendHeight, width, legendHeight+bodyHeight), timelineColors.border)
	return img
}

func timelineLegendItems(model timelineModel) []timelineLegendItem {
	items := []timelineLegendItem{
		{key: "pending", label: "Ausstehend", color: timelineColors.pending},
	}
	if len(model.InFlightStarts) > 0 {
		items = append(items, timelineLegendItem{key: "fetching", label: "Wird geladen", color: timelineColors.fetching})
	}
	items = append(items,
		timelineLegendItem{key: "prefetched", label: "Vorgeladen/Prefetched", color: color.RGBA{R: 0xf5, G: 0xf2, B: 0xea, A: 0xff}, accent: true},
		timelineLegendItem{key: "played", label: "Abgespielt (Band)", color: color.RGBA{R: 0xf5, G: 0xf2, B: 0xea, A: 0xff}, band: true},
	)
	if model.UrgentWindow > 0 {
		items = append(items, timelineLegendItem{key: "urgent", label: "Urgent Window", color: timelineColors.urgent})
	}
	if len(model.UrgentDecisions) > 0 {
		items = append(items, timelineLegendItem{key: "urgent-decision", label: "Urgent geplant", color: timelineColors.urgentDecision})
	}

	attributedPeers := make(map[string]bool)
	for _, peerID := range model.SegmentPeer {
		if peerID != "" {
			attributedPeers[peerID] = true
		}
	}
	for _, peerID := range model.PeerOrder {
		if !attributedPeers[peerID] {
			continue
		}
		colorValue, ok := model.PeerColors[peerID]
		if !ok {
			continue
		}
		items = append(items, timelineLegendItem{
			key:   peerID,
			label: timelinePeerDisplayLabel(peerID, model.PeerLabels[peerID]),
			color: colorValue,
		})
	}
	return items
}

func timelineLegendHeight(model timelineModel, width int) int {
	items := timelineLegendItems(model)
	if len(items) == 0 {
		return 0
	}
	rows := 1
	x := timelineLegendPaddingX
	for _, item := range items {
		itemWidth := timelineLegendItemWidth(item)
		if x > timelineLegendPaddingX && x+itemWidth > width-timelineLegendPaddingX {
			rows++
			x = timelineLegendPaddingX
		}
		x += itemWidth + timelineLegendItemGap
	}
	return timelineLegendPaddingY*2 + rows*timelineLegendRowHeight + (rows-1)*timelineLegendRowGap
}

const (
	timelineLegendPaddingX     = 10
	timelineLegendPaddingY     = 8
	timelineLegendRowHeight    = 20
	timelineLegendRowGap       = 6
	timelineLegendItemGap      = 14
	timelineLegendSwatchWidth  = 18
	timelineLegendSwatchHeight = 12
	timelineLegendTextGap      = 6
	timelineLegendTextScale    = 2
)

func timelineLegendItemWidth(item timelineLegendItem) int {
	return timelineLegendSwatchWidth + timelineLegendTextGap + timelineTextWidth(item.label, timelineLegendTextScale)
}

func drawTimelineLegend(img *image.RGBA, model timelineModel, width int, legendHeight int) {
	if legendHeight <= 0 {
		return
	}
	fillRect(img, image.Rect(0, 0, width, legendHeight), color.RGBA{R: 0xf5, G: 0xf2, B: 0xea, A: 0xff})
	x := timelineLegendPaddingX
	y := timelineLegendPaddingY
	for _, item := range timelineLegendItems(model) {
		itemWidth := timelineLegendItemWidth(item)
		if x > timelineLegendPaddingX && x+itemWidth > width-timelineLegendPaddingX {
			x = timelineLegendPaddingX
			y += timelineLegendRowHeight + timelineLegendRowGap
		}
		swatchY := y + (timelineLegendRowHeight-timelineLegendSwatchHeight)/2
		swatchRect := image.Rect(x, swatchY, x+timelineLegendSwatchWidth, swatchY+timelineLegendSwatchHeight)
		drawTimelineLegendSwatch(img, item, swatchRect)
		textY := y + (timelineLegendRowHeight-(7*timelineLegendTextScale))/2
		drawTimelineText(img, x+timelineLegendSwatchWidth+timelineLegendTextGap, textY, item.label, timelineLegendTextScale, color.RGBA{R: 0x2f, G: 0x2f, B: 0x2f, A: 0xff})
		x += itemWidth + timelineLegendItemGap
	}
}

func drawTimelineLegendSwatch(img *image.RGBA, item timelineLegendItem, rect image.Rectangle) {
	fillRect(img, rect, color.RGBA{R: 0xf5, G: 0xf2, B: 0xea, A: 0xff})
	if item.band {
		bandY0 := rect.Min.Y + (rect.Dy()*35)/100
		bandY1 := rect.Min.Y + (rect.Dy()*65)/100
		fillRect(img, image.Rect(rect.Min.X, bandY0, rect.Max.X, bandY1), timelineColors.played)
	} else if item.accent {
		fillRect(img, image.Rect(rect.Min.X, rect.Min.Y, rect.Max.X, rect.Min.Y+3), timelineColors.prefetchAccent)
		fillRect(img, image.Rect(rect.Min.X, rect.Max.Y-3, rect.Max.X, rect.Max.Y), timelineColors.prefetchAccent)
	} else if item.color.A < 255 {
		fillRectAlpha(img, rect, item.color)
	} else {
		fillRect(img, rect, item.color)
	}
	drawRectBorder(img, rect, color.RGBA{R: 0xc6, G: 0xb8, B: 0xa6, A: 0xff})
}

type timelinePlaybackProjection struct {
	index    int
	position float64
}

func computeTimelinePlaybackProjection(readyTimes map[int]int64, segmentCount int, playbackIntervalNs int64, currentElapsedNs int64) timelinePlaybackProjection {
	if segmentCount <= 0 || playbackIntervalNs <= 0 || currentElapsedNs < 0 {
		return timelinePlaybackProjection{}
	}
	firstReady, ok := readyTimes[0]
	if !ok {
		return timelinePlaybackProjection{}
	}
	playbackClock := maxInt64(0, firstReady)
	played := 0
	for segment := 0; segment < segmentCount; segment++ {
		readyAt, ok := readyTimes[segment]
		if !ok {
			break
		}
		if readyAt > playbackClock {
			if currentElapsedNs < readyAt {
				return timelinePlaybackProjection{index: played, position: float64(played)}
			}
			playbackClock = readyAt
		}
		consumedAt := playbackClock + playbackIntervalNs
		if currentElapsedNs < consumedAt {
			fraction := float64(currentElapsedNs-playbackClock) / float64(playbackIntervalNs)
			return timelinePlaybackProjection{
				index:    played,
				position: clampFloat(float64(played)+math.Max(0, fraction), 0, float64(segmentCount)),
			}
		}
		played++
		playbackClock = consumedAt
	}
	return timelinePlaybackProjection{index: played, position: float64(played)}
}

var timelineFont5x7 = map[rune][]string{
	'0': {"11111", "10001", "10011", "10101", "11001", "10001", "11111"},
	'1': {"00100", "01100", "00100", "00100", "00100", "00100", "01110"},
	'2': {"11110", "00001", "00001", "11110", "10000", "10000", "11111"},
	'3': {"11110", "00001", "00001", "01110", "00001", "00001", "11110"},
	'4': {"10010", "10010", "10010", "11111", "00010", "00010", "00010"},
	'5': {"11111", "10000", "10000", "11110", "00001", "00001", "11110"},
	'6': {"01111", "10000", "10000", "11110", "10001", "10001", "01110"},
	'7': {"11111", "00001", "00010", "00100", "01000", "01000", "01000"},
	'8': {"01110", "10001", "10001", "01110", "10001", "10001", "01110"},
	'9': {"01110", "10001", "10001", "01111", "00001", "00001", "11110"},
	'A': {"01110", "10001", "10001", "11111", "10001", "10001", "10001"},
	'B': {"11110", "10001", "10001", "11110", "10001", "10001", "11110"},
	'C': {"01111", "10000", "10000", "10000", "10000", "10000", "01111"},
	'D': {"11110", "10001", "10001", "10001", "10001", "10001", "11110"},
	'E': {"11111", "10000", "10000", "11110", "10000", "10000", "11111"},
	'F': {"11111", "10000", "10000", "11110", "10000", "10000", "10000"},
	'G': {"01111", "10000", "10000", "10011", "10001", "10001", "01111"},
	'H': {"10001", "10001", "10001", "11111", "10001", "10001", "10001"},
	'I': {"01110", "00100", "00100", "00100", "00100", "00100", "01110"},
	'J': {"00111", "00010", "00010", "00010", "00010", "10010", "01100"},
	'K': {"10001", "10010", "10100", "11000", "10100", "10010", "10001"},
	'L': {"10000", "10000", "10000", "10000", "10000", "10000", "11111"},
	'M': {"10001", "11011", "10101", "10101", "10001", "10001", "10001"},
	'N': {"10001", "11001", "10101", "10011", "10001", "10001", "10001"},
	'O': {"01110", "10001", "10001", "10001", "10001", "10001", "01110"},
	'P': {"11110", "10001", "10001", "11110", "10000", "10000", "10000"},
	'Q': {"01110", "10001", "10001", "10001", "10101", "10010", "01101"},
	'R': {"11110", "10001", "10001", "11110", "10100", "10010", "10001"},
	'S': {"01111", "10000", "10000", "01110", "00001", "00001", "11110"},
	'T': {"11111", "00100", "00100", "00100", "00100", "00100", "00100"},
	'U': {"10001", "10001", "10001", "10001", "10001", "10001", "01110"},
	'V': {"10001", "10001", "10001", "10001", "10001", "01010", "00100"},
	'W': {"10001", "10001", "10001", "10101", "10101", "10101", "01010"},
	'X': {"10001", "10001", "01010", "00100", "01010", "10001", "10001"},
	'Y': {"10001", "10001", "01010", "00100", "00100", "00100", "00100"},
	'Z': {"11111", "00001", "00010", "00100", "01000", "10000", "11111"},
	'-': {"00000", "00000", "00000", "11111", "00000", "00000", "00000"},
	'/': {"00001", "00010", "00010", "00100", "01000", "01000", "10000"},
	'(': {"00010", "00100", "01000", "01000", "01000", "00100", "00010"},
	')': {"01000", "00100", "00010", "00010", "00010", "00100", "01000"},
}

func timelineTextWidth(text string, scale int) int {
	if text == "" {
		return 0
	}
	width := 0
	for _, ch := range strings.ToUpper(text) {
		if ch == ' ' {
			width += 4 * scale
			continue
		}
		width += 6 * scale
	}
	if width > 0 {
		width -= scale
	}
	return width
}

func drawTimelineText(img *image.RGBA, x int, y int, text string, scale int, colorValue color.RGBA) {
	cursor := x
	for _, ch := range strings.ToUpper(text) {
		if ch == ' ' {
			cursor += 4 * scale
			continue
		}
		pattern, ok := timelineFont5x7[ch]
		if !ok {
			pattern = timelineFont5x7['-']
		}
		for row, line := range pattern {
			for col, pixel := range line {
				if pixel != '1' {
					continue
				}
				fillRect(img, image.Rect(cursor+col*scale, y+row*scale, cursor+(col+1)*scale, y+(row+1)*scale), colorValue)
			}
		}
		cursor += 6 * scale
	}
}

func fillRect(img *image.RGBA, rect image.Rectangle, colorValue color.RGBA) {
	rect = rect.Intersect(img.Bounds())
	for y := rect.Min.Y; y < rect.Max.Y; y++ {
		for x := rect.Min.X; x < rect.Max.X; x++ {
			img.SetRGBA(x, y, colorValue)
		}
	}
}

func fillRectAlpha(img *image.RGBA, rect image.Rectangle, overlay color.RGBA) {
	rect = rect.Intersect(img.Bounds())
	alpha := float64(overlay.A) / 255
	for y := rect.Min.Y; y < rect.Max.Y; y++ {
		for x := rect.Min.X; x < rect.Max.X; x++ {
			base := img.RGBAAt(x, y)
			img.SetRGBA(x, y, color.RGBA{
				R: uint8(float64(base.R)*(1-alpha) + float64(overlay.R)*alpha),
				G: uint8(float64(base.G)*(1-alpha) + float64(overlay.G)*alpha),
				B: uint8(float64(base.B)*(1-alpha) + float64(overlay.B)*alpha),
				A: 0xff,
			})
		}
	}
}

func drawBorder(img *image.RGBA, colorValue color.RGBA) {
	bounds := img.Bounds()
	drawRectBorder(img, bounds, colorValue)
}

func drawRectBorder(img *image.RGBA, rect image.Rectangle, colorValue color.RGBA) {
	rect = rect.Intersect(img.Bounds())
	fillRect(img, image.Rect(rect.Min.X, rect.Min.Y, rect.Max.X, rect.Min.Y+1), colorValue)
	fillRect(img, image.Rect(rect.Min.X, rect.Max.Y-1, rect.Max.X, rect.Max.Y), colorValue)
	fillRect(img, image.Rect(rect.Min.X, rect.Min.Y, rect.Min.X+1, rect.Max.Y), colorValue)
	fillRect(img, image.Rect(rect.Max.X-1, rect.Min.Y, rect.Max.X, rect.Max.Y), colorValue)
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func clampFloat(value float64, minValue float64, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
