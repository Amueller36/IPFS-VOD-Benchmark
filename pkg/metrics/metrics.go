package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Tracker struct {
	start           time.Time
	firstByte       time.Duration
	segmentTimes    []time.Duration
	segmentBytes    []int
	cacheHitSamples int
	cacheSamples    int

	playbackInterval time.Duration
	playbackSegments int
	segmentReady     map[int]time.Duration
}

func NewTracker() *Tracker {
	return &Tracker{
		start:        time.Now(),
		segmentReady: make(map[int]time.Duration),
	}
}

func (t *Tracker) SetPlaybackModel(segmentCount int, playbackInterval time.Duration) {
	if segmentCount <= 0 || playbackInterval <= 0 {
		t.playbackSegments = 0
		t.playbackInterval = 0
		return
	}
	t.playbackSegments = segmentCount
	t.playbackInterval = playbackInterval
}

func (t *Tracker) RecordSegmentReady(index int, readyAt time.Duration) {
	if index < 0 {
		return
	}
	if existing, ok := t.segmentReady[index]; ok {
		if existing > 0 && readyAt >= existing {
			return
		}
	}
	t.segmentReady[index] = readyAt
}

func (t *Tracker) RecordSegment(duration time.Duration, bytes int, cacheHit bool) {
	if len(t.segmentTimes) == 0 {
		t.firstByte = time.Since(t.start)
	}
	if cacheHit {
		t.cacheHitSamples++
	}
	t.cacheSamples++
	t.segmentTimes = append(t.segmentTimes, duration)
	t.segmentBytes = append(t.segmentBytes, bytes)
}

func (t *Tracker) Summary() Summary {
	end := time.Now()
	totalBytes := int64(0)
	for _, size := range t.segmentBytes {
		totalBytes += int64(size)
	}
	totalDuration := end.Sub(t.start)
	avgSegment := averageDuration(t.segmentTimes)
	throughput := float64(totalBytes) / totalDuration.Seconds()
	cacheHitRate := 0.0
	if t.cacheSamples > 0 {
		cacheHitRate = float64(t.cacheHitSamples) / float64(t.cacheSamples)
	}
	qoe := t.computeQoE(totalDuration)
	return Summary{
		TTFB:                  t.firstByte,
		StallCount:            qoe.stallCount,
		StallDurations:        qoe.stallDurations,
		AvgSegmentFetch:       avgSegment,
		CacheHitRate:          cacheHitRate,
		TotalTime:             totalDuration,
		TotalBytes:            totalBytes,
		Throughput:            throughput,
		StartupDelay:          qoe.startupDelay,
		TotalStallTime:        qoe.stallTime,
		StallRatio:            qoe.stallRatio,
		PlaybackOverheadRatio: qoe.playbackOverheadRatio,
		DeadlineMissRate:      qoe.deadlineMissRate,
		SegmentReadyP50:       qoe.readyP50,
		SegmentReadyP95:       qoe.readyP95,
		SegmentLatenessP50:    qoe.latenessP50,
		SegmentLatenessP95:    qoe.latenessP95,
		PlaybackDuration:      qoe.playbackDuration,
		PlaybackCompletion:    qoe.playbackCompletion,
	}
}

type qoeSummary struct {
	startupDelay          time.Duration
	stallCount            int
	stallDurations        []time.Duration
	stallTime             time.Duration
	stallRatio            float64
	playbackOverheadRatio float64
	deadlineMissRate      float64
	readyP50              time.Duration
	readyP95              time.Duration
	latenessP50           time.Duration
	latenessP95           time.Duration
	playbackDuration      time.Duration
	playbackCompletion    time.Duration
}

func (t *Tracker) computeQoE(totalDuration time.Duration) qoeSummary {
	if t.playbackSegments <= 0 || t.playbackInterval <= 0 {
		return qoeSummary{}
	}
	playbackDuration := time.Duration(t.playbackSegments) * t.playbackInterval
	if playbackDuration <= 0 {
		return qoeSummary{}
	}

	startupDelay := totalDuration
	if ready0, ok := t.segmentReady[0]; ok && ready0 >= 0 {
		startupDelay = ready0
	}
	if startupDelay < 0 {
		startupDelay = 0
	}
	if startupDelay > totalDuration {
		startupDelay = totalDuration
	}

	readyValues := make([]time.Duration, 0, len(t.segmentReady))
	for _, ready := range t.segmentReady {
		if ready >= 0 {
			readyValues = append(readyValues, ready)
		}
	}
	sort.Slice(readyValues, func(i, j int) bool { return readyValues[i] < readyValues[j] })

	playbackClock := startupDelay
	stallCount := 0
	stallTime := time.Duration(0)
	stallDurations := make([]time.Duration, 0)
	deadlineMisses := 0
	latenessValues := make([]time.Duration, 0, t.playbackSegments)
	inStall := false

	for i := 0; i < t.playbackSegments; i++ {
		deadline := startupDelay + time.Duration(i)*t.playbackInterval
		ready, ok := t.segmentReady[i]
		lateness := time.Duration(0)
		if !ok {
			deadlineMisses++
			ready = totalDuration
		} else if ready > deadline {
			deadlineMisses++
		}
		if ready > deadline {
			lateness = ready - deadline
		}
		latenessValues = append(latenessValues, lateness)

		if ready > playbackClock {
			wait := ready - playbackClock
			stallTime += wait
			if !inStall {
				stallCount++
				stallDurations = append(stallDurations, wait)
				inStall = true
			} else {
				stallDurations[len(stallDurations)-1] += wait
			}
			playbackClock = ready
		} else {
			inStall = false
		}
		playbackClock += t.playbackInterval
	}

	q := qoeSummary{
		startupDelay:          startupDelay,
		stallCount:            stallCount,
		stallDurations:        stallDurations,
		stallTime:             stallTime,
		stallRatio:            float64(stallTime) / float64(playbackDuration),
		playbackOverheadRatio: float64(playbackClock-playbackDuration) / float64(playbackDuration),
		deadlineMissRate:      float64(deadlineMisses) / float64(t.playbackSegments),
		playbackDuration:      playbackDuration,
		playbackCompletion:    playbackClock,
	}
	if len(readyValues) > 0 {
		q.readyP50 = percentileDuration(readyValues, 0.50)
		q.readyP95 = percentileDuration(readyValues, 0.95)
	}
	if len(latenessValues) > 0 {
		sort.Slice(latenessValues, func(i, j int) bool { return latenessValues[i] < latenessValues[j] })
		q.latenessP50 = percentileDuration(latenessValues, 0.50)
		q.latenessP95 = percentileDuration(latenessValues, 0.95)
	}
	return q
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	idx := int(p * float64(len(values)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

type Summary struct {
	TTFB            time.Duration   `json:"ttfb"`
	StallCount      int             `json:"stallCount"`
	StallDurations  []time.Duration `json:"stallDurations,omitempty"`
	AvgSegmentFetch time.Duration   `json:"avgSegmentFetch"`
	CacheHitRate    float64         `json:"cacheHitRate"`
	TotalTime       time.Duration   `json:"totalTime"`
	TotalBytes      int64           `json:"totalBytes"`
	Throughput      float64         `json:"throughputBytesPerSec"`

	StartupDelay          time.Duration `json:"startupDelay"`
	TotalStallTime        time.Duration `json:"totalStallTime"`
	StallRatio            float64       `json:"stallRatio"`
	PlaybackOverheadRatio float64       `json:"playbackOverheadRatio"`
	DeadlineMissRate      float64       `json:"deadlineMissRate"`
	SegmentReadyP50       time.Duration `json:"segmentReadyP50"`
	SegmentReadyP95       time.Duration `json:"segmentReadyP95"`
	SegmentLatenessP50    time.Duration `json:"segmentLatenessP50"`
	SegmentLatenessP95    time.Duration `json:"segmentLatenessP95"`
	PlaybackDuration      time.Duration `json:"playbackDuration"`
	PlaybackCompletion    time.Duration `json:"playbackCompletion"`
}

func (s Summary) JSON() (string, error) {
	payload := map[string]interface{}{
		"ttfb":                  s.TTFB,
		"ttfbSec":               formatSeconds(s.TTFB),
		"stallCount":            s.StallCount,
		"playbackStallCount":    s.StallCount,
		"stallDurations":        s.StallDurations,
		"stallDurationsSec":     formatDurationSecondsSlice(s.StallDurations),
		"avgSegmentFetch":       s.AvgSegmentFetch,
		"avgSegmentFetchSec":    formatSeconds(s.AvgSegmentFetch),
		"cacheHitRate":          s.CacheHitRate,
		"totalTime":             s.TotalTime,
		"totalTimeSec":          formatSeconds(s.TotalTime),
		"totalBytes":            s.TotalBytes,
		"totalMB":               formatBytesMB(s.TotalBytes),
		"throughputBytesPerSec": s.Throughput,
		"throughputMBPerSec":    formatRateMB(s.Throughput),
		"startupDelay":          s.StartupDelay,
		"startupDelaySec":       formatSeconds(s.StartupDelay),
		"totalStallTime":        s.TotalStallTime,
		"totalStallTimeSec":     formatSeconds(s.TotalStallTime),
		"stallRatio":            s.StallRatio,
		"playbackOverheadRatio": s.PlaybackOverheadRatio,
		"deadlineMissRate":      s.DeadlineMissRate,
		"segmentReadyP50":       s.SegmentReadyP50,
		"segmentReadyP50Sec":    formatSeconds(s.SegmentReadyP50),
		"segmentReadyP95":       s.SegmentReadyP95,
		"segmentReadyP95Sec":    formatSeconds(s.SegmentReadyP95),
		"segmentLatenessP50":    s.SegmentLatenessP50,
		"segmentLatenessP50Sec": formatSeconds(s.SegmentLatenessP50),
		"segmentLatenessP95":    s.SegmentLatenessP95,
		"segmentLatenessP95Sec": formatSeconds(s.SegmentLatenessP95),
		"playbackDuration":      s.PlaybackDuration,
		"playbackDurationSec":   formatSeconds(s.PlaybackDuration),
		"playbackCompletion":    s.PlaybackCompletion,
		"playbackCompletionSec": formatSeconds(s.PlaybackCompletion),
	}
	output, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (s Summary) CSV() (string, error) {
	builder := &strings.Builder{}
	writer := csv.NewWriter(builder)
	records := [][]string{
		{"ttfb_ms", "stall_count", "stall_durations_ms", "avg_segment_ms", "cache_hit_rate", "total_time_ms", "total_bytes", "throughput_bytes_per_sec", "startup_delay_ms", "stall_time_ms", "stall_ratio", "playback_overhead_ratio", "deadline_miss_rate", "segment_ready_p50_ms", "segment_ready_p95_ms", "segment_lateness_p50_ms", "segment_lateness_p95_ms", "playback_duration_ms", "playback_completion_ms"},
		{
			fmt.Sprintf("%.2f", s.TTFB.Seconds()*1000),
			fmt.Sprintf("%d", s.StallCount),
			formatDurationMillisList(s.StallDurations),
			fmt.Sprintf("%.2f", s.AvgSegmentFetch.Seconds()*1000),
			fmt.Sprintf("%.4f", s.CacheHitRate),
			fmt.Sprintf("%.2f", s.TotalTime.Seconds()*1000),
			fmt.Sprintf("%d", s.TotalBytes),
			fmt.Sprintf("%.2f", s.Throughput),
			fmt.Sprintf("%.2f", s.StartupDelay.Seconds()*1000),
			fmt.Sprintf("%.2f", s.TotalStallTime.Seconds()*1000),
			fmt.Sprintf("%.4f", s.StallRatio),
			fmt.Sprintf("%.4f", s.PlaybackOverheadRatio),
			fmt.Sprintf("%.4f", s.DeadlineMissRate),
			fmt.Sprintf("%.2f", s.SegmentReadyP50.Seconds()*1000),
			fmt.Sprintf("%.2f", s.SegmentReadyP95.Seconds()*1000),
			fmt.Sprintf("%.2f", s.SegmentLatenessP50.Seconds()*1000),
			fmt.Sprintf("%.2f", s.SegmentLatenessP95.Seconds()*1000),
			fmt.Sprintf("%.2f", s.PlaybackDuration.Seconds()*1000),
			fmt.Sprintf("%.2f", s.PlaybackCompletion.Seconds()*1000),
		},
	}
	if err := writer.WriteAll(records); err != nil {
		return "", err
	}
	writer.Flush()
	return builder.String(), nil
}

func averageDuration(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	total := time.Duration(0)
	for _, value := range values {
		total += value
	}
	return total / time.Duration(len(values))
}

func formatSeconds(value time.Duration) float64 {
	return float64(value) / float64(time.Second)
}

func formatDurationSecondsSlice(values []time.Duration) []float64 {
	output := make([]float64, 0, len(values))
	for _, value := range values {
		output = append(output, formatSeconds(value))
	}
	return output
}

func formatDurationMillisList(values []time.Duration) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%.2f", value.Seconds()*1000))
	}
	return strings.Join(parts, ";")
}

func formatBytesMB(value int64) float64 {
	return float64(value) / (1024 * 1024)
}

func formatRateMB(value float64) float64 {
	return value / (1024 * 1024)
}
