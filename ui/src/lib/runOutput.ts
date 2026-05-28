import type { RunOutputLabels } from '../i18n';
import type { RunStatusResponse } from '../types/api';

export interface SeedMetadata {
  segments: number;
  segmentSize: number;
  chunkSize: number;
}

export interface PlaybackSettings {
  playbackMs: number;
  playbackSpeed: number;
  urgentWindow: number;
  playbackIntervalNs?: number;
}

export interface ThroughputSeries {
  peerKey: string;
  label: string;
  color: string;
  data: Array<{ x: string; y: number }>;
}

export interface PeerMetricItem {
  key: string;
  label: string;
  color: string;
  value: number;
}

export interface HeatmapLegendItem {
  key: string;
  label: string;
  color: string;
  accent?: boolean;
  band?: boolean;
}

export interface ParsedRunOutput {
  throughput: {
    labels: string[];
    datasets: ThroughputSeries[];
  };
  peerMetric: {
    label: string;
    items: PeerMetricItem[];
  };
  heatmap: {
    segments: number;
    chunksPerSegment: number;
    playbackIndex: number;
    playbackPosition: number;
    urgentWindow: number;
    urgentDecisionSegments: Set<number>;
    inFlightSegments: Set<number>;
    fetchedChunks: Map<number, Set<number>>;
    segmentPeer: Map<number, string>;
    legend: HeatmapLegendItem[];
  };
  summary: {
    stallCount: number | null;
    totalStallTimeSec: number | null;
    startupDelaySec: number | null;
    playbackCompletionSec: number | null;
    downloadTimeSec: number | null;
  };
}

const heatmapColors = {
  pending: '#e6e2dc',
  fetching: '#8b5cf6',
  prefetched: '#4a90e2',
  prefetchAccent: '#2f4858',
  played: '#7eb77f',
  urgent: 'rgba(220, 53, 69, 0.25)',
  urgentDecision: '#9f2d2d'
} as const;

const peerColors = {
  fast: ['#1f77b4', '#4a90e2'],
  slow: ['#ff7f0e', '#ffa64d', '#ffbf80']
} as const;

const customPeerColors = ['#2ca02c', '#9467bd', '#8c564b', '#17becf', '#bcbd22', '#7f7f7f'];
const fallbackThroughputColor = '#8c564b';
const defaultRunOutputLabels: RunOutputLabels = {
  segmentsFetched: 'Segmente geladen',
  dataSentMb: 'Gesendete Daten (MB)',
  pending: 'Ausstehend',
  fetching: 'Wird geladen',
  prefetched: 'Vorgeladen (Akzent)',
  played: 'Abgespielt (Band)',
  urgentWindow: 'Dringendes Fenster',
  scheduledUrgent: 'Dringend geplant'
};

interface RunSummaryEvent {
  totalTime?: number;
  totalBytes?: number;
  throughputBytesPerSec?: number;
  playbackCompletion?: number;
  stallCount?: number;
  totalStallTime?: number;
  startupDelay?: number;
}

function bytesPerSecToMbitPerSec(value: number): number {
  return (value * 8) / 1_000_000;
}

function peerColorGroup(label: string): keyof typeof peerColors | null {
  if (label === 'fast') {
    return 'fast';
  }
  if (label === 'slow' || label.startsWith('slow-')) {
    return 'slow';
  }
  return null;
}

function formatPeerDisplayLabel(peerId: string, label: string): string {
  if (!peerId || !label) {
    return peerId;
  }
  const kuboHostMatch = peerId.match(/^kubo-(fast|slow)-(.+)$/);
  if (kuboHostMatch) {
    return `${kuboHostMatch[1]}-${kuboHostMatch[2]}`;
  }
  const suffix = peerId.length > 6 ? peerId.slice(-6) : peerId;
  return `${label} ${suffix}`;
}

function isClientProvider(hostOrPeer: string, label?: string, clientHost?: string, clientPeerId?: string): boolean {
  const value = (hostOrPeer || '').toLowerCase();
  const labelValue = (label || '').toLowerCase();
  return (
    labelValue === 'client' ||
    value.includes('client') ||
    (!!clientHost && hostOrPeer === clientHost) ||
    (!!clientPeerId && hostOrPeer === clientPeerId)
  );
}

function parseDurationSeconds(value: string): number {
  const match = value.match(/^([0-9.]+)(ns|us|µs|ms|s)$/);
  if (!match) {
    return 0;
  }
  const amount = Number.parseFloat(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) {
    return 0;
  }
  switch (match[2]) {
    case 'ns':
      return amount / 1e9;
    case 'us':
    case 'µs':
      return amount / 1e6;
    case 'ms':
      return amount / 1e3;
    default:
      return amount;
  }
}

function formatSampleLabel(timeNs: number): string {
  return new Date(timeNs / 1e6).toLocaleTimeString();
}

function finiteDurationNs(value: unknown): number | null {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return null;
  }
  return value;
}

function finitePositiveNumber(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  return numeric;
}

function finiteNonNegativeInteger(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  if (!Number.isFinite(numeric) || numeric < 0) {
    return null;
  }
  return Math.floor(numeric);
}

function graphsyncConfigPlayback(output: string): { playbackDelayNs: number | null; urgentWindow: number | null } {
  for (const line of output.split('\n')) {
    if (!line.startsWith('{') || !line.includes('"scheduler_config"')) {
      continue;
    }
    try {
      const sample = JSON.parse(line) as { type?: string; playbackDelayNs?: unknown; urgentWindow?: unknown };
      if (sample.type !== 'scheduler_config') {
        continue;
      }
      return {
        playbackDelayNs: finitePositiveNumber(sample.playbackDelayNs),
        urgentWindow: finiteNonNegativeInteger(sample.urgentWindow)
      };
    } catch {
      continue;
    }
  }
  return { playbackDelayNs: null, urgentWindow: null };
}

function elapsedFromStatus(status: RunStatusResponse | null, nowMs = Date.now()): number | null {
  if (!status?.startedAt) {
    return null;
  }
  const startedAt = Date.parse(status.startedAt);
  if (!Number.isFinite(startedAt)) {
    return null;
  }
  const endedAt = status.endedAt ? Date.parse(status.endedAt) : NaN;
  const endMs = Number.isFinite(endedAt) ? endedAt : nowMs;
  if (endMs < startedAt) {
    return null;
  }
  return (endMs - startedAt) * 1e6;
}

function elapsedSinceUnixNs(startNs: number | null, nowMs = Date.now()): number | null {
  if (startNs === null || !Number.isFinite(startNs)) {
    return null;
  }
  const nowNs = nowMs * 1e6;
  if (!Number.isFinite(nowNs) || nowNs < startNs) {
    return null;
  }
  return nowNs - startNs;
}

interface PlaybackProjection {
  index: number;
  position: number;
}

function clampPlaybackPosition(value: number, segmentCount: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(segmentCount, value));
}

function computeOrderedPlaybackProjection(
  readyTimesNs: Map<number, number>,
  segmentCount: number,
  playbackIntervalNs: number,
  currentElapsedNs: number
): PlaybackProjection {
  if (segmentCount <= 0 || playbackIntervalNs <= 0 || currentElapsedNs < 0) {
    return { index: 0, position: 0 };
  }
  const firstReady = readyTimesNs.get(0);
  if (firstReady === undefined) {
    return { index: 0, position: 0 };
  }

  let playbackClock = Math.max(0, firstReady);
  let played = 0;
  for (let segment = 0; segment < segmentCount; segment += 1) {
    const readyAt = readyTimesNs.get(segment);
    if (readyAt === undefined) {
      break;
    }
    if (readyAt > playbackClock) {
      if (currentElapsedNs < readyAt) {
        return { index: played, position: played };
      }
      playbackClock = readyAt;
    }
    const consumedAt = playbackClock + playbackIntervalNs;
    if (currentElapsedNs < consumedAt) {
      const fraction = (currentElapsedNs - playbackClock) / playbackIntervalNs;
      return {
        index: played,
        position: clampPlaybackPosition(played + Math.max(0, fraction), segmentCount)
      };
    }
    played += 1;
    playbackClock = consumedAt;
  }
  return { index: played, position: played };
}

function playbackIntervalNs(playback: PlaybackSettings): number {
  if (playback.playbackIntervalNs !== undefined && Number.isFinite(playback.playbackIntervalNs) && playback.playbackIntervalNs > 0) {
    return playback.playbackIntervalNs;
  }
  const speed = playback.playbackSpeed > 0 ? playback.playbackSpeed : 1;
  return (playback.playbackMs * 1e6) / speed;
}

export function getHeatmapColors(): typeof heatmapColors {
  return heatmapColors;
}

export function resolvePlaybackSettings(output: string, status: RunStatusResponse | null, fallback: PlaybackSettings): PlaybackSettings {
  const schedulerConfig = graphsyncConfigPlayback(output);
  if (schedulerConfig.playbackDelayNs !== null) {
    return {
      playbackMs: schedulerConfig.playbackDelayNs / 1e6,
      playbackSpeed: 1,
      playbackIntervalNs: schedulerConfig.playbackDelayNs,
      urgentWindow: schedulerConfig.urgentWindow ?? fallback.urgentWindow
    };
  }

  const params = status?.params;
  const runPlaybackMs = finitePositiveNumber(params?.playbackMs);
  if (runPlaybackMs !== null) {
    const mode = typeof params?.mode === 'string' ? params.mode : '';
    return {
      playbackMs: runPlaybackMs,
      playbackSpeed: finitePositiveNumber(params?.playbackSpeed) ?? 1,
      playbackIntervalNs: runPlaybackMs * 1e6,
      urgentWindow: mode === 'bitswap' ? 0 : finiteNonNegativeInteger(params?.urgentWindow) ?? fallback.urgentWindow
    };
  }

  return fallback;
}

export function parseRunOutput(
  output: string,
  status: RunStatusResponse | null,
  seed: SeedMetadata,
  playback: PlaybackSettings,
  nowMs = Date.now(),
  outputLabels: RunOutputLabels = defaultRunOutputLabels
): ParsedRunOutput {
  const chunksPerSegment = seed.segmentSize > 0 && seed.chunkSize > 0 ? Math.ceil(seed.segmentSize / seed.chunkSize) : 0;
  const inFlightSegments = new Set<number>();
  const urgentDecisionSegments = new Set<number>();
  const fetchedChunks = new Map<number, Set<number>>();
  const segmentPeer = new Map<number, string>();
  const peerSegmentCounts = new Map<string, number>();
  const peerBytesSent = new Map<string, number>();
  const bitswapBlockBytes = new Map<string, number>();
  const bitswapProviderLast = new Map<string, { bytes: number; timeNs: number }>();
  const bitswapHostToPeer = new Map<string, string>();
  const bitswapSegmentProviderBytes = new Map<number, Map<string, number>>();
  const bitswapSegmentProviderOrder = new Map<number, string[]>();
  const bitswapProviderLabels = new Map<string, string>();
  const peerColorMap = new Map<string, string>();
  const peerLabelMap = new Map<string, string>();
  const datasets = new Map<string, ThroughputSeries>();
  const labels: string[] = [];
  const labelSet = new Set<string>();
  const peerPaletteIndex = { fast: 0, slow: 0 };
  let customPeerPaletteIndex = 0;
  let schedulerPlaybackIndex = 0;
  let playbackIndex = 0;
  let playbackPosition = 0;
  let peerMetricLabel = outputLabels.segmentsFetched;
  let latestBitswapElapsedNs = 0;
  let latestGraphsyncElapsedNs = 0;
  let summary: RunSummaryEvent | null = null;
  const bitswapReadyTimesNs = new Map<number, number>();
  const graphsyncReadyTimesNs = new Map<number, number>();
  let graphsyncRunStartNs: number | null = null;
  let graphsyncPlaybackIntervalNs: number | null = null;

  function getPeerDisplayLabel(peerId: string): string {
    return peerLabelMap.get(peerId) ?? peerId;
  }

  function markChunkFetched(segmentIndex: number, chunkIndex: number): void {
    if (!fetchedChunks.has(segmentIndex)) {
      fetchedChunks.set(segmentIndex, new Set<number>());
    }
    fetchedChunks.get(segmentIndex)?.add(chunkIndex);
  }

  function markSegmentFetched(segmentIndex: number): void {
    if (!chunksPerSegment) {
      return;
    }
    inFlightSegments.delete(segmentIndex);
    const chunks = new Set<number>();
    for (let i = 0; i < chunksPerSegment; i += 1) {
      chunks.add(i);
    }
    fetchedChunks.set(segmentIndex, chunks);
  }

  function syncPeerDataset(peerKey: string): void {
    const dataset = datasets.get(peerKey);
    if (dataset) {
      dataset.label = getPeerDisplayLabel(peerKey);
      dataset.color = peerColorMap.get(peerKey) ?? dataset.color;
    }
  }

  function moveMapNumberValue(map: Map<string, number>, fromKey: string, toKey: string): void {
    if (!fromKey || !toKey || fromKey === toKey || !map.has(fromKey)) {
      return;
    }
    const fromValue = map.get(fromKey) ?? 0;
    const toValue = map.get(toKey);
    map.set(toKey, toValue === undefined ? fromValue : Math.max(toValue, fromValue));
    map.delete(fromKey);
  }

  function migrateProviderHostKey(host: string, peerId: string, label: string): void {
    if (!host || !peerId || host === peerId) {
      return;
    }
    moveMapNumberValue(peerBytesSent, host, peerId);
    moveMapNumberValue(bitswapBlockBytes, host, peerId);
    const previous = bitswapProviderLast.get(host);
    if (previous && !bitswapProviderLast.has(peerId)) {
      bitswapProviderLast.set(peerId, previous);
    }
    bitswapProviderLast.delete(host);
    if (peerColorMap.has(host) && !peerColorMap.has(peerId)) {
      peerColorMap.set(peerId, peerColorMap.get(host) ?? fallbackThroughputColor);
    }
    assignPeerColor(peerId, label || 'slow');
    peerColorMap.delete(host);
    peerLabelMap.delete(host);
    syncPeerDataset(peerId);
  }

  function assignPeerColor(peerId: string, label: string): string {
    const displayLabel = formatPeerDisplayLabel(peerId, label);
    if (displayLabel && peerLabelMap.get(peerId) !== displayLabel) {
      peerLabelMap.set(peerId, displayLabel);
    }
    let color = peerColorMap.get(peerId);
    if (!color) {
      const group = peerColorGroup(label);
      if (group) {
        const palette = peerColors[group];
        const index = peerPaletteIndex[group] % palette.length;
        peerPaletteIndex[group] += 1;
        color = palette[index];
      } else {
        color = customPeerColors[customPeerPaletteIndex % customPeerColors.length];
        customPeerPaletteIndex += 1;
      }
      peerColorMap.set(peerId, color);
    }
    syncPeerDataset(peerId);
    return color;
  }

  function providerMetricKey(host: string, peerId: string | undefined, label: string): string {
    const normalizedPeerId = (peerId || '').trim();
    if (host && normalizedPeerId) {
      bitswapHostToPeer.set(host, normalizedPeerId);
      migrateProviderHostKey(host, normalizedPeerId, label);
    }
    const key = normalizedPeerId || bitswapHostToPeer.get(host) || host;
    if (key && label) {
      assignPeerColor(key, label);
    }
    return key;
  }

  function markBitswapProvider(providerKey: string, label: string, cumulativeBytes: number, displayPeerId?: string): void {
    if (!providerKey || isClientProvider(providerKey, label) || !Number.isFinite(cumulativeBytes) || cumulativeBytes < 0) {
      return;
    }
    assignPeerColor(providerKey, label || 'slow');
    if (displayPeerId) {
      peerLabelMap.set(providerKey, formatPeerDisplayLabel(displayPeerId, label || 'slow'));
      syncPeerDataset(providerKey);
    }
    const mbSent = cumulativeBytes / (1024 * 1024);
    peerBytesSent.set(providerKey, Math.max(peerBytesSent.get(providerKey) ?? 0, mbSent));
    peerMetricLabel = outputLabels.dataSentMb;
  }

  function markSegmentPeer(segmentIndex: number, peerId: string, label: string): void {
    if (!peerId || segmentPeer.has(segmentIndex)) {
      return;
    }
    segmentPeer.set(segmentIndex, peerId);
    peerSegmentCounts.set(peerId, (peerSegmentCounts.get(peerId) ?? 0) + 1);
    assignPeerColor(peerId, label || 'slow');
  }

  function validSegmentIndex(segmentIndex: number): boolean {
    return Number.isInteger(segmentIndex) && segmentIndex >= 0 && segmentIndex < seed.segments;
  }

  function addBitswapSegmentProviderBytes(
    segmentIndexes: number[] | undefined,
    providerKey: string,
    providerLabel: string,
    bytes: number
  ): void {
    if (!providerKey || !Array.isArray(segmentIndexes) || segmentIndexes.length === 0) {
      return;
    }
    bitswapProviderLabels.set(providerKey, providerLabel || 'slow');
    const validSegments = segmentIndexes.filter(validSegmentIndex);
    if (validSegments.length === 0) {
      return;
    }
    const totalBytes = Number.isFinite(bytes) && bytes > 0 ? bytes : 1;
    const bytesPerSegment = Math.max(1, Math.floor(totalBytes / validSegments.length));
    for (const segmentIndex of validSegments) {
      let providerBytes = bitswapSegmentProviderBytes.get(segmentIndex);
      if (!providerBytes) {
        providerBytes = new Map<string, number>();
        bitswapSegmentProviderBytes.set(segmentIndex, providerBytes);
      }
      if (!providerBytes.has(providerKey)) {
        const providerOrder = bitswapSegmentProviderOrder.get(segmentIndex) ?? [];
        providerOrder.push(providerKey);
        bitswapSegmentProviderOrder.set(segmentIndex, providerOrder);
      }
      providerBytes.set(providerKey, (providerBytes.get(providerKey) ?? 0) + bytesPerSegment);
    }
  }

  function finalizeBitswapSegmentPeers(): void {
    for (const [segmentIndex, providerBytes] of bitswapSegmentProviderBytes.entries()) {
      if (segmentPeer.has(segmentIndex)) {
        continue;
      }
      let bestProvider = '';
      let bestBytes = 0;
      for (const providerKey of bitswapSegmentProviderOrder.get(segmentIndex) ?? []) {
        const bytes = providerBytes.get(providerKey) ?? 0;
        if (!bestProvider || bytes > bestBytes) {
          bestProvider = providerKey;
          bestBytes = bytes;
        }
      }
      if (bestProvider) {
        markSegmentPeer(segmentIndex, bestProvider, bitswapProviderLabels.get(bestProvider) ?? 'slow');
      }
    }
  }

  function updateThroughput(peerKey: string, ema: number, timeNs: number, labelOverride?: string): void {
    const pointLabel = labelOverride || formatSampleLabel(timeNs);
    if (!labelSet.has(pointLabel)) {
      labelSet.add(pointLabel);
      labels.push(pointLabel);
    }
    let dataset = datasets.get(peerKey);
    if (!dataset) {
      dataset = {
        peerKey,
        label: getPeerDisplayLabel(peerKey),
        color: peerColorMap.get(peerKey) ?? fallbackThroughputColor,
        data: []
      };
      datasets.set(peerKey, dataset);
    } else if (peerColorMap.has(peerKey)) {
      dataset.color = peerColorMap.get(peerKey) ?? dataset.color;
      dataset.label = getPeerDisplayLabel(peerKey);
    }
    dataset.data.push({ x: pointLabel, y: bytesPerSecToMbitPerSec(ema) });
  }

  for (const line of output.split('\n')) {
    if (!line) {
      continue;
    }

    const connectedPeerMatch = line.match(/^\-\s+(\S+)\s+(\S+)/);
    if (connectedPeerMatch) {
      const peerId = connectedPeerMatch[1];
      const label = connectedPeerMatch[2];
      assignPeerColor(peerId, label);
      continue;
    }

    if (line.startsWith('segment ')) {
      const segmentStatsMatch = line.match(/segment\s+(\d+)\s+fetched\s+bytes=(\d+)\s+duration=([0-9.]+(?:ns|us|µs|ms|s))/);
      if (segmentStatsMatch) {
        const bytes = Number.parseInt(segmentStatsMatch[2], 10);
        const durationSec = parseDurationSeconds(segmentStatsMatch[3]);
        if (Number.isFinite(bytes) && durationSec > 0) {
          updateThroughput('bitswap', bytes / durationSec, Date.now() * 1e6, 'cat window');
        }
      }
      const fetchedMatch = line.match(/segment\s+(\d+)\s+fetched/);
      if (fetchedMatch) {
        markSegmentFetched(Number.parseInt(fetchedMatch[1], 10));
      }
      const peerMatch = line.match(/segment\s+(\d+)\s+fetched\s+peer=([a-zA-Z0-9]+)\s+label=(\S+)/);
      if (peerMatch) {
        markSegmentPeer(Number.parseInt(peerMatch[1], 10), peerMatch[2], peerMatch[3]);
      }
    }

    if (line.startsWith('chunk ')) {
      const match = line.match(/chunk\s+seg=(\d+)\s+idx=(\d+)\s+bytes=(\d+)/);
      if (match) {
        markChunkFetched(Number.parseInt(match[1], 10), Number.parseInt(match[2], 10));
      }
    }

    if (line.startsWith('SUMMARY_JSON ')) {
      try {
        summary = JSON.parse(line.slice('SUMMARY_JSON '.length)) as RunSummaryEvent;
      } catch {
        // Ignore malformed summaries and keep live projection from readiness events.
      }
      continue;
    }

    if (!line.startsWith('{')) {
      continue;
    }
    try {
      const sample = JSON.parse(line) as {
        type?: string;
        peer?: string;
        ema?: number;
        timeNs?: number;
        segment?: number;
        playbackIndex?: number;
        peerLabel?: string;
        peers?: Array<{ peerId?: string; label?: string }>;
        candidates?: Array<{ peerId?: string; label?: string }>;
        selectedPeer?: string;
        selectedLabel?: string;
        success?: boolean;
        providerHost?: string;
        providerPeerId?: string;
        providerLabel?: string;
        clientHost?: string;
        clientPeerId?: string;
        windowBytesSentToClient?: number;
        cumulativeBytesSentToClient?: number;
        protocol?: string;
        readyTimeNs?: number;
        cid?: string;
        peerId?: string;
        bytes?: number;
        arrivalTimeNs?: number;
        virtualSegments?: number[];
        urgent?: boolean;
        urgentWindow?: number;
        playbackDelayNs?: number;
      };
      if (sample.type === 'scheduler_config' && typeof sample.timeNs === 'number' && Number.isFinite(sample.timeNs)) {
        graphsyncRunStartNs = sample.timeNs;
      }
      if (sample.type === 'scheduler_config') {
        graphsyncPlaybackIntervalNs = finitePositiveNumber(sample.playbackDelayNs) ?? graphsyncPlaybackIntervalNs;
      }
      if (
        sample.type === 'bitswap_virtual_segment_ready' &&
        sample.protocol === 'bitswap' &&
        typeof sample.segment === 'number' &&
        Number.isInteger(sample.segment) &&
        sample.segment >= 0
      ) {
        markSegmentFetched(sample.segment);
        const readyTimeNs = finiteDurationNs(sample.readyTimeNs);
        if (readyTimeNs !== null) {
          const currentReadyTime = bitswapReadyTimesNs.get(sample.segment);
          if (currentReadyTime === undefined || readyTimeNs < currentReadyTime) {
            bitswapReadyTimesNs.set(sample.segment, readyTimeNs);
          }
          latestBitswapElapsedNs = Math.max(latestBitswapElapsedNs, readyTimeNs);
        }
      }
      if (sample.type === 'bitswap_block_received' && sample.protocol === 'bitswap') {
        if (isClientProvider(sample.providerHost || sample.peerId || '', sample.providerLabel, sample.clientHost, sample.clientPeerId)) {
          continue;
        }
        const providerLabel = sample.providerLabel || ((sample.providerHost || sample.peerId || '').includes('fast') ? 'fast' : 'slow');
        const providerKey = providerMetricKey(sample.providerHost || '', sample.providerPeerId || sample.peerId, providerLabel);
        addBitswapSegmentProviderBytes(sample.virtualSegments, providerKey, providerLabel, sample.bytes ?? 0);
        if (typeof sample.bytes === 'number' && Number.isFinite(sample.bytes) && sample.bytes > 0) {
          const cumulativeBytes = (bitswapBlockBytes.get(providerKey) ?? 0) + sample.bytes;
          bitswapBlockBytes.set(providerKey, cumulativeBytes);
          markBitswapProvider(providerKey, providerLabel, cumulativeBytes);
          if (typeof sample.timeNs === 'number') {
            const previous = bitswapProviderLast.get(providerKey);
            if (previous && sample.timeNs > previous.timeNs) {
              const deltaBytes = cumulativeBytes - previous.bytes;
              const deltaSec = (sample.timeNs - previous.timeNs) / 1e9;
              if (deltaBytes > 0 && deltaSec > 0) {
                assignPeerColor(providerKey, providerLabel);
                updateThroughput(providerKey, deltaBytes / deltaSec, sample.timeNs);
              }
            }
            bitswapProviderLast.set(providerKey, { bytes: cumulativeBytes, timeNs: sample.timeNs });
          }
        }
      }
      if (typeof sample.playbackIndex === 'number' && Number.isFinite(sample.playbackIndex)) {
        schedulerPlaybackIndex = Math.max(schedulerPlaybackIndex, sample.playbackIndex);
      }
      if (sample.type === 'scheduler_config' && Array.isArray(sample.peers)) {
        for (const peer of sample.peers) {
          if (peer.peerId && peer.label) {
            assignPeerColor(peer.peerId, peer.label);
          }
        }
      }
      if (sample.type === 'scheduler_decision' && typeof sample.segment === 'number') {
        inFlightSegments.add(sample.segment);
        if (sample.urgent) {
          urgentDecisionSegments.add(sample.segment);
        }
        if (Array.isArray(sample.candidates)) {
          for (const candidate of sample.candidates) {
            if (candidate.peerId && candidate.label) {
              assignPeerColor(candidate.peerId, candidate.label);
            }
          }
        }
        if (sample.selectedPeer && sample.selectedLabel) {
          assignPeerColor(sample.selectedPeer, sample.selectedLabel);
        }
      }
      if (sample.type === 'scheduler_result' && typeof sample.segment === 'number') {
        inFlightSegments.delete(sample.segment);
        if (sample.peer && sample.peerLabel) {
          markSegmentPeer(sample.segment, sample.peer, sample.peerLabel);
        }
        if (
          sample.success !== false &&
          typeof sample.timeNs === 'number' &&
          Number.isFinite(sample.timeNs) &&
          graphsyncRunStartNs !== null
        ) {
          const readyTimeNs = sample.timeNs - graphsyncRunStartNs;
          if (readyTimeNs >= 0) {
            const currentReadyTime = graphsyncReadyTimesNs.get(sample.segment);
            if (currentReadyTime === undefined || readyTimeNs < currentReadyTime) {
              graphsyncReadyTimesNs.set(sample.segment, readyTimeNs);
            }
            latestGraphsyncElapsedNs = Math.max(latestGraphsyncElapsedNs, readyTimeNs);
          }
        }
      }
      if (sample.type === 'throughput_sample' && sample.peer && typeof sample.ema === 'number' && typeof sample.timeNs === 'number') {
        updateThroughput(sample.peer, sample.ema, sample.timeNs);
      }
    } catch {
      continue;
    }
  }

  finalizeBitswapSegmentPeers();

  let projectedPlayback = false;
  if (bitswapReadyTimesNs.size > 0) {
    const intervalNs = playbackIntervalNs(playback);
    const summaryPlaybackCompletionNs = finiteDurationNs(summary?.playbackCompletion);
    const summaryTotalTimeNs = finiteDurationNs(summary?.totalTime);
    const statusElapsedNs = elapsedFromStatus(status, nowMs);
    const currentElapsedNs =
      summaryPlaybackCompletionNs ??
      summaryTotalTimeNs ??
      (status?.state === 'running' ? statusElapsedNs : null) ??
      latestBitswapElapsedNs;
    const projection = computeOrderedPlaybackProjection(bitswapReadyTimesNs, seed.segments, intervalNs, currentElapsedNs);
    playbackIndex = projection.index;
    playbackPosition = projection.position;
    projectedPlayback = true;
  }

  if (graphsyncReadyTimesNs.size > 0) {
    const intervalNs = graphsyncPlaybackIntervalNs ?? playbackIntervalNs(playback);
    const summaryPlaybackCompletionNs = finiteDurationNs(summary?.playbackCompletion);
    const summaryTotalTimeNs = finiteDurationNs(summary?.totalTime);
    const liveGraphsyncElapsedNs = elapsedSinceUnixNs(graphsyncRunStartNs, nowMs);
    const currentElapsedNs =
      summaryPlaybackCompletionNs ??
      summaryTotalTimeNs ??
      (status?.state === 'running' ? liveGraphsyncElapsedNs : null) ??
      latestGraphsyncElapsedNs;
    const projection = computeOrderedPlaybackProjection(graphsyncReadyTimesNs, seed.segments, intervalNs, currentElapsedNs);
    playbackIndex = projection.index;
    playbackPosition = projection.position;
    projectedPlayback = true;
  }
  if (!projectedPlayback) {
    playbackIndex = schedulerPlaybackIndex;
    playbackPosition = schedulerPlaybackIndex;
  }
  playbackPosition = Math.max(playbackPosition, playbackIndex);

  const peerMetricSource = peerBytesSent.size > 0 ? peerBytesSent : peerSegmentCounts;
  const peerMetricItems: PeerMetricItem[] = Array.from(peerMetricSource.entries()).map(([key, value]) => ({
    key,
    label: getPeerDisplayLabel(key),
    color: peerColorMap.get(key) ?? '#888',
    value
  }));

  const attributedSegmentPeers = new Set(segmentPeer.values());
  const legend: HeatmapLegendItem[] = [
    { key: 'pending', label: outputLabels.pending, color: heatmapColors.pending },
    ...(inFlightSegments.size > 0 ? [{ key: 'fetching', label: outputLabels.fetching, color: heatmapColors.fetching }] : []),
    { key: 'prefetched', label: outputLabels.prefetched, color: '#f5f2ea', accent: true },
    { key: 'played', label: outputLabels.played, color: '#f5f2ea', band: true },
    ...(playback.urgentWindow > 0 ? [{ key: 'urgent', label: outputLabels.urgentWindow, color: heatmapColors.urgent }] : []),
    ...(urgentDecisionSegments.size > 0
      ? [{ key: 'urgent-decision', label: outputLabels.scheduledUrgent, color: heatmapColors.urgentDecision }]
      : []),
    ...Array.from(peerColorMap.entries()).filter(([peerId]) => attributedSegmentPeers.has(peerId)).map(([peerId, color]) => ({
      key: peerId,
      label: getPeerDisplayLabel(peerId),
      color
    }))
  ];

  return {
    throughput: {
      labels,
      datasets: Array.from(datasets.values())
    },
    peerMetric: {
      label: peerMetricLabel,
      items: peerMetricItems
    },
    heatmap: {
      segments: seed.segments,
      chunksPerSegment,
      playbackIndex,
      playbackPosition,
      urgentWindow: playback.urgentWindow,
      urgentDecisionSegments,
      inFlightSegments,
      fetchedChunks,
      segmentPeer,
      legend
    },
    summary: {
      stallCount: typeof summary?.stallCount === 'number' ? summary.stallCount : null,
      totalStallTimeSec: typeof summary?.totalStallTime === 'number' ? summary.totalStallTime / 1e9 : null,
      startupDelaySec: typeof summary?.startupDelay === 'number' ? summary.startupDelay / 1e9 : null,
      playbackCompletionSec: typeof summary?.playbackCompletion === 'number' ? summary.playbackCompletion / 1e9 : null,
      downloadTimeSec: typeof summary?.totalTime === 'number' ? summary.totalTime / 1e9 : null
    }
  };
}
