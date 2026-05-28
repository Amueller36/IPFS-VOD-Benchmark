export const apiEndpoints = {
  index: '/',
  run: '/run',
  status: '/status',
  cancel: '/cancel',
  queueStart: '/queue/start',
  queueStatus: '/queue/status',
  queueCancel: '/queue/cancel',
  seed: '/seed',
  seedInfo: '/seed-info',
  videoInfo: '/video-info',
  peerCount: '/peer-count',
  seedBitswap: '/seed-bitswap',
  seedBitswapInfo: '/seed-bitswap-info',
  updatePeers: '/update-peers',
  runsDownload: '/runs/download',
  timelinesGenerate: '/runs/timelines/generate',
  timelinesDownload: '/runs/timelines/download',
  runsClear: '/runs/clear',
  plotsDownload: '/plots/download',
  reshape: '/reshape'
} as const;

export type RunMode = 'graphsync' | 'bitswap';
export type RunState = 'queued' | 'running' | 'done' | 'error' | 'cancelled' | string;
export type ScenarioPreset = 'custom' | 'graphsync-adaptive' | 'boxo-bitswap-trace';
export type NetworkPreset = 'custom' | '1_heterogen' | '2_homogen' | '3_bandbreitenstress' | '4_packetloss';
export type NetworkProfile = 'default' | 'stressed';

export interface PeerNetworkCondition {
  slot: string;
  label: string;
  rateMbit: number;
  latencyMs: number;
  jitterMs: number;
  lossPct: number;
}

export interface RunRequest {
  mode: RunMode | string;
  root: string;
  prefetch: number;
  workers: number;
  raceFanout: number;
  discoveryFanout: number;
  playbackMs: number;
  playbackSpeed: number;
  logPeers: boolean;
  urgentWindow: number;
  emaAlpha: number;
  chunkBytes: number;
  scenarioPreset: string;
  networkPreset: string;
  networkProfile: string;
  peerNetwork: PeerNetworkCondition[];
  queueId: string;
  queueItemId: string;
  queuePosition: number;
  repeatCount: number;
  repeatIndex: number;
  interRunDelayMs: number;
}

export interface SeedRequest {
  segmentKb: number;
  chunkKb: number;
}

export interface BitswapSeedRequest {
  chunkKb: number;
}

export interface SeedResponse {
  root: string;
  output: string;
  error: string;
}

export interface SeedInfoResponse {
  root: string;
  size: number;
  segmentSize: number;
  chunkSize: number;
  segments: number;
  durationSec: number;
}

export interface VideoInfoResponse {
  exists: boolean;
  durationSec: number;
  quality: string;
  bitrateMbitPerSec: number;
  sizeMb: number;
  error?: string;
}

export interface BitswapSeedResponse {
  root: string;
  output: string;
  error: string;
}

export interface BitswapSeedInfoResponse {
  root: string;
}

export interface UpdatePeersResponse {
  output: string;
  error: string;
}

export interface PeerCountResponse {
  count: number;
}

export interface RunStatusResponse {
  id: string;
  state: RunState;
  output: string;
  startedAt: string;
  endedAt: string;
  error: string;
  params?: Record<string, unknown>;
}

export type QueueState = 'idle' | 'running' | 'done' | 'error' | 'cancelled' | string;
export type QueueItemState = 'pending' | 'running' | 'done' | 'error' | 'cancelled' | string;
export type QueueRunStatus = RunStatusResponse;

export interface QueueStartItem {
  id: string;
  position: number;
  preset: string;
  label: string;
  payload: RunRequest;
  repeatCount: number;
  interRunDelayMs: number;
}

export interface QueueStartRequest {
  queue: QueueStartItem[];
}

export interface QueueStatusItem {
  id: string;
  position: number;
  preset: string;
  label: string;
  state: QueueItemState;
  runIds?: string[] | null;
  repeatCount: number;
  interRunDelayMs: number;
}

export interface QueueStatusResponse {
  id: string;
  state: QueueState;
  startedAt: string;
  endedAt: string;
  error: string;
  currentRunId: string;
  currentRun?: QueueRunStatus;
  items: QueueStatusItem[];
}

export interface RunSummary {
  ttfb: number;
  stallCount: number;
  stallDurations: number[];
  stallDurationsSec: number[];
  avgSegmentFetch: number;
  cacheHitRate: number;
  totalTime: number;
  totalBytes: number;
  throughputBytesPerSec: number;
  startupDelay: number;
  totalStallTime: number;
  stallRatio: number;
  playbackOverheadRatio: number;
  deadlineMissRate: number;
  segmentReadyP50: number;
  segmentReadyP95: number;
  segmentLatenessP50: number;
  segmentLatenessP95: number;
  playbackDuration: number;
  playbackCompletion: number;
}

export interface PeerSummary {
  peerId: string;
  segments: number;
  bytes: number;
  ema: number;
  avgNetMs: number;
  failures: number;
  cooldownSec: number;
  label: string;
}

export interface RunExport {
  id: string;
  mode: string;
  root: string;
  params: Record<string, unknown>;
  startedAt: string;
  endedAt: string;
  state?: string;
  error?: string;
  outcome?: string;
  outcomeReason?: string;
  summary?: RunSummary;
  peers?: PeerSummary[];
}

export interface CancelResponse {
  id: string;
  running: boolean;
  state: RunState;
}

export interface ClearRunsResponse {
  deleted: number;
}

export interface TimelineGenerateResponse {
  generated: number;
  skipped: number;
  errors: string[];
}

export interface ReshapeResponse {
  profile: string;
  peerNetwork?: PeerNetworkCondition[];
  output: string;
  ok: boolean;
  error?: string;
}

export interface QueueScenarioDraft {
  preset: ScenarioPreset;
  label: string;
  payload: RunRequest;
  repeatCount: number;
  interRunDelayMs: number;
}

export interface QueueConfigEntry {
  id?: string;
  preset: ScenarioPreset | string;
  label: string;
  payload: Partial<RunRequest>;
  repeatCount: number;
  interRunDelayMs: number;
}

export interface QueueConfig {
  version: number;
  createdAt: string;
  queue: QueueConfigEntry[];
}
