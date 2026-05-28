import type {
  CancelResponse,
  ClearRunsResponse,
  BitswapSeedRequest,
  BitswapSeedInfoResponse,
  BitswapSeedResponse,
  PeerNetworkCondition,
  PeerCountResponse,
  QueueStartRequest,
  QueueStatusResponse,
  ReshapeResponse,
  RunRequest,
  RunStatusResponse,
  SeedInfoResponse,
  SeedRequest,
  SeedResponse,
  TimelineGenerateResponse,
  UpdatePeersResponse,
  VideoInfoResponse
} from '../types/api';
import { apiEndpoints } from '../types/api';

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return (await response.json()) as T;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  return parseJson<T>(response);
}

async function postJson<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined
  });
  return parseJson<T>(response);
}

export async function runScenario(payload: RunRequest): Promise<RunStatusResponse> {
  return postJson<RunStatusResponse>(apiEndpoints.run, payload);
}

export async function getRunStatus(id: string): Promise<RunStatusResponse> {
  return getJson<RunStatusResponse>(`${apiEndpoints.status}?id=${encodeURIComponent(id)}`);
}

export async function cancelRun(id: string): Promise<CancelResponse> {
  return postJson<CancelResponse>(`${apiEndpoints.cancel}?id=${encodeURIComponent(id)}`);
}

export async function startQueue(payload: QueueStartRequest): Promise<QueueStatusResponse> {
  return postJson<QueueStatusResponse>(apiEndpoints.queueStart, payload);
}

export async function getQueueStatus(): Promise<QueueStatusResponse> {
  return getJson<QueueStatusResponse>(apiEndpoints.queueStatus);
}

export async function cancelQueue(): Promise<QueueStatusResponse> {
  return postJson<QueueStatusResponse>(apiEndpoints.queueCancel);
}

export async function seedLayout(payload: SeedRequest): Promise<SeedResponse> {
  return postJson<SeedResponse>(apiEndpoints.seed, payload);
}

export async function getSeedInfo(): Promise<SeedInfoResponse> {
  return getJson<SeedInfoResponse>(apiEndpoints.seedInfo);
}

export async function getVideoInfo(): Promise<VideoInfoResponse> {
  return getJson<VideoInfoResponse>(apiEndpoints.videoInfo);
}

export async function getPeerCount(): Promise<PeerCountResponse> {
  return getJson<PeerCountResponse>(apiEndpoints.peerCount);
}

export async function seedBitswap(payload: BitswapSeedRequest): Promise<BitswapSeedResponse> {
  return postJson<BitswapSeedResponse>(apiEndpoints.seedBitswap, payload);
}

export async function getBitswapSeedInfo(): Promise<BitswapSeedInfoResponse> {
  return getJson<BitswapSeedInfoResponse>(apiEndpoints.seedBitswapInfo);
}

export async function updatePeers(): Promise<UpdatePeersResponse> {
  return postJson<UpdatePeersResponse>(apiEndpoints.updatePeers);
}

export async function clearRuns(): Promise<ClearRunsResponse> {
  return postJson<ClearRunsResponse>(apiEndpoints.runsClear);
}

export async function generateTimelineVideos(): Promise<TimelineGenerateResponse> {
  return postJson<TimelineGenerateResponse>(apiEndpoints.timelinesGenerate);
}

export async function reshapeNetwork(profile: string, peerNetwork?: PeerNetworkCondition[]): Promise<ReshapeResponse> {
  return postJson<ReshapeResponse>(`${apiEndpoints.reshape}?profile=${encodeURIComponent(profile)}`, peerNetwork ? { peerNetwork } : undefined);
}

export function runsDownloadUrl(): string {
  return apiEndpoints.runsDownload;
}

export function timelinesDownloadUrl(): string {
  return apiEndpoints.timelinesDownload;
}

export function plotsDownloadUrl(): string {
  return apiEndpoints.plotsDownload;
}
