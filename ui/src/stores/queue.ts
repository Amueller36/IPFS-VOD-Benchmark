import { defineStore } from 'pinia';
import type { QueueConfig, QueueConfigEntry, QueueScenarioDraft, QueueStatusItem, RunRequest } from '../types/api';
import { modeForPreset, normalizePeerNetworkInput, peerNetworkForPreset, scenarioLabelForPreset } from './ui';

export interface QueueItem extends QueueScenarioDraft {
  id: string;
  position: number;
  state: 'pending' | 'running' | 'done' | 'error' | 'cancelled';
  runIds: string[];
}

const currentStorageKey = 'ipfs-streaming-bench-queue-v1';
const legacyStorageKeys = ['ipfs-streaming-bench-queue-v2'];

function safeParse(raw: string): QueueConfig | null {
  try {
    return JSON.parse(raw) as QueueConfig;
  } catch {
    return null;
  }
}

function loadConfigFromStorage(): QueueConfig | null {
  if (typeof window === 'undefined') {
    return null;
  }
  const keys = [currentStorageKey, ...legacyStorageKeys];
  for (const key of keys) {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      continue;
    }
    const parsed = safeParse(raw);
    if (parsed && Array.isArray(parsed.queue)) {
      return parsed;
    }
  }
  return null;
}

function normalizeRaceFanout(value: unknown): number {
  const normalized = Number(value ?? -1);
  return normalized === 0 ? -1 : normalized;
}

function normalizeImportedQueueEntry(entry: QueueConfigEntry, index: number): QueueItem | null {
  const payload = entry.payload && typeof entry.payload === 'object' ? { ...entry.payload } : {};
  const preset = (entry.preset || payload.scenarioPreset || 'custom').toString();
  if (preset === 'kubo-ipfs-bitswap-baseline') {
    return null;
  }
  const presetMode = modeForPreset(preset);
  const payloadMode = payload.mode ? String(payload.mode).trim() : '';
  const normalizedMode = (payloadMode || presetMode || 'graphsync') as RunRequest['mode'];
  const root = payload.root ? String(payload.root).trim() : '';
  if (!root) {
    return null;
  }
  const defaultPrefetch = preset === 'graphsync-adaptive' ? -1 : -1;
  const defaultWorkers = preset === 'graphsync-adaptive' ? 0 : 5;
  const defaultDiscoveryFanout = preset === 'graphsync-adaptive' ? 2 : 2;
  const networkPreset = String(payload.networkPreset ?? 'custom');
  const presetPeerNetwork = peerNetworkForPreset(networkPreset);
  const peerNetwork = presetPeerNetwork ?? normalizePeerNetworkInput(payload.peerNetwork, String(payload.networkProfile ?? 'default'));
  const normalizedPayload: RunRequest = {
    mode: normalizedMode,
    root,
    prefetch: Number(payload.prefetch ?? defaultPrefetch),
    workers: Number(payload.workers ?? defaultWorkers),
    raceFanout: normalizeRaceFanout(payload.raceFanout),
    discoveryFanout: Number(payload.discoveryFanout ?? defaultDiscoveryFanout),
    playbackMs: Number(payload.playbackMs ?? 40),
    playbackSpeed: Number(payload.playbackSpeed ?? 1),
    logPeers: true,
    urgentWindow: Number(payload.urgentWindow ?? 10),
    emaAlpha: Number(payload.emaAlpha ?? 0.2),
    chunkBytes: Number(payload.chunkBytes ?? 0),
    scenarioPreset: String(payload.scenarioPreset ?? preset),
    networkPreset,
    networkProfile: String(payload.networkProfile ?? 'default'),
    peerNetwork,
    queueId: '',
    queueItemId: '',
    queuePosition: index + 1,
    repeatCount: Math.max(1, Number(entry.repeatCount ?? payload.repeatCount ?? 1)),
    repeatIndex: 0,
    interRunDelayMs: Math.max(0, Number(entry.interRunDelayMs ?? payload.interRunDelayMs ?? 0))
  };
  return {
    id: entry.id || `queue-${Date.now()}-${index + 1}-${Math.random().toString(36).slice(2, 8)}`,
    position: index + 1,
    preset: (preset as QueueItem['preset']) || 'custom',
    label: String(entry.label || scenarioLabelForPreset(preset, normalizedPayload.mode)),
    payload: normalizedPayload,
    repeatCount: normalizedPayload.repeatCount,
    interRunDelayMs: normalizedPayload.interRunDelayMs,
    state: 'pending',
    runIds: []
  };
}

function loadInitialItems(): QueueItem[] {
  const config = loadConfigFromStorage();
  if (!config) {
    return [];
  }
  const items: QueueItem[] = [];
  for (let i = 0; i < config.queue.length; i++) {
    const item = normalizeImportedQueueEntry(config.queue[i], i);
    if (item) {
      items.push(item);
    }
  }
  return items;
}

export const useQueueStore = defineStore('queue', {
  state: () => ({
    items: loadInitialItems()
  }),
  actions: {
    renumber(): void {
      this.items = this.items.map((item, index) => ({
        ...item,
        position: index + 1
      }));
    },
    toConfig(): QueueConfig {
      return {
        version: 1,
        createdAt: new Date().toISOString(),
        queue: this.items.map((item) => ({
          id: item.id,
          preset: item.preset,
          label: item.label,
          payload: JSON.parse(JSON.stringify(item.payload)),
          repeatCount: item.repeatCount,
          interRunDelayMs: item.interRunDelayMs
        }))
      };
    },
    persist(): void {
      if (typeof window === 'undefined') {
        return;
      }
      window.localStorage.setItem(currentStorageKey, JSON.stringify(this.toConfig()));
    },
    add(item: QueueScenarioDraft): void {
      this.items.push({
        ...item,
        id: `queue-${Date.now()}-${this.items.length + 1}`,
        position: this.items.length + 1,
        state: 'pending',
        runIds: []
      });
      this.persist();
    },
    remove(id: string): void {
      this.items = this.items.filter((item) => item.id !== id);
      this.renumber();
      this.persist();
    },
    clear(): void {
      this.items = [];
      this.persist();
    },
    exportConfig(): string {
      return JSON.stringify(this.toConfig(), null, 2);
    },
    importConfigText(text: string): void {
      const parsed = safeParse(text);
      if (!parsed || !Array.isArray(parsed.queue)) {
        throw new Error('Invalid queue config format.');
      }
      const imported: QueueItem[] = [];
      for (let i = 0; i < parsed.queue.length; i++) {
        const item = normalizeImportedQueueEntry(parsed.queue[i], i);
        if (item) {
          imported.push(item);
        }
      }
      if (imported.length === 0) {
        throw new Error('No valid queued scenarios found in config.');
      }
      this.items = imported;
      this.persist();
    },
    resetStatuses(): void {
      this.items = this.items.map((item) => ({
        ...item,
        state: 'pending',
        runIds: []
      }));
      this.persist();
    },
    setStatus(id: string, state: QueueItem['state']): void {
      const item = this.items.find((entry) => entry.id === id);
      if (item) {
        item.state = state;
        this.persist();
      }
    },
    appendRunId(id: string, runId: string): void {
      const item = this.items.find((entry) => entry.id === id);
      if (item) {
        item.runIds.push(runId);
        this.persist();
      }
    },
    applyServerStatus(items: QueueStatusItem[]): void {
      for (const serverItem of items) {
        const item = this.items.find((entry) => entry.id === serverItem.id);
        if (item) {
          item.state = serverItem.state as QueueItem['state'];
          item.runIds = [...(serverItem.runIds ?? [])];
          item.position = serverItem.position;
          item.repeatCount = serverItem.repeatCount;
          item.interRunDelayMs = serverItem.interRunDelayMs;
        }
      }
    }
  }
});
