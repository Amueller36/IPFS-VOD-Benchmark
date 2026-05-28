import { defineStore } from 'pinia';
import { messages, runOutputLabels, type RunOutputLabels, type UiMessages } from '../i18n';
import type { NetworkPreset, NetworkProfile, PeerNetworkCondition, QueueScenarioDraft, RunMode, RunRequest, ScenarioPreset } from '../types/api';

export interface RunFormState {
  scenarioPreset: ScenarioPreset;
  mode: RunMode;
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
  networkPreset: NetworkPreset;
  networkProfile: NetworkProfile;
  peerNetwork: PeerNetworkCondition[];
  repeatCount: number;
  interRunDelayMs: number;
}

export function defaultPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 0 },
    { slot: 'peer-2', label: 'fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 0 },
    { slot: 'peer-3', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-4', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-5', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 }
  ];
}

export function stressedPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'fast', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-2', label: 'fast', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-3', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-4', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-5', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 }
  ];
}

export function heterogenPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 0 },
    { slot: 'peer-2', label: 'fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 0 },
    { slot: 'peer-3', label: 'slow-10', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-4', label: 'slow-15', rateMbit: 15, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-5', label: 'slow-20', rateMbit: 20, latencyMs: 160, jitterMs: 30, lossPct: 0 }
  ];
}

export function homogenPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'homogen', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-2', label: 'homogen', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-3', label: 'homogen', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-4', label: 'homogen', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-5', label: 'homogen', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 }
  ];
}

export function bandwidthStressPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'fast', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-2', label: 'fast', rateMbit: 30, latencyMs: 50, jitterMs: 10, lossPct: 0 },
    { slot: 'peer-3', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-4', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 },
    { slot: 'peer-5', label: 'slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 0 }
  ];
}

export function packetLossPeerNetwork(): PeerNetworkCondition[] {
  return [
    { slot: 'peer-1', label: 'packet-loss-fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 2 },
    { slot: 'peer-2', label: 'packet-loss-fast', rateMbit: 120, latencyMs: 30, jitterMs: 8, lossPct: 2 },
    { slot: 'peer-3', label: 'packet-loss-slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 2 },
    { slot: 'peer-4', label: 'packet-loss-slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 2 },
    { slot: 'peer-5', label: 'packet-loss-slow', rateMbit: 10, latencyMs: 160, jitterMs: 30, lossPct: 2 }
  ];
}

export function peerNetworkForPreset(preset: string | undefined): PeerNetworkCondition[] | null {
  switch (preset) {
    case '1_heterogen':
      return heterogenPeerNetwork();
    case '2_homogen':
      return homogenPeerNetwork();
    case '3_bandbreitenstress':
      return bandwidthStressPeerNetwork();
    case '4_packetloss':
      return packetLossPeerNetwork();
    default:
      return null;
  }
}

export function peerNetworkForLegacyProfile(profile: string | undefined): PeerNetworkCondition[] {
  return profile === 'stressed' ? stressedPeerNetwork() : defaultPeerNetwork();
}

export function normalizePeerNetworkInput(value: unknown, legacyProfile?: string): PeerNetworkCondition[] {
  const fallback = peerNetworkForLegacyProfile(legacyProfile);
  if (!Array.isArray(value) || value.length === 0) {
    return fallback;
  }
  const bySlot = new Map<string, PeerNetworkCondition>();
  for (const item of value) {
    if (!item || typeof item !== 'object') {
      continue;
    }
    const raw = item as Partial<PeerNetworkCondition>;
    const slot = String(raw.slot || '').trim();
    const label = String(raw.label || '').trim();
    const rateMbit = Number(raw.rateMbit);
    const latencyMs = Number(raw.latencyMs);
    const jitterMs = Number(raw.jitterMs);
    const lossPct = Number(raw.lossPct ?? 0);
    if (!slot || !label || !Number.isFinite(rateMbit) || !Number.isFinite(latencyMs) || !Number.isFinite(jitterMs) || !Number.isFinite(lossPct)) {
      continue;
    }
    bySlot.set(slot, {
      slot,
      label,
      rateMbit: Math.max(1, Math.round(rateMbit)),
      latencyMs: Math.max(0, Math.round(latencyMs)),
      jitterMs: Math.max(0, Math.round(jitterMs)),
      lossPct: Math.min(100, Math.max(0, lossPct))
    });
  }
  const normalized = fallback.map((item) => bySlot.get(item.slot) ?? item);
  return normalized;
}

function defaultFormState(): RunFormState {
  return {
    scenarioPreset: 'custom',
    mode: 'graphsync',
    root: '',
    prefetch: -1,
    workers: 5,
    raceFanout: -1,
    discoveryFanout: 2,
    playbackMs: 40,
    playbackSpeed: 1,
    logPeers: true,
    urgentWindow: 10,
    emaAlpha: 0.2,
    networkPreset: 'custom',
    networkProfile: 'default',
    peerNetwork: defaultPeerNetwork(),
    repeatCount: 1,
    interRunDelayMs: 0
  };
}

export function scenarioLabelForPreset(preset: string, mode: string): string {
  switch ((preset || '').trim()) {
    case 'graphsync-adaptive':
      return 'GraphSync (Adaptive)';
    case 'boxo-bitswap-trace':
      return 'Boxo Bitswap Trace Baseline';
    default:
      return `Custom (${mode || 'graphsync'})`;
  }
}

export function modeForPreset(preset: string): RunMode | '' {
  if (preset === 'graphsync-adaptive') {
    return 'graphsync';
  }
  if (preset === 'boxo-bitswap-trace') {
    return 'bitswap';
  }
  return '';
}

export const useUiStore = defineStore('ui', {
  state: () => ({
    form: defaultFormState(),
    workersTouched: false,
    playbackMsTouched: false
  }),
  getters: {
    text(): UiMessages {
      return messages;
    },
    runOutputText(): RunOutputLabels {
      return runOutputLabels;
    },
    modeLocked(state): boolean {
      return state.form.scenarioPreset !== 'custom';
    }
  },
  actions: {
    resetForm(): void {
      this.form = defaultFormState();
      this.workersTouched = false;
      this.playbackMsTouched = false;
    },
    setWorkers(value: number): void {
      this.form.workers = value;
      this.workersTouched = true;
    },
    maybeAutoSetWorkers(count: number): void {
      if (!this.workersTouched && this.form.mode === 'graphsync' && count > 0) {
        this.form.workers = count;
      }
    },
    setPlaybackMs(value: number): void {
      this.form.playbackMs = Number.isFinite(value) ? value : 0;
      this.playbackMsTouched = true;
    },
    setRepeatCount(value: number): void {
      this.form.repeatCount = Number.isFinite(value) ? Math.max(1, Math.round(value)) : 1;
    },
    setInterRunDelayMs(value: number): void {
      this.form.interRunDelayMs = Number.isFinite(value) ? Math.max(0, Math.round(value)) : 0;
    },
    updatePeerNetwork(index: number, patch: Partial<PeerNetworkCondition>): void {
      const current = this.form.peerNetwork[index];
      if (!current) {
        return;
      }
      const next = { ...current, ...patch };
      next.label = String(next.label || '').trim();
      next.rateMbit = Number.isFinite(Number(next.rateMbit)) ? Math.max(1, Math.round(Number(next.rateMbit))) : current.rateMbit;
      next.latencyMs = Number.isFinite(Number(next.latencyMs)) ? Math.max(0, Math.round(Number(next.latencyMs))) : current.latencyMs;
      next.jitterMs = Number.isFinite(Number(next.jitterMs)) ? Math.max(0, Math.round(Number(next.jitterMs))) : current.jitterMs;
      next.lossPct = Number.isFinite(Number(next.lossPct)) ? Math.min(100, Math.max(0, Number(next.lossPct))) : current.lossPct;
      this.form.peerNetwork[index] = next;
      this.form.networkPreset = 'custom';
      this.form.networkProfile = 'default';
    },
    applyNetworkPreset(preset: NetworkPreset): void {
      this.form.networkPreset = preset;
      this.form.networkProfile = 'default';
      const peerNetwork = peerNetworkForPreset(preset);
      if (peerNetwork) {
        this.form.peerNetwork = peerNetwork;
      }
    },
    maybeAutoSetPlaybackMs(value: number): void {
      if (this.playbackMsTouched || !Number.isFinite(value) || value <= 0) {
        return;
      }
      this.form.playbackMs = Math.round(value);
    },
    setMode(mode: RunMode): void {
      if (!this.modeLocked) {
        this.form.mode = mode;
      }
    },
    applyScenarioPreset(preset: ScenarioPreset): void {
      this.form.scenarioPreset = preset;
      if (preset === 'custom') {
        return;
      }
      if (preset === 'graphsync-adaptive') {
        this.form.mode = 'graphsync';
        this.form.prefetch = -1;
        this.form.workers = 0;
        this.form.raceFanout = -1;
        this.form.discoveryFanout = 2;
        this.form.urgentWindow = 10;
        this.form.emaAlpha = 0.2;
      } else if (preset === 'boxo-bitswap-trace') {
        this.form.mode = 'bitswap';
      }
    },
    buildRunPayload(layoutRoot: string, bitswapRoot: string): RunRequest {
      const speed = this.form.playbackSpeed;
      const basePlayback = this.form.playbackMs;
      const root = this.form.mode === 'bitswap' ? bitswapRoot : layoutRoot;
      return {
        mode: this.form.mode,
        root,
        prefetch: this.form.prefetch,
        workers: this.form.workers,
        raceFanout: this.form.raceFanout,
        discoveryFanout: this.form.discoveryFanout,
        playbackMs: speed > 0 ? Math.max(1, Math.round(basePlayback / speed)) : basePlayback,
        playbackSpeed: speed,
        logPeers: true,
        urgentWindow: this.form.urgentWindow,
        emaAlpha: this.form.emaAlpha,
        chunkBytes: 0,
        scenarioPreset: this.form.scenarioPreset,
        networkPreset: this.form.networkPreset,
        networkProfile: this.form.networkProfile,
        peerNetwork: normalizePeerNetworkInput(this.form.peerNetwork, this.form.networkProfile),
        queueId: '',
        queueItemId: '',
        queuePosition: 0,
        repeatCount: this.form.repeatCount,
        repeatIndex: 0,
        interRunDelayMs: this.form.interRunDelayMs
      };
    },
    buildQueueDraft(layoutRoot: string, bitswapRoot: string): QueueScenarioDraft {
      const payload = this.buildRunPayload(layoutRoot, bitswapRoot);
      return {
        preset: this.form.scenarioPreset,
        label: scenarioLabelForPreset(this.form.scenarioPreset, payload.mode),
        payload,
        repeatCount: Math.max(1, this.form.repeatCount || 1),
        interRunDelayMs: Math.max(0, this.form.interRunDelayMs || 0)
      };
    }
  }
});
