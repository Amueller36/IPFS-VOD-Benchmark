import { defineStore } from 'pinia';
import { getBitswapSeedInfo, getPeerCount, getSeedInfo, getVideoInfo, seedBitswap, seedLayout } from '../api/client';
import { useUiStore } from './ui';

const seedSegmentOptions = [256, 512, 1024, 2048, 4096, 8192];
const seedChunkOptions = [256, 512, 1024];
const bitswapChunkOptions = [256, 512, 1024];
const bitswapDefaultChunkKb = 1024;
type SeedStatusKey = '' | 'seeding' | 'seedComplete' | 'seedFailed' | 'bitswapSeeding' | 'bitswapComplete' | 'bitswapFailed' | 'refreshing' | 'refreshComplete' | 'refreshFailed';

interface RefreshAllOptions {
  silent?: boolean;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isNotFoundError(error: unknown): boolean {
  return /\b404\b/.test(errorMessage(error));
}

function nearestChoice(value: number, choices: number[]): number {
  return choices.reduce((best, candidate) => {
    return Math.abs(candidate - value) < Math.abs(best - value) ? candidate : best;
  }, choices[0]);
}

export function formatVideoDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return 'n/v';
  }
  const totalSeconds = Math.round(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const secs = totalSeconds % 60;
  const minuteText = hours > 0 ? String(minutes).padStart(2, '0') : String(minutes);
  const secondText = String(secs).padStart(2, '0');
  return hours > 0 ? `${hours}:${minuteText}:${secondText}` : `${minuteText}:${secondText}`;
}

export function formatVideoBitrate(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return 'n/v';
  }
  return `${value.toFixed(2)} Mbit/s`;
}

export function formatVideoSize(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return 'n/v';
  }
  return `${value.toFixed(2)} MB`;
}

export const useSeedStore = defineStore('seed', {
  state: () => ({
    layoutRoot: '',
    bitswapRoot: '',
    size: 0,
    segmentSize: 0,
    chunkSize: 0,
    segments: 0,
    durationSec: 0,
    videoExists: false,
    videoDurationSec: 0,
    videoQuality: '',
    videoBitrateMbitPerSec: 0,
    videoSizeMb: 0,
    videoError: '',
    peerCount: 0,
    seedSegmentKb: 1024,
    seedChunkKb: 256,
    bitswapSeedChunkKb: bitswapDefaultChunkKb,
    statusKey: '' as SeedStatusKey,
    statusText: '',
    lastOutput: '',
    loading: false
  }),
  getters: {
    seedChunkOptions(state): Array<{ value: number; disabled: boolean }> {
      return seedChunkOptions.map((value) => ({
        value,
        disabled: value > state.seedSegmentKb || state.seedSegmentKb % value !== 0
      }));
    },
    bitswapSeedChunkOptions(): Array<{ value: number; disabled: boolean }> {
      return bitswapChunkOptions.map((value) => ({
        value,
        disabled: false
      }));
    },
    videoDurationLabel(state): string {
      return formatVideoDuration(state.videoDurationSec);
    },
    videoBitrateLabel(state): string {
      return formatVideoBitrate(state.videoBitrateMbitPerSec);
    },
    videoSizeLabel(state): string {
      return formatVideoSize(state.videoSizeMb);
    },
    videoQualityLabel(state): string {
      return state.videoQuality || 'n/v';
    }
  },
  actions: {
    syncPlaybackMsFromSeedMetadata(): void {
      if (!this.durationSec || !this.size || !this.segmentSize) {
        return;
      }
      const playbackMs = Math.round((this.segmentSize / (this.size / this.durationSec)) * 1000);
      useUiStore().maybeAutoSetPlaybackMs(playbackMs);
    },
    normalizeSeedChunk(): void {
      const validOptions = this.seedChunkOptions.filter((option) => !option.disabled);
      const bestValid = validOptions[validOptions.length - 1];
      if (!bestValid) {
        return;
      }
      if (this.seedChunkKb > this.seedSegmentKb || this.seedSegmentKb % this.seedChunkKb !== 0) {
        this.seedChunkKb = bestValid.value;
      }
    },
    setSeedSegmentKb(value: number): void {
      this.seedSegmentKb = value;
      this.normalizeSeedChunk();
    },
    setSeedChunkKb(value: number): void {
      this.seedChunkKb = value;
      this.normalizeSeedChunk();
    },
    normalizeBitswapSeedChunk(): void {
      if (!Number.isFinite(this.bitswapSeedChunkKb) || this.bitswapSeedChunkKb <= 0) {
        this.bitswapSeedChunkKb = bitswapDefaultChunkKb;
        return;
      }
      if (this.bitswapSeedChunkKb > bitswapDefaultChunkKb) {
        this.bitswapSeedChunkKb = bitswapDefaultChunkKb;
        return;
      }
      this.bitswapSeedChunkKb = nearestChoice(this.bitswapSeedChunkKb, bitswapChunkOptions);
    },
    setBitswapSeedChunkKb(value: number): void {
      this.bitswapSeedChunkKb = value;
      this.normalizeBitswapSeedChunk();
    },
    async refreshSeedInfo(reportErrors = false): Promise<void> {
      try {
        const response = await getSeedInfo();
        this.layoutRoot = response.root;
        this.size = response.size;
        this.segmentSize = response.segmentSize;
        this.chunkSize = response.chunkSize;
        this.segments = response.segments;
        this.durationSec = response.durationSec;
        this.syncPlaybackMsFromSeedMetadata();
      } catch (error) {
        this.layoutRoot = '';
        this.size = 0;
        this.segmentSize = 0;
        this.chunkSize = 0;
        this.segments = 0;
        this.durationSec = 0;
        if (reportErrors) {
          throw error;
        }
      }
    },
    async refreshVideoInfo(reportErrors = false): Promise<void> {
      try {
        const response = await getVideoInfo();
        this.videoExists = response.exists;
        this.videoDurationSec = response.durationSec || 0;
        this.videoQuality = response.quality || '';
        this.videoBitrateMbitPerSec = response.bitrateMbitPerSec || 0;
        this.videoSizeMb = response.sizeMb || 0;
        this.videoError = response.error || '';
      } catch (error) {
        this.videoExists = false;
        this.videoDurationSec = 0;
        this.videoQuality = '';
        this.videoBitrateMbitPerSec = 0;
        this.videoSizeMb = 0;
        this.videoError = error instanceof Error ? error.message : String(error);
        if (reportErrors) {
          throw error;
        }
      }
    },
    async refreshBitswapSeedInfo(reportErrors = false): Promise<void> {
      try {
        const response = await getBitswapSeedInfo();
        this.bitswapRoot = response.root;
      } catch (error) {
        this.bitswapRoot = '';
        if (reportErrors) {
          throw error;
        }
      }
    },
    async refreshPeerCount(reportErrors = false): Promise<void> {
      try {
        const response = await getPeerCount();
        this.peerCount = response.count;
        useUiStore().maybeAutoSetWorkers(response.count);
      } catch (error) {
        this.peerCount = 0;
        if (reportErrors) {
          throw error;
        }
      }
    },
    async refreshAll(options: RefreshAllOptions = {}): Promise<void> {
      const silent = options.silent === true;
      const seedText = useUiStore().text.seed;
      if (!silent) {
        this.loading = true;
        this.statusKey = 'refreshing';
        this.statusText = seedText.status.refreshing;
        this.lastOutput = seedText.status.refreshing;
      }

      const checks = [
        { label: seedText.refreshItems.video, run: () => this.refreshVideoInfo(!silent) },
        { label: seedText.refreshItems.graphsync, run: () => this.refreshSeedInfo(!silent), missing: seedText.noGraphsyncSeed },
        { label: seedText.refreshItems.bitswap, run: () => this.refreshBitswapSeedInfo(!silent), missing: seedText.noBitswapSeed },
        { label: seedText.refreshItems.peers, run: () => this.refreshPeerCount(!silent) }
      ];
      const results = await Promise.all(checks.map(async (check) => {
        try {
          await check.run();
          return { ok: true, message: `${seedText.refreshOk}: ${check.label}` };
        } catch (error) {
          const detail = check.missing && isNotFoundError(error) ? check.missing : errorMessage(error);
          return { ok: false, message: `${seedText.refreshError}: ${check.label}: ${detail}` };
        }
      }));

      if (!silent) {
        const failures = results.filter((result) => !result.ok);
        this.statusKey = failures.length > 0 ? 'refreshFailed' : 'refreshComplete';
        this.statusText = failures.length > 0 ? seedText.status.refreshFailed : seedText.status.refreshComplete;
        this.lastOutput = results.map((result) => result.message).join('\n');
        this.loading = false;
      }
    },
    async runSeed(): Promise<void> {
      this.loading = true;
      this.statusKey = 'seeding';
      this.statusText = useUiStore().text.seed.status.seeding;
      try {
        const response = await seedLayout({
          segmentKb: this.seedSegmentKb,
          chunkKb: this.seedChunkKb
        });
        this.layoutRoot = response.root;
        this.lastOutput = response.error ? `${response.error}\n${response.output || ''}`.trim() : (response.output || '');
        this.statusKey = response.error ? 'seedFailed' : 'seedComplete';
        this.statusText = response.error ? useUiStore().text.seed.status.seedFailed : useUiStore().text.seed.status.seedComplete;
        await this.refreshSeedInfo();
      } catch (error) {
        this.statusKey = 'seedFailed';
        this.statusText = useUiStore().text.seed.status.seedFailed;
        this.lastOutput = useUiStore().text.seed.requestFailed(error instanceof Error ? error.message : String(error));
      } finally {
        this.loading = false;
      }
    },
    async runBitswapSeed(): Promise<void> {
      this.loading = true;
      this.statusKey = 'bitswapSeeding';
      this.statusText = useUiStore().text.seed.status.bitswapSeeding;
      try {
        const response = await seedBitswap({
          chunkKb: this.bitswapSeedChunkKb
        });
        this.bitswapRoot = response.root;
        this.lastOutput = response.error ? `${response.error}\n${response.output || ''}`.trim() : (response.output || '');
        this.statusKey = response.error ? 'bitswapFailed' : 'bitswapComplete';
        this.statusText = response.error ? useUiStore().text.seed.status.bitswapFailed : useUiStore().text.seed.status.bitswapComplete;
        await this.refreshBitswapSeedInfo();
      } catch (error) {
        this.statusKey = 'bitswapFailed';
        this.statusText = useUiStore().text.seed.status.bitswapFailed;
        this.lastOutput = useUiStore().text.seed.bitswapRequestFailed(error instanceof Error ? error.message : String(error));
      } finally {
        this.loading = false;
      }
    }
  }
});
