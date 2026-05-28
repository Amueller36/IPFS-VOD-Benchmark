import { defineStore } from 'pinia';
import { cancelQueue, cancelRun, getQueueStatus, getRunStatus, runScenario, startQueue } from '../api/client';
import type { QueueStatusResponse, RunRequest, RunStatusResponse } from '../types/api';
import { useQueueStore } from './queue';
import { useSeedStore } from './seed';
import { useUiStore } from './ui';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function seedRootForMode(mode: string, layoutRoot: string, bitswapRoot: string): string {
  return mode === 'bitswap' ? bitswapRoot : layoutRoot;
}

let activeQueuePoll: Promise<QueueStatusResponse> | null = null;

export const useRunStore = defineStore('run', {
  state: () => ({
    currentRunId: '',
    status: null as RunStatusResponse | null,
    batchRunning: false,
    stopBatchRequested: false,
    queuePolling: false
  }),
  getters: {
    isRunning(state): boolean {
      return state.status?.state === 'running';
    },
    output(state): string {
      return state.status?.output ?? '';
    }
  },
  actions: {
    async pollStatus(id: string): Promise<RunStatusResponse> {
      const status = await getRunStatus(id);
      this.status = status;
      return status;
    },
    async pollUntilDone(id: string): Promise<RunStatusResponse> {
      for (;;) {
        const status = await this.pollStatus(id);
        if (status.state !== 'running') {
          return status;
        }
        await sleep(500);
      }
    },
    applyQueueStatus(queueStatus: QueueStatusResponse): void {
      const queueStore = useQueueStore();
      queueStore.applyServerStatus(queueStatus.items);
      this.batchRunning = queueStatus.state === 'running';
      this.currentRunId = queueStatus.state === 'running' ? queueStatus.currentRunId : '';
      this.status = queueStatus.currentRun ?? this.status;
      if (queueStatus.state === 'cancelled') {
        this.stopBatchRequested = true;
      }
      if (queueStatus.state === 'idle') {
        this.currentRunId = '';
      }
    },
    async pollQueueStatus(): Promise<QueueStatusResponse> {
      const status = await getQueueStatus();
      this.applyQueueStatus(status);
      return status;
    },
    ensureQueuePolling(): Promise<QueueStatusResponse> {
      if (activeQueuePoll) {
        return activeQueuePoll;
      }
      this.queuePolling = true;
      activeQueuePoll = (async () => {
        try {
          for (;;) {
            const status = await this.pollQueueStatus();
            if (status.state !== 'running') {
              return status;
            }
            await sleep(500);
          }
        } finally {
          activeQueuePoll = null;
          this.queuePolling = false;
          this.batchRunning = false;
          this.currentRunId = '';
        }
      })();
      return activeQueuePoll;
    },
    async pollQueueUntilDone(): Promise<QueueStatusResponse> {
      return this.ensureQueuePolling();
    },
    async resumeBatch(): Promise<void> {
      const status = await this.pollQueueStatus();
      if (status.state === 'running') {
        void this.ensureQueuePolling();
      }
    },
    async requestCancelCurrentRun(): Promise<boolean> {
      if (!this.currentRunId) {
        return false;
      }
      try {
        await cancelRun(this.currentRunId);
        return true;
      } catch {
        return false;
      }
    },
    async cancelCurrent(): Promise<void> {
      const cancelled = await this.requestCancelCurrentRun();
      if (cancelled && this.currentRunId) {
        await this.pollStatus(this.currentRunId);
      }
    },
    async startSingle(payload: RunRequest): Promise<RunStatusResponse> {
      this.status = null;
      this.currentRunId = '';
      const status = await runScenario(payload);
      this.status = status;
      this.currentRunId = status.id;
      const done = await this.pollUntilDone(status.id);
      this.currentRunId = '';
      return done;
    },
    async runQueue(): Promise<void> {
      const queueStore = useQueueStore();
      const seedStore = useSeedStore();
      const uiStore = useUiStore();
      if (queueStore.items.length === 0) {
        return;
      }

      try {
        const needsGraphsyncRoot = queueStore.items.some((item) => item.payload.mode !== 'bitswap');
        const needsBitswapRoot = queueStore.items.some((item) => item.payload.mode === 'bitswap');
        if (needsGraphsyncRoot) {
          await seedStore.refreshSeedInfo(true);
        }
        if (needsBitswapRoot) {
          await seedStore.refreshBitswapSeedInfo(true);
        }
        const missingMode = queueStore.items.find((item) => !seedRootForMode(item.payload.mode, seedStore.layoutRoot, seedStore.bitswapRoot))?.payload.mode;
        if (missingMode) {
          const message = missingMode === 'bitswap' ? uiStore.text.runForm.seedBitswapFirst : uiStore.text.runForm.seedLayoutFirst;
          throw new Error(message);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.status = {
          id: '',
          state: 'error',
          output: message,
          startedAt: '',
          endedAt: new Date().toISOString(),
          error: message
        };
        this.batchRunning = false;
        this.currentRunId = '';
        return;
      }

      this.batchRunning = true;
      this.stopBatchRequested = false;
      this.status = null;
      this.currentRunId = '';
      queueStore.resetStatuses();
      queueStore.renumber();
      queueStore.persist();

      try {
        const response = await startQueue({
          queue: queueStore.items.map((item) => ({
            id: item.id,
            position: item.position,
            preset: item.preset,
            label: item.label,
            payload: {
              ...item.payload,
              root: seedRootForMode(item.payload.mode, seedStore.layoutRoot, seedStore.bitswapRoot),
              networkPreset: item.payload.networkPreset || 'custom',
              queueId: '',
              queueItemId: item.id,
              queuePosition: item.position,
              repeatCount: item.repeatCount,
              repeatIndex: 0,
              interRunDelayMs: item.interRunDelayMs
            } as RunRequest,
            repeatCount: item.repeatCount,
            interRunDelayMs: item.interRunDelayMs
          }))
        });
        this.applyQueueStatus(response);
        if (response.state === 'running') {
          await this.ensureQueuePolling();
        }
      } finally {
        if (!this.queuePolling) {
          this.batchRunning = false;
          this.currentRunId = '';
        }
      }
    },
    async runQueueOrCurrentDraft(): Promise<void> {
      const queueStore = useQueueStore();
      if (queueStore.items.length === 0) {
        const uiStore = useUiStore();
        const seedStore = useSeedStore();
        queueStore.add(uiStore.buildQueueDraft(seedStore.layoutRoot, seedStore.bitswapRoot));
      }
      await this.runQueue();
    },
    async stopBatch(): Promise<void> {
      this.stopBatchRequested = true;
      const status = await cancelQueue();
      this.applyQueueStatus(status);
    }
  }
});
