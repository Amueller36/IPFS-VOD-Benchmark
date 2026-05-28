<script setup lang="ts">
import { computed, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useQueueStore } from '../stores/queue';
import { useRunStore } from '../stores/run';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';
import ScenarioPresetSelect from './ScenarioPresetSelect.vue';

const uiStore = useUiStore();
const seedStore = useSeedStore();
const queueStore = useQueueStore();
const runStore = useRunStore();
const { form, text } = storeToRefs(uiStore);
const importFile = ref<HTMLInputElement | null>(null);

const runtimeEstimate = computed(() => {
  if (!seedStore.layoutRoot || !seedStore.segments) {
    return text.value.runForm.runtimeMissingSeed;
  }
  if (form.value.playbackMs <= 0 || form.value.playbackSpeed <= 0) {
    return '';
  }
  const totalMs = (seedStore.segments * form.value.playbackMs) / form.value.playbackSpeed;
  const totalSeconds = Math.round(totalMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${hours}h ${minutes}m ${seconds}s`;
});

const playbackHint = computed(() => {
  if (!seedStore.durationSec || !seedStore.size || !seedStore.segmentSize) {
    return text.value.runForm.playbackHintFallback;
  }
  const playbackMs = Math.round((seedStore.segmentSize / (seedStore.size / seedStore.durationSec)) * 1000);
  return text.value.runForm.playbackHintValue(playbackMs);
});

const isGraphsyncMode = computed(() => form.value.mode === 'graphsync');
const requiredSeedHint = computed(() => {
  if (form.value.mode === 'bitswap') {
    return seedStore.bitswapRoot ? '' : text.value.runForm.seedBitswapFirst;
  }
  return seedStore.layoutRoot ? '' : text.value.runForm.seedLayoutFirst;
});
const canAddCurrentScenario = computed(() => requiredSeedHint.value === '');
const canRun = computed(() => queueStore.items.length > 0 || canAddCurrentScenario.value);

function addCurrentScenarioToQueue(): void {
  if (!canAddCurrentScenario.value) {
    return;
  }
  queueStore.add(uiStore.buildQueueDraft(seedStore.layoutRoot, seedStore.bitswapRoot));
}

function clearQueue(): void {
  if (!runStore.batchRunning) {
    queueStore.clear();
  }
}

function exportQueue(): void {
  const payload = queueStore.exportConfig();
  const blob = new Blob([payload], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const link = document.createElement('a');
  link.href = url;
  link.download = `bench-queue-${stamp}.json`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function openImportDialog(): void {
  importFile.value?.click();
}

async function onImportChange(event: Event): Promise<void> {
  const target = event.target as HTMLInputElement;
  const file = target.files?.[0];
  if (!file) {
    return;
  }
  const text = await file.text();
  queueStore.importConfigText(text);
  target.value = '';
}

async function runQueue(): Promise<void> {
  await runStore.runQueueOrCurrentDraft();
}
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ text.runForm.title }}</h2>
    <div class="form-grid">
      <ScenarioPresetSelect />

      <div class="field">
        <label for="networkPreset">{{ text.networkPreset.label }}</label>
        <select id="networkPreset" :value="form.networkPreset" @change="uiStore.applyNetworkPreset(($event.target as HTMLSelectElement).value as typeof form.networkPreset)">
          <option value="custom">{{ text.networkPreset.custom }}</option>
          <option value="1_heterogen">{{ text.networkPreset.heterogen }}</option>
          <option value="2_homogen">{{ text.networkPreset.homogen }}</option>
          <option value="3_bandbreitenstress">{{ text.networkPreset.bandwidthStress }}</option>
          <option value="4_packetloss">{{ text.networkPreset.packetLoss }}</option>
        </select>
      </div>

      <div class="field">
        <label for="mode">{{ text.runForm.mode }}</label>
        <select id="mode" :value="form.mode" :disabled="uiStore.modeLocked" @change="uiStore.setMode(($event.target as HTMLSelectElement).value as 'graphsync' | 'bitswap')">
          <option value="graphsync">{{ text.common.graphsync }}</option>
          <option value="bitswap">{{ text.common.bitswap }}</option>
        </select>
      </div>

      <div class="field field--wide">
        <label>{{ text.runForm.networkProfile }}</label>
        <div class="peer-network-table">
          <div class="peer-network-table__head">{{ text.runForm.peerSlot }}</div>
          <div class="peer-network-table__head">{{ text.runForm.peerLabel }}</div>
          <div class="peer-network-table__head">{{ text.runForm.peerRateMbit }}</div>
          <div class="peer-network-table__head">{{ text.runForm.peerLatencyMs }}</div>
          <div class="peer-network-table__head">{{ text.runForm.peerJitterMs }}</div>
          <div class="peer-network-table__head">{{ text.runForm.peerLossPct }}</div>
          <template v-for="(peer, index) in form.peerNetwork" :key="peer.slot">
            <div class="peer-network-table__slot">{{ peer.slot }}</div>
            <input :value="peer.label" type="text" @input="uiStore.updatePeerNetwork(index, { label: ($event.target as HTMLInputElement).value })" />
            <input :value="peer.rateMbit" min="1" type="number" @input="uiStore.updatePeerNetwork(index, { rateMbit: Number(($event.target as HTMLInputElement).value) })" />
            <input :value="peer.latencyMs" min="0" type="number" @input="uiStore.updatePeerNetwork(index, { latencyMs: Number(($event.target as HTMLInputElement).value) })" />
            <input :value="peer.jitterMs" min="0" type="number" @input="uiStore.updatePeerNetwork(index, { jitterMs: Number(($event.target as HTMLInputElement).value) })" />
            <input :value="peer.lossPct" max="100" min="0" step="0.01" type="number" @input="uiStore.updatePeerNetwork(index, { lossPct: Number(($event.target as HTMLInputElement).value) })" />
          </template>
        </div>
        <small>{{ text.runForm.networkHint }}</small>
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="prefetch">{{ text.runForm.prefetch }}</label>
        <input id="prefetch" v-model.number="form.prefetch" type="number" />
        <small>{{ text.runForm.prefetchHint }}</small>
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="workers">{{ text.runForm.workers }}</label>
        <input id="workers" :value="form.workers" type="number" @input="uiStore.setWorkers(Number(($event.target as HTMLInputElement).value))" />
        <small>{{ text.runForm.workersHint }}</small>
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="raceFanout">{{ text.runForm.raceFanout }}</label>
        <input id="raceFanout" v-model.number="form.raceFanout" type="number" />
        <small>{{ text.runForm.raceFanoutHint }}</small>
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="discoveryFanout">{{ text.runForm.discoveryFanout }}</label>
        <input id="discoveryFanout" v-model.number="form.discoveryFanout" min="0" type="number" />
      </div>

      <div class="field">
        <label for="playbackMs">{{ text.runForm.playbackMs }}</label>
        <input
          id="playbackMs"
          :value="form.playbackMs"
          type="number"
          @input="uiStore.setPlaybackMs(Number(($event.target as HTMLInputElement).value))"
        />
        <small>{{ playbackHint }}</small>
      </div>

      <div class="field">
        <label for="playbackSpeed">{{ text.runForm.playbackSpeed }}</label>
        <input id="playbackSpeed" v-model.number="form.playbackSpeed" step="0.1" type="number" />
      </div>

      <div class="field">
        <label for="runtimeEstimate">{{ text.runForm.runtimeEstimate }}</label>
        <input id="runtimeEstimate" :value="runtimeEstimate" readonly />
        <small>{{ text.runForm.runtimeHint }}</small>
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="urgentWindow">{{ text.runForm.urgentWindow }}</label>
        <input id="urgentWindow" v-model.number="form.urgentWindow" type="number" />
      </div>

      <div v-if="isGraphsyncMode" class="field">
        <label for="emaAlpha">{{ text.runForm.emaAlpha }}</label>
        <input id="emaAlpha" v-model.number="form.emaAlpha" step="0.01" type="number" />
      </div>

      <div class="field">
        <label for="repeatCount">{{ text.runForm.repeatCount }}</label>
        <input
          id="repeatCount"
          :value="form.repeatCount"
          min="1"
          type="number"
          @input="uiStore.setRepeatCount(Number(($event.target as HTMLInputElement).value))"
        />
      </div>

      <div class="field">
        <label for="interRunDelayMs">{{ text.runForm.interRunDelayMs }}</label>
        <input
          id="interRunDelayMs"
          :value="form.interRunDelayMs"
          min="0"
          type="number"
          @input="uiStore.setInterRunDelayMs(Number(($event.target as HTMLInputElement).value))"
        />
      </div>
    </div>

    <p v-if="requiredSeedHint" class="panel__hint">{{ requiredSeedHint }}</p>
    <div class="toolbar">
      <button class="button" type="button" :disabled="runStore.batchRunning || !canAddCurrentScenario" @click="addCurrentScenarioToQueue">{{ text.runForm.addScenario }}</button>
      <button class="button" type="button" :disabled="runStore.batchRunning || queueStore.items.length === 0" @click="clearQueue">{{ text.runForm.clearQueue }}</button>
      <button class="button button--secondary" type="button" :disabled="runStore.batchRunning || queueStore.items.length === 0" @click="exportQueue">{{ text.runForm.exportQueue }}</button>
      <button class="button button--secondary" type="button" :disabled="runStore.batchRunning" @click="openImportDialog">{{ text.runForm.importQueue }}</button>
      <input ref="importFile" accept="application/json" style="display:none" type="file" @change="onImportChange" />
      <button class="button" type="button" :disabled="runStore.batchRunning || !canRun" @click="runQueue">{{ text.runForm.runQueue }}</button>
      <button class="button button--secondary" type="button" :disabled="!runStore.batchRunning || !runStore.currentRunId" @click="runStore.cancelCurrent()">{{ text.runForm.stopCurrent }}</button>
      <button class="button button--secondary" type="button" :disabled="!runStore.batchRunning" @click="runStore.stopBatch()">{{ text.runForm.stopBatch }}</button>
    </div>
  </section>
</template>
