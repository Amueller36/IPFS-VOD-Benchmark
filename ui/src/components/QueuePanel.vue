<script setup lang="ts">
import { storeToRefs } from 'pinia';
import { type QueueItem, useQueueStore } from '../stores/queue';
import { useRunStore } from '../stores/run';
import { useUiStore } from '../stores/ui';

const queueStore = useQueueStore();
const runStore = useRunStore();
const uiStore = useUiStore();
const { items } = storeToRefs(queueStore);
const { text } = storeToRefs(uiStore);

function queueItemLabel(item: QueueItem): string {
  if (item.preset === 'graphsync-adaptive') {
    return text.value.scenario.graphsyncAdaptive;
  }
  if (item.preset === 'boxo-bitswap-trace') {
    return text.value.scenario.boxoBitswapTrace;
  }
  return `${text.value.common.custom} (${item.payload.mode || 'graphsync'})`;
}

function queueNetworkPresetLabel(item: QueueItem): string {
  switch (item.payload.networkPreset) {
    case '1_heterogen':
      return text.value.networkPreset.heterogen;
    case '2_homogen':
      return text.value.networkPreset.homogen;
    case '3_bandbreitenstress':
      return text.value.networkPreset.bandwidthStress;
    case '4_packetloss':
      return text.value.networkPreset.packetLoss;
    default:
      return text.value.networkPreset.custom;
  }
}

function queueItemParamsText(item: QueueItem): string {
  const payload = item.payload;
  const params = [
    `mode=${payload.mode}`,
    `root=${payload.root}`,
    `playbackMs=${payload.playbackMs}`,
    `playbackSpeed=${payload.playbackSpeed}`
  ];
  if (payload.mode === 'graphsync') {
    params.push(
      `prefetch=${payload.prefetch}`,
      `workers=${payload.workers}`,
      `raceFanout=${payload.raceFanout}`,
      `discoveryFanout=${payload.discoveryFanout}`,
      `urgentWindow=${payload.urgentWindow}`
    );
  }
  params.push('cachePolicy=always-clear');
  return params.join(', ');
}
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ text.queue.title }}</h2>
    <div class="queue-list">
      <article v-for="item in items" :key="item.id" class="queue-item" :class="`queue-item--${item.state}`">
        <div class="queue-item__header">
          <strong>[{{ item.state.toUpperCase() }}] #{{ item.position }} {{ queueItemLabel(item) }} · {{ queueNetworkPresetLabel(item) }}</strong>
          <button class="button button--danger" type="button" :disabled="runStore.batchRunning" @click="queueStore.remove(item.id)">{{ text.queue.remove }}</button>
        </div>
        <div class="queue-meta">
          <div>{{ text.queue.repeats }}={{ item.repeatCount }}, interRunDelayMs={{ item.interRunDelayMs }}</div>
          <div>{{ queueItemParamsText(item) }}</div>
          <div v-if="item.runIds.length > 0">runIds={{ item.runIds.join(', ') }}</div>
        </div>
      </article>
      <div v-if="items.length === 0" class="placeholder">{{ text.queue.empty }}</div>
    </div>
  </section>
</template>
