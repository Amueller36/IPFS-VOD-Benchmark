<script setup lang="ts">
import { computed, onMounted } from 'vue';
import { storeToRefs } from 'pinia';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';

const seedStore = useSeedStore();
const uiStore = useUiStore();
const { layoutRoot, bitswapRoot, lastOutput, loading, seedSegmentKb, seedChunkKb, bitswapSeedChunkKb, statusKey, statusText } = storeToRefs(seedStore);
const { form, text } = storeToRefs(uiStore);

const statusMessage = computed(() => {
  switch (statusKey.value) {
    case 'seeding':
      return text.value.seed.status.seeding;
    case 'seedComplete':
      return text.value.seed.status.seedComplete;
    case 'seedFailed':
      return text.value.seed.status.seedFailed;
    case 'bitswapSeeding':
      return text.value.seed.status.bitswapSeeding;
    case 'bitswapComplete':
      return text.value.seed.status.bitswapComplete;
    case 'bitswapFailed':
      return text.value.seed.status.bitswapFailed;
    case 'refreshing':
      return text.value.seed.status.refreshing;
    case 'refreshComplete':
      return text.value.seed.status.refreshComplete;
    case 'refreshFailed':
      return text.value.seed.status.refreshFailed;
    default:
      break;
  }
  return statusText.value || text.value.seed.idle;
});

onMounted(async () => {
  await seedStore.refreshAll({ silent: true });
});
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ text.seed.title }}</h2>
    <div class="form-grid">
      <div v-if="form.mode === 'graphsync'" class="field">
        <label for="seedSegmentKb">{{ text.seed.segmentKb }}</label>
        <select id="seedSegmentKb" :value="seedSegmentKb" @change="seedStore.setSeedSegmentKb(Number(($event.target as HTMLSelectElement).value))">
          <option :value="256">256</option>
          <option :value="512">512</option>
          <option :value="1024">1024</option>
          <option :value="2048">2048</option>
          <option :value="4096">4096</option>
          <option :value="8192">8192</option>
        </select>
        <small>{{ text.seed.segmentHint }}</small>
      </div>

      <div v-if="form.mode === 'graphsync'" class="field">
        <label for="seedChunkKb">{{ text.seed.chunkKb }}</label>
        <select id="seedChunkKb" :value="seedChunkKb" @change="seedStore.setSeedChunkKb(Number(($event.target as HTMLSelectElement).value))">
          <option v-for="option in seedStore.seedChunkOptions" :key="option.value" :disabled="option.disabled" :value="option.value">{{ option.value }}</option>
        </select>
        <small>{{ text.seed.chunkHint }}</small>
      </div>

      <div v-if="form.mode === 'bitswap'" class="field">
        <label for="bitswapSeedChunkKb">{{ text.seed.bitswapChunkKb }}</label>
        <select id="bitswapSeedChunkKb" :value="bitswapSeedChunkKb" @change="seedStore.setBitswapSeedChunkKb(Number(($event.target as HTMLSelectElement).value))">
          <option v-for="option in seedStore.bitswapSeedChunkOptions" :key="option.value" :disabled="option.disabled" :value="option.value">{{ option.value }}</option>
        </select>
        <small>{{ text.seed.bitswapChunkHint }}</small>
      </div>

      <div class="field">
        <label>{{ text.seed.layoutCid }}</label>
        <input :value="layoutRoot" readonly />
      </div>

      <div class="field">
        <label>{{ text.seed.bitswapCid }}</label>
        <input :value="bitswapRoot" readonly />
      </div>

    </div>

    <div class="toolbar">
      <button class="button" type="button" :disabled="loading" @click="seedStore.runSeed()">{{ text.seed.seedLayout }}</button>
      <button class="button" type="button" :disabled="loading" @click="seedStore.runBitswapSeed()">{{ text.seed.seedBitswap }}</button>
      <button class="button button--secondary" type="button" :disabled="loading" @click="seedStore.refreshAll()">{{ text.seed.refresh }}</button>
    </div>

    <p v-if="form.mode === 'bitswap'" class="panel__hint">{{ text.seed.bitswapHint }}</p>
    <p v-else class="panel__hint">{{ text.seed.graphsyncHint }}</p>
    <p class="panel__hint">{{ statusMessage }}</p>
    <div class="placeholder">{{ lastOutput || text.seed.noOutput }}</div>
  </section>
</template>
