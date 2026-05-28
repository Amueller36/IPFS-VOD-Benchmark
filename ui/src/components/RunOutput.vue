<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useRunStore } from '../stores/run';
import { useUiStore } from '../stores/ui';

const runStore = useRunStore();
const uiStore = useUiStore();
const { status } = storeToRefs(runStore);
const { text } = storeToRefs(uiStore);
const copyState = ref<'idle' | 'copied' | 'failed'>('idle');
let resetTimer: number | undefined;

const hasOutput = computed(() => runStore.output.length > 0);
const copyLabel = computed(() => {
  if (copyState.value === 'copied') {
    return text.value.runOutput.copied;
  }
  if (copyState.value === 'failed') {
    return text.value.runOutput.copyFailed;
  }
  return text.value.runOutput.copy;
});

async function copyLogs(): Promise<void> {
  if (!hasOutput.value) {
    return;
  }
  window.clearTimeout(resetTimer);
  try {
    await navigator.clipboard.writeText(runStore.output);
    copyState.value = 'copied';
  } catch {
    copyState.value = 'failed';
  }
  resetTimer = window.setTimeout(() => {
    copyState.value = 'idle';
  }, 1600);
}

onBeforeUnmount(() => {
  window.clearTimeout(resetTimer);
});
</script>

<template>
  <section class="panel stack">
    <div class="panel-header">
      <h2 class="panel__title">{{ text.runOutput.title }}</h2>
      <button class="button button--secondary button--compact" type="button" :disabled="!hasOutput" @click="copyLogs">
        {{ copyLabel }}
      </button>
    </div>
    <p class="panel__hint">
      {{ text.runOutput.currentState }}: <strong>{{ status?.state ?? text.runOutput.idle }}</strong>
      <span v-if="status?.id"> · runId={{ status.id }}</span>
      <span v-if="status?.startedAt"> · {{ text.runOutput.started }}={{ status.startedAt }}</span>
    </p>
    <p v-if="status?.error" class="panel__hint panel__hint--error">{{ status.error }}</p>
    <pre class="run-output">{{ runStore.output || text.runOutput.noOutput }}</pre>
  </section>
</template>
