<script setup lang="ts">
import { ref } from 'vue';
import { storeToRefs } from 'pinia';
import { clearRuns, generateTimelineVideos, plotsDownloadUrl, runsDownloadUrl, timelinesDownloadUrl } from '../api/client';
import { useUiStore } from '../stores/ui';

const uiStore = useUiStore();
const { text } = storeToRefs(uiStore);
const message = ref('');
const generatingTimelines = ref(false);

async function clearSavedRuns(): Promise<void> {
  try {
    const response = await clearRuns();
    message.value = text.value.downloads.deleted(response.deleted || 0);
  } catch (error) {
    message.value = error instanceof Error ? error.message : String(error);
  }
}

async function generateTimelines(): Promise<void> {
  generatingTimelines.value = true;
  try {
    const response = await generateTimelineVideos();
    const suffix = response.errors?.length ? ` (${response.errors.slice(0, 3).join('; ')})` : '';
    message.value = `${text.value.downloads.timelinesGenerated(response.generated || 0, response.skipped || 0)}${suffix}`;
  } catch (error) {
    message.value = error instanceof Error ? error.message : String(error);
  } finally {
    generatingTimelines.value = false;
  }
}
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ text.downloads.title }}</h2>
    <div class="toolbar">
      <a class="button button--secondary" :href="runsDownloadUrl()">{{ text.downloads.runs }}</a>
      <button class="button button--secondary" type="button" :disabled="generatingTimelines" @click="generateTimelines">
        {{ text.downloads.generateTimelines }}
      </button>
      <a class="button button--secondary" :href="timelinesDownloadUrl()">{{ text.downloads.timelines }}</a>
      <a class="button button--secondary" :href="plotsDownloadUrl()">{{ text.downloads.plots }}</a>
      <button class="button" type="button" @click="clearSavedRuns">{{ text.downloads.clear }}</button>
    </div>
    <p v-if="message" class="panel__hint">{{ message }}</p>
  </section>
</template>
