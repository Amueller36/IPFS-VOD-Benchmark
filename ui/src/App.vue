<script setup lang="ts">
import { storeToRefs } from 'pinia';
import { onMounted } from 'vue';
import DownloadActions from './components/DownloadActions.vue';
import PlaybackHeatmap from './components/PlaybackHeatmap.vue';
import PeerPieChart from './components/PeerPieChart.vue';
import QueuePanel from './components/QueuePanel.vue';
import RunForm from './components/RunForm.vue';
import RunOutput from './components/RunOutput.vue';
import SeedControls from './components/SeedControls.vue';
import ThroughputChart from './components/ThroughputChart.vue';
import VideoInfoPanel from './components/VideoInfoPanel.vue';
import { useRunStore } from './stores/run';
import { useUiStore } from './stores/ui';

const uiStore = useUiStore();
const runStore = useRunStore();
const { text } = storeToRefs(uiStore);

onMounted(() => {
  void runStore.resumeBatch();
});
</script>

<template>
  <main class="page">
    <header class="page-header">
      <h1 class="page__title">{{ text.app.title }}</h1>
    </header>

    <VideoInfoPanel />
    <SeedControls />
    <DownloadActions />
    <RunForm />

    <section class="grid grid--two">
      <QueuePanel />
      <RunOutput />
    </section>

    <ThroughputChart />
    <PeerPieChart />
    <PlaybackHeatmap />
  </main>
</template>
