<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { ArcElement, Chart, Legend, PieController, Title, Tooltip, type ChartConfiguration } from 'chart.js';
import { useRunStore } from '../stores/run';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';
import { parseRunOutput, resolvePlaybackSettings } from '../lib/runOutput';

Chart.register(PieController, ArcElement, Title, Tooltip, Legend);

const canvas = ref<HTMLCanvasElement | null>(null);
const runStore = useRunStore();
const seedStore = useSeedStore();
const uiStore = useUiStore();
let chart: Chart<'pie'> | null = null;

const playbackSettings = computed(() =>
  resolvePlaybackSettings(runStore.output, runStore.status, {
    playbackMs: uiStore.form.playbackMs,
    playbackSpeed: uiStore.form.playbackSpeed,
    urgentWindow: uiStore.form.mode === 'bitswap' ? 0 : uiStore.form.urgentWindow
  })
);

const parsed = computed(() =>
  parseRunOutput(
    runStore.output,
    runStore.status,
    {
      segments: seedStore.segments,
      segmentSize: seedStore.segmentSize,
      chunkSize: seedStore.chunkSize
    },
    playbackSettings.value,
    Date.now(),
    uiStore.runOutputText
  )
);

function ensureChart(): void {
  if (!canvas.value || chart) {
    return;
  }
  const config: ChartConfiguration<'pie'> = {
    type: 'pie',
    data: {
      labels: [],
      datasets: [{ label: uiStore.runOutputText.segmentsFetched, data: [], backgroundColor: [] }]
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true },
        title: { display: true, text: uiStore.runOutputText.segmentsFetched }
      }
    }
  };
  chart = new Chart(canvas.value, config);
}

function renderChart(): void {
  ensureChart();
  if (!chart) {
    return;
  }
  chart.data.labels = parsed.value.peerMetric.items.map((item) => item.label);
  chart.data.datasets[0].label = parsed.value.peerMetric.label;
  chart.data.datasets[0].data = parsed.value.peerMetric.items.map((item) => item.value);
  chart.data.datasets[0].backgroundColor = parsed.value.peerMetric.items.map((item) => item.color);
  if (chart.options.plugins?.title) {
    chart.options.plugins.title.text = parsed.value.peerMetric.label;
  }
  chart.update();
}

function resetChart(): void {
  ensureChart();
  if (!chart) {
    return;
  }
  chart.data.labels = [];
  chart.data.datasets[0].label = uiStore.runOutputText.segmentsFetched;
  chart.data.datasets[0].data = [];
  chart.data.datasets[0].backgroundColor = [];
  if (chart.options.plugins?.title) {
    chart.options.plugins.title.text = uiStore.runOutputText.segmentsFetched;
  }
  chart.update();
}

watch(() => runStore.status?.id ?? '', resetChart);
watch(parsed, renderChart, { immediate: true });

onMounted(() => {
  renderChart();
});

onBeforeUnmount(() => {
  chart?.destroy();
  chart = null;
});
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ uiStore.text.peerPie.title }}</h2>
    <p class="panel__hint">{{ uiStore.text.peerPie.hint }}</p>
    <div class="chart-wrap chart-wrap--pie">
      <canvas ref="canvas" />
    </div>
    <p v-if="parsed.peerMetric.items.length === 0" class="panel__hint">{{ uiStore.text.peerPie.empty }}</p>
  </section>
</template>
