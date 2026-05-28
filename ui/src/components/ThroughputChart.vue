<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import {
  CategoryScale,
  Chart,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Title,
  Tooltip,
  type ChartConfiguration
} from 'chart.js';
import { useRunStore } from '../stores/run';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';
import { parseRunOutput, resolvePlaybackSettings } from '../lib/runOutput';

Chart.register(LineController, LineElement, PointElement, CategoryScale, LinearScale, Title, Tooltip, Legend);

const canvas = ref<HTMLCanvasElement | null>(null);
const runStore = useRunStore();
const seedStore = useSeedStore();
const uiStore = useUiStore();
let chart: Chart<'line', Array<{ x: string; y: number }>, string> | null = null;

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
  const config: ChartConfiguration<'line', Array<{ x: string; y: number }>, string> = {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          title: {
            display: true,
            text: uiStore.text.throughput.axis
          }
        }
      }
    }
  };
  chart = new Chart<'line', Array<{ x: string; y: number }>, string>(canvas.value, config);
}

function renderChart(): void {
  ensureChart();
  if (!chart) {
    return;
  }
  if (chart.options.scales?.y?.title) {
    chart.options.scales.y.title.text = uiStore.text.throughput.axis;
  }
  chart.data.labels = parsed.value.throughput.labels;
  chart.data.datasets = parsed.value.throughput.datasets.map((dataset) => ({
    label: dataset.label,
    data: dataset.data,
    stepped: 'after',
    borderWidth: 2,
    borderColor: dataset.color,
    backgroundColor: `${dataset.color}33`,
    pointBackgroundColor: dataset.color,
    pointRadius: 2
  }));
  chart.update();
}

function resetChart(): void {
  ensureChart();
  if (!chart) {
    return;
  }
  chart.data.labels = [];
  chart.data.datasets = [];
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
    <h2 class="panel__title">{{ uiStore.text.throughput.title }}</h2>
    <p class="panel__hint">{{ uiStore.text.throughput.hint }}</p>
    <div class="chart-wrap chart-wrap--line">
      <canvas ref="canvas" />
    </div>
    <p v-if="parsed.throughput.datasets.length === 0" class="panel__hint">{{ uiStore.text.throughput.empty }}</p>
  </section>
</template>
