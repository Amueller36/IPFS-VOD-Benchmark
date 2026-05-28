<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useRunStore } from '../stores/run';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';
import { getHeatmapColors, parseRunOutput, resolvePlaybackSettings, type HeatmapLegendItem } from '../lib/runOutput';

const canvas = ref<HTMLCanvasElement | null>(null);
const runStore = useRunStore();
const seedStore = useSeedStore();
const uiStore = useUiStore();
const heatmapColors = getHeatmapColors();
const animationNow = ref(Date.now());
let animationFrame: number | null = null;

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
    animationNow.value,
    uiStore.runOutputText
  )
);

function legendSwatchStyle(item: HeatmapLegendItem): Record<string, string> {
  if (item.band) {
    return { '--legend-band-color': heatmapColors.played } as Record<string, string>;
  }
  return { background: item.color };
}

function formatSeconds(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return uiStore.text.common.notAvailable;
  }
  return `${value.toFixed(3)}s`;
}

function draw(): void {
  const value = parsed.value.heatmap;
  const target = canvas.value;
  if (!target) {
    return;
  }
  const ctx = target.getContext('2d');
  if (!ctx) {
    return;
  }
  if (!value.segments || !value.chunksPerSegment) {
    ctx.clearRect(0, 0, target.width, target.height);
    return;
  }

  const cellWidth = 4;
  const baseCellHeight = 12;
  const width = Math.max(value.segments * cellWidth, 1);
  const height = Math.max(value.chunksPerSegment * baseCellHeight, 260);
  const cellHeight = height / value.chunksPerSegment;
  if (target.width !== width) {
    target.width = width;
  }
  if (target.height !== height) {
    target.height = height;
  }

  ctx.clearRect(0, 0, target.width, target.height);
  for (let seg = 0; seg < value.segments; seg += 1) {
    const isPlayed = seg < value.playbackIndex;
    const isFetching = value.inFlightSegments.has(seg);
    const fetched = value.fetchedChunks.get(seg);
    const peerId = value.segmentPeer.get(seg);
    const peerLegend = value.legend.find((item) => item.key === peerId);
    const peerColor = peerLegend?.color ?? null;
    for (let row = 0; row < value.chunksPerSegment; row += 1) {
      const y = row * cellHeight;
      let color: string = heatmapColors.pending;
      let isPrefetched = false;
      if (fetched?.has(row)) {
        color = peerColor ?? heatmapColors.prefetched;
        isPrefetched = !isPlayed;
      } else if (isFetching) {
        color = heatmapColors.fetching;
      }
      ctx.fillStyle = color;
      ctx.fillRect(seg * cellWidth, y, cellWidth, cellHeight);
      if (isPrefetched) {
        const accentHeight = Math.max(1, Math.min(2, Math.round(cellHeight / 5)));
        ctx.fillStyle = heatmapColors.prefetchAccent;
        ctx.fillRect(seg * cellWidth, y, cellWidth, accentHeight);
        ctx.fillRect(seg * cellWidth, y + cellHeight - accentHeight, cellWidth, accentHeight);
      }
      if (isPlayed) {
        const bandHeight = Math.max(2, Math.min(4, Math.round(cellHeight / 3)));
        const bandY = y + Math.max(0, Math.floor((cellHeight - bandHeight) / 2));
        ctx.fillStyle = heatmapColors.played;
        ctx.fillRect(seg * cellWidth, bandY, cellWidth, bandHeight);
      }
    }
  }

  if (value.urgentDecisionSegments.size > 0) {
    ctx.fillStyle = heatmapColors.urgentDecision;
    const markerHeight = 2;
    for (const seg of value.urgentDecisionSegments) {
      if (seg < 0 || seg >= value.segments) {
        continue;
      }
      const x = seg * cellWidth;
      for (let row = 0; row < value.chunksPerSegment; row += 1) {
        const rowTop = row * cellHeight;
        const markerY = rowTop + Math.max(markerHeight, Math.floor(cellHeight * 0.72));
        ctx.fillRect(x, Math.min(markerY, rowTop + cellHeight - markerHeight), cellWidth, markerHeight);
      }
    }
  }

  if (value.urgentWindow > 0) {
    const start = value.playbackIndex;
    const end = Math.min(value.segments, value.playbackIndex + value.urgentWindow);
    ctx.fillStyle = heatmapColors.urgent;
    ctx.fillRect(start * cellWidth, 0, (end - start) * cellWidth, target.height);
  }

  const playbackX = Math.max(0, Math.min(value.segments, value.playbackPosition)) * cellWidth;
  ctx.strokeStyle = heatmapColors.played;
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(playbackX, 0);
  ctx.lineTo(playbackX, target.height);
  ctx.stroke();

  ctx.strokeStyle = '#b7b0a7';
  ctx.lineWidth = 1;
  ctx.strokeRect(0, 0, target.width, target.height);
}

function startAnimation(): void {
  if (animationFrame !== null) {
    return;
  }
  const tick = (): void => {
    animationNow.value = Date.now();
    animationFrame = window.requestAnimationFrame(tick);
  };
  animationFrame = window.requestAnimationFrame(tick);
}

function stopAnimation(): void {
  if (animationFrame === null) {
    return;
  }
  window.cancelAnimationFrame(animationFrame);
  animationFrame = null;
}

watch(parsed, draw, { immediate: true, deep: true });
watch(
  () => runStore.status?.state,
  (state) => {
    if (state === 'running') {
      startAnimation();
    } else {
      stopAnimation();
      animationNow.value = Date.now();
    }
  },
  { immediate: true }
);
onMounted(draw);
onBeforeUnmount(stopAnimation);
</script>

<template>
  <section class="panel stack">
    <h2 class="panel__title">{{ uiStore.text.playback.timelineTitle }}</h2>
    <p class="panel__hint">{{ uiStore.text.playback.timelineHint }}</p>
    <div v-if="parsed.summary.stallCount !== null" class="qoe-summary">
      <div>
        <span>{{ uiStore.text.playback.stalls }}</span>
        <strong>{{ parsed.summary.stallCount }}</strong>
      </div>
      <div>
        <span>{{ uiStore.text.playback.stallTime }}</span>
        <strong>{{ formatSeconds(parsed.summary.totalStallTimeSec) }}</strong>
      </div>
      <div>
        <span>{{ uiStore.text.playback.startup }}</span>
        <strong>{{ formatSeconds(parsed.summary.startupDelaySec) }}</strong>
      </div>
      <div>
        <span>{{ uiStore.text.playback.playbackDone }}</span>
        <strong>{{ formatSeconds(parsed.summary.playbackCompletionSec) }}</strong>
      </div>
      <div>
        <span>{{ uiStore.text.playback.downloadTime }}</span>
        <strong>{{ formatSeconds(parsed.summary.downloadTimeSec) }}</strong>
      </div>
    </div>
    <div class="heatmap-legend">
      <div v-for="item in parsed.heatmap.legend" :key="item.key" class="legend-item">
        <span
          class="legend-swatch"
          :class="{ 'legend-swatch--accent': item.accent, 'legend-swatch--band': item.band }"
          :style="legendSwatchStyle(item)"
        />
        <span>{{ item.label }}</span>
      </div>
    </div>
    <div class="heatmap-wrap">
      <canvas ref="canvas" />
    </div>
  </section>
</template>

<style scoped>
.qoe-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 8px;
}

.qoe-summary div {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 10px;
  background: #f9f7f2;
}

.qoe-summary span,
.qoe-summary strong {
  display: block;
}

.qoe-summary span {
  color: var(--muted);
  font-size: 12px;
}

.qoe-summary strong {
  margin-top: 2px;
  font-size: 15px;
}

.download-progress {
  display: grid;
  gap: 12px;
}

.download-progress__bar {
  width: 100%;
  height: 14px;
  overflow: hidden;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: #e6e2dc;
}

.download-progress__fill {
  height: 100%;
  background: #1f77b4;
  transition: width 180ms ease;
}

.download-progress__stats {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 8px;
}

.download-progress__stats div {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 10px;
  background: #f9f7f2;
}

.download-progress__stats span,
.download-progress__stats strong {
  display: block;
}

.download-progress__stats span {
  color: var(--muted);
  font-size: 12px;
}

.download-progress__stats strong {
  margin-top: 2px;
  font-size: 15px;
}

@media (max-width: 720px) {
  .qoe-summary,
  .download-progress__stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
