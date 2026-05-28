<script setup lang="ts">
import { storeToRefs } from 'pinia';
import { useSeedStore } from '../stores/seed';
import { useUiStore } from '../stores/ui';

const seedStore = useSeedStore();
const uiStore = useUiStore();
const { text } = storeToRefs(uiStore);
</script>

<template>
  <section class="panel video-info">
    <h2 class="panel__title">{{ text.video.title }}</h2>
    <p v-if="!seedStore.videoExists" class="panel__hint">{{ text.video.missing }}</p>
    <div v-else class="video-info__grid">
      <div class="video-info__item">
        <span>{{ text.video.duration }}</span>
        <strong>{{ seedStore.videoDurationLabel }}</strong>
      </div>
      <div class="video-info__item">
        <span>{{ text.video.quality }}</span>
        <strong>{{ seedStore.videoQualityLabel }}</strong>
      </div>
      <div class="video-info__item">
        <span>{{ text.video.bitrate }}</span>
        <strong>{{ seedStore.videoBitrateLabel }}</strong>
      </div>
      <div class="video-info__item">
        <span>{{ text.video.size }}</span>
        <strong>{{ seedStore.videoSizeLabel }}</strong>
      </div>
    </div>
  </section>
</template>
