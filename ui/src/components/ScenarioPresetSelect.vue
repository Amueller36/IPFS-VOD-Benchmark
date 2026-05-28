<script setup lang="ts">
import { storeToRefs } from 'pinia';
import { useUiStore } from '../stores/ui';

const uiStore = useUiStore();
const { form, text } = storeToRefs(uiStore);

function onChange(event: Event): void {
  uiStore.applyScenarioPreset((event.target as HTMLSelectElement).value as typeof form.value.scenarioPreset);
}
</script>

<template>
  <div class="field">
    <label for="scenarioPreset">{{ text.scenario.label }}</label>
    <select id="scenarioPreset" :value="form.scenarioPreset" @change="onChange">
      <option value="custom">{{ text.common.custom }}</option>
      <option value="graphsync-adaptive">{{ text.scenario.graphsyncAdaptive }}</option>
      <option value="boxo-bitswap-trace">{{ text.scenario.boxoBitswapTrace }}</option>
    </select>
    <small>{{ uiStore.modeLocked ? text.scenario.lockedHint : text.scenario.unlockedHint }}</small>
  </div>
</template>
