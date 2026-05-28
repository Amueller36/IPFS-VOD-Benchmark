export const runOutputLabels = {
  segmentsFetched: 'Segmente geladen',
  dataSentMb: 'Gesendete Daten (MB)',
  pending: 'Ausstehend',
  fetching: 'Wird geladen',
  prefetched: 'Vorgeladen/Prefetched',
  played: 'Abgespielt (grüne horizontale Linie)',
  urgentWindow: 'Urgent Window',
  scheduledUrgent: 'Als Urgent gescheduled (rotes horizontale Linie)'
} as const;

export const messages = {
  app: {
    title: 'IPFS Streaming Benchmark'
  },
  video: {
    title: 'Input Video',
    duration: 'Videodauer',
    quality: 'Qualität',
    bitrate: 'Bitrate',
    size: 'Größe',
    missing: 'input.mp4 nicht gefunden'
  },
  common: {
    notAvailable: 'n/v',
    custom: 'Benutzerdefiniert',
    graphsync: 'GraphSync',
    bitswap: 'Bitswap'
  },
  downloads: {
    title: 'Downloads',
    runs: 'Runs herunterladen',
    generateTimelines: 'Timeline Videos erzeugen',
    timelines: 'Timeline Videos herunterladen',
    plots: 'Python Plots herunterladen (SVG)',
    clear: 'Gespeicherte Runs löschen',
    deleted: (count: number) => `Gelöschte Run Dateien: ${count}`,
    timelinesGenerated: (generated: number, skipped: number) => `Timeline Videos erzeugt: ${generated}, übersprungen: ${skipped}`
  },
  seed: {
    title: '1. Seeding',
    segmentKb: 'Seed Segment KB (GraphSync)',
    chunkKb: 'Seed Chunk KB (GraphSync)',
    bitswapChunkKb: 'Seed Block KB (Bitswap)',
    segmentHint: 'Segmentgröße im POSL Layout.',
    chunkHint: 'Chunk Größe muss die Segmentgröße teilen.',
    bitswapChunkHint: 'Kubo Fixed-Size Chunking, maximal 1024 KB.',
    layoutCid: 'Layout CID',
    bitswapCid: 'Bitswap CID',
    seedLayout: 'POSL Layout seeden (GraphSync)',
    seedBitswap: 'Bitswap/Kubo Provider seeden',
    refresh: 'Seed Info aktualisieren',
    bitswapHint: '',
    graphsyncHint: '',
    idle: 'Bereit.',
    noOutput: 'Noch keine Seed Ausgabe.',
    status: {
      seeding: 'Seeding läuft...',
      seedComplete: 'Seed abgeschlossen.',
      seedFailed: 'Seeding fehlgeschlagen.',
      bitswapSeeding: 'Bitswap Provider Seeding läuft...',
      bitswapComplete: 'Bitswap Provider Seed abgeschlossen.',
      bitswapFailed: 'Bitswap Provider Seed fehlgeschlagen.',
      refreshing: 'Seed Info wird aktualisiert...',
      refreshComplete: 'Seed Info aktualisiert.',
      refreshFailed: 'Seed Info Aktualisierung mit Fehlern abgeschlossen.'
    },
    refreshItems: {
      video: 'Video Info',
      graphsync: 'GraphSync Seed Info',
      bitswap: 'Bitswap/Kubo Seed Info',
      peers: 'Peer Anzahl'
    },
    refreshOk: 'OK',
    refreshError: 'Fehler',
    noGraphsyncSeed: 'Kein GraphSync Seed gefunden.',
    noBitswapSeed: 'Kein Bitswap/Kubo Seed gefunden.',
    requestFailed: (message: string) => `Seed Anfrage fehlgeschlagen: ${message}`,
    bitswapRequestFailed: (message: string) => `Bitswap Provider Seed Anfrage fehlgeschlagen: ${message}`
  },
  scenario: {
    label: 'Szenario Preset',
    graphsyncAdaptive: 'GraphSync (adaptiver Scheduler)',
    boxoBitswapTrace: 'Boxo Bitswap Baseline',
    lockedHint: 'Preset aktiv: Modus ist gesperrt. Zu Benutzerdefiniert wechseln, um den Modus manuell zu ändern.',
    unlockedHint: ''
  },
  networkPreset: {
    label: 'Netzwerk Preset',
    custom: 'Benutzerdefiniert',
    heterogen: '1 heterogen',
    homogen: '2 homogen',
    bandwidthStress: '3 Bandbreitenstress',
    packetLoss: '4 Packet Loss'
  },
  runForm: {
    title: '2. Run Konfiguration',
    mode: 'Modus',
    networkProfile: 'Peer Netzwerkkonfiguration',
    networkHint: 'Gilt pro logischem Peer Slot für GraphSync und Bitswap/Kubo.',
    peerSlot: 'Peer',
    peerLabel: 'Label',
    peerRateMbit: 'Mbit/s',
    peerLatencyMs: 'Latency ms',
    peerJitterMs: 'Jitter ms',
    peerLossPct: 'Packet Loss %',
    prefetch: 'Prefetch Segmente',
    prefetchHint: '-1 = so viele Segmente wie möglich.',
    workers: 'Prefetch Worker',
    workersHint: '0 = automatisch, Anzahl verbundener GraphSync Peers.',
    raceFanout: 'Race Fanout',
    raceFanoutHint: '-1 = alle verbundenen GraphSync Peers.',
    discoveryFanout: 'Discovery Fanout',
    playbackMs: 'Playback ms',
    playbackHintFallback: 'Automatische Playback ms = SegmentGröße / (Videogröße / Videodauer) = ${value} ms',
    playbackHintValue: (value: number) => `Automatische Playback ms = SegmentGröße / (Videogröße / Videodauer) = ${value} ms`,
    playbackSpeed: 'Playback Geschwindigkeit (x)',
    runtimeEstimate: 'Geschätzte Laufzeit (s), wenn keine Stalls auftreten:',
    runtimeMissingSeed: 'zuerst Layout seeden',
    runtimeHint: 'Geschätzt als Anzahl Segmente * playbackMs / playbackSpeed.',
    urgentWindow: 'Urgent Window',
    emaAlpha: 'EMA alpha',
    repeatCount: 'Runs wiederholen',
    interRunDelayMs: 'Pause zwischen Runs ms',
    seedBitswapFirst: 'Zuerst Bitswap Provider seeden.',
    seedLayoutFirst: 'Zuerst Layout seeden.',
    addScenario: '2. Szenario zur Queue hinzufügen',
    clearQueue: 'Queue leeren',
    exportQueue: 'Queue Konfiguration exportieren',
    importQueue: 'Queue Konfiguration importieren',
    runQueue: '3. Queue starten',
    stopCurrent: 'Aktuellen Run stoppen',
    stopBatch: 'Batch stoppen'
  },
  queue: {
    title: 'Queue',
    remove: 'Entfernen',
    empty: 'Noch keine Szenarien in der Queue.',
    repeats: 'Wiederholungen'
  },
  runOutput: {
    title: 'Run Ausgabe',
    copy: 'Logs kopieren',
    copied: 'Kopiert',
    copyFailed: 'Kopieren fehlgeschlagen',
    currentState: 'Aktueller Status',
    started: 'gestartet',
    idle: 'bereit',
    noOutput: 'Noch keine Run Ausgabe.'
  },
  throughput: {
    title: 'Übertragungsrate',
    axis: 'Übertragungsrate (Mbit/s)',
    hint: 'Zeigt, wie viele Videodaten pro Sekunde tatsächlich beim Client ankommen. Höher ist besser. Einbrüche zeigen langsamere Transfers oder Pausen.',
    empty: 'Noch keine Messwerte zur Übertragungsrate in der aktuellen Run Ausgabe.'
  },
  peerPie: {
    title: 'Peer Kreisdiagramm',
    hint: '',
    empty: 'Noch keine Peer Beiträge in der aktuellen Run Ausgabe.'
  },
  playback: {
    timelineTitle: 'Virtuelle Playback Zeitachse/Timeline',
    timelineHint: '',
    stalls: 'Stalls',
    stallTime: 'Stall Zeit',
    startup: 'Start',
    playbackDone: 'Playback fertiggestellt',
    downloadTime: 'Download Zeit'
  }
} as const;

type WidenText<T> = T extends string
  ? string
  : T extends (...args: infer Args) => string
    ? (...args: Args) => string
    : { [K in keyof T]: WidenText<T[K]> };

export type UiMessages = WidenText<typeof messages>;
export type RunOutputLabels = WidenText<typeof runOutputLabels>;
