# Analyse und Thesis-Figuren

Der Ordner `analysis/` erzeugt SVG-Abbildungen und CSV-Tabellen aus den Run-Exports der Benchmark-UI. Die Auswertung ist für Thesis-Abbildungen gedacht und gruppiert Runs nach Scenario-Presets, insbesondere `graphsync-adaptive` und `boxo-bitswap-trace`.

Die Analyse liest strukturierte Kennzahlen aus dem top-level Feld `summary` der Run-JSON-Dateien. Fehlende Summaries werden nicht aus dem rohen `output` rekonstruiert. Wenn ein Run kein `summary` enthält, sollte der Benchmark neu ausgeführt werden.

## Setup

Die Python-Abhängigkeiten stehen in `analysis/requirements.txt`:

```bash
python -m pip install -r analysis/requirements.txt
```

Verwendet werden vor allem `pandas`, `matplotlib`, `seaborn` und `scienceplots`.

## Nutzung

Aus einem ZIP-Export der UI (`/runs/download`):

```bash
python analysis/plot_runs.py --input bench-runs.zip --out analysis/figures
```

Aus einem Ordner mit Run-JSON-Dateien:

```bash
python analysis/plot_runs.py --input path/to/runs --out analysis/figures
```

Optionale Parameter:

- `--experiment 1_heterogen`: filtert auf einen Experimentordner oder Experiment-Key, zum Beispiel `1_heterogen`.

Die Abbildungen werden standardmaessig im `paper`-Stil erzeugt. Es gibt keine separate Kontext-Auswahl fuer `notebook`, `talk` oder `poster`.

Beispiel:

```bash
python analysis/plot_runs.py --input bench-runs.zip --out analysis/figures/1_heterogen --experiment 1_heterogen
```

Die Web-UI nutzt intern den Plot-Server und ruft `/plots/download` auf. Für lokale Auswertung ist `analysis/plot_runs.py` der direktere Weg.

## SVG-Ausgaben

Das Skript erzeugt, je nach vorhandenen Daten, folgende SVG-Dateien im Ausgabeordner:

| Datei | Inhalt |
| --- | --- |
| `qoe_summary.svg` | QoE-Kennzahlen wie Stalls, Stall-Zeit und Deadline-Miss-Rate |
| `throughput_summary.svg` | Durchsatzvergleich der Szenarien |
| `segment_timing_summary.svg` | Segment-Ready-Zeiten und Playback-Lateness |
| `provider_load.svg` | Lastverteilung auf Provider/Peers |
| `peer_utilization.svg` | Peer-Auslastung und Provider-Anteile |
| `provider_usage.svg` | Provider-Nutzung als Konzentrations- und Anteilsauswertung |
| `overhead.svg` | Overhead-Auswertung, insbesondere Racing- und Bitswap-Overhead |
| `gs_scheduler_analysis.svg` | GraphSync-Scheduler-Entscheidungen nach Dringlichkeit, Distanz und Peer-Klasse |
| `duration_prediction_accuracy.svg` | Genauigkeit der Dauerschätzungen von Segment-Fetches |
| `ema_accuracy.svg` | Genauigkeit der EMA-Durchsatzschätzung |

Wenn für eine Abbildung keine passenden Daten vorhanden sind, wird sie übersprungen.

## CSV-Ausgaben

Zusätzlich exportiert das Skript CSV-Tabellen für weitere Auswertungen:

| Datei | Inhalt |
| --- | --- |
| `runs_flat.csv` | Eine Zeile pro Run mit den Summary-Metriken |
| `peers_flat.csv` | Peer-Statistiken pro Run |
| `provider_shares.csv` | Berechnete Byte-Anteile pro Provider und Run |
| `provider_concentration.csv` | Konzentrationsmasse, HHI und effektive Provider-Anzahl |
| `samples_flat.csv` | Rohe GraphSync-Durchsatzsamples |
| `predictions_flat.csv` | Dauerprognosen vor Requests und tatsächliche Ergebnisse |
| `scheduler_events.csv` | Rohe GraphSync-Scheduler-Events |
| `bitswap_segment_readiness.csv` | Virtuelle Segment-Ready-Events aus dem Boxo/Bitswap-Trace |
| `segment_lateness.csv` | Segment-Ready-Zeiten relativ zur Playback-Deadline |

Nicht jede Datei entsteht bei jedem Input. GraphSync-spezifische Tabellen setzen GraphSync-Runs voraus, Bitswap-spezifische Tabellen setzen Bitswap-Trace-Runs voraus.
