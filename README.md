# IPFS Streaming Bench

Dieses Repository enthält eine Benchmark Umgebung für IPFS-basiertes Video-Streaming. Der Benchmark vergleicht einen eigenen GraphSync-Client mit adaptiver Peer-Auswahl gegen eine Boxo/Bitswap-Baseline. Ziel ist, ein Videostream Abruf unter dezentralen Bedingungen reproduzierbar zu simulieren. Dabei werden QoE und andere Metriken erhoben um so verschiedene Ansätze miteinander vergleichen zu können. 

Die Umgebung besteht aus:

- GraphSync-Client und GraphSync-Peers (`cmd/gs-client`, `cmd/gs-peer`)
- Boxo/Bitswap-Client als Baseline (`cmd/boxo-bitswap-client`)
- Seeder für das POSL-Layout, also das segmentierte DAG-Layout des Videos (`cmd/seed`)
- Docker-Compose-Topologie mit schnellen und langsamen Peers (`docker-compose.yml`, `docker/`)
- Web-UI zum Seeden, Konfigurieren und Starten von Runs (`cmd/benchmark-ui`, `ui/`)
- Plot- und CSV-Auswertung für exportierte Runs (`analysis/`)

## Voraussetzungen

Der Benchmark sollte auf einem nativen Linux-Host laufen. Eventuell funktioniert es auch auf MacOS, wurde aber nicht getestet. Die Netzwerkprofile werden mit `tc/netem` in den Containern gesetzt. Docker Desktop mit WSL2 erzeugt hierbei Fehler und kann nicht genutzt werden.

Nötig sind:

- Linux-Host mit Docker und Docker Compose
- Zugriff auf `/var/run/docker.sock` für die Benchmark-UI
- ein Video, das als `videos/input.mp4` im Repository liegt

Vor dem ersten Lauf sollten auf dem Docker Host die Host Netzwerkparameter gesetzt werden:

```bash
sudo bash scripts/setup-linux-host.sh
```

Das Skript setzt größere UDP-Socket-Buffer für libp2p/quic-go und bricht ab, wenn es WSL erkennt.

## Setup

1. Video ablegen:

```bash
mkdir -p videos
cp /pfad/zum/video.mp4 videos/input.mp4
```

Der Dateiname muss `input.mp4` sein, weil die Docker-Services diesen Pfad erwarten.

2. Docker-Images bauen:

```bash
docker compose build
```

3. Stack starten:

```bash
docker compose up -d
```

Der optionale `seed`-Service erzeugt denselben GraphSync/POSL-Seed-Pfad wie die UI: `/data/seed/manifest.json` und `/data/seed/blocks` im `graphsync-data` Volume.

4. Web-UI öffnen:

```text
http://localhost:54444
```

## Einen einzelnen Benchmark ausführen

Der normale Weg für einen einzelnen Run führt über die Web-UI.

1. Im Bereich für das Seeding die Werte wählen: GraphSync nutzt `Seed Segment KB` und `Seed Chunk KB`, Bitswap nutzt nur `Seed Block KB` bis maximal 1024 KB.
2. `Seed Info Aktualisieren` ausführen, um vorhandene Seed-Metadaten zu laden.
3. `POSL Layout seeden` ausführen. Falls die GraphSync-Peers beim ersten Mal noch neu starten oder das Manifest noch nicht sehen, den Schritt wiederholen, bis die UI einen erfolgreichen Seed meldet.
4. Für einen Bitswap-Run zusätzlich `Bitswap seeden` ausführen. GraphSync/POSL und Bitswap/Kubo verwenden unterschiedliche Roots; der Bitswap-Root wird in `/data/kubo_cid.txt` im gemeinsamen Kubo-Volume gespeichert.
5. Ein Szenario auswählen:
   - `graphsync-adaptive`: GraphSync mit adaptivem Scheduler, Prefetching, Racing und Durchsatzmessung.
   - `boxo-bitswap-trace`: Boxo/Bitswap-Baseline mit Trace-Auswertung.
   - `custom`: manuelle Parameterwahl.
6. Ein Netzwerkprofil wählen oder die Peer-Netzwerkwerte manuell setzen. Die Profile bilden unterschiedliche Bandbreiten, Latenzen, Jitter und Paketverlust ab.
7. Das Szenario zur Queue hinzufügen.
8. Die Queue starten.
9. Nach Abschluss die Ergebnisse in der UI herunterladen.

## Ergebnisse und Analyse

Die Web-UI speichert Run-Daten als JSON und bietet Downloads für Runs, Plots und Timeline-Exporte an. Die persistenten Run-Daten liegen im Docker-Volume `runs-data`.

Für die Thesis-Plots kann ein Run-Export aus der UI mit dem Analyse-Skript verarbeitet werden, oder einfach per Web UI heruntergeladen werden:

```bash
python -m pip install -r analysis/requirements.txt
python analysis/plot_runs.py --input bench-runs.zip --out analysis/figures
```

Alternativ kann statt der ZIP-Datei ein Ordner mit Run-JSON-Dateien angegeben werden:

```bash
python analysis/plot_runs.py --input path/to/runs --out analysis/figures
```

Die Analyse erzeugt SVG-Abbildungen und CSV-Tabellen, unter anderem `runs_flat.csv`, `peers_flat.csv`, `scheduler_events.csv`, `bitswap_segment_readiness.csv` und `segment_lateness.csv`.


## Nützliche Befehle

Stack starten:

```bash
docker compose up -d
```

Logs der UI anzeigen:

```bash
docker compose logs -f benchmark-ui
```

Stack stoppen:

```bash
docker compose down
```

Stack inklusive Volumes löschen, wenn Seeds und Runs komplett neu erzeugt werden sollen:

```bash
docker compose down -v
```

## Troubleshooting

- `tc/netem` funktioniert nicht oder Netzwerkprofile wirken nicht: auf einem nativen Linux-Host ausführen. Docker Desktop mit WSL2 ist für diese Messungen nicht geeignet.
- `quic-go` meldet zu kleine UDP-Buffer: `sudo bash scripts/setup-linux-host.sh` ausführen und Container danach neu erstellen.
- Die UI meldet, dass `videos/input.mp4` fehlt: Datei exakt unter diesem Pfad ablegen und den Stack bei Bedarf neu starten.
- GraphSync-Peers finden den Seed nicht: `POSL Layout seeden` erneut ausführen oder die Container mit `docker compose up -d --force-recreate` neu erstellen.
- Ein Run verwendet alte Daten: Run- und Seed-Volumes mit `docker compose down -v` entfernen und danach neu bauen/starten.
