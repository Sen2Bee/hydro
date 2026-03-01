# Runbook SA Auto-Events + Chunk-Runner (Paper)

Stand: 2026-02-28

## Scope-Strategie (fuer Folgechats verbindlich)
1. `Pilot`: `50 Chunks` (ca. 50.000 Felder) sind ausreichend fuer methodischen Nachweis und operativen Demonstrator.
2. `Belastbarer Projekt-Hinweis`: `200+ Chunks` als empfohlene Groesse fuer robustere Aussagen.
3. `Landesweit SA`: `2705 Chunks` (alle ~2,704,579 Flaechen) nur bei explizitem Auftrag/Budget.
4. Default-Entscheidung ohne neue Freigabe:
  - nicht automatisch auf `2705` hochskalieren,
  - zuerst Pilot/200+ sauber abschliessen (Run + QA + Merge + Quickcheck).

## Methodik-Update 2026-02-26 (wichtig fuer Paper)
1. `rowid`-Chunking bleibt fuer Betriebslaeufe ok, ist aber fuer Karten/Publikation potentiell verzerrt.
2. Fuer Paper-Samples wird ab sofort ein raeumlich stratifizierter Sampler genutzt:
   - `backend/build_sa_spatial_sample.py`
   - Wrapper: `run_build_sa_spatial_sample.bat`
3. Damit wird die Verteilung ueber SA fuer Karten und Evaluation konsistenter.

## Auto-Event Standard 2026-02-27 (verbindlich)
1. Produktivpfad fuer Paper/Batch: `source=icon2d`.
2. Verbindliche Fenster:
  - `2023-04-01..2023-10-31`
  - `2024-04-01..2024-10-31`
  - `2025-04-01..2025-10-31`
3. Radar/DWD nur als Sensitivitaets-/Vergleichslauf, nicht als Default.
4. Hintergrund:
  - `icon2d`-only liefert fuer die Pilotfenster stabile und schnelle Events.
  - 10-Flaechen-3-Fenster-Probe erfolgreich (`30/30` HTTP 200).

## C/Crop-Update 2026-02-28 (verbindlich fuer Paper-Pipeline)
1. Dynamischer `C` ist jetzt methodisch versioniert (`data/config/c_factor_method_v1*.json`).
2. Open Crop-History wird aus frei verfuegbaren Datensaetzen aufgebaut:
   - Downloader: `backend/fetch_open_crop_history.py` (`run_fetch_open_crop_history.bat`)
   - Crop-History-Build: `backend/build_crop_history_from_open_data.py` (`run_build_crop_history.bat`)
3. Automatische Kette (ohne manuelle Eingriffe):
   - `backend/queue_build_crop_history_after_fetch.py`
   - `backend/queue_dynamic_c_after_crop_history.py`
4. C-Build mit Crop-Historie:
   - `run_build_dynamic_c_windows_with_crop.bat`
5. Sensitivitaetslauf C-Methodik:
   - `run_c_factor_sensitivity.bat`
6. `P` bleibt standardmaessig `1.0` (Baseline); Szenarien separat rechnen.

## Ziel
Dieses Runbook dokumentiert den produktiven, reproduzierbaren SA-Workflow fuer:
1. `ABAG` (langfristiger Risikoindex) und
2. `erosion_events_ml` (ereignisbezogene Wahrscheinlichkeit)
auf Schlagflaechen mit automatisch ermittelten Starkregen-Eventfenstern.

## Kurzfazit (fachlich)
Der Ansatz ist praxisnah und plausibel, weil:
1. Ereignisfenster je Schlag aus derselben App-Wetterpipeline kommen (`/abflussatlas/weather/events`).
2. Topographie lokal aus SA-DGM1-COG genutzt wird (`dem_source=cog`), nicht langsames Live-WCS.
3. Ergebnisse als tabellarische Schlag x Event x Modus-Daten reproduzierbar exportiert werden.

## Methodik (technisch)
1. Schlagquelle: `data/raw/sa_flurstuecke/cache/flurstuecke.sqlite` (SA-weit).
2. Chunking: `run_field_event_batch_sa_chunks.py` zerlegt in RowID-Bloecke.
3. Event-Fenster:
   - `events_source=auto`
   - Endpoint: `/abflussatlas/weather/events`
   - Quelle: `icon2d`
   - Fensterzeitraeume (mehrjaehrig): `2023/2024/2025` jeweils `04-01..10-31`
   - je Schlag: `top_n=3` (empfohlen), `min_severity=1`
4. Analyse pro Schlag x Event:
   - `analysis_modes=erosion_events_ml,abag`
   - `dem_source=cog`, `provider=auto`, `threshold=200`
5. Checkpoints:
   - partielle CSV-Schreibungen alle `100` Zeilen.
6. Resume:
   - Zustand in `paper/exports/sa_chunks/sa_chunk_run_state.json`.

## Relevante Robustheitsfixes (vor Lauf umgesetzt)
Datei: `backend/run_field_event_batch.py`
1. Auto-Events mit `event_start_iso == event_end_iso` werden auf min. 1h normalisiert (`_ensure_nonzero_event_window`).
2. Dieselbe Normalisierung gilt auch beim Laden aus `paper/cache/auto_events` (Cache-konsistent).
3. Damit werden systematische `400 Bad Request` fuer Null-Fenster vermieden.

## Exakte Startkommandos
### Backend (lokale COG-Nutzung)
```bat
set ST_COG_DIR=C:\Users\thoko\.gemini\antigravity\scratch\hydrowatch-berlin\data\dem_cache\st_dgm1_cog
run_backend.bat
```

### Paper-Sample (raeumlich stratifiziert, 50k)
```bat
run_build_sa_spatial_sample.bat --target-count 50000 --grid-rows 20 --grid-cols 20 --seed 42 --out-geojson paper\input\schlaege_sa_spatial_50k.geojson
```

Danach Batch auf dem Sample:
```bat
run_field_event_batch.bat --fields-geojson paper\input\schlaege_sa_spatial_50k.geojson --events-source auto --events-auto-source icon2d --events-auto-start 2024-04-01 --events-auto-end 2024-10-31 --events-auto-top-n 3 --analysis-modes erosion_events_ml,abag --dem-source cog --api-base-url http://127.0.0.1:8001 --checkpoint-every 100 --continue-on-error --out-csv paper\exports\field_event_results_spatial_50k_icon2d_2024.csv
```

Mehrjaehrige Probe mit sauberem Progress-Log:
```bat
run_event_probe_icon2d_windows.bat --fields-geojson paper\input\schlaege_sa_spatial_10.geojson --windows "2023-04-01:2023-10-31,2024-04-01:2024-10-31,2025-04-01:2025-10-31"
```

### Produktionslauf (5 Chunks, Auto-Events, lokal COG)
```bat
run_field_event_batch_sa_chunks.bat --chunk-size 1000 --start-chunk 1 --max-chunks 5 --dem-source cog --events-source auto --events-auto-source icon2d --events-auto-start 2024-04-01 --events-auto-end 2024-10-31 --events-auto-top-n 3 --checkpoint-every 100 --continue-on-error
```

### Hinweis fuer 3-Fenster-Produktion
`run_field_event_batch*.py` verarbeitet pro Lauf genau ein Auto-Event-Fenster (`events-auto-start/end`).
Fuer die 3 Fenster daher drei separate Laeufe starten und danach mergen.

## Neue Standardpfade (2026-02-28)
1. Open Crop Downloads:
   - `data/raw/crop_history_open/downloads/`
   - `data/raw/crop_history_open/sa_clips/`
   - `data/raw/crop_history_open/logs/`
2. Crop-History:
   - `data/derived/crop_history/crop_history.csv`
   - `data/derived/crop_history/crop_history.meta.json`
   - `data/derived/crop_history/logs/`
3. Dynamischer C:
   - `data/layers/c_dynamic_sa/ndvi/`
   - `data/layers/c_dynamic_sa/c_factor/`
   - `data/layers/c_dynamic_sa/run_manifest.json`
   - `data/layers/c_dynamic_sa/logs/`

## Aktueller Betriebsstatus (t05-Run)
1. Aktiver Chunk-Run:
   - `paper/exports/sa_chunks_icon2d_t05/automation/icon2d_multiwindow_20260228_050725.log`
2. Exportziel:
   - `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/`
3. Parameter:
   - `ml_threshold=0.05`
   - `events_auto_source=icon2d`
   - `chunk_size=1000`, `max_chunks=100`
4. Hinweis:
   - Lauf nicht mit neuen Methoden im selben Exportbaum mischen.
   - Nach Methodenwechsel immer separaten Export-Root verwenden.

## Aktueller Lauf (dokumentierter Start)
1. Konsolenlog:
   - `paper/exports/sa_chunks_cog_auto_run_20260224_213052.log`
2. Run-State:
   - `paper/exports/sa_chunks/sa_chunk_run_state.json`
3. Run-Manifest:
   - `paper/exports/sa_chunks/runs/sa_chunk_run_20260224T203053Z.json`

## Monitoring (CMD-kompatibel, ohne PowerShell)
```bat
type paper\exports\sa_chunks_cog_auto_run_20260224_213052.log
type paper\exports\sa_chunks\sa_chunk_run_state.json
dir paper\exports\sa_chunks
```

## QA und Plausibilitaetspruefung (pflicht fuer Paper)
Nach jedem Chunklauf:
```bat
run_validate_field_event_results.bat paper\exports\sa_chunks\field_event_results_chunk_00001.csv
```

Pruefpunkte:
1. keine harten Fehler (`ok=true` in QA-JSON).
2. niedrige Fehlerquote (`error_rate_percent`).
3. NoData-Anteile im erwartbaren Rahmen (`nodata_rate_percent`).
4. keine unplausiblen Extremwerte in:
   - `risk_score_mean`
   - `event_probability_mean`
   - `abag_index_mean`
5. `nodata_only=true` und `dem_valid_cell_share` konsistent interpretieren.

## Konsolidierung nach mehreren Chunks
Nach Abschluss eines Chunk-Blocks (z. B. 5 Chunks):
```bat
run_merge_sa_chunk_results.bat --exports-dir paper\exports\sa_chunks --run-quickcheck
```

Erzeugte Gesamtartefakte:
1. `paper/exports/sa_chunks/field_event_results_merged.csv`
2. `paper/exports/sa_chunks/field_event_results_merged.qa.json`
3. `paper/exports/sa_chunks/field_event_results_merged.report.md`
4. `paper/exports/sa_chunks/field_event_results_merged.manifest.json`

Optional (bei `--run-quickcheck`):
1. `paper/exports/quickcheck/quickcheck_*_top10.csv`
2. `paper/exports/quickcheck/quickcheck_*_top10_measures.csv`
3. `paper/exports/quickcheck/quickcheck_*_report.md`

## Paper-Einordnung vs. Pedro-Ansatz
1. Nicht identisch zur urspruenglichen Pedro-Studie:
   - dort: Training mit manuell/extern gelabelten Erosionsereignissen.
2. Hier: operationaler, app-integrierter Event-Workflow fuer SA-Skalierung.
3. Publikationsfaehig als:
   - reproduzierbare Methodik,
   - skalierbarer SA-Batchprozess,
   - klar dokumentierte Grenzen (fehlende externe Ground-Truth-Labels).

## Mindestartefakte fuer Manuskript/Supplement
1. Runbook: diese Datei.
2. Run-Manifest JSON: `paper/exports/sa_chunks/runs/sa_chunk_run_20260224T203053Z.json`
3. State JSON: `paper/exports/sa_chunks/sa_chunk_run_state.json`
4. Chunk-CSV + QA-JSON je Chunk.
5. Versionsangaben:
   - `backend/run_field_event_batch.py`
   - `backend/run_field_event_batch_sa_chunks.py`
   - `backend/main.py`

## Grenzen (transparent berichten)
1. Auto-Events sind meteorologisch getrieben, keine direkten Erosionsbeobachtungslabels.
2. Kleine/degenerierte Geometrien koennen numerisch instabil sein.
3. Aussage staerker auf methodischer Operationalisierung als auf kausaler Endgueltigkeit.
4. Mehrfensterlaeufe erfordern aktuell mehrere sequenzielle Batch-Runs (pro Fenster ein Run).

## Naechster Schritt (paper-praktisch)
Nach Abschluss der 5 Chunks:
1. pro Chunk QA laufen lassen,
2. Chunk-CSV zusammenfuehren,
3. Gesamt-QA und Tabellen fuer Methoden-/Ergebnisteil erzeugen.

## 429-Schutz (ICON2D/Open-Meteo)
1. Hintergrund:
   - Bei hoher Last kann der Event-Dienst trotz HTTP 200 intern `429 Too Many Requests` melden.
2. Ab jetzt umgesetzt:
   - Solche Antworten werden nicht mehr als echte `no events` interpretiert.
   - Event-Fetch hat jetzt Throttling + Retry/Backoff.
3. Relevante Parameter:
   - `--events-auto-request-retries` (Default `6`)
   - `--events-auto-retry-backoff-initial-s` (Default `5`)
   - `--events-auto-retry-backoff-max-s` (Default `90`)
   - `--events-auto-min-interval-s` (Default `1.5`)
4. Betriebsregel:
   - Chunks, die vor dem Fix gelaufen sind und viele `no events` enthalten, neu rechnen.
   - Methodik nicht im selben Export-Root mischen.
