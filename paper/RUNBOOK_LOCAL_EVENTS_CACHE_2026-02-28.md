# Runbook Lokaler Event-Cache (429-sicher)

Stand: 2026-02-28

## Ziel
Event-Ermittlung von der Modellrechnung trennen:
1. Events einmalig vorrechnen und lokal cachen.
2. Chunk-Analyse nur noch aus lokalem Cache (`cache-only`) laufen lassen.

Damit werden Live-API-Limits (`429 Too Many Requests`) aus der eigentlichen Produktionsrechnung entfernt.

## Neue Komponenten
1. Precompute:
   - `backend/precompute_auto_events_cache.py`
   - `run_precompute_auto_events_cache.bat`
2. Cache-only Verarbeitung:
   - `backend/run_field_event_batch.py` mit:
     - `--events-auto-cache-only`
     - `--events-auto-use-cached-empty`
3. Durchgereicht bis Chunk/Multiwindow:
   - `backend/run_field_event_batch_sa_chunks.py`
   - `backend/run_sa_icon2d_multiwindow_chunks.py`

## Wichtige Details
1. Radar-Limit:
   - Das Radar-Backend akzeptiert max. `4320h` pro Abfrage.
   - Lange Fenster werden automatisch in Teilfenster gesplittet und zusammengefuehrt.
2. 429-Handling:
   - 429 wird nicht als gueltiges `no events` interpretiert.
   - Retry + Backoff + Mindestabstand zwischen Requests sind aktiv.

## Schritt A: Event-Cache vorrechnen
Beispiel (10 Felder, Test):
```bat
run_precompute_auto_events_cache.bat --fields-geojson paper\input\event_probe_chunk6_10.geojson --cache-dir paper\cache\auto_events_radar_test\20230401_20231031 --source radar --start 2023-04-01 --end 2023-10-31 --top-n 3 --min-severity 1 --request-retries 3 --retry-backoff-initial-s 2 --retry-backoff-max-s 10 --min-interval-s 0.5 --checkpoint-every 5 --out-csv paper\exports\precompute_radar_chunk6_10_2023.csv
```

Ergebnis:
1. Cache-Dateien pro Feld im `cache-dir`.
2. Protokoll:
   - `paper/exports/precompute_radar_chunk6_10_2023.csv`
   - `paper/exports/precompute_radar_chunk6_10_2023.meta.json`

## Schritt B: Analyse nur aus Cache (ohne Live-Events)
```bat
run_field_event_batch.bat --fields-geojson paper\input\event_probe_chunk6_10.geojson --events-source auto --events-auto-source radar --events-auto-start 2023-04-01 --events-auto-end 2023-10-31 --events-auto-top-n 3 --events-auto-min-severity 1 --events-auto-cache-dir paper\cache\auto_events_radar_test\20230401_20231031 --events-auto-cache-only --events-auto-use-cached-empty --analysis-modes erosion_events_ml,abag --dem-source cog --out-csv paper\exports\cache_only_batch_test.csv --continue-on-error
```

## Produktionsschema (SA-Chunks)
1. Pro Fenster zuerst Cache-Precompute auf den betreffenden Chunk-GeoJSONs.
2. Danach Multiwindow/Chunk-Runner mit:
   - `--events-auto-cache-only`
   - `--events-auto-use-cached-empty`
3. Erst dann Merge/QA/Quickcheck.

## SA-weit (3 Jahre) - aktuelle Produktionsvariante
Die Produktionsvariante fuer das Paper laeuft nicht als 50k-Sample, sondern SA-weit in drei parallelen Jahresfenstern:
1. `2023-04-01 .. 2023-10-31`
2. `2024-04-01 .. 2024-10-31`
3. `2025-04-01 .. 2025-10-31`

Technische Form:
1. Stage A SA-weit ueber `backend/precompute_sa_events_chunks.py`
2. `chunk_size=1000`, `total_chunks=2705` je Jahrfenster
3. Resume-faehig ueber `precompute_state.json` + `.done` Flags
4. Zentrale Caches:
   - Feld-Cache: `data/events/sa_2km/icon2d_<window>/field_cache`
   - Zell-Cache (2 km): `data/events/sa_2km/icon2d_<window>/cell_cache`

Warum dauert Stage A trotz "nur Wetter" laenger:
1. Es wird nicht nur Wetter geladen, sondern pro Feld ein Event-Set erzeugt.
2. Pro Feld werden Cache-Dateien und Metadaten persistiert.
3. Das geschieht SA-weit fuer Millionen Felder und drei Zeitfenster.
4. Der Lauf ist I/O- und API-intensiv, nicht nur CPU-intensiv.

Aktueller Gesamtfortschritt (menschenlesbar):
1. `paper/exports/automation/stage_a_sawide_3years_<tag>/overall_progress.log`
2. Zeigt pro Minute:
   - Gesamt `completed/total` ueber alle drei Jahre
   - Fortschritt je Jahrfenster
   - `failed` Zaehler

## Paper-Hinweis
Im Methodenabschnitt klar trennen:
1. Event-Precompute (meteorologische Erkennung),
2. Modelllauf (ABAG + Event-ML aus lokalem Event-Cache),
3. Ergebnisaggregation.
