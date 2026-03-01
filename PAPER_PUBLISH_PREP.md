# Paper-Prep (Technisch + Reproduzierbar)

Stand: 23.02.2026

## Ziel
Sicherstellen, dass ein Paper kurzfristig einreichbar ist mit:
- reproduzierbarer Methodik,
- klarer Datenlinie,
- nachvollziehbaren Artefakten.

## 1) Mindestumfang fuer Einreichung
1. Methodikdokument:
- Modelltrennung sauber darstellen: `ABAG` (langfristig) vs. `Event-ML` (ereignisbasiert).
- Alle Faktoren/Features explizit mit Quelle und Aufloesung nennen.

2. Reproduzierbarer Lauf:
- Laufkommandos dokumentieren.
- Eingabeparameter (BBox, Zeitraum, Modellversion) dokumentieren.
- Outputdateien + SHA256 dokumentieren.

3. Daten-/Codeverweis:
- Persistente Quellen (DOI/URL) nennen.
- Git-Commit und Skriptversionen festhalten.

## 2) Was im Repo schon vorbereitet ist
- Laufstandard: `DOKUMENTATION_NACHVOLLZIEHBARKEIT.md`
- C-Pipeline:
  - `run_fetch_sentinel_ndvi.bat`
  - `run_build_c_factor_proxy.bat`
- Lauf-Logs:
  - `data/layers/st_mwl_erosion/logs/c_pipeline.log`
- Laufmetadaten:
  - `data/layers/st_mwl_erosion/NDVI_latest.json`
  - `data/layers/st_mwl_erosion/C_Faktor_proxy.json`
- Paper-Manifest:
  - `run_paper_prep.bat`
  - Ausgabe: `paper/manifest/paper_artifact_manifest.json`

## 3) Standard-Workflow (vor Manuskript-Freeze)
1. Faktoren aktualisieren:
```bat
run_st_mwl_erosion_fetch.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50 --layers "K-Faktor,R-Faktor,S-Faktor"
```

2. C-Pipeline ausfuehren:
```bat
run_fetch_sentinel_ndvi.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50 --start 2025-05-01 --end 2025-08-31
run_build_c_factor_proxy.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50
```

3. Paper-Artefaktmanifest erzeugen:
```bat
run_paper_prep.bat --scenario-label "sa-aoi-halle-v1"
```

4. Manifest + Logs + JSON-Metadaten in den Supplement-Ordner uebernehmen.

## 3b) Schlag x Ereignis Batch-Export (App-Pipeline automatisiert)
Voraussetzung:
- Backend laeuft (`run_backend.bat`), Standard `http://127.0.0.1:8001`
- Schlagflaechen als GeoJSON (Polygon/MultiPolygon, WGS84)
- Ereignisliste als CSV (`event_id,event_start_iso,event_end_iso`)

Schlagpolygone schnell erzeugen (SA ALKIS Flurstuecke):
```bat
run_fetch_sa_flurstuecke.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50 --out-geojson paper\input\schlaege.geojson
```
Ausgabe:
- `paper/input/schlaege.geojson`
- `paper/input/schlaege.meta.json`

Template:
- `paper/templates/events_template.csv`

Batchlauf:
```bat
run_field_event_batch.bat --fields-geojson paper\input\schlaege.geojson --events-csv paper\templates\events_template.csv --out-csv paper\exports\field_event_results.csv
```

Eventquellen:
1. Reproduzierbar (feste Fenster, Standard):
- `--events-source csv --events-csv paper\templates\events_template.csv`

2. Dynamisch pro Schlag (neu):
- `--events-source auto --events-auto-source hybrid_radar --events-auto-start 2025-07-01 --events-auto-end 2025-08-31 --events-auto-top-n 2`
- nutzt je Schlag den bestehenden Endpunkt `/abflussatlas/weather/events` und schreibt Event-Metadaten in die CSV
  (`event_source,event_peak_iso,event_severity`).

Hinweis:
- Bei nicht verfuegbarer Radar-/DWD-Historie koennen fuer einzelne Schlaege keine Auto-Events gefunden werden
  (`no events (auto)`), was fachlich korrekt ist.

Smart-Variante fuer grosse SA-Datensaetze (empfohlen fuer den ersten Lauf):
```bat
run_field_event_batch_smart.bat
```
Standard:
- zieht automatisch eine sichere Stichprobe (500 Felder) aus `data/raw/sa_flurstuecke/cache/flurstuecke.sqlite`,
- erstellt `paper/input/schlaege_sample.geojson`,
- startet danach den normalen Feld x Ereignis Batch.

Outputs:
- `paper/exports/field_event_results.csv`
- `paper/exports/field_event_results.meta.json`
- Run-Manifest (automatisch):
  - `paper/exports/runs/smart_run_<UTC>.json`

## 3c) Nachtlauf-Protokoll (10k Ergebniszeilen)
Ziel:
- Bis zum Folgetag ein belastbares Zwischenartefakt mit `10.000` Ergebniszeilen erzeugen
  (`2500 Felder x 2 Events x 2 Modi`).

Startkommando:
```bat
run_field_event_batch_smart.bat --max-fields 2500 --sample-strategy spread --sample-geojson paper\input\schlaege_overnight_10k.geojson --out-csv paper\exports\field_event_results_overnight_10k.csv --checkpoint-every 50
```

Live-Monitoring:
```bat
powershell -NoProfile -Command "Get-Content -Tail 60 -Wait 'C:\Users\thoko\.gemini\antigravity\scratch\hydrowatch-berlin\paper\exports\overnight_10k_20260223_235109.log'"
```

Artefakte:
- Log: `paper/exports/overnight_10k_20260223_235109.log`
- Ergebnis-CSV: `paper/exports/field_event_results_overnight_10k.csv`
- Sample-Geometrie: `paper/input/schlaege_overnight_10k.geojson`
- Sample-Meta: `paper/input/schlaege_overnight_10k.meta.json`
- Run-Manifest: `paper/exports/runs/smart_run_20260223T225110Z.json`

Qualitaetssicherung am Folgetag:
```bat
run_validate_field_event_results.bat paper\exports\field_event_results_overnight_10k.csv
```
Erwartete QA-Artefakte:
- `paper/exports/field_event_results_overnight_10k.qa.json`

Mindestkriterien fuer "paper-ready Zwischenstand":
1. `rows_total >= 10000`
2. `error_rate_percent <= 5`
3. `nodata_rate_percent <= 20`
4. keine harten QA-Fehler (`ok=true`)

## 3d) SA Chunk-Workflow mit Auto-Events (operativer Hauptpfad)
Vollstaendige Doku:
- `paper/RUNBOOK_SA_AUTO_EVENTS_CHUNKS_2026-02-24.md`

Kernpunkte:
1. Ereignisse pro Schlag automatisch aus `/abflussatlas/weather/events` (`events_source=auto`).
2. DEM lokal via COG (`dem_source=cog`) fuer stabile, schnelle SA-Laeufe.
3. Resume-faehige Chunk-Batches (`run_field_event_batch_sa_chunks.bat`).
4. Verbindliche QA pro Chunk + konsolidierte Gesamt-QA.

## 4) Empfohlene Paper-Struktur (kurz)
1. Datenbasis:
- DGM (Aufloesung, CRS, Quelle)
- R/K/S (Quelle, Aufloesung)
- C (Herleitung aus Feldblock-Proxy + NDVI)

2. Modell:
- ABAG-Faktorpfad und Aggregation
- Unsicherheiten pro Faktor

3. Reproduzierbarkeit:
- Kommandos
- Commit-ID
- Manifest-JSON
- Dateihashes

## 4b) Quick-Check Exportpaket (PDF/Karten/Top-10)
Aus einem FeldxEvent-CSV kann ein behoerdentaugliches Paket erzeugt werden:
```bat
run_quickcheck_export.bat --results-csv paper\exports\field_event_results_overnight_10k.csv --fields-geojson paper\input\schlaege_overnight_10k.geojson --out-dir paper\exports\quickcheck --label "SA Quick-Check" --export-pdf
```

Outputs:
1. `*_top10.csv`
2. `*_top10_measures.csv`
3. `*_top10.geojson` (wenn Geometrien uebergeben)
4. `*_report.md` und `*_report.html`
5. optional `*_report.pdf` (wenn lokaler PDF-Renderer verfuegbar)

## 5) Offene Punkte vor Einreichung
1. Ground-Truth fuer Ereignisvalidierung (falls Event-ML im Paper enthalten ist).
2. Freigabe/Nutzungsrechte final pruefen (Datenbereitsteller).
3. Sensitivitaetsanalyse fuer C-Proxy (z. B. NDVI-Zeitraumvarianten).
