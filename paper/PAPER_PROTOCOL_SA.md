# PAPER_PROTOCOL_SA

Stand: 2026-02-28

## Ziel
Reproduzierbare, publikationsfeste SA-Pipeline fuer:
1. `ABAG` als langfristiger Risiko-Prior.
2. `Event-ML` als ereignisbezogene Lokalisierung/Schwere.

## Kernhypothese
Die Kombination aus:
1. statischer Disposition (`ABAG` / Topographie / Boden),
2. dynamischer Exposition (`C` bzw. Vegetations-/Management-Proxys),
3. Ereignis-Trigger (ICON2D als produktiver Standardpfad, optional Radar/DWD fuer Sensitivitaet),
verbessert die Ereigniserkennung gegenueber wetterbasierten Baselines.

## Verbindliche Methodikregeln
1. Keine `rowid`-nahe Stichprobe fuer Karten/Ergebnisdarstellung.
2. Zeitliche Trennung Train/Test (keine Event-Leakage).
3. Raeumliche Blockierung in CV/Evaluation.
4. AoA/Domain-Shift transparent berichten.
5. Null-Event-Faelle als Ergebnisfall behandeln, nicht als technischer Fehler.

## Datensatzaufbau (SA Pilot)
1. Stichprobe raeumlich stratifiziert (`grid`), deterministisch mit Seed.
2. Eventfenster automatisch pro Schlag via `/abflussatlas/weather/events` mit `source=icon2d`.
3. Analysemodi: `erosion_events_ml,abag`.
4. Ergebnisstruktur: Schlag x Event x Modus.

## C-Faktor Methodik (aktuell verbindlich)
1. Basisregelwerk aus versionierter JSON:
   - `data/config/c_factor_method_v1.json`
2. Rechenlogik:
   - `C_base` aus Feldblockklasse (`hbn_kurz`, Standard: `GL=0.03`, `A=0.20`).
   - NDVI-Modifier: `modifier = clip(a - NDVI, min, max)`.
   - `C_final = clip(C_base * modifier, c_min, c_max)`.
3. Implementierung:
   - `backend/build_c_factor_proxy.py`
   - `backend/build_dynamic_c_windows.py`
4. Optionaler Upgradepfad (bereits implementiert):
   - Crop-Historie aus CSV (`flik,crop_code,year`) via `--crop-history-csv` / `--crop-year`.
5. P-Faktor:
   - produktiv derzeit `P=1.0` (kein Schutzabschlag),
   - Szenarioanalysen mit abweichendem `P` separat dokumentieren.

## Verbindliche Ereignisfenster (Pilot/Pre-Paper)
1. `2023-04-01` bis `2023-10-31`
2. `2024-04-01` bis `2024-10-31`
3. `2025-04-01` bis `2025-10-31`
4. Begruendung: hohe Erosionsrelevanz in Vegetationsperiode, gute Laufzeit/Signal-Balance.

## Reproduzierbarkeit (Pflichtartefakte)
1. Lauf-CSV + Lauf-Log mit Zeitstempel.
2. Fensterdefinition explizit im Log.
3. Progress pro Feld/Fenster (`[n/total]`) im Log.
4. Summary je Fenster (`ok/err/events`) im Log.
5. Methodik-JSON fuer C-Faktor + zugehoerige Meta-JSON je C-Raster.
6. Paper-Asset-Runbook fuer Karten/Figures:
   - `paper/RUNBOOK_PAPER_ASSETS_2026-02-28.md`
7. Event-Cache-Runbook (429-sicher):
   - `paper/RUNBOOK_LOCAL_EVENTS_CACHE_2026-02-28.md`

## Open-Data Erweiterung (Crop-Historie)
1. Open-Download (laufend):
   - `backend/fetch_open_crop_history.py`
   - `run_fetch_open_crop_history.bat`
2. Ableitung `crop_history.csv` aus offenen Crop-Rastern:
   - `backend/build_crop_history_from_open_data.py`
   - `run_build_crop_history.bat`
3. Automationskette:
   - `backend/queue_build_crop_history_after_fetch.py`
   - `backend/queue_dynamic_c_after_crop_history.py`
4. Ergebnisdateien:
   - `data/derived/crop_history/crop_history.csv`
   - `data/derived/crop_history/crop_history.meta.json`
   - `data/layers/c_dynamic_sa/run_manifest.json`

## Technischer Referenz-Runner (Probe)
1. Script: `backend/run_event_probe_icon2d_windows.py`
2. Batch: `run_event_probe_icon2d_windows.bat`
3. Standardausgabe:
   - `paper/exports/event_probe_icon2d_windows_<timestamp>.csv`
   - `paper/exports/event_probe_icon2d_windows_<timestamp>.log`

## Pflicht-Ablation
1. Wetter-only.
2. Wetter + Pheno/NDVI.
3. Wetter + C-Proxy/Cseq.
4. Wetter + C-Proxy/Cseq + Disposition (ABAG/K/LS).
5. Sensitivitaet C-Methodik (low/base/high):
   - `backend/run_c_factor_sensitivity.py`
   - `run_c_factor_sensitivity.bat`

## Pflichtmetriken
1. Klassifikation: Precision, Recall, F1, PR-AUC.
2. Kalibrierung: Reliability/Kalibrierungsfehler.
3. Unsicherheit: Konfidenzintervalle, Fehlertypenkarte (FP/FN).
4. Segmentierung: Ergebnisse nach Region/Frucht/Eventintensitaet.

## Go/No-Go fuer Einreichung
1. Datenpfad und Lizenzen voll dokumentiert.
2. Reproduzierbarer Run mit Manifest + QA + Versionen.
3. Keine versteckte Selektion durch leere Chunks oder stilles Dropping.
4. Karten zeigen raeumliche Verteilung der Stichprobe nachvollziehbar.
5. Figure-Inputs (Histogramme/Counts) sind versioniert exportiert:
   - `run_build_paper_assets.bat` / `backend/build_paper_assets.py`
