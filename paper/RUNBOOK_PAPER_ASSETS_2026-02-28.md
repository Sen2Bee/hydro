# Runbook Paper Assets (Karten/Diagramme)

Stand: 2026-02-28

## Ziel
Aus den SA-Chunk-Ergebnissen konsistente, zitierfaehige Paper-Artefakte erzeugen:
1. Merged-CSV (gesamter Lauf oder Teilfenster),
2. QA-Report,
3. Top-10-Massnahmen + Kartenlayer,
4. Diagramm-Tabellen (CSV/JSON) fuer Figures.

## Voraussetzung
1. Chunk-Run ist aktiv oder abgeschlossen (resume-faehig).
2. Ergebnisse liegen unter:
   - `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/`
3. Backend muss fuer neue Berechnungen laufen, fuer reine Auswertung nicht.

## Aktueller Laufstatus (Referenz)
1. State-Datei:
   - `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/sa_chunk_run_state.json`
2. Orchestrator-Log:
   - `paper/exports/sa_chunks_icon2d_t05/automation/icon2d_multiwindow_20260228_060319.log`

## Schritt 1: Merge + Gesamt-QA
```bat
run_merge_sa_chunk_results.bat --exports-dir paper\exports\sa_chunks_icon2d_t05\20230401_20231031 --run-quickcheck
```

Erwartete Artefakte:
1. `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/field_event_results_merged.csv`
2. `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/field_event_results_merged.qa.json`
3. `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/field_event_results_merged.report.md`
4. `paper/exports/sa_chunks_icon2d_t05/20230401_20231031/field_event_results_merged.manifest.json`

## Schritt 2: Top-10 + Kartenpaket
```bat
run_quickcheck_export.bat --input-csv paper\exports\sa_chunks_icon2d_t05\20230401_20231031\field_event_results_merged.csv --fields-geojson paper\input\schlaege_sa_spatial_50k.geojson --label SA_20230401_20231031 --out-dir paper\exports\quickcheck\sa_20230401_20231031 --top-k 10 --export-pdf
```

Erwartete Artefakte:
1. `.../quickcheck_*_top10.csv`
2. `.../quickcheck_*_top10.geojson` (direkt in QGIS ladbar)
3. `.../quickcheck_*_top10_measures.csv`
4. `.../quickcheck_*_report.md`
5. `.../quickcheck_*_report.html`
6. Optional: `.../quickcheck_*_report.pdf` (wenn PDF-Engine verfuegbar)

## Schritt 3: Diagramm-Tabellen fuer Paper-Figures
```bat
run_build_paper_assets.bat --input-csv paper\exports\sa_chunks_icon2d_t05\20230401_20231031\field_event_results_merged.csv --out-dir paper\exports\paper_assets\sa_20230401_20231031 --hist-bins 20
```

Erwartete Artefakte:
1. `paper_summary.json` (Kernzahlen)
2. `analysis_mode_counts.csv`
3. `event_year_counts.csv`
4. `status_counts.csv`
5. `hist_event_probability_max.csv`
6. `hist_abag_index_mean.csv`
7. `hist_risk_score_max.csv`

## Schritt 4: Karten und Diagramme erzeugen
1. Karten:
   - Lade `quickcheck_*_top10.geojson` in QGIS.
   - Hintergrund: DGM/WMS oder Feldblock-Layer.
   - Symbolisierung nach `score` oder `priority`.
2. Diagramme:
   - Direkt aus den Histogramm-CSVs (Excel/LibreOffice/Python/R).
   - Pflichtdiagramme: Verteilung `event_probability_max`, `abag_index_mean`, `risk_score_max`.
   - Zusatz: Status-Anteile (`ok/error`) und Event-Jahresverteilung.

## Qualitaetsregeln fuer Paper
1. Methoden nicht in denselben Export-Root mischen.
2. Immer Manifest + QA-Datei mit ablegen.
3. Figure-Daten nur aus `field_event_results_merged.csv` des jeweiligen Laufs erzeugen.
4. In jeder Figure die Quelle als Pfad referenzieren.

## Nächster Schritt nach diesem Run
1. Dieselbe Kette fuer Fenster `2024-04-01..2024-10-31`.
2. Dieselbe Kette fuer Fenster `2025-04-01..2025-10-31`.
3. Danach Fenster zusammenfassen (vergleichende Tabellen/Abbildungen).
