# Agent Workplan (Hydrowatch)

Stand: 2026-02-24

## Run-Scope Entscheidung (verbindlich)
1. `Pilot/Methodik`: `50 Chunks` (ca. 50.000 Felder) reichen fuer Demo, Methodik und ersten Stakeholder-Check.
2. `Belastbarer Projekthinweis`: `200+ Chunks` als Zielbereich fuer robustere Aussagen.
3. `Vollabdeckung Sachsen-Anhalt`: `2705 Chunks` (alle ~2,704,579 Felder) nur bei explizitem Budget/Projektauftrag.
4. Aktuelle Prioritaet:
  - zuerst stabiler `50er`/`200+` Betrieb,
  - landesweit erst danach und nur auf Freigabe.

## Ziel
Die App wird in parallele Arbeitsstraenge ("Agenten") zerlegt. Jeder Agent hat klare Deliverables, DoD und Schnittstellen.

## Agenten-Hierarchie
1. `Coordinator (C0)`
- Rolle: priorisiert Arbeitspakete, steuert Reihenfolge, entscheidet bei Zielkonflikten.
- Verantwortung:
  - Sprintplanung und Freigabe von Runs.
  - Gatekeeping fuer "run-ready", "qa-ready", "paper-ready".
  - Eskalation bei Blockern (Daten, Laufzeit, Qualitaet).

2. `Leadebene (L1)`
- `L1-A DataOps Lead`
- `L1-B Modeling Lead` (ABAG + Event-ML)
- `L1-C Operations Lead` (Batch + QA + Reporting)

3. `Subagenten (L2)`
- Unter `L1-A DataOps Lead`:
  - `A1 SourceIngest` (Downloads, Schnittstellen, Aktualitaet)
  - `A2 LayerCatalog` (Versionen, Metadaten, Pfadkonsistenz)
- Unter `L1-B Modeling Lead`:
  - `B1 ABAGEngine` (ABAG-Parameter/Fallbacks)
  - `B2 EventMLEngine` (Inference/Features/Model keys)
  - `C1 EventWindows` (Auto-Events, Window-Qualitaet)
- Unter `L1-C Operations Lead`:
  - `D1 ChunkRunner` (Resume, Checkpoints, Queueing)
  - `D2 WindowOrchestrator` (CMD-Fenster, Prozesshygiene, Port-Disziplin)
  - `E1 QAValidation` (Regeln, QA-Reports, Grenzwerte)
  - `F1 ProductOutputs` (Quick-Check, Top-10, Karten, Berichte)

4. `Entscheidungsregel`
- Fachliche Korrektheit > Laufgeschwindigkeit > Komfort.
- Bei Konflikten entscheidet zuerst der zustaendige L1-Lead.
- Bei bereichsuebergreifenden Konflikten entscheidet `C0 Coordinator`.

5. `Freigabe-Gates`
- `Gate 1 (run-ready)`: DataOps + EventWindows + ChunkRunner gruen.
- `Gate 2 (qa-ready)`: QAValidation gruen.
- `Gate 3 (paper-ready)`: ProductOutputs + Runbook-Update + Artefaktmanifest gruen.

## Agenten und Aufgaben
1. `Agent A - DataOps`
- Fokus: Datenbeschaffung, Caches, Layer-Versionierung, Metadaten.
- Primaere Dateien: `backend/fetch_*.py`, `backend/st_cog_dem.py`, `data/*`, `run_fetch_*.bat`.
- Deliverables:
  - stabile SA-Datenpfade mit Versionstempeln.
  - konsistente Manifeste pro Datenlauf.
  - dokumentierte Quellen inkl. Datum/Lizenzhinweis.

2. `Agent B - HydroModel`
- Fokus: ABAG/Event-ML Kernlogik, Parameterstabilitaet, Modellartefakte.
- Primaere Dateien: `backend/erosion_abag.py`, `backend/erosion_event_ml.py`, `backend/train_*.py`.
- Deliverables:
  - nachvollziehbare Modellversionen.
  - klare Kennzahlen je Lauf (ML + ABAG).
  - definierte Grenzwerte und Fallbacks.

3. `Agent C - EventPipeline`
- Fokus: automatische Ereignisfenster pro Schlag, Qualitaetsflags.
- Primaere Dateien: `backend/run_field_event_batch.py`, `backend/abflussatlas_weather.py`, `backend/weather_*.py`.
- Deliverables:
  - `events_source=auto` fachlich robust.
  - Eventfenster nie 0-laengig.
  - pro Event Quelle/Qualitaet im Output.

4. `Agent D - BatchOps`
- Fokus: SA-Skalierung, Chunking, Resume, Monitoring.
- Primaere Dateien: `backend/run_field_event_batch_sa_chunks.py`, `backend/queue_sa_chunks_after_5_to_50k.py`, `run_field_event_batch_sa_chunks.bat`.
- Deliverables:
  - langlaufstabile Pipelines.
  - sichtbarer Progress + klare Logs.
  - reproduzierbare State-/Run-Manifest-Dateien.
  - keine konkurrierenden Altprozesse/Fenster fuer dieselbe Aufgabe.

5. `Agent E - QA`
- Fokus: Plausibilitaetsregeln, Regressionchecks, Freigabekriterien.
- Primaere Dateien: `backend/validate_field_event_results.py`, `run_validate_field_event_results.bat`, `paper/exports/*.qa.json`.
- Deliverables:
  - QA-Scorecards pro Lauf.
  - harte Abbruchkriterien + Warnschwellen.
  - konsolidierte QA fuer Paper/Delivery.

6. `Agent F - ProductPaper`
- Fokus: Behoerdenpakete, Kartenexports, Berichte, Runbooks.
- Primaere Dateien: `backend/export_quickcheck_package.py`, `run_quickcheck_export.bat`, `paper/*.md`, `PAPER_PUBLISH_PREP.md`.
- Deliverables:
  - Quick-Check Paket (Top-10/Karten/Bericht).
  - paper-taugliche Methodik-/Runbook-Doku.
  - konsistente Artefaktlisten fuer Einreichung.

## Abhaengigkeiten
1. `A -> B/C/D`: Ohne saubere Datenquellen keine stabile Modellierung.
2. `C -> D/E/F`: Eventfensterqualitaet beeinflusst Batch, QA und Berichte.
3. `D -> E/F`: Erst stabile Laeufe, dann belastbare QA und Produktoutputs.
4. `E -> F`: Nur QA-gepruefte Ergebnisse in Reports/Paper.

## Definition of Done (DoD) je Agent
1. `A DataOps`
- Lauf erzeugt Daten + Metadaten + Quellenhinweis.
- Pfade und Zeitstempel dokumentiert.

2. `B HydroModel`
- Modellversion/Parameter im Output sichtbar.
- bekannte Randfaelle abgefangen.

3. `C EventPipeline`
- keine 400er wegen Eventfensterformat/-laenge.
- Eventquelle im Ergebnis protokolliert.

4. `D BatchOps`
- resume-faehig nach Abbruch/Neustart.
- State + Run-Manifest vorhanden.
- nur benoetigte Projektfenster aktiv; veraltete Fenster/Prozesse sauber beendet.

5. `E QA`
- QA-JSON erzeugt.
- Fehlerquote und NoData-Quote gegen Schwellen geprueft.

6. `F ProductPaper`
- Top-10 CSV + Massnahmen + GeoJSON + Report erzeugbar.
- Runbook auf aktuellem Workflow.

## Naechster 2-Wochen-Sprint (priorisiert)
1. `P0 - EventPipeline + BatchOps`
- Auto-Events als Standardpfad in SA-Chunks festziehen.
- Abschluss-Job: automatische Chunk-Zusammenfuehrung + Gesamt-QA.

2. `P0 - QA`
- neues Konsolidierungsskript:
  - input: `paper/exports/sa_chunks/field_event_results_chunk_*.csv`
  - output: `paper/exports/sa_chunks/field_event_results_merged.csv` + `*.qa.json`

3. `P1 - ProductPaper`
- Quick-Check Report um Abschnitt "Datenabdeckung/Unsicherheit" erweitern.
- optional PDF-Exportadapter (`wkhtmltopdf` oder Edge headless) dokumentieren.

4. `P1 - HydroModel`
- Scoredeckelung fuer ABAG-Werte > 1 im Quick-Check klar ausweisen.
- Sensitivitaetslauf fuer `abag_p_factor` (z. B. 0.8/1.0/1.2) dokumentieren.

5. `P2 - DataOps`
- einheitliches Datenmanifest fuer SA-Layer:
  - Quelle, Abrufdatum, Aufloesung, CRS, Lizenzstatus.

## Operative Regeln
1. Jede groessere Aenderung muss ein Runbook-Update enthalten.
2. Keine neuen Batch-Laeufe ohne Checkpoint + Resume.
3. Paper-Outputs nur aus QA-positiven Laeufen.
4. `D2 WindowOrchestrator` muss vor jedem grossen Lauf Port- und Prozesskonflikte pruefen.

## Startkommandos (Schnellzugriff)
1. SA Chunk Lauf:
```bat
run_field_event_batch_sa_chunks.bat --chunk-size 1000 --start-chunk 1 --max-chunks 5 --dem-source cog --events-source auto --events-auto-source hybrid_radar --events-auto-start 2025-06-01 --events-auto-end 2025-09-30 --events-auto-top-n 2 --checkpoint-every 100 --continue-on-error
```

2. QA:
```bat
run_validate_field_event_results.bat paper\exports\field_event_results_overnight_10k.csv
```

3. Quick-Check Export:
```bat
run_quickcheck_export.bat --results-csv paper\exports\field_event_results_overnight_10k.csv --fields-geojson paper\input\schlaege_overnight_10k.geojson --out-dir paper\exports\quickcheck --label "SA Quick-Check" --export-pdf
```
