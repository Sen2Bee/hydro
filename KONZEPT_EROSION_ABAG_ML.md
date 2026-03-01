# Hydrowatch Integrationskonzept: ABAG + Event-ML

Stand: 22.02.2026

## 1) Ziel und Abgrenzung
Hydrowatch ergaenzt den bestehenden Erosions-Screeningmodus um zwei klar getrennte Fachmodi:

1. `ABAG` (klassisch):
- langfristiger Erosionsrisiko-Index (jaehrliche/mittlere Verlusttendenz)
- stark fuer Flaechenpriorisierung und Strategien

2. `Event-ML` (Batista-Logik):
- Lokalisierung und Ranking konkreter Erosionsereignisse
- stark fuer operative Lagebewertung nach/waehrend Ereignissen

Nicht-Ziel:
- Kein vollhydraulisches Sedimenttransportmodell im ersten Schritt.
- Kein methodischer "Mittelwert" aus ABAG und Event-ML.

## 2) Ist-Stand (Code)
- Legacy API: `backend/main.py`
- Analysekern: `backend/processing.py` (`analysis_type=starkregen|erosion`)
- Jobs-Worker: `services/compute-service/app/worker.py`
- Job API: `services/api-service/app/main.py`
- Schema: `infra/postgres/init/001_init.sql`

Aktuell ist `erosion` ein topo-getriebener Screeningmodus (LS-Proxy), nicht ABAG und nicht eventbasiertes ML.

## 3) Zielarchitektur
Einheitliche Orchestrierung, getrennte Modellkerne:

- `backend/erosion_abag.py`
  - Berechnung ABAG-Index aus R, K, LS, C, P
  - Raster-/AOI-Aggregation + Segment/Hotspot-Ableitung
- `backend/erosion_event_ml.py`
  - Feature-Build fuer Eventzeitfenster
  - Inferenz (Occurrence + Severity)
- `backend/erosion_features.py`
  - gemeinsame Feature-Extraktion (DEM, Wetter, Landnutzung, Boden, Management)
- `backend/processing.py`
  - Router: delegiert je `analysis_type`

Wichtig:
- Starkregenpfad bleibt unangetastet.
- Bestehende Response-Felder bleiben kompatibel.

## 4) API-Contract (Legacy + Jobs)
### 4.1 Legacy-Endpunkt
`POST /analyze-bbox`

Erweiterung:
- `analysis_type`: `starkregen | erosion | abag | erosion_events_ml`

Neue optionale Parameter:
- ABAG:
  - `abag_year` (int, optional)
  - `c_factor_mode` (`stats|eo_sequence`)
  - `p_factor_mode` (`default|regional`)
  - `r_factor_source` (`dwd_grid|fixed`)
- Event-ML:
  - `event_start_iso`, `event_end_iso`
  - `ml_model_key` (z. B. `erosion_rf_v1`)
  - `ml_threshold` (0..1, fuer Ereignisdetektion)
  - `crop_group_hint` (optional)

### 4.2 Jobs-Mode
`POST /v1/jobs` bleibt unveraendert, `parameters` wird erweitert um obige Felder.

Worker-Verhalten:
- `analysis_type=abag` -> ABAG-Pipeline
- `analysis_type=erosion_events_ml` -> ML-Inferenzpipeline

## 5) Response-Schema (einheitlich)
`geojson.analysis` wird um folgende Pflichtfelder erweitert:
- `kind`: `abag` oder `erosion_events_ml`
- `model_version`: z. B. `abag-v1-de` / `event-ml-rf-v1`
- `metric_type`: `long_term_index` oder `event_probability`
- `sources`: Datensatzversionen (R/K/LS/C/P bzw. Wetter/Crop/Eventdaten)
- `assumptions`: zentrale fachliche Annahmen
- `limits`: Gueltigkeitsbereich (Raum, Zeit, Crop-Gruppen)

Bestehende Blocs (`metrics`, `hotspots`, `class_distribution`) bleiben, Semantik ist modusabhaengig dokumentiert.

## 6) Datenmodell und Migrationen
`model_runs.parameters` und `model_run_outputs.metadata` sind bereits JSONB-flexibel.
Zusaetzlich empfohlen (neue Migrationen unter `infra/postgres/init/`):

1. `003_erosion_ml_artifacts.sql`
- Tabelle `ml_model_artifacts`
  - `id`, `model_key`, `version_label`, `storage_uri`, `feature_schema`, `created_at`
- Zweck: reproduzierbare Modellartefakte + Feature-Schema

2. `004_erosion_event_labels.sql`
- Tabelle `erosion_event_labels`
  - `id`, `region_key`, `event_id`, `event_start`, `event_end`, `geom` (Polygon/MultiPolygon), `severity_class`, `source_ref`, `created_at`
- Zweck: Trainings-/Validierungslabels (z. B. Sachsen-Anhalt)

3. `005_erosion_feature_store.sql` (optional Phase 2)
- Tabelle `erosion_feature_store`
  - `event_id`, `parcel_id` oder `grid_id`, `features` (JSONB), `label`, `split`, `created_at`
- Zweck: reproduzierbares Training ohne teure Neuableitung

## 7) Implementierungsplan (Phasen)
### Phase A: ABAG-MVP (2-3 Sprints)
1. Neue Datei `backend/erosion_abag.py` mit minimalem Faktorpfad:
- LS aus DEM (bestehende Ableitung nutzbar)
- K/C/P zuerst aus konfigurierbaren Rasterquellen + fallback
- R als erster Schritt ueber statischen regionalen Rasterstand oder kontrollierte Konstante
2. Router in `backend/processing.py` erweitern (`analysis_type=abag`)
3. UI: Auswahl "Erosion (ABAG)" in `frontend/src/App.jsx`
4. Dokumentation in `HILFE_WISSENSCHAFT.md` erweitern

Abnahmekriterium:
- AOI liefert reproduzierbaren ABAG-Index inkl. Quellen- und Annahmenblock.

### Phase B: Event-ML-Inferenz (2-3 Sprints)
1. `backend/erosion_event_ml.py` + `backend/erosion_features.py` anlegen
2. Modellartefakt-Lader (lokal/S3) und Inferenzpfad implementieren
3. API-Parameter fuer Eventzeitfenster + Modellschluessel integrieren
4. Outputs um Wahrscheinlichkeits-/Severity-Felder erweitern

Abnahmekriterium:
- Fuer gegebenes Eventzeitfenster werden Erosions-Hotspots mit Score + Severity gerankt.

### Phase C: Training-Pipeline (3-4 Sprints)
1. Trainingsscript (z. B. `backend/train_erosion_event_ml.py`) mit:
- Daten-Splits (raum/zeitlich robust)
- Klassenbalance
- Modell- und Metrikpersistenz
2. Importpfad fuer Labeldaten (Sachsen-Anhalt)
3. Registrieren des Artefakts in `ml_model_artifacts`

Abnahmekriterium:
- Reproduzierbares Training mit dokumentierten Kennzahlen und registriertem Artefakt.

## 8) Tests und QA
### 8.1 Unit/Contract
- `backend/test_core.py` erweitern:
  - `analysis_type=abag` liefert erwartete Pflichtfelder
  - `analysis_type=erosion_events_ml` validiert Eventparameter
- Worker-Tests:
  - korrekter Routingpfad je `analysis_type`

### 8.2 Fachliche Plausibilisierung
- 3 Referenz-AOIs in Sachsen-Anhalt:
  - ABAG-Sensitivitaet bei C/P-Aenderung
  - Event-ML Hit-Rate gegen bekannte Ereignisse

### 8.3 Regressionsschutz
- Bestehende Modi (`starkregen`, `erosion`) duerfen sich nicht veraendern (Snapshot-Vergleich auf Kernmetriken).

## 9) Risiken und Gegenmassnahmen
1. Labelqualitaet zu heterogen:
- Gegenmassnahme: Label-QA-Protokoll + Unsicherheitsklasse

2. Domain Shift (Bayern -> Sachsen-Anhalt):
- Gegenmassnahme: regionenspezifisches Retraining + AoA-Check (Area of Applicability)

3. Faktorinkonsistenzen (ABAG):
- Gegenmassnahme: Versionspflicht fuer jede Faktorquelle im Output

4. Laufzeit bei grossen AOIs:
- Gegenmassnahme: vorhandene Downsampling-/Output-Limits wiederverwenden

## 10) Ticket-Schnitt (direkt umsetzbar)
1. `BE-ABAG-01`: `backend/erosion_abag.py` + Routing in `backend/processing.py`
2. `BE-API-02`: Parameter-Validierung fuer `abag`/`erosion_events_ml` in `backend/main.py`
3. `DB-ML-03`: Migrationen `003` + `004`
4. `BE-ML-04`: `backend/erosion_event_ml.py` Inferenzpfad
5. `FE-UX-05`: Modusauswahl + Ergebnislabeling in `frontend/src/App.jsx`
6. `DOC-06`: Methodikupdate in `HILFE_WISSENSCHAFT.md`

## 11) Definition of Done
Die Integration gilt als abgeschlossen, wenn:
1. beide Modi (`abag`, `erosion_events_ml`) in Legacy und Jobs laufen,
2. Outputs reproduzierbar mit Quellen-/Versionsangaben sind,
3. Basistests gruen sind und Regressionen fuer bestehende Modi ausgeschlossen sind,
4. die UI die methodische Trennung fuer Nutzer klar sichtbar macht.
