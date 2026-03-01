# Roadmap Nächste Schritte (ab jetzt)

Stand: 2026-02-28

## Ziel
Den laufenden Stand ohne Brüche in einen paper-festen Produktionslauf überführen:
1. saubere Datenbasis,
2. belastbare Methodik,
3. reproduzierbare Ergebnisse.

## Aktueller Zustand (Ausgangspunkt)
1. Chunk-Run aktiv (`t05`, ICON2D, 100 Chunks):
   - `paper/exports/sa_chunks_icon2d_t05/automation/icon2d_multiwindow_20260228_050725.log`
2. Open-Crop-Download aktiv:
   - `data/raw/crop_history_open/logs/fetch_open_crop_history_20260228_064043.log`
3. Automatische Folgekette aktiv:
   - Download -> Crop-History-Build -> Dynamic-C-with-crop
   - Queue-Logs:
     - `data/derived/crop_history/logs/queue_build_crop_history_20260228_064416.log`
     - `data/layers/c_dynamic_sa/logs/queue_dynamic_c_after_crop_20260228_064549.log`

## Phase A: Laufende Prozesse stabil abschließen (keine Umbauten)
1. Aktive Läufe nicht mischen und nicht im selben Export-Pfad neu starten.
2. Nur Monitoring:
   - Chunk-Log Fortschritt,
   - Open-Crop-Log Fortschritt,
   - Queue-Logs.
3. Abbruch nur bei harten Fehlern:
   - wiederkehrende HTTP-/IO-Fehler,
   - keine Dateibewegung > 30 min,
   - konsistente QA-Fehler pro Chunk.

## Phase B: Datenabnahme nach Fertigstellung (Gate 1)
Abnahmekriterien für Crop-Historie + Dynamic-C:
1. Datei vorhanden:
   - `data/derived/crop_history/crop_history.csv`
   - `data/derived/crop_history/crop_history.meta.json`
   - `data/layers/c_dynamic_sa/run_manifest.json`
2. Mindestqualität:
   - `crop_history.csv` hat Einträge für 2024/2025.
   - `crop_history_matches` im C-Meta deutlich > 0.
   - Keine systematische NoData-Fläche im C-Raster.
3. Reproduzierbarkeit:
   - Methodikdatei verlinkt (`c_factor_method_v1*.json`),
   - Zeitfenster + Parameter in Meta enthalten.
4. Automatischer Check:
   - `run_gate1_crop_dynamic_c.bat`
   - Report-Ausgabe: `paper/exports/qa/gate1_crop_dynamic_c_<timestamp>.json`

## Phase C: Vergleich alt vs. neu (Gate 2)
1. Smoke-Test auf 10 räumlich verteilten Feldern:
   - alter C-Proxy vs. C mit Crop-Historie.
2. Prüfen:
   - C-Verteilung (Mean/P90),
   - ABAG-Index-Verschiebung,
   - stabile Modellantwort (kein numerisches Ausreißen).
3. Ergebnis dokumentieren (kurz, tabellarisch, mit Dateipfaden).

## Phase D: Paper-Produktionslauf neu starten (sauber getrennt)
1. Neuer Export-Root (kein Mischen mit `sa_chunks_icon2d_t05`):
   - Beispiel: `paper/exports/sa_chunks_icon2d_t05_crop`
2. Fenster verbindlich:
   - `2023-04-01..2023-10-31`
   - `2024-04-01..2024-10-31`
   - `2025-04-01..2025-10-31`
3. Methodik:
   - `events_auto_source=icon2d`
   - `ml_threshold=0.05`
   - `P=1.0` (Baseline)
   - C aus Dynamic-C-with-crop.
4. Chunking:
   - Pilot: 100 Chunks
   - Erweiterung: 200+ Chunks (bei Bedarf)

## Phase E: QA, Merge, Sensitivität, Paper-Assets
1. Pro Chunk QA (Pflicht).
2. Merge + Gesamt-QA.
3. C-Sensitivität (low/base/high) als Robustheitsanhang.
4. Export für Manuskript:
   - Kernmetriken,
   - Abbildungen/Karten,
   - Methoden- und Datenanhang.

## Nicht tun (wichtig)
1. Keine Vermischung alter und neuer Methodik im selben Output-Ordner.
2. Keine Ad-hoc-Parameteränderung mitten im Lauf.
3. Keine Interpretation ohne Gate-1/Gate-2-Abnahme.

## Konkreter nächster operativer Schritt
1. Läufe weiterlaufen lassen.
2. Sobald `crop_history.csv` fertig ist, sofort Gate-1 ausführen.
3. Danach Gate-2 Smoke-Test.
4. Dann Produktionslauf im neuen Export-Root starten.
