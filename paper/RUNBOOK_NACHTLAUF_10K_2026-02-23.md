# Runbook Nachtlauf 10k (Sachsen-Anhalt)

Stand: 2026-02-23 (UTC)

Hinweis (2026-02-24):
- Dieses Runbook dokumentiert den statischen CSV-Eventlauf (`events_source=csv`).
- Fuer den aktuellen operativen Auto-Event-Ansatz siehe:
  - `paper/RUNBOOK_SA_AUTO_EVENTS_CHUNKS_2026-02-24.md`

## Zweck
Reproduzierbarer Overnight-Lauf zur Erzeugung von `10.000` FeldxEreignis-Ergebniszeilen
als belastbarer Zwischenstand fuer Methodik, QA und Paper-Auswertung.

## Zielgroesse
- Felder: `2500`
- Ereignisse: `2`
- Modi: `2` (`erosion_events_ml`, `abag`)
- Erwartete Zeilen: `2500 x 2 x 2 = 10000`

## Startkonfiguration
Gestartet mit:
```bat
run_field_event_batch_smart.bat --max-fields 2500 --sample-strategy spread --sample-geojson paper\input\schlaege_overnight_10k.geojson --out-csv paper\exports\field_event_results_overnight_10k.csv --checkpoint-every 50
```

Pipeline:
1. Ziehe 2500 Schlaege als spread-Sample aus `data/raw/sa_flurstuecke/cache/flurstuecke.sqlite`
2. Verarbeite pro Schlag beide Events und beide Modi via `/analyze-bbox`
3. Schreibe Checkpoints alle 50 Zeilen in die Ergebnis-CSV
4. Schreibe Run-Manifest mit Parametern und Status

## Live-Monitoring
```bat
powershell -NoProfile -Command "Get-Content -Tail 60 -Wait 'C:\Users\thoko\.gemini\antigravity\scratch\hydrowatch-berlin\paper\exports\overnight_10k_20260223_235109.log'"
```

Fortschrittsformat im Log:
```text
[n/10000 | xx.x% | ETA hh:mm:ss] field=... event=... mode=...
```

## Artefakte
- Lauf-Log:
  - `paper/exports/overnight_10k_20260223_235109.log`
- Ergebnisdaten:
  - `paper/exports/field_event_results_overnight_10k.csv`
  - `paper/exports/field_event_results_overnight_10k.meta.json` (bei Laufende)
- Eingabestichprobe:
  - `paper/input/schlaege_overnight_10k.geojson`
  - `paper/input/schlaege_overnight_10k.meta.json`
- Run-Manifest:
  - `paper/exports/runs/smart_run_20260223T225110Z.json`

## QA nach Laufende
```bat
run_validate_field_event_results.bat paper\exports\field_event_results_overnight_10k.csv
```

Erzeugte QA-Datei:
- `paper/exports/field_event_results_overnight_10k.qa.json`

## Abnahmekriterien (paper-tauglicher Zwischenstand)
1. `rows_total >= 10000`
2. `error_rate_percent <= 5`
3. `nodata_rate_percent <= 20`
4. QA `ok=true`

## Ergebnisse (Ist)
- Laufstatus: abgeschlossen
- Ergebniszeilen: `10000`
- QA-Status: `ok=true`
- QA-Bericht:
  - `paper/exports/field_event_results_overnight_10k.qa.json`

Kernkennzahlen:
1. `rows_ok = 9936`
2. `rows_error = 64` (Fehlerquote `0.64%`)
3. `rows_nodata_only = 0` (NoData-Quote `0.0%`)
4. `event_probability_mean` Mittelwert: `0.4412` (n=4968)
5. `abag_index_mean` Mittelwert: `0.3398` (n=4968)
6. `risk_score_mean` Mittelwert: `35.66` (n=9936)

Hinweise aus QA-Warnungen:
- Es gibt viele Warnungen `aoi_area_km2=0` bei `nodata_only=false` fuer sehr kleine/degenerierte Geometrien.
- Diese Faelle fuer Auswertung/Paper explizit filtern oder separat berichten
  (z. B. Mindestflaeche > 0).

## Hinweise fuer Methodenabschnitt
- Samplingstrategie explizit angeben: `spread` ueber SA-weite Flurstueck-RowIDs.
- Ereignisfenster benennen:
  - `evt_2025_07_14` (2025-07-14T00:00:00Z bis 2025-07-14T23:00:00Z)
  - `evt_2025_08_03` (2025-08-03T00:00:00Z bis 2025-08-03T23:00:00Z)
- Modell-/Parameter im Supplement auf Run-Manifest verlinken.
