# Nachvollziehbare Projektdokumentation (Arbeitsstandard)

Ziel: Jeder Lauf muss spaeter fachlich und technisch reproduzierbar sein.

## Mindeststandard pro Lauf
1. Zweck kurz notieren (z. B. "C-Faktor Proxy fuer AOI Halle").
2. Exakte Eingaben erfassen:
- BBox (`west,south,east,north`)
- Zeitraum (`start,end`)
- Skript/Command
3. Ergebnisdateien mit Pfad festhalten.
4. Metadaten-JSON sichern/mit ablegen.
5. Dateihashes dokumentieren (`SHA256`).

## Pflichtartefakte (C-Pipeline)
- `data/layers/st_mwl_erosion/NDVI_latest.tif`
- `data/layers/st_mwl_erosion/NDVI_latest.json`
- `data/layers/st_mwl_erosion/C_Faktor_proxy.tif`
- `data/layers/st_mwl_erosion/C_Faktor_proxy.json`
- Laufprotokoll: `data/layers/st_mwl_erosion/logs/c_pipeline.log`

## Standardablauf
```bat
run_fetch_sentinel_ndvi.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50 --start 2025-05-01 --end 2025-08-31
run_build_c_factor_proxy.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50
```

## Hashes erzeugen (PowerShell)
```powershell
Get-FileHash data\layers\st_mwl_erosion\NDVI_latest.tif -Algorithm SHA256
Get-FileHash data\layers\st_mwl_erosion\C_Faktor_proxy.tif -Algorithm SHA256
```

## Protokollvorlage (copy/paste)
```text
Datum/Uhrzeit:
Bearbeiter:
Zweck:
AOI (WGS84):
Zeitraum:
Kommandos:
Outputs:
  - NDVI_latest.tif
  - C_Faktor_proxy.tif
Metadaten:
  - NDVI_latest.json
  - C_Faktor_proxy.json
SHA256:
Anmerkungen/Abweichungen:
```

## Geltung
- Dieser Standard gilt fuer alle weiteren Daten-/Modelllaeufe.
- Wenn etwas fehlt: Lauf gilt als nicht abgeschlossen.

## Zusatzstandard Feld x Event (SA-Chunk)
Pflicht pro Lauf:
1. Startkommando inkl. Parameter (`events_source`, `dem_source`, `chunk-size`) dokumentieren.
2. Run-State sichern:
- `paper/exports/sa_chunks/sa_chunk_run_state.json`
3. Run-Manifest sichern:
- `paper/exports/sa_chunks/runs/sa_chunk_run_<UTC>.json`
4. Chunk-CSV + QA-JSON je Chunk ablegen.
5. Spezifisches Runbook verlinken:
- `paper/RUNBOOK_SA_AUTO_EVENTS_CHUNKS_2026-02-24.md`
