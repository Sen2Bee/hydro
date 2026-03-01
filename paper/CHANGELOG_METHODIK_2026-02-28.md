# Changelog Methodik (2026-02-28)

## Ziel
Schneller Ueberblick ueber alle methodischen Aenderungen seit dem Stand 2026-02-27.

## 1) Dynamischer C-Faktor auf versioniertes Regelwerk umgestellt
- `backend/build_c_factor_proxy.py` nutzt jetzt zentrale Config:
  - `data/config/c_factor_method_v1.json`
  - Varianten: `data/config/c_factor_method_v1_low.json`, `data/config/c_factor_method_v1_high.json`
- Metadaten je C-Raster enthalten jetzt:
  - `method_version`
  - `method_params`
  - `season_label`

## 2) Crop-Historie technisch integriert (optional, aber vorbereitet)
- `build_c_factor_proxy.py` akzeptiert:
  - `--crop-history-csv`
  - `--crop-year`
- Erwartetes CSV-Format:
  - `flik,crop_code,year`

## 3) Open-Data Pipeline fuer Crop-Historie implementiert
- Downloader:
  - `backend/fetch_open_crop_history.py`
  - `run_fetch_open_crop_history.bat`
- Crop-History-Build:
  - `backend/build_crop_history_from_open_data.py`
  - `run_build_crop_history.bat`

## 4) Automationskette ohne manuelle Schritte
- Nach Open-Download:
  - `backend/queue_build_crop_history_after_fetch.py`
- Nach Crop-History:
  - `backend/queue_dynamic_c_after_crop_history.py`
- Dynamic-C mit Crop-Historie:
  - `run_build_dynamic_c_windows_with_crop.bat`

## 5) Sensitivitaetsanalyse C eingefuehrt
- `backend/run_c_factor_sensitivity.py`
- `run_c_factor_sensitivity.bat`
- Zweck:
  - Robustheit gegen C-Parameterwahl belegen (low/base/high).

## 6) Laufkonventionen (wichtig fuer Paper)
- Aktiver Chunk-Lauf:
  - `paper/exports/sa_chunks_icon2d_t05/...`
  - `ml_threshold=0.05`, `events_auto_source=icon2d`
- Methodenwechsel nie in denselben Export-Root mischen.
- `P` aktuell Baseline `1.0`; Szenarien getrennt rechnen.

## 7) Dokumente aktualisiert
- `paper/PAPER_PROTOCOL_SA.md`
- `paper/RUNBOOK_SA_AUTO_EVENTS_CHUNKS_2026-02-24.md`

## 8) Event-Fetch-Guard gegen 429-Throttling
- Datei:
  - `backend/run_field_event_batch.py`
  - `backend/run_field_event_batch_sa_chunks.py`
  - `backend/run_sa_icon2d_multiwindow_chunks.py`
  - `run_sa_icon2d_multiwindow_chunks.bat`
- Aenderung:
  - Wenn `/abflussatlas/weather/events` zwar HTTP 200 liefert, aber in `meta.notes` auf `429` bzw. `Too Many Requests` hinweist, wird das jetzt als Fehler behandelt.
  - Solche Antworten werden nicht mehr als gueltige `no events` gecacht.
  - Auto-Event-Fetch nutzt jetzt Throttling + Retry/Backoff (konfigurierbar per CLI).
- Grund:
  - Verhindert falsche Null-Event-Faelle durch Provider-Limitierung.

## 9) Lokaler Event-Cache Betriebsmodus
- Neu:
  - `backend/precompute_auto_events_cache.py`
  - `run_precompute_auto_events_cache.bat`
  - `paper/RUNBOOK_LOCAL_EVENTS_CACHE_2026-02-28.md`
- Funktion:
  - Events werden vorab lokal gecacht.
  - Batch-/Chunk-Lauf kann im `cache-only` Modus ohne Live-Event-Requests laufen.
- Zusatz:
  - Radar-/Hybrid-Radar-Fenster werden automatisch in <=4320h Teilfenster gesplittet.
