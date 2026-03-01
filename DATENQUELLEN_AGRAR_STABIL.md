# Stabile Datenquellen fuer Agrarraeume (frei verfuegbar)

Stand: 22.02.2026

## Ziel
Moeglichst einfache, robuste Quellen fuer ABAG/Event-ML mit Fokus auf laendlich-agrarische Anwendungen.

## Empfohlene Minimalkette (frei + stabil)
1. DEM/Topographie
- Primaer: Landes-DGM (wenn verfuegbar, 1-10 m, lokal gespiegelt)
- Fallback (deutschlandweit): Copernicus DEM (30 m)
  - https://dataspace.copernicus.eu/explore-data/data-collections/copernicus-contributing-missions/collections-description/COP-DEM

2. Boden/K-Proxy
- BGR BUEK250 (Vektor, bundesweit):
  - https://download.bgr.de/bgr/boden/BUEK250/shp/buek250_mgm_utm_v60.zip
- BGR Raster-Fallbacks (250 m, nur Screening):
  - FK10dm1000: https://download.bgr.de/bgr/Boden/FK10DM1000/geotiff/FK10dm1000_250.zip
  - PEGWASSER1000: https://download.bgr.de/bgr/Boden/PEGWASSER1000/geotiff/pegwasser1000_250_v10.zip

3. C-Faktor/Landbedeckung
- ESA WorldCover 10 m (global, offen):
  - Data access: https://esa-worldcover.org/en/data-access
  - DOI v200: https://doi.org/10.5281/zenodo.7254221
  - AWS Open Data (Cloud-lesen): https://registry.opendata.aws/esa-worldcover/
- Optional fuer feldnaehere Dynamik:
  - Sentinel-2 L2A COG (AWS): https://registry.opendata.aws/sentinel-2-l2a-cogs/
  - STAC API (Earth Search): https://element84.com/geospatial/earth-search/

4. Event-Regen (R/Event-Features)
- DWD OpenData CDC (stabiler Verzeichniszugriff):
  - Stundenniederschlag: https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/
  - RADOLAN RW: https://opendata.dwd.de/weather/radar/radolan/rw/

## Quellen, die fachlich stark sind, aber operativ weniger "plug-and-play"
- ESDAC RUSLE K-Faktor:
  - https://esdac.jrc.ec.europa.eu/content/soil-erodibility-k-factor-high-resolution-dataset-europe
- ESDAC C-Faktor:
  - https://esdac.jrc.ec.europa.eu/themes/cover-management-factor

Hinweis:
- Diese ESDAC-Datensaetze sind wissenschaftlich relevant, aber je nach Produkt mit Nutzungs-/Downloadprozess verbunden.

## Praktische Empfehlung fuer dieses Projekt
1. Fuer schnellen stabilen Betrieb:
- Kette: `Landes-DGM oder Copernicus DEM` + `BUEK250/FK10` + `WorldCover 10 m` + `DWD CDC/RADOLAN`

2. Fuer bessere agrarische Aussagekraft:
- DEM auf 1-10 m heben (Landesdaten)
- Boden von 250 m auf feinere Landes-/Fachdaten migrieren
- C-Faktor saisonal aus Sentinel-2 ableiten (statt rein statischer Klassenwerte)

3. Erwartungsmanagement:
- 250 m Bodenlayer sind fuer nationales/regionales Screening nutzbar,
- nicht fuer schlaggenaue Entscheidungen auf Einzelflaechen.

## Sachsen-Anhalt Fokus (praktisch nutzbar)
Stand der Recherche: 22.02.2026

1. Topographie (sehr gut, bereits im Projekt):
- DGM1 OpenData WCS (1 m):
  - https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_DGM1_WCS_OpenData/guest
- Empfehlung Betrieb:
  - Genau wie aktuell: lokal gespiegelt/cached nutzen (WCS nur als Quelle).

2. Schlag-/Nutzungsnaehe fuer C-Faktor (gut):
- ALKIS WMS OpenData:
  - https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_ALKIS_WMS_OpenData/guest
- ALKIS WFS OpenData:
  - https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_ALKIS_WFS_OpenData/guest
- ALKIS FeatureService (Polygon-Flurstuecke, praktisch fuer AOI-Schlagproxy):
  - https://www.geodatenportal.sachsen-anhalt.de/arcgis/rest/services/Geobasisdaten/alkis_xtra_fme/FeatureServer/0
- INSPIRE Bodennutzung WMS/WFS:
  - https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_INSPPIRE_Annex2u3_ALKIS_WMS_OpenData/guest
  - https://www.geodatenportal.sachsen-anhalt.de/wss/service/ST_LVermGeo_INSPIRE_Annex2u3_ALKIS_WFS_OpenData/guest

3. Event-Regen (ausreichend fuer Event-ML):
- DWD RADOLAN RW (ca. 1 km, hohe Zeitauflosung):
  - https://opendata.dwd.de/weather/radar/radolan/rw/
- DWD CDC Stundenniederschlag:
  - https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/

4. Boden/K-Faktor in Sachsen-Anhalt (engpass):
- Frei und stabil verfuegbar: BUEK250/FK10 (250 m, zu grob fuer Schlagebene)
- Praktische Loesung bis feinere oeffentliche Landesdaten verfuegbar sind:
  - K weiterhin aus BGR (Screening),
  - Priorisierung auf Basis von DGM1 + Regenereignis + Nutzungsnaehe (ALKIS/WorldCover).

## Kurzfazit Sachsen-Anhalt
- Mit oeffentlichen Daten ist fuer Sachsen-Anhalt eine fachlich brauchbare Kette fuer
  Hotspot-Screening und Event-ML moeglich.
- Fuer "schlaggenaue" absolute ABAG-Ertragsverluste bleibt der K-Faktor derzeit die groesste
  Aufloesungsgrenze, solange nur bundesweite 250-m-Bodenkarten genutzt werden.

## Praktischer Schlag-Polygon-Import (AOI)
Direkt im Projekt:
```bat
run_fetch_sa_flurstuecke.bat --west 11.998 --south 51.458 --east 12.002 --north 51.460 --out-geojson paper\input\schlaege.geojson
```
Ausgabe:
- `paper/input/schlaege.geojson`
- `paper/input/schlaege.meta.json`

Hinweis:
- Das sind ALKIS-Flurstuecke (guter Polygon-Proxy), nicht zwingend identisch mit Bewirtschaftungsschlaegen.

## SA-weiter Download (robust, gekachelt)
Fuer landesweiten Abruf:
```bat
run_fetch_sa_flurstuecke_sa.bat
```

Ablage (optimiert):
- Roh-/Arbeitsverzeichnis: `data/raw/sa_flurstuecke/`
- Dedup-Cache: `data/raw/sa_flurstuecke/cache/flurstuecke.sqlite`
- Kachelprotokolle: `data/raw/sa_flurstuecke/tiles/`
- Merge-Ausgabe: `data/raw/sa_flurstuecke/sa_flurstuecke.geojson`
- Komprimiert: `data/raw/sa_flurstuecke/sa_flurstuecke.geojson.gz`
- Metadaten: `data/raw/sa_flurstuecke/sa_flurstuecke.meta.json`

## Direkt loslegen: ST MWL Erosion-WMS (intern/testweise)
Dienst:
- https://www.geodatenportal.sachsen-anhalt.de/wss-org1/service/ST_MWL_Erosion/guest

ABAG-relevante Layer im Dienst (u. a.):
- `K-Faktor`
- `R-Faktor`
- `S-Faktor`
- `Wasser_Erosion`
- `Wind_Erosion`

Schnellabruf im Projekt (BBox in WGS84):
```bat
run_st_mwl_erosion_fetch.bat
```

Mit eigener BBox:
```bat
run_st_mwl_erosion_fetch.bat --west 11.80 --south 51.40 --east 12.10 --north 51.60
```

SA-weit, gekachelt (empfohlen fuer belastbare Ausdehnung/Aufloesung):
```bat
run_st_mwl_erosion_fetch_sa_tiled.bat
```
Parameterbeispiel:
```bat
run_st_mwl_erosion_fetch_sa_tiled.bat --target-res-m 10 --tile-px 5000
```
Ausgabe:
- `data/layers/st_mwl_erosion_sa_tiled/<LAYER>/tiles/*.tif`
- `data/layers/st_mwl_erosion_sa_tiled/<LAYER>/<LAYER>.vrt` (falls `gdalbuildvrt` verfuegbar)
- `data/layers/st_mwl_erosion_sa_tiled/run_manifest.json`

Effizienter Abruf:
- identische Requests werden aus Cache bedient (schneller, weniger Traffic),
- `--force` erzwingt Neuladen.

Beispiel:
```bat
run_st_mwl_erosion_fetch.bat --west 11.95 --south 51.45 --east 12.02 --north 51.50 --layers "K-Faktor,R-Faktor,S-Faktor" --force
```

Ausgabe:
- `data/layers/st_mwl_erosion/`
- Cache: `data/layers/st_mwl_erosion/_cache/<cache_key>/`

Zusatz fuer C-Faktor (technischer Proxy, sofort nutzbar):
```bat
run_fetch_sentinel_ndvi.bat --west 11.80 --south 51.40 --east 12.10 --north 51.60 --start 2025-04-01 --end 2025-09-30
run_build_c_factor_proxy.bat --west 11.80 --south 51.40 --east 12.10 --north 51.60
```
Erzeugt:
- `data/layers/st_mwl_erosion/NDVI_latest.tif`
- `data/layers/st_mwl_erosion/C_Faktor_proxy.tif`

Passend zur aktuellen Projektablage:
- `run_backend.bat` erkennt automatisch vorhandene Sachsen-Anhalt-Defaults:
  - `data/dem_cache/st_dgm1_cog/st_dgm1_cog.vrt` -> `ST_DEM_LOCAL_PATH`
  - `data/layers/st_mwl_erosion/K_Faktor.tif` -> `SOIL_RASTER_PATH`
  - `data/layers/st_mwl_erosion/Wasser_Erosion.tif` -> `IMPERVIOUS_RASTER_PATH`
  - `data/layers/st_mwl_erosion/K_Faktor.tif` -> `ABAG_K_FACTOR_RASTER_PATH`
  - `data/layers/st_mwl_erosion/R_Faktor.tif` -> `ABAG_R_FACTOR_RASTER_PATH`
  - `data/layers/st_mwl_erosion/S_Faktor.tif` -> `ABAG_S_FACTOR_RASTER_PATH`
  - `data/layers/st_mwl_erosion/C_Faktor_proxy.tif` -> `ABAG_C_FACTOR_RASTER_PATH`

Wichtig:
- Bis zur expliziten Freigabe nur intern/testweise verarbeiten.
- Siehe `NUTZUNGSHINWEIS_DATEN.md`.

## Abgleich mit Steininger-Veröffentlichungen (Sachsen-Anhalt)
Ausgewertete Quellen:
1. LLG Heft 1/2021 (Steininger, Wurbs): Starkregen-Gefahrenvorsorge im laendlichen Raum
   - https://llg.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MLU/LLFG/Dokumente/03_service/Schriftenreihe/Schriftenreihe_LLG_1_2021.pdf
2. LLG Heft 1/2019 (Schmidt, Steininger, Wurbs, Koschitzki): sedimentgebundener P-Austrag
   - https://llg.sachsen-anhalt.de/fileadmin/Bibliothek/Politik_und_Verwaltung/MLU/LLFG/Dokumente/03_service/Schriftenreihe/Schriftenreihe_LLG_1_2019.pdf

Zentrale methodische Punkte aus den Publikationen:
- DGM1 wird als Ausgangsdatenbasis genutzt, fuer landesweite Modellierung auf DGM5 aggregiert.
- ABAG wird fuer langjaehrigen mittleren Bodenabtrag eingesetzt (R, K, L, S, C).
- Fuer Ereignisse wird ein MUSLE-naher Ansatz verwendet (Abflussfaktor ersetzt R fuer Einzelregen).
- Feldblockgrenzen/Landschaftselemente werden als Barrieren explizit in LS/Abflusspfaden beruecksichtigt.
- Datengrundlagen enthalten u. a. Feldblockgrenzen und InVeKoS-Anbaudaten (mehrjaehrig), Bodenschaetzung,
  VBK50, DWD-Daten, Gewaessernetz.
- Wichtig fuer die Aufloesung: In der 2021er Studie wird betont, dass mittelmassstaebige Eingangsdatensaetze
  fuer kleinraeumige Teilraeume nicht ausreichen; fuer LS wird max. 5 m Rasterweite gefordert.

Konsequenz fuer unser Setup (nur oeffentliche Daten):
- Sehr gut abbildbar: DGM1/DGM5, Abflussbahnen, Hotspots, Gewaesseranschluss, Event-Regen.
- Gut abbildbar: C-Proxy ueber Landnutzung (ALKIS/WorldCover), optional Sentinel-Zeitreihen.
- Nur eingeschraenkt abbildbar: K auf Schlagebene, wenn keine feineren (landes-/fachspezifischen) Bodendaten
  frei verfuegbar sind.
- Praktischer Weg: ABAG als belastbares Screening + Event-ML/MUSLE-nahe Ereignispriorisierung; absolute
  Flachenschaetzungen mit Unsicherheitsvermerk ausgeben.
