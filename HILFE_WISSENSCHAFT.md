# Hydrowatch Hilfe: Was, Warum, Wie (mit wissenschaftlichem Hintergrund)

## Zielbild
Hydrowatch dient der **indikativ-hydrologischen Risikoanalyse** fuer Starkregenabfluss, Erosionstendenzen und priorisierte Hotspots.
Es ersetzt keine rechtsverbindliche Gefahrenkarte, liefert aber eine belastbare **Entscheidungsgrundlage fuer Priorisierung**.

## Was berechnet wird
Aktueller MVP-Fokus: **Starkregen** und **Erosion** als Screening sowie **Hochwasser (amtlich)** als Referenzdarstellung.

Outputs:
1. Fliesswege/Fliesssegmente auf Basis des Hoehenmodells (DEM)
2. Risiko-Score (0-100) pro Segment
3. Risikoklassen (`niedrig`, `mittel`, `hoch`, `sehr_hoch`)
4. Hotspots (kritische Punkte mit Begruendung)
5. Szenarien fuer 30/50/100 mm Niederschlag in 1h
6. Optional: amtliche Hochwasser-Overlays (WMS) als Referenzdarstellung
7. Optional: historisches Wetter (ICON-D2, gridbasiert) zur Einordnung

Demnaechst: Schlamm/Sediment und Massnahmen-Vergleich.

## Warum diese Methodik
Starkregenfolgen entstehen aus dem Zusammenspiel:
- Topographie (Gefaelle, Konzentration von Abfluss)
- Boden (Infiltration / Leitfaehigkeit)
- Versiegelung (direkter Oberflaechenabfluss)
- Niederschlagsintensitaet

Ein einzelner Layer ist nie ausreichend. Der kombinierte Score ermoeglicht robuste Priorisierung trotz heterogener Datenguetigkeit.

## Datenbasis
### Primaer
- DEM Sachsen-Anhalt: DGM1 (LVermGeo Sachsen-Anhalt), lokal als COG-Katalog.
- Boden Sachsen-Anhalt: BGR BUEK250 (Raster-Ausschnitt), wenn `SOIL_RASTER_PATH` gesetzt ist.
- Versiegelung Sachsen-Anhalt: Copernicus HRL Imperviousness 10 m, wenn `IMPERVIOUS_RASTER_PATH` gesetzt ist.

### Fallback
Wenn Boden/Versiegelung fehlen:
- Proxy-Schaetzung aus Terrain-/Akkumulationsmustern
- Kennzeichnung im Output: `assumptions.soil` / `assumptions.impervious` = `proxy`

## Wissenschaftlicher Kern
### 1) Hydrologische Ableitung (PySheds)
1. Senken fuellen (`fill_depressions`)
2. Flaechen ohne Gefaelle aufloesen (`resolve_flats`)
3. D8-Fliessrichtung
4. Fliessakkumulation
5. Netzwerkextraktion oberhalb Schwelle (`threshold`)

### 1b) Senken / potenzielles Stauwasser (Screening)
Aus dem Unterschied zwischen gefuelltem DEM und Original-DEM wird eine **Stauwasser-Tiefe** (indikativ) abgeleitet:
- `ponding_depth_m = fill_depressions(DEM) - DEM` (nur positive Werte)

Die App kann daraus:
- zusaetzliche Hotspots ("Senke / pot. Stauwasser") ableiten
- Kennzahlen im Output liefern: `ponding_area_km2`, `ponding_volume_m3` (Volumenindikator), `ponding_max_depth_m`

Wichtig: Das ist **kein** hydraulisches Modell, sondern ein topographischer Hinweis, wo Wasser sich sammeln koennte.

### 2) Hangneigung
Aus DEM-Gradient:
- `slope = atan(sqrt((dz/dx)^2 + (dz/dy)^2))`

### 3) Normierung
Kontinuierliche Faktoren werden auf 0..1 skaliert.

### 4) Risiko-Score v2
Vereinfachte Formel:

`Risk = 0.35*Acc + 0.25*Slope + 0.15*Soil + 0.15*Impervious + 0.10*Rain`

Danach Skalierung auf 0..100.

Interpretation:
- `Acc` (Akkumulation): Konzentration von Abfluss
- `Slope`: Fliessdynamik / Erosionsenergie
- `Soil`: geringere Infiltration -> hoeheres Risiko
- `Impervious`: mehr Versiegelung -> mehr Direktabfluss
- `Rain`: Hintergrundintensitaet (aktuell baseline/proxy, spaeter historisch/operativ)

## Risikoklassen
- `niedrig`: <45
- `mittel`: 45-69
- `hoch`: 70-84
- `sehr_hoch`: >=85

Diese Schwellen sind bewusst praktisch gesetzt und koennen spaeter kalibriert werden.

## Hotspot-Logik
Hotspots werden aus hohen Score-Zellen mit Mindestabstand selektiert.
Begruendung kombiniert:
- starke Fliessakkumulation
- hohe Hangneigung
- geringe Infiltration (wenn ableitbar)
- hoher Versiegelungsgrad (wenn ableitbar)

### Einzugsgebiet am Hotspot (indikativ)
Zusaetzlich wird eine **Einzugsgebiets-Flaeche** pro Hotspot angegeben:
- `upstream_area_km2` wird aus der Flow Accumulation am Hotspot abgeleitet (Zellanzahl * Pixel-Flaeche).

Das ist ein Screening-Proxy fuer "wie viel Flaeche sammelt hier Wasser", ohne ein separates Catchment-Polygon zu berechnen.

### Einzugsgebiet als Polygon (optional)
Optional kann die App fuer einen angeklickten Punkt/Hotspot ein **Einzugsgebiet als Polygon** abgrenzen (Catchment-Delineation, D8-basiert).
Das ist fuer die Praxis hilfreich, weil "Einzugsgebiet" als Flaeche besser verstanden wird als nur als Zahl.

Hinweise:
- Das ist weiterhin Screening (DEM-basiert; Bauwerke/Kanalnetz fehlen).
- Das Polygon wird im MVP auf die gezeichnete AOI (Polygon) geclippt, damit nichts ausserhalb angezeigt wird.
- Je nach AOI kann die Catchment-Berechnung ein paar Sekunden dauern; waehrenddessen zeigt die App einen Busy-Cursor und "Berechne..." im Layer-Menue.

### Abflusskorridore (Layer)
Die App kann das abgeleitete Netzwerk auch als **Abflusskorridore** darstellen.
Dabei wird die Linienstaerke/Farbe aus `upstream_area_km2` abgeleitet (indikativ: wo sich Abfluss sammelt).

Hinweis zur AOI-Form:
- Die Berechnung laeuft technisch auf dem DEM-Ausschnitt der Bounding Box.
- Wenn du ein **Polygon** zeichnest/laedst, werden **Segmente und Hotspots anschliessend auf das Polygon gefiltert**, damit nichts ausserhalb angezeigt/ausgewertet wird (MVP-Clip).

#### Netz anzeigen + Netzdichte (Fein <-> Grob)
Im UI gibt es fuer dieses Netzwerk:
- Checkbox **"Netz anzeigen"**
- Slider **"Netzdichte"** (Fein, Mittel, Grob, Hauptachsen)

Das ist ein **Darstellungsfilter**:
- je grober die Netzdichte, desto hoeher die Schwelle fuer Einzugsgebiet (beitragende Flaeche)
- dadurch bleiben nur die Linien uebrig, wo sich Abfluss wirklich deutlich buendelt

**Einzugsgebiet (beitragende Flaeche)** ist ein Proxy fuer die oberhalb liegende Flaeche, die hierhin entwassert, und wird aus der Flow Accumulation abgeleitet.

Pragmatisch im MVP:
- Die Netzdichte-Presets werden als **Anteil des maximalen Einzugsgebiets** in der aktuellen AOI umgesetzt (damit kleine und grosse AOIs gleichermassen sinnvoll funktionieren).

#### Klassifizierung (Lesbarkeit)
Zusaetzlich wird das Netz im UI in **4 Klassen** dargestellt (Farbe/Linienstaerke), ebenfalls relativ zum maximalen Einzugsgebiet der AOI:
- Nebenrinnen
- Sammellinien
- Hauptbahnen
- Hauptachsen

Hinweis fuer **Erosion**: Das vollstaendige Netzwerk kann sehr dicht wirken. Im UI werden deshalb im Erosionsmodus
standardmaessig nur die **kritischsten Segmente** dargestellt (Screening, zur besseren Lesbarkeit).

## Szenarien 30/50/100 mm in 1h
Szenarien sind aktuell relative Skalierungen auf Basis des Grundrisikos.
Sie dienen der **Vergleichbarkeit** (Wie stark steigt die Belastung bei intensiverem Regen?).

Hinweis: In der Analyse **Erosion** (topographischer Treiber) sind Regen-Szenarien im MVP nicht der Fokus und koennen entfallen.

## Performance-Strategie (wichtig fuer grosse AOI)
1. Adaptive WCS-Tilings + Merge
2. Large-AOI Modus (internes Downsampling)
3. Ausgabe-Reduktion bei sehr vielen Segmenten
4. Externe Raster werden als **AOI-Fenster** geladen, nicht komplett

Das minimiert RAM und UI-Haenger.

## Amtliche Hochwasser-Overlays (NRW)
Hydrowatch kann zusaetzlich amtliche Overlays anzeigen, um Ergebnisse schneller einordnen zu koennen:
- Quelle: NRW WMS "Hochwasser-Gefahrenkarte" (WMS)
- Layer-Logik: drei Szenarien `HW/MW/NW` (hohe/mittlere/niedrige Wahrscheinlichkeit)
  - "Ueberflutungsgrenze" (amtliche Ausdehnung)
  - optional "Ueberflutungstiefe" (amtliche Tiefe)

Wichtig:
- Diese Overlays sind eine Visualisierung amtlicher Karten. Hydrowatch berechnet hieraus keinen Rechtsanspruch.
- Kartenwerke koennen sich aendern; deshalb sind die Overlays bewusst als separate Ebene gekennzeichnet.

### Sachsen-Anhalt (Stand: MVP)
- Quelle: LHW Sachsen-Anhalt, WMS (HWRM-RL) mit Wassertiefenlayern fuer HQ10/20, HQ100, HQ200.
- In der UI wird das automatisch anhand der AOI umgeschaltet.
- Hoehenmodell: Fuer Sachsen-Anhalt wird im MVP standardmaessig ein **lokaler DGM1-COG-Katalog** genutzt (schnell, reproduzierbar). WCS kann je nach Verfuegbarkeit/Provider als Fallback dienen.
- Hinweis: Ein oeffentlicher DGM1-ZIP-Download existiert als technische Fallback-Option, ist aber bewusst nur fuer Dev/Tests gedacht (sehr gross).

## Historisches Wetter (Ist/Soll)
Hydrowatch nutzt Wetter aktuell als **Kontext** fuer Screening (keine hydraulische Tiefensimulation).

Aktueller Stand (Ist):
- Primaerquelle: flaechendeckende ICON-D2 Grid-/Modell-Zeitreihen (Open-Meteo), analog BeeApp-Logik mit `icon2dSmartFetch`.
- Smart-Host-Logik: historisch/live/mixed je nach Zeitfenster.

Zielbild (Soll):
- Wetter als standardisierter Kontextbaustein fuer Starkregen und Erosion (Vorfeuchte + Quantile + Presets) in allen AOIs.
- Optionaler Proxy-Betrieb fuer interne ICON2D-Services bleibt moeglich.

Sampling (automatisch, ohne extra UI-Schalter):
- kleine AOI: 1 repraesentativer Punkt (AOI-Zentrum)
- mittlere AOI: 5 Stichprobenpunkte (Zentrum + 4 "eingesetzte" BBox-Ecken, 10% inset)
- grosse AOI: 9 Stichprobenpunkte (3x3 Raster, inset)

API:
- `POST /weather-metrics`: Legacy/DWD-Metriken (optional, nicht Standardpfad der UI).
- `GET /abflussatlas/weather/stats`: Statistik je Stichprobenpunkt (Quantile + API14).
- `GET /abflussatlas/weather/events`: automatische Starkregen-Ereigniserkennung (Peak, Max1h/Max6h, Warnstufe); backendseitig mit Quellenwahl:
  - `icon2d` (Modell),
  - `dwd` (Station),
  - `radar` (DWD RADOLAN, stündlich),
  - `hybrid` (icon2d+dwd),
  - `hybrid_radar` (radar+dwd+icon2d, mit Fallback).
- `GET /abflussatlas/weather/preset`: kompakte Ausgabe fuer die UI (`mode=auto|standard|genauer`, Vorfeuchte + Regenpresets moderat/stark/extrem).
- Provider-Umschaltung im Backend via ENV:
  - `WEATHER_PROVIDER=auto|icon2d|dwd`
  - `auto`: verhaelt sich wie `icon2d` (kein stiller DWD-Fallback).
  - `ICON2D_TRANSPORT=direct|proxy|auto` (Open-Meteo direkt, lokaler Proxy, oder auto).

Hinweis zum Zeitraum:
- Fuer `/abflussatlas/weather/stats` und `/abflussatlas/weather/preset` wird ein "safe window" aus `daysAgo` + `hours` verwendet.
- Damit werden unvollstaendige "heutige" Daten vermieden.
- Bei `start/end` wird `end` automatisch auf "heute" begrenzt, falls ein Zukunftsdatum gesetzt wurde.
- Bei `hybrid_radar` wird standardmaessig der eingebaute DWD-RADOLAN-Reader genutzt (`RADAR_PROVIDER=dwd_radolan`).
- Optional kann stattdessen ein externer Connector genutzt werden (`RADAR_PROVIDER=connector` + `RADAR_EVENTS_URL=...`).
- Wenn Radar im Zeitraum nicht verfuegbar ist, faellt das Backend automatisch auf die verfuegbaren Quellen zurueck.

UI-Default (wichtig):
- Kein Quellen-Toggle in der Hauptoberflaeche.
- Wetter liegt im Ablauf vor der Analyse (`Gebiet -> Wetter -> Analyse`).
- Datumsauswahl + `Uebernehmen` berechnet Ereignisse und Wetterkontext.
- Ereignisse werden als kompakter Bar-Chart gezeigt; Auswahl nur ueber Balken.
- Wetterkennwerte (Vorfeuchte, P90/P95/P99) werden kompakt direkt im Wetterblock angezeigt.
- Schwellen-/Quellen-Details bleiben in der Doku, nicht als dauerhafte UI-Texte.

Integration in Starkregen-Analyse:
- Bei `POST /analyze-bbox` wird im Starkregen-Modus standardmaessig ein Wetter-Kontext berechnet (`weather_auto=true`).
- Daraus werden ein Regen-Proxy (0..1) und dynamische Szenario-Intensitaeten abgeleitet; Erosion bleibt topographisch dominiert.
- Optional kann ein erkanntes Ereignis direkt uebergeben werden (`weather_event_mm_h`), um die Starkregen-Analyse gezielt auf dieses Ereignis zu setzen.

Fuer den gewaehlten Zeitraum werden Metriken berechnet:
- Max 1h / 6h / 24h Niederschlag (mm)
- Summe Niederschlag (mm)
- Anzahl Stunden ueber Schwellwerten (z. B. >=10/25/40 mm)
- Top-Stundenereignisse (zur schnellen Plausibilisierung)

Warum diese Reihenfolge:
- Grid/Modell (`icon2dSmartFetch`) liefert flaechige Basiswerte.
- Radar (wenn verfuegbar) verbessert lokale Ereigniserkennung.
- Stationen dienen als Zusatz/Referenz im Hybridmodus.

## Unsicherheiten / Grenzen
1. Modell ist indikatorbasiert, keine vollstaendige 2D-Hydrodynamik
2. Niederschlagskomponente aktuell vereinfacht
3. Qualitaet haengt von Datenaktualitaet/-aufloesung ab
4. Rechtlich nicht als amtliche Gefahrenkarte zu verwenden

## Empfohlene Weiterentwicklung
1. Oeffentliche Basislayer verbindlich integrieren (Boden, Versiegelung, Landbedeckung) inkl. Cache/Versionierung
2. Massnahmenmodule (Retention, Entsiegelung) mit Wirkungsabschaetzung
3. Kalibrierung gegen bekannte Ereignisse (Rueckblick)

## Roadmap (geordnet)
Siehe `ROADMAP.md` (MVP -> Ausbau mit OSM, Versiegelung, Landbedeckung, Boden, Wetter-Presets, Massnahmen, Projektmodus).
4. Regionale Provider-Erweiterung (Sachsen/Sachsen-Anhalt) mit gleicher Pipeline

## Technische Orientierung
- API/Streaming: `backend/main.py`
- Analysekern: `backend/processing.py`
- WCS/Provider: `backend/wcs_client.py`
- Wetter (DWD, Legacy): `backend/weather_dwd.py`
- Wetter (Batch/Stats, safe window): `backend/abflussatlas_weather.py`, `backend/weather_stats.py`, `backend/weather_window.py`
- WMS Utils: `backend/wms_utils.py`
- Frontend-Interaktion: `frontend/src/App.jsx`
