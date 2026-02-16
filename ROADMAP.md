# Hydrowatch Roadmap (geordnet, MVP -> Ausbau)

Stand: 16.02.2026

## 0) Wo stehen wir heute (MVP)
- AOI (Rechteck/Polygon/GeoJSON) -> DEM-Clip (Sachsen-Anhalt: lokaler DGM1-COG Katalog; sonst WCS/Fallback).
- Analyse-Modi:
  - Starkregen: DEM-basiertes Abflussnetz + Risikoindikator + Hotspots (Screening).
  - Erosion: topographischer Treiber + Hotspots (Screening).
  - Amtliche Layer: als WMS-Overlays (Referenz).
- Abflussnetz-Darstellung: "Netz anzeigen" + Slider "Netzdichte" (Filter relativ zum max. Einzugsgebiet in der AOI).
- Catchment (Einzugsgebiet) als Polygon fuer angeklicktes Segment (optional, D8-basiert, AOI-Clip, Busy-UI).
- Wetter ist derzeit als Kontext nutzbar (Ist):
  - `POST /weather-metrics` (DWD-Station, 1 Punkt).
  - `GET /abflussatlas/weather/stats` (5 Stichprobenpunkte in der AOI-BBox, 10% inset, Verdichtung).
  - `GET /abflussatlas/weather/preset` (kompakt fuer UI: Vorfeuchte + Regenpresets).
- Wetter-Zielbild (Soll):
  - Primaerquelle: flaechendeckendes Grid/Modell (`icon2dSmartFetch` wie BeeApp).
  - DWD stationbasiert nur als Fallback/Referenz.

## 1) Naechste Stabilisierung (kurzfristig, "Produkt statt Prototyp")
Ziel: weniger Reibung, weniger UI-Fehler, reproduzierbare Ergebnisse.
- Starkregen/Netzdichte: sichtbare Dichte-Aenderung, klare Klassen, stabile Legende.
- Objekt-Check: robustes "Snap-to-Line" je Zoomstufe (gross -> grobe Linien, klein -> fein).
- Catchment UX:
  - Progress/Busy-Feedback (Cursor/Spinner) durchgehend.
  - Klicks nur wenn Segment gesnapt wurde (keine Punkte im "Nichts").
  - Kein Auto-Zoom nach Catchment (Map bleibt steuerbar).
- Performance:
  - AOI Soft/Hard-Limits + Downsampling/Reduktion fuer grosse AOIs.
  - Result-Caching: identische AOI + Parameter -> schneller.
- Exporte (minimal):
  - GeoJSON Export: Hotspots + Netz + Catchment.
  - PDF Kurzreport (1 Seite) nur mit Kernaussagen + Quellen + Disclaimer.

## 2) Wetterdaten: von "Kontext" zu "Parameter" (Phase 1)
Ziel: Wetter in die Berechnung einfliessen lassen, ohne UI aufzublasen.
- Datenstrategie (verbindlich):
  - Primaer: Grid/Modell (`icon2dSmartFetch`) fuer flaechendeckende Werte.
  - Fallback: DWD-Station, falls Grid nicht verfuegbar.
- Ein API-Endpunkt fuer UI:
  - `GET /abflussatlas/weather/preset?bbox=...&mode=auto|standard|genauer`
  - Response: `moisture` (trocken/normal/nass) + `rainPreset` (moderat/stark/extrem mm/h) + `meta`
  - `mode=auto`: kleinere AOI -> 1 Punkt, groessere AOI -> 5 Punkte (10% inset)
- In Starkregen:
  - Regen-Preset beeinflusst den `Rain`-Term im Risiko-Score (nicht hydraulisch, aber konsistent)
  - Zeitraum wird aus `daysAgo/hours` im Preset-Endpunkt abgeleitet (safe window)
- In Erosion:
  - Wetter als Trigger/Skalierung (Vorfeuchte + Ereigniskontext), nicht als Haupttreiber
- Apple-UI:
  - Im Panel nur "Wetter: Auto" + optional "Genauer"
  - Keine Zeitreihen im UI; Zeitreihen bleiben Debug/Export

### 2.1 Technischer Umbau Wetterquelle (direkt nach MVP-Stabilisierung)
- Adapter `icon2dFacade`/`icon2dSmartFetch` in dieses Backend integrieren.
- `GET /abflussatlas/weather` und `/stats` auf Grid-Quelle umstellen.
- DWD als fallback-freundliche Provider-Option behalten.
- Ergebnisvergleich (Grid vs DWD) als kurzer QA-Check in 2-3 Referenz-AOIs.

## 3) Oeffentliche Basisdaten: "50/50 Kommune + Landwirtschaft" (Phase 2)
Prinzip: keine neuen Menues pro Datensatz; Daten fliessen automatisch in Starkregen/Erosion ein.

### 3.1 OSM Infrastruktur (sehr hoher Praxisnutzen)
Vorhalten: ja (SA-Extrakt als PBF/GeoPackage; nightly refresh optional).
Ergebnisse:
- Engstellen-Ranking: Bruecken/Durchlaesse an Abflusskorridoren (kommunal).
- "Wasser trifft Strasse": gefaehrdete Strassenabschnitte/Unterfuehrungen.
- Vorflut-Pfad: Abflusskorridor -> naechster Graben/Gewaesser (Screening).

### 3.2 Versiegelung / Built-Up (Copernicus Imperviousness, 10 m)
Vorhalten: SA-Ausschnitt sinnvoll (oder live + Cache).
Ergebnisse:
- Runoff-Verstaerker: Versiegelung als Faktor im Score.
- Hotspot-Begruendung: "hoher Versiegelungsgrad" besser belegt.

### 3.3 Landbedeckung (ESA WorldCover 10 m)
Vorhalten: SA-Tiles (on-demand Cache ok).
Ergebnisse:
- Basisklassen fuer Runoff/rauheit (Screening).
- Saison-Presets fuer Erosion (Sommer/Winter/zwischenfrucht) als einfache Auswahl.

## 4) Boden & Erosion fachlich robuster (Phase 3)
Vorhalten: BGR BUEK200/250 oder Landesbodendaten (je Verfuegbarkeit) als AOI-Cache.
Ergebnisse:
- Versickerungs-/Abflussneigung (Boden-Proxy) als Layer + Score-Term.
- Erosionsvulnerabilitaet: Kombination aus Hang (DEM) + Boden (BUEK) + Bedeckung (WorldCover/Presets).
- Optional (klar als Screening markieren): USLE-nahe Kennzahl als Index (nicht "Gutachten").

## 5) Amtliche Daten + Schutzgebiete als "Konfliktcheck" (Phase 4)
Live: WMS/WFS, optional Cache.
Ergebnisse:
- Betroffenheit: Anteil AOI in UeSG/HQ-Zonen (amtlich).
- Schutzgebiete/Wasserschutz: Overlay + automatisch generierte Hinweise (keine Rechtsberatung).

## 6) Massnahmen & Vergleich (Phase 5)
Ziel: Praxisnutzen (Priorisierung + Wirksamkeit), ohne Simulation zu versprechen.
- Massnahmen-Bibliothek (parametrisiert, verstaendlich):
  - Pufferstreifen, Rueckhalt, Begruenung, Entsiegelung, Check-Dams.
- Output:
  - Vorher/Nachher Hotspots (Delta).
  - "Top 10 Stellen mit groesster Wirkung" (Screening).

## 7) Projektmodus (Phase 6)
- Projekte: AOIs + Szenarien speichern.
- Rollen: Kommune/Bauhof, Landwirtschaft, Planer.
- Teilen: Link + Export (PDF/GeoJSON) inkl. Quellenblock.

## UI-Prinzip (wichtig)
- Keine "Daten-Menues". Stattdessen:
  - Starkregen nutzt automatisch: Wetter + Versiegelung + Vorflut/OSM.
  - Erosion nutzt automatisch: Boden + Landbedeckung + Saison-Preset.
  - Optional Toggle: "Mehr Kontext" (zeigt zusaetzliche Overlays).
