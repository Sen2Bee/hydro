# ArcEGMO-Bruecke auf Hydrowatch-Basis

Stand: 2026-03-01

## Ziel
Aus der aktuellen SA-Pipeline ein praxisnahes, behordenfaehiges Entscheidungswerkzeug aufbauen, das in Richtung ArcEGMO-Nutzenbild geht:

1. Risikoanalyse (langfristig, ABAG-orientiert).
2. Ereignisorientierte Bewertung (Event-ML + Wetterfenster).
3. Massnahmenpriorisierung (Top-10 + Karten + Berichtsausgabe).
4. Szenarienvergleich (Status quo vs. Massnahme).

## Was bereits vorhanden ist

1. ABAG-Berechnung auf Feld-/AOI-Basis (`analysis_type=abag`).
2. Ereignisbezogene Inferenz (`analysis_type=erosion_events_ml`).
3. SA-weite, resume-faehige Chunk-Pipeline (Stage A/B).
4. Lokaler Event-Cache mit Rasterzellen-Logik (2 km).
5. QA- und Paper-Assets-Generator (`run_build_paper_assets.bat`).
6. Quickcheck-Export mit priorisierten Feldern (`backend/export_quickcheck_package.py`).

## ArcEGMO-aehnliche Kernfunktionen und Mapping

1. **Disposition (statisch)**  
   Mapping: ABAG-Metriken (`abag_index_mean/p90/max`), LS/K/R/S/C/P.

2. **Ereigniswirkung (dynamisch)**  
   Mapping: Event-ML Wahrscheinlichkeiten + ereignisfensterbasierte Auswertung pro Feld.

3. **Massnahmenwirkung (Szenarien)**  
   Mapping: Parameter- und Layer-Szenarien:
   - `P`-Faktor (z. B. 1.0 -> 0.8 -> 0.6),
   - C-Faktor-Varianten (base/low/high bzw. crop-enhanced),
   - optional Schwellen/Ranking-Varianten.

4. **Entscheidungsausgabe (verwaltungsnah)**  
   Mapping:
   - Top-10 priorisierte Flaechen,
   - Kartenebenen (Hotspots, Risiko, Event-Score),
   - CSV/GeoJSON/PDF-Bericht fuer Nachvollziehbarkeit.

## Fachlich sinnvolle Filterung (Pflicht)

Um unsinnige Flaechen fuer Erosionsmodellierung auszuschliessen:

1. Geometriepruefung (ungueltige/degenerierte Polygone raus).
2. Mindestflaeche (aktuell Default `min_field_area_ha=0.05`).
3. Optional Whitelist (nur fachlich geeignete IDs, z. B. Acker-Subset).

Status: in Stage-B-Runner eingebaut.

## Empfohlene Betriebsstrategie

1. **Stage A**: SA-weit Eventfenster zentral vorrechnen (3 Vegetationsfenster, 2-km-Zellenlogik).
2. **Stage B**: nur noch lokal aus Cache rechnen (`cache-only`), dadurch robust und reproduzierbar.
3. **Parallelisierung**: 2 Worker als Standard; 3 Worker nur nach Lasttest.

## Mindestprodukt (MVP) in ArcEGMO-Richtung

1. SA-Run fuer 50k Felder (3 Fenster) mit aktivem Feldfilter.
2. Aggregation je Feld:
   - `event_probability_max`,
   - `abag_index_mean`,
   - kombinierter Prioritaetsscore.
3. Ausgabe:
   - Top-10 Massnahmenflaechen,
   - Karten-Layer (GeoJSON),
   - kurzer Entscheidungsbericht.

## Was noch fehlt fuer "voll ArcEGMO-nah"

1. Expliziter Massnahmenkatalog mit regelbasierter Wirkung (z. B. Begruenung, Mulch, Querbewirtschaftung).
2. Szenario-Engine, die Massnahmen als Delta auf C/P/Flaechenregeln rechnet.
3. Einheitlicher "Planungsbericht"-Export mit Vorher/Nachher-Kennzahlen.

## Konkreter naechster Schritt

1. 50k Stage-B-Produktion mit Filter + 2 Workern fertigstellen.
2. Danach ein erster Massnahmen-Szenariolauf:
   - Baseline (`P=1.0`, C-base),
   - Szenario A (`P=0.8`),
   - Szenario B (`P=0.6`),
   - Differenzkarten und Top-10-Verschiebungen dokumentieren.

Damit haben wir eine belastbare, nachvollziehbare und direkt kommunizierbare Bruecke zur ArcEGMO-Logik.
