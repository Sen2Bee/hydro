# Roadmap

## Zielbild
Eine modulare Fachplattform aufbauen, die in Sachsen-Anhalt startet und aus einem gemeinsamen technischen Kern mehrere Fachsichten bedient: Landkreis/Kommune, LHW, LAU und Forschung. Der Einstieg erfolgt nicht als große neue Plattform, sondern als belastbarer Demonstrator, Fachmodul und Priorisierungswerkzeug.

## Phase 1: Fundament in Sachsen-Anhalt
Ziel: belastbare offene Datengrundlage lokal aufbauen.

Arbeitspakete:
1. DGM1 und DOM1 für Sachsen-Anhalt serverseitig vorhalten.
2. Lizenz- und Namensnennungsregeln sauber dokumentieren.
3. Weitere Grunddaten anbinden:
   - Boden
   - Regen und Ereignisse
   - Versiegelung
   - relevante offene Geodaten
4. Erste Standardableitungen vorberechnen:
   - Relief
   - Fließwege
   - Hangneigung
   - Senken
   - DOM1 minus DGM1 als Hindernis- und Barrierenproxy

Ergebnis:
Ein sauber dokumentierter, lokal verfügbarer Layer-Stack für Sachsen-Anhalt.

## Phase 2: Fachlicher Kern statt Großmodell
Ziel: zuerst einen glaubwürdigen Rechenkern, noch ohne schweres Fremdmodell.

Arbeitspakete:
1. Regel- und proxybasierten Kern definieren für:
   - Starkregen- und Abfluss-Hotspots
   - Erosionspriorisierung
   - Hinderniswirkung
   - Maßnahmenräume
2. Bestehende Pipeline als Grundlage nutzen:
   - Cache
   - Chunking
   - Resume
   - QA
   - Schlag- und Event-Logik
3. Zusatzmodelle nur als spätere optionale Rechenmodi vorsehen.

Ergebnis:
Ein robuster Kern, der fachlich nachvollziehbar ist und schnell demonstrierbar bleibt.

## Phase 3: App-Kern bauen
Ziel: ein zeigbarer Demonstrator.

Gemeinsamer Kern:
1. Datenhaltung
2. Karten und Layer
3. Ereignisse
4. Hotspots
5. Priorisierung
6. Export
7. Dokumentation

Wichtige Regel:
Nicht institutionszentriert bauen, sondern rollen- und fragenzentriert.

Ergebnis:
Ein kleines, verständliches, glaubwürdiges Frontend mit klaren Kernfunktionen.

## Phase 4: Fachsichten ableiten
Ziel: aus einem Kern mehrere Nutzungssichten machen.

Sichten:
1. Landkreis/Kommune
   - Hotspots
   - Maßnahmenräume
   - Starkregenvorsorge
2. LHW
   - Fließwege
   - Gewässerbezug
   - Rückhalt
   - Priorisierung kritischer Bereiche
3. LAU
   - Erosionsgefährdung
   - Bodenschutz
   - Bodenfunktionsbezug
4. Forschung
   - Methodenvergleich
   - Szenarien
   - Validierung
   - Publikationen

Ergebnis:
Keine getrennten Produkte, sondern mehrere fachliche Ansichten desselben Systems.

## Phase 5: Paper-Linie parallel ausbauen
Ziel: wissenschaftliche Legitimation und Türöffner schaffen.

Arbeitspakete:
1. Manuskriptlinie weiterführen:
   - produktionsreife Pipeline
   - offene Datenbasis
   - reproduzierbare Methodik
2. Weitere Paper-Linien prüfen:
   - Erosions-, Ereignis- und Maßnahmenbezug
   - hydrologisch-geoökologische Vertiefung
3. Fachlich passende Personen gezielt ansprechen:
   - Markus Möller
   - Michael Steininger
   - Gerd Schmidt

Ergebnis:
App plus Paper statt nur App.

## Phase 6: Go-to-Market
Ziel: niedrigschwelliger Einstieg in reale Kontexte.

Vorgehen:
1. kleinen Demonstrator zeigen
2. gezielt Ansprechpartner identifizieren
3. fachlich andocken, nicht hart verkaufen
4. Feedback einholen
5. Resonanz prüfen
6. daraus Pilot oder Fachmodul entwickeln

Positionierung:
1. Pilot
2. Analysemodul
3. Priorisierungsdienst
4. Zuarbeit für Konzepte, Planungen und Förderlogik

Wichtige Regel:
Nicht frontal gegen Platzhirsche antreten, sondern ergänzen statt ersetzen.

## Phase 7: Strategische Andockpunkte
Ziel: offene Fenster nutzen.

Geeignete Felder:
1. KLIMA III
2. Starkregen
3. Klimaanpassung
4. Verwundbarkeitsanalysen
5. Monitoring
6. Maßnahmenkulissen

Ergebnis:
Nicht auf den großen Universalauftrag warten, sondern in neue Bedarfsfelder hineinwachsen.

## Phase 8: Mittelfristiger Ausbau
Ziel: aus dem Demonstrator eine belastbare Fachplattform machen.

Arbeitspakete:
1. mehr Szenarien
2. bessere Maßnahmenlogik
3. zusätzliche Rechenmodi
4. stärkere Export- und Berichtsfunktionen
5. offen einbindbare Modellpfade später prüfen:
   - Landlab
   - pywatershed
   - OpenLISEM

Ergebnis:
Mehr Tiefe, ohne den modularen Kern aufzugeben.

## Phase 9: Zweite Linie außerhalb Sachsen-Anhalt
Ziel: methodisch verwandten, aber fachlich anderen Pfad aufbauen.

Pfad B:
1. sozialräumliche Muster
2. Vulnerabilität
3. Benachteiligung
4. Erreichbarkeit
5. urbane Simulation

Wichtige Einordnung:
Das ist keine Nebenfunktion derselben Fachplattform, sondern eher eine zweite Anwendungslinie auf ähnlicher methodischer Basis.

## Prioritätenfolge
1. Sachsen-Anhalt-Datenbasis lokal stabil aufbauen.
2. einfachen fachlichen Kern definieren.
3. kleinen Demonstrator bauen.
4. Paper-Linie parallel schärfen.
5. wenige gute Kontakte gezielt ansprechen.
6. Pilot- oder Fachmodul-Einstieg suchen.
7. erst danach modellseitig vertiefen.

## Kurzfassung
Zuerst eine glaubwürdige Sachsen-Anhalt-Fachplattform mit offenem Datenkern und klarer Priorisierungslogik bauen. Parallel die Paper- und Kontaktlinie nutzen, um daraus Schritt für Schritt Pilotprojekte, Fachmodule und spätere Ausbaustufen zu entwickeln.
