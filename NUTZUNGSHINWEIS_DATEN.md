# Nutzungshinweis Daten (intern)

Stand: 22.02.2026

## Zweck
Dieser Hinweis legt fest, welche externen Daten bis zur finalen Freigabe nur intern/testweise
in Hydrowatch genutzt werden.

## Grundsatz
1. Kein eigener Endnutzer-Export der fremden Rohdaten.
2. Keine Weitergabe/Verteilung von Rohdaten an Dritte.
3. Nutzung vorerst nur fuer interne Berechnung/Validierung.

## Sachsen-Anhalt Agraratlas / MWL Erosion WMS
Dienst:
- `https://www.geodatenportal.sachsen-anhalt.de/wss-org1/service/ST_MWL_Erosion/guest`

In den WMS-Capabilities sind AccessConstraints hinterlegt (Erlaubnisvorbehalt fuer weitergehende
Vervielfaeltigung/Veröffentlichung). Bis zur schriftlichen Freigabe gilt daher:

Erlaubt (intern):
- testweiser Abruf,
- serverseitige Verarbeitung zu ABAG/MUSLE,
- temporaerer Cache fuer Rechenlauf.

Nicht erlaubt (ohne Freigabe):
- Rohdaten-Download fuer Endnutzer,
- Rohdaten-Weitergabe,
- externe Verteilung der abgeleiteten Layer als Datensatz.

## To-do fuer produktive Nutzung
1. Schriftliche Freigabe von MWL/LLG einholen (Nutzungszweck, Verarbeitung, Caching, Ergebnisdarstellung).
2. Quelle/Lizenz in der App transparent ausweisen.
3. Datenfluss dokumentieren (welcher Layer wird wann wie verarbeitet).
