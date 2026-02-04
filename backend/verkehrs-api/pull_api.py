import asyncio
import httpx
import json
from datetime import datetime

# Beispiel RBL-Nummern (Stephansplatz, Karlsplatz, Westbahnhof)
STATION_IDS = ["4212", "4201", "4911"]

async def fetch_wiener_linien_batch(rbl_list):
    """
    Holt Daten für mehrere Stationen gleichzeitig via Batch-Request.
    """
    # Die Wiener Linien API erlaubt mehrere rbl Parameter in einer URL
    base_url = "https://www.wienerlinien.at/ogd_realtime/monitor"
    params = [("rbl", rbl) for rbl in rbl_list]
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(base_url, params=params, timeout=10.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Fehler beim Abruf: {e}")
            return None

def transform_to_events(data):
    """
    Transformiert die API-Antwort in dein gewünschtes Event-Format.
    """
    events = []
    if not data or "data" not in data:
        return events

    for monitor in data["data"]["monitors"]:
        station_name = monitor["locationStop"]["properties"]["title"]
        
        for line in monitor["lines"]:
            line_name = line["name"]
            # Wir nehmen die nächste Abfahrt als Event
            departures = line["departures"]["departure"]
            
            if departures:
                next_dep = departures[0]["departureTime"]
                
                # Event-Objekt erstellen
                event = {
                    "station": station_name,
                    "linie": line_name,
                    "richtung": line["towards"],
                    "geplant": next_dep.get("timePlanned"),
                    "tatsaechlich": next_dep.get("timeReal"),
                    "countdown": next_dep.get("countdown"), # Minuten bis Abfahrt
                    "timestamp_abruf": datetime.now().isoformat()
                }
                events.append(event)
    return events

async def main():
    print(f"Starte Daten-Pull für {len(STATION_IDS)} Stationen...")
    
    raw_data = await fetch_wiener_linien_batch(STATION_IDS)
    processed_events = transform_to_events(raw_data)
    
    # Ausgabe der Ergebnisse
    print(f"\nGefundene Events (Abfahrten): {len(processed_events)}")
    print("-" * 50)
    for ev in processed_events:
        print(f"[{ev['linie']}] {ev['station']} -> {ev['richtung']}: {ev['countdown']} Min.")

if __name__ == "__main__":
    asyncio.run(main())