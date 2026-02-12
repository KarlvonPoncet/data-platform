import asyncio
import httpx
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class WienerLinienAPI:
    """
    Client for the Wiener Linien Realtime API.
    """
    BASE_URL = "https://www.wienerlinien.at/ogd_realtime/monitor"

    def __init__(self, timeout: float = 10.0, max_retries: int = 4, base_delay: float = 1.0):
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_delay = base_delay

    def validate_response(self, response: httpx.Response) -> dict:
        """
        Validates the HTTP response: checks status code and JSON structure.
        Returns the parsed JSON data if valid.
        Raises httpx.HTTPStatusError or ValueError if invalid.
        """
        # 1. Check HTTP Status Code
        response.raise_for_status()

        # 2. Check content-type (optional, warnings for now)
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            logger.warning(f"Unexpected content-type: {content_type}")

        # 3. Parse JSON
        try:
            data = response.json()
        except Exception as e:
            raise ValueError(f"Failed to parse JSON response: {e}")

        # 4. Validate API Structure
        # Expected structure: { "data": { "monitors": [...] }, "message": { ... } }
        if "data" not in data:
            # Sometimes API returns validation errors in 'message' field without 'data'
            if "message" in data:
                msg = data["message"].get("messageCode") or data["message"].get("value")
                raise ValueError(f"API returned error message: {msg}")
            raise ValueError("Response missing 'data' field")

        if "monitors" not in data["data"]:
            raise ValueError("Response missing 'data.monitors' structure")

        return data

    async def fetch_batch(self, rbl_list: list[str]) -> dict:
        """
        Fetches realtime data for multiple RBL IDs in a single batch request.
        Retries with exponential backoff on failures.
        """
        params = [("rbl", rbl) for rbl in rbl_list]
        
        async with httpx.AsyncClient() as client:
            for attempt in range(self.max_retries + 1):
                try:
                    logger.info(f"Requesting data for {len(rbl_list)} stations from {self.BASE_URL} (Attempt {attempt + 1}/{self.max_retries + 1})...")
                    response = await client.get(self.BASE_URL, params=params, timeout=self.timeout)

                    data = self.validate_response(response)
                    logger.info("Response validated successfully.")
                    return data

                except (httpx.HTTPStatusError, httpx.RequestError, ValueError) as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    
                    if attempt < self.max_retries:
                        sleep_time = self.base_delay * (2 ** attempt)
                        logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.error(f"All {self.max_retries + 1} attempts failed.")
                        return None
                        
                except Exception as e:
                    # Non-recoverable unexpected invalid usage or severe errors
                    logger.error(f"Unexpected Error on attempt {attempt + 1}: {e}")
                    return None

    def transform_to_events(self, data: dict) -> list[dict]:
        """
        Transforms the raw API response into a simplified event list.
        """
        events = []
        if not data:
            return events

        # Helper to safely traverse
        monitors = data.get("data", {}).get("monitors", [])
        
        for monitor in monitors:
            try:
                station_name = monitor["locationStop"]["properties"]["title"]
                
                # Attempt to find RBL in locationStop attributes or monitor attributes
                # In WL API, 'locationStop' usually has 'properties' -> 'attributes' -> 'rbl'
                rbl_num = "Unknown"
                try:
                    rbl_num = monitor["locationStop"]["properties"]["attributes"]["rbl"]
                except (KeyError, TypeError):
                    # Fallback: check if it's available elsewhere or just keep "Unknown"
                    pass

                for line in monitor["lines"]:
                    line_name = line["name"]
                    # Get departures
                    departures = line.get("departures", {}).get("departure", [])
                    
                    if departures:
                        # We only take the next departure for now, as in the original code
                        next_dep = departures[0]["departureTime"]
                        
                        event = {
                            "rbl": rbl_num,
                            "station": station_name,
                            "linie": line_name,
                            "richtung": line["towards"],
                            "geplant": next_dep.get("timePlanned"),
                            "tatsaechlich": next_dep.get("timeReal"),
                            "countdown": next_dep.get("countdown"), # Minutes
                            "timestamp_abruf": datetime.now().isoformat()
                        }
                        events.append(event)
            except KeyError as e:
                logger.warning(f"Skipping a monitor/line due to missing key: {e}")
                continue
                
        return events

async def main():
    # Setup basic logging for standalone run
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Default station IDs (examples)
    STATION_IDS = ['13', '31', '51', '1490', '1502', '1678', '1680', '1710', '1756', '2610', '2611', '2923', '4109', '4120', '4202', '4213', '4416', '4421', '5407', '5416', '5573', '5600', '5601', '5602', '5603', '5604', '5605', '5606', '5607', '5943']
    
    api = WienerLinienAPI()
    
    logger.info(f"Starting data pull for {len(STATION_IDS)} stations...")
    raw_data = await api.fetch_batch(STATION_IDS)
    
    if raw_data:
        processed_events = api.transform_to_events(raw_data)
        
        print(f"\nFound Events: {len(processed_events)}")
        print("-" * 50)
        for ev in processed_events:
            print(f"[RBL: {ev['rbl']}] [{ev['linie']}] {ev['station']} -> {ev['richtung']}: {ev['countdown']} Min.")
    else:
        print("No data received.")

if __name__ == "__main__":
    asyncio.run(main())