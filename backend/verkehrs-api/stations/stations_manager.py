import pandas as pd
import os
import logging
import asyncio
import sys

# Add parent directory to path to allow importing realtime_api from parent folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from realtime_api import WienerLinienAPI
except ImportError:
    # Fallback if running from a different context where realtime_api is not found easily
    # (e.g. if sys.path hack fails, though it usually works for scripts)
    logging.warning("Could not import WienerLinienAPI. Realtime features will not work.")
    WienerLinienAPI = None

# Set up module-level logger
logger = logging.getLogger(__name__)

class StationsManager:
    """
    Manages the loading and filtering of station data.
    """

    URL_STATIONS_RBL = "https://www.wienerlinien.at/ogd_realtime/doku/ogd/wienerlinien-ogd-haltepunkte.csv"

    def __init__(self, url=URL_STATIONS_RBL, data_dir="data"):
        self.base_dir = os.path.dirname(__file__)
        self.data_dir = os.path.join(self.base_dir, data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.url = url

    def get_stations_from_url(self):
        """
        Fetches station data from the given URL and saves the raw data.
        """
        logger.info(f"Fetching data from {self.url}...")
        try:
            # Daten laden (CSV)
            # Using utf-8 and separator ';' as used in the file
            df = pd.read_csv(self.url, 
                         encoding='utf-8',
                         on_bad_lines='skip',
                         sep=';')
            
            # Save raw data - using ';' to be consistent with load_local_data expectation
            output_path = os.path.join(self.data_dir, "vienna_stations_all_raw_rbl.csv")
            df.to_csv(output_path, index=False, sep=';')
            logger.info(f"Raw data saved to {output_path}")
            
            return df
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return None

    def load_local_data(self, filename="vienna_stations_all_raw_rbl.csv", sep=';'):
        """
        Loads station data from a local file in the data directory.
        """
        file_path = os.path.join(self.data_dir, filename)
        logger.info(f"Loading data from {file_path}...")
        try:
            df = pd.read_csv(file_path, 
                         encoding='utf-8', 
                         on_bad_lines='skip',
                         sep=sep) 
            return df
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            return None
        except Exception as e:
            # logger.error(f"Error loading data: {e}") # Optional: silent fail to try remote next?
            # But get_stations handles None return.
            logger.error(f"Error loading local data: {e}")
            return None

    def get_stations(self, filename="vienna_stations_all_raw_rbl.csv"):
        """
        Tries to load data locally first. If not found, fetches from URL.
        """
        logger.info("Checking local storage for stations data...")
        df = self.load_local_data(filename)
        if df is not None:
            logger.info("Successfully loaded data locally.")
            return df
        
        logger.info("Local data not found or error loading. Fetching from API...")
        df = self.get_stations_from_url()
        if df is not None:
            return df
            
        logger.error("Failed to get stations data.")
        return None

    async def get_ubahn_stations(self):
        """
        Checks if vienna_ubahn_all.csv exists locally and returns it as a DataFrame 
        (station and rbl columns). If not, creates it by calling find_all_ubahn().
        """
        filename = "vienna_ubahn_all.csv"
        df = self.load_local_data(filename)
        
        if df is None:
            logger.info(f"{filename} not found. Creating it via find_all_ubahn()...")
            await self.find_all_ubahn()
            df = self.load_local_data(filename)
            
        if df is not None:
            # Return only station and rbl, unique
            return df[['station', 'rbl']].drop_duplicates()
        return None

    def save_data(self, df, filename):
        """
        Saves the DataFrame to a CSV file in the data directory.
        """
        if df is None:
            logger.warning("No DataFrame to save.")
            return

        output_path = os.path.join(self.data_dir, filename)
        df.to_csv(output_path, index=False, sep=';')
        logger.info(f"Data saved to {output_path}")

    async def find_all_ubahn(self):
        """
        Loads all StopIDs, fetches realtime data, filters for U-Bahn lines, and saves the result.
        """
        if WienerLinienAPI is None:
            logger.error("WienerLinienAPI not available.")
            return

        logger.info("Starting global U-Bahn search via Realtime API...")
        
        # 1. Get data (Local -> URL)
        df = self.get_stations()
        if df is None:
            logger.error("No local data found.")
            return
            
        # Get unique RBLs
        rbl_ids = df['StopID'].astype(str).str.replace(r'\.0$', '', regex=True).unique().tolist()
        logger.info(f"Loaded {len(rbl_ids)} unique station IDs to check.")
        
        batch_size = 100
        api = WienerLinienAPI()
        all_ubahn_events = []
        
        # Process in batches
        total_batches = (len(rbl_ids) + batch_size - 1) // batch_size
        
        for i in range(0, len(rbl_ids), batch_size):
            batch = rbl_ids[i:i+batch_size]
            current_batch = (i // batch_size) + 1
            if current_batch % 5 == 0: # Log every 5th batch to reduce noise
                logger.info(f"Processing batch {current_batch}/{total_batches}...")
            
            try:
                data = await api.fetch_batch(batch)
                if data:
                    events = api.transform_to_events(data)
                    # Filter for U-lines (line name starts with 'U', e.g., 'U1', 'U2')
                    u_events = [
                        e for e in events 
                        if e.get('linie', '').strip().upper().startswith('U')
                    ]
                    if u_events:
                        all_ubahn_events.extend(u_events)
            except Exception as e:
                logger.error(f"Error in batch {current_batch}: {e}")
                
        logger.info(f"Finished search. Total U-Bahn events found: {len(all_ubahn_events)}")
        
        # Save results
        if all_ubahn_events:
            # Convert list of dicts to DataFrame
            df_results = pd.DataFrame(all_ubahn_events)
            self.save_data(df_results, "vienna_ubahn_all.csv")
        else:
            logger.info("No U-Bahn events found at this time.")

if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    manager = StationsManager()
        
    # Find all U-Bahn stations
    logger.info("--- Starting Realtime U-Bahn Fetch ---")
    asyncio.run(manager.find_all_ubahn())
