import pandas as pd
import os
import logging
import asyncio
import sys
import io
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path to allow importing realtime_api from parent folder
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
try:
    from wiener_linien_api import WienerLinienAPI
except ImportError:
    # Fallback if running from a different context where realtime_api is not found easily
    # (e.g. if sys.path hack fails, though it usually works for scripts)
    logging.warning("Could not import WienerLinienAPI. Realtime features will not work.")
    WienerLinienAPI = None

# Set up module-level logger
logger = logging.getLogger(__name__)

class StationsManager:
    """
    Manages the loading and filtering of station data, interacting with MinIO.
    """

    URL_STATIONS_RBL = "https://www.wienerlinien.at/ogd_realtime/doku/ogd/wienerlinien-ogd-haltepunkte.csv"

    def __init__(self, url=URL_STATIONS_RBL, bucket_name=None):
        self.url = url
        self.bucket = bucket_name or os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        
        # MinIO Client Configuration
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            if not self.minio_client.bucket_exists(self.bucket):
                self.minio_client.make_bucket(self.bucket)
                logger.info(f"Created bucket '{self.bucket}'")
        except Exception as e:
            logger.error(f"Failed to check/create bucket: {e}")

    def get_stations_from_url(self):
        """
        Fetches station data from the given URL and saves the raw data to MinIO.
        """
        logger.info(f"Fetching data from {self.url}...")
        try:
            # Daten laden (CSV)
            # Using utf-8 and separator ';' as used in the file
            df = pd.read_csv(self.url, 
                         encoding='utf-8',
                         on_bad_lines='skip',
                         sep=';')
            
            # Save raw data to MinIO
            self.save_data(df, "vienna_stations_all_raw_rbl.csv")
            
            return df
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return None

    def load_data(self, filename):
        """
        Loads station data from MinIO.
        """
        logger.info(f"Loading data from MinIO: {self.bucket}/{filename}...")
        try:
            response = self.minio_client.get_object(self.bucket, filename)
            # Read into pandas
            df = pd.read_csv(io.BytesIO(response.read()), 
                         encoding='utf-8', 
                         on_bad_lines='skip',
                         sep=';')
            response.close()
            response.release_conn()
            return df
        except Exception as e:
             # If object doesn't exist or connection fails
            logger.warning(f"Could not load {filename} from MinIO: {e}")
            return None

    def get_stations(self, filename="vienna_stations_all_raw_rbl.csv"):
        """
        Tries to load data from MinIO first. If not found, fetches from URL.
        """
        logger.info("Checking MinIO for stations data...")
        df = self.load_data(filename)
        if df is not None:
            logger.info("Successfully loaded data from MinIO.")
            return df
        
        logger.info("Data not found in MinIO. Fetching from API...")
        df = self.get_stations_from_url()
        if df is not None:
            return df
            
        logger.error("Failed to get stations data.")
        return None

    async def get_ubahn_stations(self):
        """
        Checks if vienna_ubahn_all.csv exists in MinIO and returns it as a DataFrame 
        (station and rbl columns). If not, creates it by calling find_all_ubahn().
        """
        filename = "vienna_ubahn_all.csv"
        df = self.load_data(filename)
        
        if df is None:
            logger.info(f"{filename} not found in MinIO. Creating it via find_all_ubahn()...")
            
            await self.find_all_ubahn()
            df = self.load_data(filename)
            
        if df is not None:
            # Return only station and rbl, unique
            # Ensure columns exist
            if 'station' in df.columns and 'rbl' in df.columns:
                return df[['station', 'rbl']].drop_duplicates()
            elif 'StopText' in df.columns and 'StopID' in df.columns:
                 # Fallback if raw data was returned?
                 df = df.rename(columns={'StopText': 'station', 'StopID': 'rbl'})
                 return df[['station', 'rbl']].drop_duplicates()
        return None

    def save_data(self, df, filename):
        """
        Saves the DataFrame to a CSV file in MinIO.
        """
        if df is None:
            logger.warning("No DataFrame to save.")
            return

        try:
            csv_bytes = df.to_csv(index=False, sep=';').encode('utf-8')
            csv_buffer = io.BytesIO(csv_bytes)
            
            self.minio_client.put_object(
                self.bucket,
                filename,
                csv_buffer,
                len(csv_bytes),
                content_type='application/csv'
            )
            logger.info(f"Data saved to MinIO: {self.bucket}/{filename}")
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {e}")

    async def find_all_ubahn(self):
        """
        Filters raw station data for IDs between 4000 and 4999 (U-Bahn range) and saves the result.
        """
        logger.info("Starting U-Bahn station filtering (Range 4000-4999)...")
        
        # 1. Get raw data
        df = self.get_stations()
        if df is None:
            logger.error("No local data found.")
            return
            
        # 2. Filter for StopID in range [4000, 4999]
        # Ensure StopID is numeric
        try:
            df['StopID'] = pd.to_numeric(df['StopID'], errors='coerce')
            df_ubahn = df[(df['StopID'] >= 4000) & (df['StopID'] <= 4999)].copy()
        except Exception as e:
             logger.error(f"Error filtering IDs: {e}")
             return

        logger.info(f"Found {len(df_ubahn)} U-Bahn stations in range 4000-4999.")
        
        # 3. Rename columns to match expected output format (rbl, station)
        # Old format had: rbl;station;linie;richtung;... (from API)
        # New format relies on raw data: StopID;DIVA;StopText;...
        # We map StopID -> rbl, StopText -> station
        df_ubahn = df_ubahn.rename(columns={'StopID': 'rbl', 'StopText': 'station'})
        
        # Minimal columns
        df_ubahn = df_ubahn[['rbl', 'station']]
        
        # Save results
        if not df_ubahn.empty:
            self.save_data(df_ubahn, "vienna_ubahn_all.csv")
        else:
            logger.info("No U-Bahn stations found in range.")

if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    manager = StationsManager()
        
    # Find all U-Bahn stations
    logger.info("--- Starting U-Bahn Station Filter ---")
    asyncio.run(manager.find_all_ubahn())
