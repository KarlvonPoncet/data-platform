import asyncio
import logging
import os
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import local modules
from clients.wiener_linien_api import WienerLinienAPI
from clients.stations_manager import StationsManager
from core.minio_manager import MinioManager
from core.data_formatter import DataFormatter


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IngestionService:
    """
    Service to periodically fetch real-time U-Bahn data and save it to MinIO.
    """
    def __init__(self, interval_seconds: int = None, bucket_name: str = None):
        self.interval_seconds = interval_seconds or int(os.getenv("INGESTION_INTERVAL", 60))
        self.bucket = bucket_name or os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        
        self.scheduler = AsyncIOScheduler()
        self.api = WienerLinienAPI()
        
        # Storage Configuration
        self.storage = MinioManager(bucket_name=self.bucket)
        
        # Initialize StationsManager with the default URL
        URL_STATIONS_RBL = os.getenv("WIENER_P_STATIONS_URL", "https://www.wienerlinien.at/ogd_realtime/doku/ogd/wienerlinien-ogd-haltepunkte.csv")
        self.stations_manager = StationsManager(url=URL_STATIONS_RBL)
        
        self.rbl_list = []
        
        # Initialize Transformation Service


    async def initialize(self):
        """
        Initializes the service by fetching the list of U-Bahn stations and ensuring MinIO bucket exists.
        """
        logger.info("Initializing Ingestion Service...")
        
        # Ensure bucket exists
        try:
            self.storage.ensure_bucket_exists()
        except Exception:
            return # Don't proceed if MinIO is down. Better to return early.

        # Get U-Bahn stations (cached or fetch new)
        df_stations = await self.stations_manager.get_ubahn_stations()
        
        if df_stations is not None and not df_stations.empty:
            # Extract unique RBLs
            self.rbl_list = df_stations['rbl'].unique().astype(str).tolist()
            logger.info(f"Initialized with {len(self.rbl_list)} U-Bahn RBLs.")
        else:
            logger.error("Failed to initialize U-Bahn stations. Service may not work correctly.")

    async def fetch_and_save(self):
        """
        Job to fetch data from API and upload to MinIO Parquet.
        """
        if not self.rbl_list:
            logger.warning("No RBLs to fetch. Skipping job.")
            return

        logger.info("Starting scheduled data fetch...")
        try:
            # Fetch data
            raw_data = await self.api.fetch_batch(self.rbl_list)

            if not raw_data:
                logger.warning("No data received from API.")
                return
            
            now = datetime.now(timezone.utc)
            df = DataFormatter.format_raw_to_dataframe(raw_data, now)
            object_name = DataFormatter.generate_partitioned_object_name(now)
            
            self.storage.upload_dataframe_as_parquet(df, object_name)

        except Exception as e:
            logger.error(f"Error in fetch_and_save job: {e}")

    async def backfill(self, start_date: datetime, end_date: datetime):
        """
        Backfill historical data.
        
        Args:
            start_date: Start datetime for backfill
            end_date: End datetime for backfill
        """
        # This functionality should not be used yet
        logger.warning("Backfill mechanic requested but not yet implemented.")
        raise NotImplementedError("Backfill mechanic is not yet implemented.")

    def start(self):
        """
        Starts the scheduler.
        """
        if not self.scheduler.running:
            # Add the job
            self.scheduler.add_job(
                self.fetch_and_save,
                trigger=IntervalTrigger(seconds=self.interval_seconds),
                id='fetch_ubahn_data',
                name='Fetch U-Bahn Realtime Data',
                replace_existing=True
            )
            
            # Add transformation job (e.g., every hour)

            
            logger.info(f"Starting scheduler with fetch interval {self.interval_seconds}s...")
            self.scheduler.start()

async def main():
    # Example usage
    service = IngestionService() 
    
    await service.initialize()
    
    service.start()
    
    # Keep the script running to allow the scheduler to work
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping service...")
        service.scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
