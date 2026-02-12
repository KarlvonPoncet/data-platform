import asyncio
import logging
import os
import json
import pandas as pd
import io
from datetime import datetime, timezone
from minio import Minio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import local modules
from wiener_linien_api import WienerLinienAPI
from stations_manager import StationsManager
from transformation_service import TransformationService

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
        
        # MinIO Client Configuration
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        
        # Initialize StationsManager with the default URL
        URL_STATIONS_RBL = os.getenv("WIENER_P_STATIONS_URL", "https://www.wienerlinien.at/ogd_realtime/doku/ogd/wienerlinien-ogd-haltepunkte.csv")
        self.stations_manager = StationsManager(url=URL_STATIONS_RBL)
        
        self.rbl_list = []
        
        # Initialize Transformation Service
        self.transformation_service = TransformationService(bucket_name=self.bucket)

    async def initialize(self):
        """
        Initializes the service by fetching the list of U-Bahn stations and ensuring MinIO bucket exists.
        """
        logger.info("Initializing Ingestion Service...")
        
        # Ensure bucket exists
        try:
            if not self.minio_client.bucket_exists(self.bucket):
                self.minio_client.make_bucket(self.bucket)
                logger.info(f"Created bucket '{self.bucket}'")
            else:
                logger.info(f"Bucket '{self.bucket}' already exists.")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            return # Don't proceed if MinIO is down? Or run anyway? Better to return early if no storage.

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
            
            if raw_data:
                # Create a simple schema: timestamp + raw_json_blob
                json_blob = json.dumps(raw_data)
                
                df = pd.DataFrame([{
                    "ingestion_timestamp": datetime.now(timezone.utc),
                    "raw_json": json_blob
                }])
                
                # Generate path components
                now = datetime.now(timezone.utc)
                year = now.strftime("%Y")
                month = now.strftime("%m")
                day = now.strftime("%d")
                # Format: YYYYMMDDTHHMMSSZ
                ts_str = now.strftime("%Y%m%dT%H%M%SZ")
                
                object_name = f"raw/year={year}/month={month}/day={day}/snapshot_ts={ts_str}.parquet"
                
                # Check if file already exists (unlikely given timestamp in name)
                try:
                    self.minio_client.stat_object(self.bucket, object_name)
                    logger.warning(f"Object {object_name} already exists. Skipping upload to ensure uniqueness.")
                    return
                except Exception:
                    pass

                # Convert DataFrame to Parquet in-memory
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                data_length = len(buffer.getvalue())
                
                # Upload to MinIO
                self.minio_client.put_object(
                    self.bucket,
                    object_name,
                    buffer,
                    data_length,
                    content_type="application/octet-stream"
                )
                
                logger.info(f"Uploaded raw snapshot to s3://{self.bucket}/{object_name} ({data_length} bytes)")

            else:
                logger.warning("No data received from API.")
                
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
            # You might want to make this configurable via env var
            transformation_interval = int(os.getenv("TRANSFORMATION_INTERVAL", 3600))
            self.scheduler.add_job(
                self.transformation_service.process_raw_data,
                trigger=IntervalTrigger(seconds=transformation_interval),
                id='transform_data',
                name='Transform Raw Data to Refined Layer',
                replace_existing=True
            )
            
            logger.info(f"Starting scheduler with fetch interval {self.interval_seconds}s and transform interval {transformation_interval}s...")
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
