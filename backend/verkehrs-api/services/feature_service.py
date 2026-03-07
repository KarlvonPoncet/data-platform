import duckdb
import os
import logging
import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

from core.state_manager import StateManager
from core.minio_manager import MinioManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FeatureService:
    """
    Service to transform refined U-Bahn data into business-relevant features
    (e.g., delay times, boolean delay flags) and save to the 'features' tier.
    """
    def __init__(self, interval_seconds: int = None):
        self.bucket = os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        self.interval_seconds = interval_seconds or int(os.getenv("FEATURE_INTERVAL", 3600))
        
        # Connect to Storage
        self.storage = MinioManager(bucket_name=self.bucket)
        
        self.scheduler = AsyncIOScheduler()
        self.state_manager = StateManager()

    async def process_refined_data(self):
        """
        Reads refined parquet files incrementally, computes features,
        and saves the result to the features layer in MinIO.
        """
        logger.info("Starting feature extraction job...")
        
        # Ensure 'features' bucket exists or we can just use a prefix in the same bucket.
        # Here we'll just write to s3://{bucket}/features/
        
        try:
            con = duckdb.connect()
            
            # Install and configure extensions for S3/Minio
            con.execute("INSTALL httpfs; LOAD httpfs;")
            
            con.execute(f"""
                SET s3_endpoint='{os.getenv("MINIO_ENDPOINT", "localhost:9000")}';
                SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY", "minioadmin")}';
                SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY", "minioadmin")}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            # Apply memory ceiling as per Rule: Memory Management
            con.execute("SET max_memory='4GB';")
            
            # Determine high-water mark via StateManager
            max_ts = self.state_manager.get_last_processed_timestamp("refined_to_features")
            
            timestamp_filter = ""
            if max_ts:
                logger.info(f"Loaded high-water mark for features: {max_ts}")
                try:
                    # Time is stored as a string, but we can safely string compare it against the ingestion_timestamp
                    timestamp_filter = f"WHERE ingestion_timestamp > '{max_ts}'"
                except Exception as e:
                    logger.warning(f"Failed to create timestamp filter: {e}")
            else:
                logger.info("No existing state found for features (starting fresh).")
            
            query = f"""
                CREATE OR REPLACE TABLE new_features AS
                WITH source AS (
                    SELECT * FROM read_parquet('s3://{self.bucket}/refined/*.parquet')
                    {timestamp_filter}
                ),
                parsed_times AS (
                    SELECT 
                        *,
                        -- attempt to cast the timestamps assuming format string if possible; Wiene Linien API gives strings like '2026-03-07T21:05:00.000+0100'
                        -- DuckDB can usually cast ISO8601 strings to TIMESTAMP WITH TIME ZONE directly
                        TRY_CAST(time_planned AS TIMESTAMP WITH TIME ZONE) as time_planned_ts,
                        TRY_CAST(time_real AS TIMESTAMP WITH TIME ZONE) as time_real_ts
                    FROM source
                ),
                calculated_delays AS (
                    SELECT 
                        *,
                        EXTRACT('epoch' FROM time_real_ts) - EXTRACT('epoch' FROM time_planned_ts) AS delay_seconds
                    FROM parsed_times
                )
                SELECT 
                    *,
                    CASE WHEN delay_seconds > 60 THEN true ELSE false END as is_delayed
                FROM calculated_delays
                ORDER BY ingestion_timestamp ASC;
            """
            
            logger.info("Executing feature extraction query...")
            con.execute(query)
            
            # Check validation
            count = con.execute("SELECT count(*) FROM new_features").fetchone()[0]
            logger.info(f"New feature rows processed: {count}")
            
            if count > 0:
                now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                output_path = f"s3://{self.bucket}/features/feature_traffic_{now_str}.parquet"
                
                logger.info(f"Saving new feature data to {output_path}...")
                con.execute(f"COPY new_features TO '{output_path}' (FORMAT PARQUET);")
                logger.info("Feature generation complete.")
                
                # Update StateManager
                max_new_ts = con.execute("SELECT MAX(ingestion_timestamp) FROM new_features").fetchone()[0]
                if max_new_ts:
                    if not isinstance(max_new_ts, str):
                        max_new_ts = max_new_ts.strftime("%Y-%m-%d %H:%M:%S.%f%z")
                    self.state_manager.set_last_processed_timestamp("refined_to_features", str(max_new_ts))
                    
            else:
                logger.info("No new data found to extract features from.")
                
            con.close()
            
        except Exception as e:
            logger.error(f"Feature extraction failed: {e}")

    def start(self):
        """Starts the scheduler."""
        if not self.scheduler.running:
            self.scheduler.add_job(
                self.process_refined_data,
                trigger=IntervalTrigger(seconds=self.interval_seconds),
                id='extract_features',
                name='Extract Business Features',
                replace_existing=True
            )
            logger.info(f"Starting feature service scheduler with interval {self.interval_seconds}s...")
            self.scheduler.start()

async def main():
    service = FeatureService()
    await service.process_refined_data()
    service.start()
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping feature service...")
        service.scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
