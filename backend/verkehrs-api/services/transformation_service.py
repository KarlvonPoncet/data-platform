import duckdb
import os
import logging
import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

from core.state_manager import StateManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransformationService:
    """
    Service to transform raw JSON data from MinIO into a structured DuckDB table
    and save it back to MinIO as a refined layer.
    """
    def __init__(self, bucket_name: str = None, interval_seconds: int = None):
        self.bucket = bucket_name or os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        self.api_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        self.interval_seconds = interval_seconds or int(os.getenv("TRANSFORMATION_INTERVAL", 3600))
        self.scheduler = AsyncIOScheduler()
        self.state_manager = StateManager()
        
    async def process_raw_data(self):
        """
        Reads raw parquet files from MinIO, extracts structured data using DuckDB,
        and saves the result to the refined layer in MinIO.
        """
        logger.info("Starting transformation job...")
        
        try:
            # specialized connection
            con = duckdb.connect()
            
            # Install and load necessary extensions
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("INSTALL json; LOAD json;")
            
            # Configure MinIO/S3 access
            # Note: s3_url_style='path' is crucial for MinIO
            con.execute(f"""
                SET s3_endpoint='{self.api_endpoint}';
                SET s3_access_key_id='{self.access_key}';
                SET s3_secret_access_key='{self.secret_key}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            
            logger.info("DuckDB connected to MinIO.")
            
            # 1. Determine the high-water mark via StateManager
            max_ts = self.state_manager.get_last_processed_timestamp("raw_to_refined")
            if max_ts:
                logger.info(f"Loaded high-water mark from state: {max_ts}")
            else:
                logger.info("No existing state found (starting fresh).")
            
            # 2. Define the transformation query with incremental logic
            timestamp_filter = ""
            if max_ts:
                try:
                    # Handle both datetime objects and strings
                    if isinstance(max_ts, str):
                        max_ts_obj = datetime.fromisoformat(max_ts.replace('Z', '+00:00'))
                    else:
                        max_ts_obj = max_ts
                        
                    max_ts_str = max_ts_obj.strftime("%Y%m%dT%H%M%SZ")
                    max_year = max_ts_obj.strftime("%Y")
                    
                    # Partition filtering added to drastically reduce files scanned
                    # ingestion_timestamp > max_ts prevents duplicates
                    timestamp_filter = f"WHERE ingestion_timestamp > '{max_ts}' AND year >= '{max_year}' AND snapshot_ts >= '{max_ts_str}'"
                except Exception as e:
                    logger.warning(f"Failed to create partition filters: {e}")
                    timestamp_filter = f"WHERE ingestion_timestamp > '{max_ts}'"
            
            # define the transformation query
            query = f"""
                CREATE OR REPLACE TABLE new_refined_data AS
                WITH raw_source AS (
                    SELECT 
                        ingestion_timestamp,
                        json(raw_json) as data
                    FROM read_parquet('s3://{self.bucket}/raw/**/*.parquet', hive_partitioning=1)
                    {timestamp_filter}
                ),
                unnested_monitors AS (
                    SELECT 
                        ingestion_timestamp,
                        unnest(CAST(data.data.monitors AS JSON[])) as monitor
                    FROM raw_source
                ),
                unnested_lines AS (
                    SELECT 
                        ingestion_timestamp,
                        monitor.locationStop.properties.title::VARCHAR as station_name,
                        monitor.locationStop.properties.attributes.rbl::INTEGER as rbl,
                        unnest(CAST(monitor.lines AS JSON[])) as line
                    FROM unnested_monitors
                ),
                unnested_departures AS (
                    SELECT 
                        ingestion_timestamp,
                        station_name,
                        rbl,
                        line.name::VARCHAR as line,
                        line.towards::VARCHAR as direction,
                        unnest(CAST(line.departures.departure AS JSON[])) as dep
                    FROM unnested_lines
                )
                SELECT 
                    ingestion_timestamp,
                    station_name,
                    rbl,
                    line,
                    direction,
                    dep.departureTime.timePlanned::VARCHAR as time_planned,
                    dep.departureTime.timeReal::VARCHAR as time_real,
                    dep.departureTime.countdown::INTEGER as countdown
                FROM unnested_departures
                WHERE line LIKE 'U%'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY rbl, line, direction, dep.departureTime.timePlanned::VARCHAR 
                    ORDER BY ingestion_timestamp DESC
                ) = 1
                ORDER BY ingestion_timestamp DESC;
            """
            
            logger.info("Executing transformation query...")
            con.execute(query)
            
            # Check validation
            count = con.execute("SELECT count(*) FROM new_refined_data").fetchone()[0]
            logger.info(f"New data to add: {count} rows.")
            
            if count > 0:
                # Save to MinIO as a NEW file to append to the dataset
                # Generate unique filename based on current time
                now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
                output_path = f"s3://{self.bucket}/refined/traffic_data_{now_str}.parquet"
                
                logger.info(f"Saving new refined data to {output_path}...")
                
                con.execute(f"COPY new_refined_data TO '{output_path}' (FORMAT PARQUET);")
                logger.info("Incremental update complete.")
                
                # Update StateManager with newest timestamp (max of this batch)
                max_new_ts = con.execute("SELECT MAX(ingestion_timestamp) FROM new_refined_data").fetchone()[0]
                if max_new_ts:
                    # Convert object to string if needed
                    if not isinstance(max_new_ts, str):
                        max_new_ts = max_new_ts.strftime("%Y-%m-%d %H:%M:%S.%f%z")
                    self.state_manager.set_last_processed_timestamp("raw_to_refined", str(max_new_ts))
                    
            else:
                logger.info("No new data found to transform.")
                
            con.close()
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            # Don't raise in scheduled job or it might stop the scheduler depending on config.
            # But logging is enough.

    def start(self):
        """
        Starts the scheduler.
        """
        if not self.scheduler.running:
            self.scheduler.add_job(
                self.process_raw_data,
                trigger=IntervalTrigger(seconds=self.interval_seconds),
                id='transform_data',
                name='Transform Raw Data to Refined Layer',
                replace_existing=True
            )
            logger.info(f"Starting transformation scheduler with interval {self.interval_seconds}s...")
            self.scheduler.start()

async def main():
    service = TransformationService()
    service.start()
    
    # Keep the script running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping transformation service...")
        service.scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
