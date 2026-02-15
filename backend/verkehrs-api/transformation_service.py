import duckdb
import os
import logging
import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

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
            
            # 1. Determine the high-water mark (latest ingestion timestamp)
            max_ts = None
            try:
                # Check if refined data exists using glob pattern
                # If no files match, this will likely raise an exception
                result = con.execute(f"SELECT MAX(ingestion_timestamp) FROM read_parquet('s3://{self.bucket}/refined/*.parquet')").fetchone()
                if result and result[0]:
                    max_ts = result[0]
                    logger.info(f"Found existing refined data. Max timestamp: {max_ts}")
                else:
                    logger.info("No existing refined data found (or it is empty).")
            except Exception as e:
                # Likely no files found or bucket empty
                logger.info(f"No existing refined data found (starting fresh). info: {e}")
                pass
            
            # 2. Define the transformation query with incremental logic
            timestamp_filter = ""
            if max_ts:
                # Filter strictly greater than max_ts to avoid duplicates
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
                        unnest(CAST(monitor.lines AS JSON[])) as line
                    FROM unnested_monitors
                ),
                unnested_departures AS (
                    SELECT 
                        ingestion_timestamp,
                        station_name,
                        line.name::VARCHAR as line,
                        line.towards::VARCHAR as direction,
                        unnest(CAST(line.departures.departure AS JSON[])) as dep
                    FROM unnested_lines
                )
                SELECT DISTINCT
                    ingestion_timestamp,
                    station_name,
                    line,
                    direction,
                    dep.departureTime.timePlanned::VARCHAR as time_planned,
                    dep.departureTime.timeReal::VARCHAR as time_real,
                    dep.departureTime.countdown::INTEGER as countdown
                FROM unnested_departures
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
