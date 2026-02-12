import duckdb
import os
import logging
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
    def __init__(self, bucket_name: str = None):
        self.bucket = bucket_name or os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        self.api_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
    def process_raw_data(self):
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
            
            # define the transformation query
            # We use distinct on the result to avoid duplicates if we re-run on same source files
            query = f"""
                CREATE OR REPLACE TABLE refined_data AS
                WITH raw_source AS (
                    SELECT 
                        ingestion_timestamp,
                        json(raw_json) as data
                    FROM read_parquet('s3://{self.bucket}/raw/**/*.parquet', hive_partitioning=1)
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
            count = con.execute("SELECT count(*) FROM refined_data").fetchone()[0]
            logger.info(f"Transformed data contains {count} rows.")
            
            if count > 0:
                # Save to MinIO (overwriting the single refined file for now, or partitioned)
                # Let's use a single file for the 'current' view, or maybe partition by day?
                # For simplicity as requested "clean table", let's dump to a file.
                output_path = f"s3://{self.bucket}/refined/traffic_data.parquet"
                logger.info(f"Saving refined data to {output_path}...")
                
                con.execute(f"COPY refined_data TO '{output_path}' (FORMAT PARQUET);")
                logger.info("Transformation and save complete.")
            else:
                logger.warning("No data found to transform.")
                
            con.close()
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise

if __name__ == "__main__":
    service = TransformationService()
    service.process_raw_data()
