import io
import os
import logging
import pandas as pd
from minio import Minio
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class MinioManager:
    """
    Manages connections and operations with MinIO object storage.
    """
    def __init__(self, bucket_name: str = None):
        self.bucket = bucket_name or os.getenv("MINIO_BUCKET_NAME", "traffic-data")
        self.client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )

    def ensure_bucket_exists(self):
        """
        Ensures that the configured bucket exists, creates it if not.
        """
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket '{self.bucket}'")
            else:
                logger.info(f"Bucket '{self.bucket}' already exists.")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise e

    def object_exists(self, object_name: str) -> bool:
        """
        Checks if an object exists in the configured bucket.
        """
        try:
            self.client.stat_object(self.bucket, object_name)
            return True
        except Exception:
            return False

    def upload_dataframe_as_parquet(self, df: pd.DataFrame, object_name: str) -> bool:
        """
        Converts a Pandas DataFrame to Parquet format in memory and uploads it to MinIO.
        Returns True if successful, False if the object already exists.
        """
        if self.object_exists(object_name):
            logger.warning(f"Object '{object_name}' already exists. Skipping upload to ensure uniqueness.")
            return False

        try:
            # Convert DataFrame to Parquet in-memory
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            data_length = len(buffer.getvalue())
            
            # Upload to MinIO
            self.client.put_object(
                self.bucket,
                object_name,
                buffer,
                data_length,
                content_type="application/octet-stream"
            )
            logger.info(f"Uploaded raw snapshot to s3://{self.bucket}/{object_name} ({data_length} bytes)")
            return True
        except Exception as e:
            logger.error(f"Failed to upload DataFrame as Parquet to '{object_name}': {e}")
            raise e
