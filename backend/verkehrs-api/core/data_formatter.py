import json
import pandas as pd
from datetime import datetime, timezone

class DataFormatter:
    """
    Utility class to format raw data payloads into unified structures
    and generate structured naming conventions for object storage.
    """
    
    @staticmethod
    def format_raw_to_dataframe(raw_data: dict, timestamp: datetime) -> pd.DataFrame:
        """
        Wraps raw json response inside a Pandas DataFrame along with the ingestion timestamp.
        """
        json_blob = json.dumps(raw_data)
        return pd.DataFrame([{
            "ingestion_timestamp": timestamp,
            "raw_json": json_blob
        }])

    @staticmethod
    def generate_partitioned_object_name(timestamp: datetime, base_path: str = "raw") -> str:
        """
        Generates a hive-partitioned object name based on a timestamp.
        Example: 'raw/year=2024/month=10/day=25/snapshot_ts=20241025T153000Z.parquet'
        """
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        ts_str = timestamp.strftime("%Y%m%dT%H%M%SZ")
        
        return f"{base_path}/year={year}/month={month}/day={day}/snapshot_ts={ts_str}.parquet"
