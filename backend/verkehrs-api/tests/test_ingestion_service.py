import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
from ingestion_service import IngestionService
import io
import json

@pytest.fixture
def mock_minio():
    with patch("ingestion_service.Minio") as mock:
        yield mock

@pytest.fixture
def mock_api():
    with patch("ingestion_service.WienerLinienAPI") as mock:
        yield mock

@pytest.fixture
def mock_stations_manager():
    with patch("ingestion_service.StationsManager") as mock:
        yield mock

@pytest.mark.asyncio
async def test_fetch_and_save_success(mock_minio, mock_api, mock_stations_manager):
    # Setup
    # Mock os.getenv to avoid side effects or connection attempts if real init runs
    with patch("os.getenv", return_value="test_value"):
        service = IngestionService(interval_seconds=60, bucket_name="test-bucket")
    
    service.rbl_list = ["123"]
    service.minio_client = MagicMock() # Ensure we use a fresh mock for the client instance
    
    # Mock API response
    raw_data = {
        "data": {
            "monitors": [
                {
                    "locationStop": {"properties": {"title": "X"}},
                    "lines": [{"name": "U1"}]
                }
            ]
        }
    }
    service.api = MagicMock()
    service.api.fetch_batch = AsyncMock(return_value=raw_data)
    service.api.transform_to_events.return_value = [{"station": "A", "rbl": 123}]
    
    # Mock MinIO
    service.minio_client.bucket_exists.return_value = True
    service.minio_client.stat_object.side_effect = Exception("Not found") # File doesn't exist
    
    # Execute
    await service.fetch_and_save()
    
    # Verify
    assert service.minio_client.put_object.call_count == 1
    
    # Verify Parquet upload
    call_args_parquet = service.minio_client.put_object.call_args_list[0]
    bucket, name, data, length = call_args_parquet[0]
    kwargs = call_args_parquet[1]
    
    assert bucket == "test-bucket"
    assert "raw/year=" in name
    assert name.endswith(".parquet")
    assert kwargs.get("content_type") == "application/octet-stream"

    # Verify content structure
    data.seek(0)
    df = pd.read_parquet(data)
    assert not df.empty
    assert len(df) == 1
    assert "ingestion_timestamp" in df.columns
    assert "raw_json" in df.columns
    
    # Check if raw_json contains our data
    saved_json = json.loads(df.iloc[0]["raw_json"])
    assert saved_json == raw_data
