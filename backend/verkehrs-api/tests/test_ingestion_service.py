import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd
from services.ingestion_service import IngestionService
import io
import json

@pytest.fixture
def mock_minio():
    with patch("services.ingestion_service.MinioManager") as mock:
        yield mock

@pytest.fixture
def mock_api():
    with patch("services.ingestion_service.WienerLinienAPI") as mock:
        yield mock

@pytest.fixture
def mock_stations_manager():
    with patch("services.ingestion_service.StationsManager") as mock:
        yield mock


@pytest.mark.asyncio
async def test_fetch_and_save_success(mock_minio, mock_api, mock_stations_manager):
    # Setup
    # Mock os.getenv to avoid side effects or connection attempts if real init runs
    with patch("os.getenv", return_value="test_value"):
        service = IngestionService(interval_seconds=60, bucket_name="test-bucket")
    
    service.rbl_list = ["123"]
    
    # Storage is now MinioManager
    service.storage = MagicMock()
    
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
    
    # Execute
    await service.fetch_and_save()
    
    # Verify
    assert service.storage.upload_dataframe_as_parquet.call_count == 1
    
    # Verify the arguments
    call_args = service.storage.upload_dataframe_as_parquet.call_args[0]
    df = call_args[0]
    object_name = call_args[1]
    
    assert "raw/year=" in object_name
    assert object_name.endswith(".parquet")

    # Verify content structure
    assert not df.empty
    assert len(df) == 1
    assert "ingestion_timestamp" in df.columns
    assert "raw_json" in df.columns
    
    # Check if raw_json contains our data
    saved_json = json.loads(df.iloc[0]["raw_json"])
    assert saved_json == raw_data
