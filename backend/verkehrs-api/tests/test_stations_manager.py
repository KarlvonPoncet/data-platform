import pytest
from unittest.mock import MagicMock, patch, mock_open, AsyncMock
import pandas as pd
import os
from clients.stations_manager import StationsManager

# Helper to create a dummy DataFrame
def create_dummy_df():
    return pd.DataFrame({
        'StopID': [1, 2, 3],
        'Name': ['Station A', 'Station B', 'Station C']
    })

@pytest.fixture
def manager():
    # Avoid real MinIO connection during init
    with patch("stations_manager.Minio") as mock_minio:
         mgr = StationsManager(url="http://fake.url")
         mgr.minio_client = mock_minio.return_value
         return mgr

def test_load_data_success(manager):
    csv_content = b"StopID;Name\n1;Station A"
    
    mock_response = MagicMock()
    mock_response.read.return_value = csv_content
    
    manager.minio_client.get_object.return_value = mock_response
    
    df = manager.load_data("test.csv")
    assert df is not None
    assert len(df) == 1
    assert df.iloc[0]['Name'] == 'Station A'
    manager.minio_client.get_object.assert_called_with(manager.bucket, "test.csv")

def test_load_data_file_not_found(manager):
    manager.minio_client.get_object.side_effect = Exception("Not found")
    df = manager.load_data("nonexistent.csv")
    assert df is None

def test_get_stations_from_url_success(manager):
    with patch("pandas.read_csv") as mock_read_csv, \
         patch.object(manager, 'save_data') as mock_save:
        
        mock_read_csv.return_value = create_dummy_df()
        
        df = manager.get_stations_from_url()
        assert df is not None
        assert len(df) == 3
        mock_save.assert_called_once() # Should save to check

def test_get_stations_from_url_failure(manager):
    with patch("pandas.read_csv", side_effect=Exception("Download failed")):
        df = manager.get_stations_from_url()
        assert df is None

def test_get_stations_prefers_minio(manager):
    with patch.object(manager, 'load_data') as mock_load, \
         patch.object(manager, 'get_stations_from_url') as mock_url:
        
        mock_load.return_value = create_dummy_df()
        
        df = manager.get_stations()
        assert df is not None
        mock_load.assert_called_once()
        mock_url.assert_not_called()

def test_get_stations_falls_back_to_url(manager):
    with patch.object(manager, 'load_data', return_value=None), \
         patch.object(manager, 'get_stations_from_url') as mock_url:
        
        mock_url.return_value = create_dummy_df()
        
        df = manager.get_stations()
        assert df is not None
        mock_url.assert_called_once()

@pytest.mark.asyncio
async def test_get_ubahn_stations_cached(manager):
    with patch.object(manager, 'load_data') as mock_load, \
         patch.object(manager, 'find_all_ubahn') as mock_find:
        
        mock_load.return_value = pd.DataFrame({'station': ['A'], 'rbl': [1]})
        
        df = await manager.get_ubahn_stations()
        assert df is not None
        assert 'station' in df.columns
        mock_find.assert_not_called()

@pytest.mark.asyncio
async def test_get_ubahn_stations_needs_creation(manager):
    # First load returns None, second (after find) returns DF
    with patch.object(manager, 'load_data', side_effect=[None, pd.DataFrame({'station': ['A'], 'rbl': [1]})]), \
         patch.object(manager, 'find_all_ubahn', new_callable=AsyncMock) as mock_find:
        
        df = await manager.get_ubahn_stations()
        assert df is not None
        mock_find.assert_called_once()

def test_save_data(manager):
    df = create_dummy_df()
    manager.minio_client.put_object = MagicMock()
    
    manager.save_data(df, "test.csv")
    manager.minio_client.put_object.assert_called_once()
