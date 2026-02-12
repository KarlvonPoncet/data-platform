import pytest
from unittest.mock import MagicMock, patch, call
from transformation_service import TransformationService

@pytest.fixture
def mock_duckdb():
    with patch("duckdb.connect") as mock:
        yield mock

def test_process_raw_data_success(mock_duckdb):
    # Setup
    mock_con = MagicMock()
    mock_duckdb.return_value = mock_con
    
    # Mock row count result
    mock_con.execute.return_value.fetchone.return_value = [100]
    
    service = TransformationService(bucket_name="test-bucket")
    
    # Execute
    service.process_raw_data()
    
    # Verify connection and setup
    mock_duckdb.assert_called_once()
    assert call("INSTALL httpfs; LOAD httpfs;") in mock_con.execute.call_args_list
    assert call("INSTALL json; LOAD json;") in mock_con.execute.call_args_list
    
    # Verify query execution
    # We expect multiple executes: setup, main query, count check, copy, close (implicit or explicit)
    
    # Check if main query was executed
    # We can check if any call argument contains "CREATE OR REPLACE TABLE refined_data"
    found_main_query = False
    for call_args in mock_con.execute.call_args_list:
        query_arg = call_args[0][0]
        if "CREATE OR REPLACE TABLE new_refined_data" in query_arg:
            found_main_query = True
            assert "s3://test-bucket/raw/**/*.parquet" in query_arg
            break
    assert found_main_query, "Main transformation query was not executed"

    # Check if COPY was executed
    found_copy = False
    for call_args in mock_con.execute.call_args_list:
        query_arg = call_args[0][0]
        if "COPY new_refined_data TO" in query_arg:
            found_copy = True
            assert "s3://test-bucket/refined/traffic_data" in query_arg # output path includes timestamp now
            break
    assert found_copy, "COPY command was not executed"

def test_process_raw_data_no_data(mock_duckdb):
    # Setup
    mock_con = MagicMock()
    mock_duckdb.return_value = mock_con
    
    # Mock row count result = 0
    mock_con.execute.return_value.fetchone.return_value = [0]
    
    service = TransformationService(bucket_name="test-bucket")
    
    # Execute
    service.process_raw_data()
    
    # Verify COPY was NOT executed
    for call_args in mock_con.execute.call_args_list:
        query_arg = call_args[0][0]
        assert "COPY refined_data TO" not in query_arg
