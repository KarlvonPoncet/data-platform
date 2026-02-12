import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import httpx
from wiener_linien_api import WienerLinienAPI

# Fixture for the API instance
@pytest.fixture
def api():
    return WienerLinienAPI(timeout=1.0, max_retries=1, base_delay=0.1)

# Test validate_response
def test_validate_response_success(api):
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {
        "data": {
            "monitors": []
        },
        "message": {}
    }
    
    result = api.validate_response(mock_response)
    assert result == mock_response.json.return_value

def test_validate_response_http_error(api):
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError("Error", request=None, response=mock_response)
    
    with pytest.raises(httpx.HTTPStatusError):
        api.validate_response(mock_response)

def test_validate_response_invalid_structure(api):
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {"error": "Invalid structure"} # Missing 'data'
    
    with pytest.raises(ValueError, match="Response missing 'data' field"):
        api.validate_response(mock_response)

# Test fetch_batch (Async)
@pytest.mark.asyncio
async def test_fetch_batch_success(api):
    expected_data = {"data": {"monitors": []}}
    
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = expected_data
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        
        result = await api.fetch_batch(["123"])
        assert result == expected_data
        mock_get.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_batch_retry_success(api):
    # First call fails, second succeeds
    expected_data = {"data": {"monitors": []}}
    
    success_response = MagicMock(spec=httpx.Response)
    success_response.status_code = 200
    success_response.headers = {"content-type": "application/json"}
    success_response.json.return_value = expected_data
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [
            httpx.RequestError("Network error"), # Fail 1
            success_response                     # Success 2
        ]
        
        result = await api.fetch_batch(["123"])
        assert result == expected_data
        assert mock_get.call_count == 2

@pytest.mark.asyncio
async def test_fetch_batch_all_retries_fail(api):
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = httpx.RequestError("Persistent error")
        
        result = await api.fetch_batch(["123"])
        assert result is None
        assert mock_get.call_count == api.max_retries + 1

# Test transform_to_events
def test_transform_to_events_success(api):
    raw_data = {
        "data": {
            "monitors": [
                {
                    "locationStop": {
                        "properties": {
                            "title": "Stephansplatz",
                            "attributes": {"rbl": 1234}
                        }
                    },
                    "lines": [
                        {
                            "name": "U1",
                            "towards": "Leopoldau",
                            "departures": {
                                "departure": [
                                    {
                                        "departureTime": {
                                            "timePlanned": "2026-02-10T14:00:00Z",
                                            "timeReal": "2026-02-10T14:00:05Z",
                                            "countdown": 2
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }
    
    events = api.transform_to_events(raw_data)
    assert len(events) == 1
    event = events[0]
    assert event["station"] == "Stephansplatz"
    assert event["rbl"] == 1234
    assert event["linie"] == "U1"
    assert event["countdown"] == 2

def test_transform_to_events_empty(api):
    assert api.transform_to_events({}) == []
    assert api.transform_to_events({"data": {"monitors": []}}) == []
