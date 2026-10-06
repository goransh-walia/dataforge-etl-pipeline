import os
import json
from typing import Generator, Dict, Any, Tuple
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

# Ensure safe default environment variables before loading any configurations
os.environ["MINIO_ENDPOINT"] = "http://mock-minio:9000"
os.environ["MINIO_ACCESS_KEY"] = "mock-access"
os.environ["MINIO_SECRET_KEY"] = "mock-secret"
os.environ["POSTGRES_HOST"] = "mock-postgres"
os.environ["POSTGRES_USER"] = "mock-user"
os.environ["POSTGRES_PASSWORD"] = "mock-password"
os.environ["POSTGRES_DB"] = "mock-db"


class MockS3Client:
    """
    In-memory Mock S3 Client simulating key boto3 S3 actions.
    Ensures tests are run without local MinIO or AWS S3 network requirements.
    """

    def __init__(self) -> None:
        # Dictionary structure: { bucket_name: { s3_key: binary_payload } }
        self.buckets: Dict[str, Dict[str, bytes]] = {}

    def put_object(self, Bucket: str, Key: str, Body: Any, ContentType: str = "") -> None:
        """Simulates boto3 put_object."""
        if Bucket not in self.buckets:
            self.buckets[Bucket] = {}
        
        # Resolve body payload
        if hasattr(Body, "read"):
            data = Body.read()
        elif isinstance(Body, str):
            data = Body.encode("utf-8")
        else:
            data = Body
            
        self.buckets[Bucket][Key] = data

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        """Simulates boto3 get_object."""
        if Bucket not in self.buckets or Key not in self.buckets[Bucket]:
            from botocore.exceptions import ClientError
            raise ClientError(
                error_response={
                    "Error": {
                        "Code": "NoSuchKey",
                        "Message": f"The specified key '{Key}' does not exist in bucket '{Bucket}'."
                    }
                },
                operation_name="GetObject"
            )
        
        payload = self.buckets[Bucket][Key]

        class MockStreamingBody:
            def __init__(self, data: bytes) -> None:
                self._data = data
            def read(self) -> bytes:
                return self._data

        return {"Body": MockStreamingBody(payload)}


@pytest.fixture
def mock_s3() -> Generator[MockS3Client, None, None]:
    """
    Provides a MockS3Client and patches the utility S3 connection logic.
    """
    client = MockS3Client()
    with patch("pipeline.utils.get_s3_client", return_value=client), \
         patch("pipeline.extract.get_s3_client", return_value=client), \
         patch("pipeline.transform.get_s3_client", return_value=client), \
         patch("pipeline.load.get_s3_client", return_value=client):
        yield client


@pytest.fixture
def mock_db_engine() -> Generator[Tuple[MagicMock, MagicMock], None, None]:
    """
    Mocks the SQLAlchemy database Engine and connection context to intercept
    ACID transaction executions and PG-specific raw inserts.
    """
    mock_engine = MagicMock()
    mock_conn = MagicMock()

    class MockBegin:
        def __enter__(self) -> MagicMock:
            return mock_conn
        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

    mock_engine.begin.return_value = MockBegin()

    with patch("pipeline.utils.get_db_engine", return_value=mock_engine), \
         patch("pipeline.load.get_db_engine", return_value=mock_engine):
        yield mock_engine, mock_conn


@pytest.fixture
def sample_api_response() -> Dict[str, Any]:
    """
    Provides standard raw mock response matching the Open-Meteo current endpoint structure.
    """
    return {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "generationtime_ms": 0.12,
        "utc_offset_seconds": -18000,
        "timezone": "America/New_York",
        "timezone_abbreviation": "EDT",
        "elevation": 10.0,
        "current_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "wind_speed_10m": "km/h"
        },
        "current": {
            "time": "2023-10-25T12:00",
            "temperature_2m": 18.5,
            "relative_humidity_2m": 60.0,
            "wind_speed_10m": 12.5
        }
    }


@pytest.fixture
def mock_api_http(sample_api_response: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    """
    Intercepts urllib web request calls to return deterministic mock weather records.
    """
    class MockHTTPResponse:
        def __init__(self, data: bytes) -> None:
            self._data = data
            self.status = 200
        def read(self) -> bytes:
            return self._data
        def __enter__(self) -> "MockHTTPResponse":
            return self
        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

    encoded_data = json.dumps(sample_api_response).encode("utf-8")
    with patch("urllib.request.urlopen", return_value=MockHTTPResponse(encoded_data)) as mock_urlopen:
        yield sample_api_response


@pytest.fixture
def sample_raw_payload() -> Dict[str, Any]:
    """
    Provides raw serialized data complete with operational audit metadata wrapper.
    """
    return {
        "audit": {
            "execution_id": "test-uuid-4444-8888",
            "extracted_at": "2023-10-25T12:05:00.000000",
            "target_execution_date": "2023-10-25T12:00:00.000000",
            "source_endpoint": "https://api.mocked-endpoint.com",
            "pipeline_schema_version": "1.0"
        },
        "results": {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "elevation": 10.0,
            "timezone": "America/New_York",
            "current": {
                "time": "2023-10-25T12:00",
                "temperature_2m": 18.5,
                "relative_humidity_2m": 60.0,
                "wind_speed_10m": 12.5
            }
        }
    }


@pytest.fixture
def sample_temporal_df() -> pd.DataFrame:
    """
    Returns a pandas DataFrame representation of cleaned, temporal weather data
    for direct integration tests.
    """
    return pd.DataFrame([{
        "execution_id": "test-uuid-4444-8888",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "elevation": 10.0,
        "timezone": "America/New_York",
        "current_time": "2023-10-25 12:00:00",
        "temperature_2m": 18.5,
        "relative_humidity_2m": 60.0,
        "wind_speed_10m": 12.5,
        "extracted_at": "2023-10-25 12:05:00"
    }])
