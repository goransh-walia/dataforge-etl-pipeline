import io
import json
import datetime
from typing import Dict, Any, Tuple
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from pydantic import ValidationError

import config
from pipeline.extract import fetch_raw_api_data, run_extract
from pipeline.transform import run_transform, WeatherSchema
from pipeline.load import run_load, init_db_schema

def test_fetch_raw_api_data_success(mock_api_http: Dict[str, Any]) -> None:
    """
    Test that fetch_raw_api_data makes a web request and retrieves raw json correctly.
    """
    result = fetch_raw_api_data("http://mock-url")
    assert result == mock_api_http
    assert result["latitude"] == 40.7128
    assert result["timezone"] == "America/New_York"
    assert "current" in result

def test_run_extract_success(mock_s3: Any, mock_api_http: Dict[str, Any]) -> None:
    """
    Test that run_extract orchestrates pulling raw data, wraps it with operational
    audit metadata, and pushes it to the mock MinIO raw bucket.
    """
    s3_key = run_extract(execution_date_str="2023-10-25T12:00:00Z")
    
    # Check partition pattern logic
    assert s3_key.startswith("weather/year=2023/month=10/day=25/raw_weather_20231025T120000_")
    assert s3_key.endswith(".json")
    
    # Retrieve mock payload and assert schema compliance
    obj = mock_s3.get_object(config.MINIO_RAW_BUCKET, s3_key)
    payload = json.loads(obj["Body"].read().decode("utf-8"))
    
    assert "audit" in payload
    assert "results" in payload
    assert payload["results"]["latitude"] == 40.7128
    assert payload["audit"]["pipeline_schema_version"] == "1.0"
    assert "execution_id" in payload["audit"]

def test_weather_schema_boundary_constraints() -> None:
    """
    Unit test evaluating strict boundary constraints via Pydantic on WeatherSchema.
    """
    valid_data = {
        "execution_id": "test-id",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "elevation": 10.0,
        "timezone": "America/New_York",
        "current_time": "2023-10-25T12:00:00",
        "temperature_2m": 18.5,
        "relative_humidity_2m": 60.0,
        "wind_speed_10m": 12.5,
        "extracted_at": "2023-10-25T12:05:00"
    }
    
    # Validate successful schema verification
    record = WeatherSchema(**valid_data)
    assert record.relative_humidity_2m == 60.0

    # Underflow check: relative humidity < 0%
    invalid_humidity_low = valid_data.copy()
    invalid_humidity_low["relative_humidity_2m"] = -5.0
    with pytest.raises(ValidationError):
        WeatherSchema(**invalid_humidity_low)

    # Overflow check: relative humidity > 100%
    invalid_humidity_high = valid_data.copy()
    invalid_humidity_high["relative_humidity_2m"] = 100.1
    with pytest.raises(ValidationError):
        WeatherSchema(**invalid_humidity_high)

    # Negative check: wind speed < 0
    invalid_wind = valid_data.copy()
    invalid_wind["wind_speed_10m"] = -0.5
    with pytest.raises(ValidationError):
        WeatherSchema(**invalid_wind)

def test_run_transform_success(mock_s3: Any, sample_raw_payload: Dict[str, Any]) -> None:
    """
    Verifies the transformation sequence: reads from raw, transforms schema,
    validates Pydantic types, and uploads clean Parquet data to storage.
    """
    raw_key = "weather/year=2023/month=10/day=25/raw_weather_test.json"
    mock_s3.put_object(
        Bucket=config.MINIO_RAW_BUCKET,
        Key=raw_key,
        Body=json.dumps(sample_raw_payload)
    )

    clean_key = run_transform(raw_key)
    assert "clean_weather/" in clean_key
    assert clean_key.endswith(".parquet")

    # Read clean parquet back from S3 memory to assert values
    obj = mock_s3.get_object(config.MINIO_CLEAN_BUCKET, clean_key)
    parquet_data = obj["Body"].read()
    df = pd.read_parquet(io.BytesIO(parquet_data))

    assert len(df) == 1
    assert df.loc[0, "temperature_2m"] == 18.5
    assert df.loc[0, "relative_humidity_2m"] == 60.0
    assert df.loc[0, "latitude"] == 40.7128

def test_run_transform_validation_failure(mock_s3: Any, sample_raw_payload: Dict[str, Any]) -> None:
    """
    Ensures transform run fails with an error if no records pass strict schemas.
    """
    # Break boundary rules (relative humidity 120%)
    sample_raw_payload["results"]["current"]["relative_humidity_2m"] = 120.0
    
    raw_key = "weather/year=2023/month=10/day=25/raw_weather_bad.json"
    mock_s3.put_object(
        Bucket=config.MINIO_RAW_BUCKET,
        Key=raw_key,
        Body=json.dumps(sample_raw_payload)
    )

    with pytest.raises(ValueError, match="Zero records successfully passed strict schema validation constraints"):
        run_transform(raw_key)

def test_pandas_deduplication(mock_s3: Any, sample_raw_payload: Dict[str, Any]) -> None:
    """
    Asserts Pandas drop_duplicates performs correctly on duplicate input streams.
    """
    raw_key = "weather/year=2023/month=10/day=25/raw_weather_dupe.json"
    mock_s3.put_object(
        Bucket=config.MINIO_RAW_BUCKET,
        Key=raw_key,
        Body=json.dumps(sample_raw_payload)
    )

    original_dataframe_init = pd.DataFrame

    # Subclass and inject mock payload to simulate multi-row duplicated extraction sets
    def mock_dataframe_duplicator(*args: Any, **kwargs: Any) -> pd.DataFrame:
        df = original_dataframe_init(*args, **kwargs)
        if len(df) == 1:
            # Replicate row records to assert deduplication
            df = pd.concat([df, df, df], ignore_index=True)
        return df

    with patch("pipeline.transform.pd.DataFrame", side_effect=mock_dataframe_duplicator):
        clean_key = run_transform(raw_key)

    obj = mock_s3.get_object(config.MINIO_CLEAN_BUCKET, clean_key)
    parquet_data = obj["Body"].read()
    df = pd.read_parquet(io.BytesIO(parquet_data))

    # Assert duplication logic cleaned down to singular unique row (subset ["current_time", "latitude", "longitude"])
    assert len(df) == 1

def test_run_load_success(mock_s3: Any, mock_db_engine: Tuple[MagicMock, MagicMock]) -> None:
    """
    Tests the PostgreSQL load pipeline: parses Parquet files from clean storage
    and executes transaction bulk upserts.
    """
    engine, conn = mock_db_engine

    clean_data = {
        "execution_id": ["test-uuid-4444-8888"],
        "latitude": [40.7128],
        "longitude": [-74.0060],
        "elevation": [10.0],
        "timezone": ["America/New_York"],
        "current_time": ["2023-10-25T12:00:00"],
        "temperature_2m": [18.5],
        "relative_humidity_2m": [60.0],
        "wind_speed_10m": [12.5],
        "extracted_at": ["2023-10-25T12:05:00"]
    }
    df = pd.DataFrame(clean_data)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    clean_key = "clean_weather/year=2023/month=10/day=25/clean_weather_test.parquet"
    mock_s3.put_object(
        Bucket=config.MINIO_CLEAN_BUCKET,
        Key=clean_key,
        Body=parquet_buffer.getvalue()
    )

    metrics = run_load(clean_key)

    assert metrics["status"] == "success"
    assert metrics["records_loaded"] == 1
    assert metrics["execution_id"] == "test-uuid-4444-8888"

    # Assert that upsert and audit logs were successfully invoked
    assert conn.execute.call_count >= 2

def test_run_load_empty_dataset(mock_s3: Any, mock_db_engine: Tuple[MagicMock, MagicMock]) -> None:
    """
    Verifies that run_load exits cleanly and skips transactions when Parquet data is empty.
    """
    engine, conn = mock_db_engine
    df = pd.DataFrame(columns=[
        "execution_id", "latitude", "longitude", "elevation", "timezone",
        "current_time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "extracted_at"
    ])
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    clean_key = "clean_weather/year=2023/month=10/day=25/clean_weather_empty.parquet"
    mock_s3.put_object(
        Bucket=config.MINIO_CLEAN_BUCKET,
        Key=clean_key,
        Body=parquet_buffer.getvalue()
    )

    metrics = run_load(clean_key)
    assert metrics["status"] == "skipped"
    assert metrics["records_loaded"] == 0
    assert conn.execute.call_count == 0
