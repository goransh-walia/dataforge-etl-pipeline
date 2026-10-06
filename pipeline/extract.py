import json
import urllib.request
import datetime
import uuid
from typing import Optional, Dict, Any

import config
from pipeline.utils import get_logger, get_s3_client

logger = get_logger("pipeline.extract")

def fetch_raw_api_data(url: str) -> Dict[str, Any]:
    """
    Fetches raw meteorological data from the configured public API.
    
    Args:
        url (str): The destination URL to hit.
        
    Returns:
        Dict[str, Any]: Decoded raw JSON dictionary.
    """
    logger.info(f"Initiating raw API fetch from endpoint: {url}")
    try:
        # Using standard library urllib to minimize external runtime dependencies
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'DataForge-ETL-Pipeline/1.0'}
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError(f"API HTTP request failed with status: {response.status}")
            data_bytes = response.read()
            raw_json = json.loads(data_bytes.decode('utf-8'))
            logger.info("API request completed successfully. Byte content retrieved.")
            return raw_json
    except Exception as e:
        logger.error(f"Failed to pull data from public weather API: {e}")
        raise

def run_extract(execution_date_str: Optional[str] = None) -> str:
    """
    Executes the extraction sequence: fetches raw metrics, appends operational audit
    metadata keys, and posts the payload to the MinIO/S3 raw landing area.
    
    Args:
        execution_date_str (Optional[str]): Operational execution date provided by Airflow runner.
        
    Returns:
        str: S3 storage location key for downstream extraction tracking.
    """
    logger.info("Executing extract phase...")
    
    # Establish consistent reference timing
    if execution_date_str:
        try:
            # Clean possible trailing 'Z' or timezone offsets for python ISO parser compatibility
            clean_date = execution_date_str.split('+')[0].rstrip('Z')
            execution_time = datetime.datetime.fromisoformat(clean_date)
        except Exception as e:
            logger.warning(f"Could not parse provided execution date '{execution_date_str}' due to: {e}. Fallback to UTC clock.")
            execution_time = datetime.datetime.utcnow()
    else:
        execution_time = datetime.datetime.utcnow()

    execution_id = str(uuid.uuid4())
    
    # Extract
    raw_response = fetch_raw_api_data(config.API_URL)
    
    # Construct persistent operational wrapper enclosing the raw payload
    audit_payload = {
        "audit": {
            "execution_id": execution_id,
            "extracted_at": datetime.datetime.utcnow().isoformat(),
            "target_execution_date": execution_time.isoformat(),
            "source_endpoint": config.API_URL,
            "pipeline_schema_version": "1.0"
        },
        "results": raw_response
    }
    
    serialized_payload = json.dumps(audit_payload, indent=2).encode('utf-8')
    
    # Construct deterministic object-path partition key
    year = execution_time.strftime("%Y")
    month = execution_time.strftime("%m")
    day = execution_time.strftime("%d")
    unique_suffix = execution_id[:8]
    
    s3_key = f"weather/year={year}/month={month}/day={day}/raw_weather_{execution_time.strftime('%Y%m%dT%H%M%S')}_{unique_suffix}.json"
    
    # Upload Object to S3 Target
    s3_client = get_s3_client()
    logger.info(f"Targeting bucket '{config.MINIO_RAW_BUCKET}' at destination key '{s3_key}'")
    
    try:
        s3_client.put_object(
            Bucket=config.MINIO_RAW_BUCKET,
            Key=s3_key,
            Body=serialized_payload,
            ContentType="application/json"
        )
        logger.info(f"Successfully serialized and uploaded payload metadata. S3 key: {s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Critical error during upload to object store: {e}")
        raise

if __name__ == "__main__":
    # Local CLI execution harness
    try:
        saved_key = run_extract()
        print(f"Extraction successful. Object saved to: {saved_key}")
    except Exception as exc:
        print(f"Failed execution harness run: {exc}")
