import datetime
import io
from typing import Dict, Any
import pandas as pd
from sqlalchemy import MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert

import config
from pipeline.utils import get_logger, get_s3_client, get_db_engine

logger = get_logger("pipeline.load")

# Define target warehouse schemas using SQLAlchemy MetaData
metadata = MetaData()

weather_table = Table(
    "weather_data",
    metadata,
    Column("execution_id", String(50), nullable=False),
    Column("latitude", Float, primary_key=True),
    Column("longitude", Float, primary_key=True),
    Column("elevation", Float, nullable=True),
    Column("timezone", String(50), nullable=False),
    Column("current_time", DateTime, primary_key=True),
    Column("temperature_2m", Float, nullable=False),
    Column("relative_humidity_2m", Float, nullable=False),
    Column("wind_speed_10m", Float, nullable=False),
    Column("extracted_at", DateTime, nullable=False)
)

audit_table = Table(
    "load_audit_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("execution_id", String(50), nullable=False),
    Column("s3_key", String(255), nullable=False),
    Column("records_loaded", Integer, nullable=False),
    Column("loaded_at", DateTime, default=datetime.datetime.utcnow)
)


def init_db_schema() -> None:
    """
    Ensures that both the target dimensions/facts schema and operational audit tables
    exist in the destination PostgreSQL database before execution.
    """
    logger.info("Initializing database schema and ensuring tables exist...")
    engine = get_db_engine()
    try:
        metadata.create_all(engine)
        logger.info("Database schema initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        raise


def run_load(clean_s3_key: str) -> Dict[str, Any]:
    """
    Reads cleaned Parquet records from MinIO, performs a transactional bulk upsert (merge)
    into the PostgreSQL target warehouse schema, and records pipeline metrics to audit tables.

    Args:
        clean_s3_key (str): S3 storage destination key containing clean Parquet dataset.

    Returns:
        Dict[str, Any]: Execution metrics and load completion data.
    """
    logger.info(f"Initializing PostgreSQL load phase for clean target: '{clean_s3_key}'")
    
    # Initialize target schema and engine
    init_db_schema()
    engine = get_db_engine()
    s3_client = get_s3_client()

    # Retrieve clean Parquet dataset
    try:
        logger.info(f"Downloading cleaned Parquet payload from bucket '{config.MINIO_CLEAN_BUCKET}'")
        response = s3_client.get_object(Bucket=config.MINIO_CLEAN_BUCKET, Key=clean_s3_key)
        parquet_bytes = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        logger.info(f"Successfully retrieved Parquet payload. Total row count: {len(df)}")
    except Exception as e:
        logger.error(f"Failed to read Parquet data from clean storage bucket: {e}")
        raise

    if df.empty:
        logger.warning("Retrieved DataFrame contains zero records. Skipping PostgreSQL load operation.")
        return {
            "status": "skipped",
            "records_loaded": 0,
            "execution_id": "none"
        }

    # Ensure timestamp representations are correctly formatted as timezone-naive datetime objects
    df["current_time"] = pd.to_datetime(df["current_time"]).dt.tz_localize(None)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.tz_localize(None)

    records = df.to_dict(orient="records")
    execution_id = str(records[0].get("execution_id", "unknown-execution"))
    records_loaded = 0

    # Perform Transactional Upsert and Audit entry mapping
    try:
        with engine.begin() as conn:
            logger.info("Starting database upsert operation...")
            for record in records:
                insert_stmt = pg_insert(weather_table).values(record)
                
                # Setup conflict strategy targeting composite primary keys (current_time, latitude, longitude)
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=["current_time", "latitude", "longitude"],
                    set_={
                        "execution_id": insert_stmt.excluded.execution_id,
                        "elevation": insert_stmt.excluded.elevation,
                        "timezone": insert_stmt.excluded.timezone,
                        "temperature_2m": insert_stmt.excluded.temperature_2m,
                        "relative_humidity_2m": insert_stmt.excluded.relative_humidity_2m,
                        "wind_speed_10m": insert_stmt.excluded.wind_speed_10m,
                        "extracted_at": insert_stmt.excluded.extracted_at,
                    }
                )
                conn.execute(upsert_stmt)
                records_loaded += 1

            # Append metrics record to the operational audit log
            audit_record = {
                "execution_id": execution_id,
                "s3_key": clean_s3_key,
                "records_loaded": records_loaded,
                "loaded_at": datetime.datetime.utcnow()
            }
            conn.execute(audit_table.insert().values(audit_record))
            
        logger.info(f"Load phase complete. Transaction committed. Total records upserted: {records_loaded}")
        return {
            "status": "success",
            "records_loaded": records_loaded,
            "execution_id": execution_id
        }
    except Exception as e:
        logger.error(f"Failed to execute target transaction block during database upsert: {e}")
        raise


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.load <clean_s3_key>")
        sys.exit(1)

    target_key = sys.argv[1]
    try:
        metrics = run_load(target_key)
        print(f"Harness execution completed. Metrics: {metrics}")
    except Exception as exc:
        print(f"Harness execution failed: {exc}")
        sys.exit(1)
