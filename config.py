import os
from typing import Dict, Any

"""
DataForge Configuration Module.

This module centralizes config management by reading environment variables
with safe defaults for local development and Docker deployment. It configures
MinIO/S3 endpoints, PostgreSQL databases, and public API data sources.
"""

# --- MinIO / S3 Storage Configuration ---
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_RAW_BUCKET: str = os.getenv("MINIO_RAW_BUCKET", "dataforge-raw")
MINIO_CLEAN_BUCKET: str = os.getenv("MINIO_CLEAN_BUCKET", "dataforge-clean")

# --- Postgres Database Configuration ---
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "dataforge")

# --- Public API Configuration ---
# Defaults to a free public Open-Meteo weather API call for New York City (mockable/testable)
API_URL: str = os.getenv(
    "API_URL",
    "https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060&current=temperature_2m,relative_humidity_2m,wind_speed_10m&timezone=America/New_York"
)

def get_postgres_uri() -> str:
    """
    Generates the SQLAlchemy-compatible database URI from PostgreSQL settings.
    
    Returns:
        str: Format connection URI.
    """
    return f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
