"""
DataForge Pipeline Package.

This package contains modules for executing the end-to-end ELT pipeline stages:
- extract: Fetches raw meteorological/geocoding data and uploads it to S3-compatible raw storage.
- transform: Pulls raw JSON, cleanses types, deduplicates, validates schema, and writes clean Parquet.
- load: Reads cleansed Parquet and executes a bulk upsert into PostgreSQL.
- utils: Provides centralized shared services such as MinIO clients, DB engines, alerts, and logging.
"""

__version__ = "1.0.0"
__author__ = "DataForge Team"
