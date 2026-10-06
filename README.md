# DataForge: End-to-End Data Engineering Pipeline

DataForge is a robust, production-ready ETL pipeline designed to ingest, process, and load meteorological data. It features automated schema validation, deduplication, transactional database loading, and integrated alerting using Apache Airflow, MinIO, and PostgreSQL.

## Architecture Overview

The pipeline follows a modular extract-transform-load pattern:

```text
[Public API] 
     |
     v
[Extract] ----> [MinIO Raw Bucket (JSON)]
     |
[Transform] --> [MinIO Clean Bucket (Parquet)]
     |
[Load] -------> [PostgreSQL (Warehouse)]
     |
[Audit Log] <-- (Monitored by Airflow/Alerting)
```

1. **Extract**: Fetches raw data from the API and saves it to a "raw" bucket with audit metadata.
2. **Transform**: Performs schema validation (Pydantic), type conversion, and deduplication (Pandas), writing parquet files to a "clean" bucket.
3. **Load**: Performs ACID-compliant upserts into PostgreSQL, ensuring data consistency via primary key constraints on coordinates and time.

## Local Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.10+

### Deployment
1. Initialize the environment:
   ```bash
   docker-compose up -d
   ```
2. Access the Airflow UI at `http://localhost:8080` (admin/admin).
3. The pipeline will automatically create MinIO buckets and initialize the PostgreSQL database schema upon the first task run.

## Project Structure

- `dags/`: Airflow DAG definitions and scheduling logic.
- `pipeline/`: Core ETL logic (extract, transform, load, utils).
- `tests/`: Unit and integration tests for data processing logic.
- `config.py`: Centralized configuration management.
- `docker-compose.yml`: Local container orchestration.

## Data Validation & Resilience

- **Schema Validation**: Uses Pydantic to ensure incoming data strictly adheres to numerical boundaries (e.g., humidity 0-100%, non-negative wind speed).
- **Deduplication**: Pandas-based logic handles record uniqueness before warehouse ingestion.
- **Transactional Integrity**: SQLAlchemy with PostgreSQL `ON CONFLICT DO UPDATE` ensures atomic upserts.
- **Resilience**: Airflow tasks are configured with exponential backoff retries and SLA monitoring.
- **Alerting**: The `airflow_on_failure_callback` hook catches pipeline failures, logging critical errors for observability.

## Testing

To verify the pipeline logic without hitting external APIs:

1. Install dependencies: `pip install -r requirements.txt`
2. Run the test suite:
   ```bash
   pytest tests/
   ```
The test suite utilizes custom fixtures (`conftest.py`) to mock S3 storage and database engines, ensuring unit tests remain deterministic and isolated.

## Execution Flow

Each DAG run corresponds to an hourly schedule. The pipeline passes S3 keys between tasks using Airflow XComs, ensuring that downstream processes only operate on data validated by the upstream stage. All stages log process metrics into a dedicated `load_audit_metrics` table for operational visibility.
