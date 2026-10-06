import logging
import sys
from typing import Any, Dict
import boto3
from botocore.client import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import config

# --- Logging Configuration ---
def get_logger(name: str) -> logging.Logger:
    """
    Creates and configures a standardized logger for audit logging across pipeline stages.
    
    Args:
        name (str): Name of the logger (typically __name__).
        
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s]: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console Handler outputting to stdout for standard container log collection
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger

logger = get_logger("pipeline.utils")

# --- S3/MinIO Client Utility ---
def get_s3_client() -> Any:
    """
    Establishes and returns a boto3 S3 client configured for local MinIO or AWS S3.
    
    Returns:
        botocore.client.S3: Initialized boto3 client connection.
    """
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=config.MINIO_ENDPOINT,
            aws_access_key_id=config.MINIO_ACCESS_KEY,
            aws_secret_access_key=config.MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

# --- Database Engine Utility ---
def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database connection engine.
    
    Returns:
        Engine: SQLAlchemy Engine connection instance with standard pooling strategies.
    """
    try:
        engine = create_engine(
            config.get_postgres_uri(),
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20
        )
        return engine
    except Exception as e:
        logger.error(f"Failed to create SQLAlchemy engine: {e}")
        raise

# --- Alerting & Notification Utilities ---
def notify_failure(subject: str, message: str) -> None:
    """
    Dispatches failure notifications to external channels.
    Logs standard CRITICAL warnings, serving as an extensible hook for
    Slack notifications, PagerDuty, or SMTP emails.
    
    Args:
        subject (str): Brief summary describing the failure event.
        message (str): Explanatory traceback or contextual data of the exception.
    """
    alert_msg = f"\n=== FAILURE ALERT DISPATCHED ===\nSubject: {subject}\nMessage: {message}\n================================="
    logger.critical(alert_msg)

def airflow_on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow task failure callback context parser. Extracts structured job execution
    metadata and channels alerting payloads safely.
    
    Args:
        context (Dict[str, Any]): Airflow execution context dictionary.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id if task_instance else 'Unknown DAG'
    task_id = task_instance.task_id if task_instance else 'Unknown Task'
    execution_date = context.get('execution_date', 'Unknown Time')
    exception = context.get('exception', 'No exception traces caught.')
    
    subject = f"DataForge DAG Failure: {dag_id}.{task_id}"
    message = (
        f"DAG ID: {dag_id}\n"
        f"Task ID: {task_id}\n"
        f"Execution Timestamp: {execution_date}\n"
        f"Error Stack Trace: {exception}"
    )
    notify_failure(subject, message)
