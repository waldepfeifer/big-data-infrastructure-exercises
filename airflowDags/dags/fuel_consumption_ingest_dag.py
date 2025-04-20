# Standard library imports
import json
import logging
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# Third-party imports
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configuration
SOURCE_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
PARQUET_COMPRESSION = 'SNAPPY'
S3_BUCKET = Variable.get("S3_BUCKET", default_var="bdi-aircraft-waldepfeifer")

# Database settings
MIN_CONNECTIONS, MAX_CONNECTIONS = 1, 5
connection_pool = None

def create_session():
    """Create a session with retry logic."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_db_config():
    """Get database configuration."""
    config = {
        'host': os.getenv('BDI_DB_HOST'),
        'port': os.getenv('BDI_DB_PORT'),
        'user': os.getenv('BDI_DB_USERNAME'),
        'password': os.getenv('BDI_DB_PASSWORD'),
        'database': os.getenv('BDI_DB_NAME'),
        'sslmode': 'require',
        'connect_timeout': 10
    }
    if None in config.values():
        raise ValueError("Missing database configuration")
    return config

def initialize_connection_pool():
    """Initialize the connection pool."""
    global connection_pool
    try:
        logger = logging.getLogger("airflow.task")
        db_config = get_db_config()
        safe_config = {k: '***' if k == 'password' else v for k, v in db_config.items()}
        logger.info(f"Creating connection pool with config: {safe_config}")

        connection_pool = pool.ThreadedConnectionPool(MIN_CONNECTIONS, MAX_CONNECTIONS, **db_config)
        logger.info("Connection pool created successfully")
    except Exception as e:
        logger.error(f"Failed to establish connection pool: {e}")
        raise

@contextmanager
def get_db_cursor():
    """Get a database cursor."""
    connection = None
    try:
        if connection_pool is None:
            raise ValueError("Connection pool is not initialized")
        connection = connection_pool.getconn()
        connection.autocommit = True
        cursor = connection.cursor()
        yield cursor
    except Exception as e:
        logger = logging.getLogger("airflow.task")
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection_pool.putconn(connection)

@dag(
    schedule="@once",
    start_date=datetime(2023, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aircraft", "ingest", "fuel"],
)
def fuel_consumption_ingest():
    @task(retries=3, retry_delay=60)
    def download_to_bronze():
        """Download and store in bronze layer."""
        logger = logging.getLogger("airflow.task")
        logger.info("Starting download to bronze layer")

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                output_file = Path(temp_dir) / "aircraft_fuel_data.json"
                session = create_session()
                response = session.get(SOURCE_URL)
                response.raise_for_status()

                with open(output_file, 'wb') as f:
                    f.write(response.content)

                s3_client = boto3.client("s3")
                s3_client.upload_file(
                    output_file,
                    S3_BUCKET,
                    "bronze/fuel_consumption/aircraft_fuel_data.json",
                    ExtraArgs={'ContentType': 'application/json'}
                )
                logger.info("Successfully downloaded and stored in bronze layer")
                return True
            except Exception as e:
                logger.error(f"Failed to download aircraft database: {e}")
                return False

    @task(retries=3, retry_delay=60)
    def transform_to_silver():
        """Transform to silver layer."""
        logger = logging.getLogger("airflow.task")
        logger.info("Starting transformation to silver layer")

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                bronze_file = Path(temp_dir) / "aircraft_fuel_data.json"
                parquet_file = Path(temp_dir) / "aircraft_fuel_data.parquet"

                s3_client = boto3.client("s3")
                s3_client.download_file(
                    S3_BUCKET,
                    "bronze/fuel_consumption/aircraft_fuel_data.json",
                    bronze_file
                )

                with open(bronze_file, encoding='utf-8') as f:
                    data = json.load(f)

                records = [
                    {
                        'aircraft_type': aircraft_type,
                        'name': details.get('name', ''),
                        'galph': details.get('galph', 0.0),
                        'category': details.get('category', ''),
                        'source': details.get('source', '')
                    }
                    for aircraft_type, details in data.items()
                ]

                df = pd.DataFrame(records)
                df['galph'] = pd.to_numeric(df['galph'], errors='coerce')

                schema = pa.schema([
                    ('aircraft_type', pa.string()),
                    ('name', pa.string()),
                    ('galph', pa.float64()),
                    ('category', pa.string()),
                    ('source', pa.string())
                ])

                table = pa.Table.from_pandas(df, schema=schema)
                pq.write_table(table, parquet_file, compression=PARQUET_COMPRESSION, version='2.6')

                s3_client.upload_file(
                    parquet_file,
                    S3_BUCKET,
                    "silver/fuel_consumption/aircraft_fuel_data.parquet",
                    ExtraArgs={'ContentType': 'application/parquet'}
                )
                logger.info("Successfully transformed and stored in silver layer")
                return True
            except Exception as e:
                logger.error(f"Failed to transform aircraft database: {e}")
                return False

    @task(retries=3, retry_delay=60)
    def upload_to_postgres():
        """Upload to PostgreSQL."""
        logger = logging.getLogger("airflow.task")
        logger.info("Starting upload to PostgreSQL")

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                parquet_file = Path(temp_dir) / "aircraft_fuel_data.parquet"

                s3_client = boto3.client("s3")
                s3_client.download_file(
                    S3_BUCKET,
                    "silver/fuel_consumption/aircraft_fuel_data.parquet",
                    parquet_file
                )

                df = pd.read_parquet(parquet_file)
                initialize_connection_pool()

                with get_db_cursor() as cur:
                    cur.execute("""
                        DROP TABLE IF EXISTS fuel_consumption_table;
                        CREATE TABLE fuel_consumption_table (
                            aircraft_type VARCHAR(10) PRIMARY KEY,
                            name VARCHAR(255),
                            galph FLOAT,
                            category VARCHAR(100),
                            source VARCHAR(255),
                            _loaded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            _file_location TEXT,
                            _systems_passed TEXT[]
                        );
                        CREATE INDEX idx_fuel_consumption_table_category ON fuel_consumption_table(category);
                    """)

                    data = [
                        (
                            str(row['aircraft_type']),
                            str(row['name']),
                            float(row['galph']),
                            str(row['category']),
                            str(row['source']),
                            "silver/fuel_consumption/aircraft_fuel_data.parquet",
                            ['bronze', 'silver', 'postgres']
                        )
                        for _, row in df.iterrows()
                    ]

                    if data:
                        execute_values(
                            cur,
                            """INSERT INTO fuel_consumption_table
                            (aircraft_type, name, galph, category, source,
                            _file_location, _systems_passed) VALUES %s""",
                            data
                        )
                        logger.info(f"Successfully inserted {len(data)} records into PostgreSQL")
                    else:
                        logger.warning("No valid data to insert into PostgreSQL")

                logger.info("Successfully uploaded data to PostgreSQL")
                return True
            except Exception as e:
                logger.error(f"Failed to upload data to PostgreSQL: {e}")
                return False
            finally:
                if connection_pool is not None:
                    connection_pool.closeall()

    # Set task dependencies
    download_to_bronze() >> transform_to_silver() >> upload_to_postgres()

fuel_consumption_ingest = fuel_consumption_ingest()

