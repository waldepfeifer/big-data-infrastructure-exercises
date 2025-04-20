# Standard library imports
import concurrent.futures
import gzip
import hashlib
import json
import logging
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

# Third-party imports
import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from dotenv import load_dotenv
from psycopg2 import pool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configuration
SOURCE_URL = "https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
PARQUET_COMPRESSION = 'SNAPPY'
S3_BUCKET = Variable.get("S3_BUCKET", default_var="bdi-aircraft-waldepfeifer")
CHUNK_SIZE = 10000  # Number of records to process in each chunk

# Database settings
MIN_CONNECTIONS, MAX_CONNECTIONS = 5, 20
connection_pool = None

def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def get_latest_version(s3_client, bucket: str, prefix: str) -> str:
    """Get the latest version number from S3 for a specific date."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/'
        )
        # Extract version numbers from filenames like "1_aircraft_db.json.gz"
        versions = []
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if '_aircraft_db' in filename:
                version_str = filename.split('_')[0]
                if version_str.isdigit():
                    versions.append(int(version_str))
        return str(max(versions) + 1) if versions else "1"
    except Exception as e:
        logging.error(f"Error getting latest version: {e}")
        return "1"

def create_session():
    """Create an optimized session with retry logic and connection pooling."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=['GET', 'HEAD']
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
        pool_block=False
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_db_config():
    """Get database configuration with optimized settings."""
    config = {
        'host': os.getenv('BDI_DB_HOST'),
        'port': os.getenv('BDI_DB_PORT'),
        'user': os.getenv('BDI_DB_USERNAME'),
        'password': os.getenv('BDI_DB_PASSWORD'),
        'database': os.getenv('BDI_DB_NAME'),
        'sslmode': 'require',
        'connect_timeout': 10,
        'application_name': 'aircraft_db_ingest',
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 5
    }
    if None in config.values():
        raise ValueError("Missing database configuration")
    return config

def initialize_connection_pool():
    """Initialize the connection pool with optimized settings."""
    global connection_pool
    try:
        logger = logging.getLogger("airflow.task")
        db_config = get_db_config()
        safe_config = {k: '***' if k == 'password' else v for k, v in db_config.items()}
        logger.info(f"Creating connection pool with config: {safe_config}")

        connection_pool = pool.ThreadedConnectionPool(
            MIN_CONNECTIONS,
            MAX_CONNECTIONS,
            **db_config
        )
        logger.info("Connection pool created successfully")
    except Exception as e:
        logger.error(f"Failed to establish connection pool: {e}")
        raise

@contextmanager
def get_db_cursor():
    """Get a database cursor with optimized settings."""
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

def process_chunk(chunk: pd.DataFrame) -> list:
    """Process a chunk of records for database insertion."""
    return [
        (
            str(row['icao']) if pd.notna(row['icao']) else '',
            str(row['reg']) if pd.notna(row['reg']) else '',
            str(row['icaotype']) if pd.notna(row['icaotype']) else '',
            int(row['year']) if pd.notna(row['year']) else None,
            str(row['manufacturer']).strip() if pd.notna(row['manufacturer']) else '',
            str(row['model']).strip() if pd.notna(row['model']) else '',
            str(row['owner']).strip() if pd.notna(row['owner']) else '',
            bool(row['faa_pia']) if pd.notna(row['faa_pia']) else False,
            bool(row['faa_ladd']) if pd.notna(row['faa_ladd']) else False,
            str(row['short_type']).strip() if pd.notna(row['short_type']) else '',
            bool(row['mil']) if pd.notna(row['mil']) else False
        )
        for _, row in chunk.iterrows()
    ]

def get_file_version(s3_client, bucket: str, prefix: str) -> str:
    """Get the next version number for a file in a specific date partition."""
    try:
        versions = []
        page_iterator = s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            PaginationConfig={'PageSize': 1000}
        )

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    filename = obj['Key'].split('/')[-1]
                    if filename.startswith('aircrafts_database'):
                        version = filename.split('_v')[1].split('.')[0] if '_v' in filename else '0'
                        versions.append(int(version))

        return str(max(versions) + 1) if versions else "1"
    except Exception as e:
        logging.error(f"Error getting file version: {e}")
        return "1"

def validate_file_integrity(s3_client, bucket: str, key: str, expected_hash: str) -> bool:
    """Validate file integrity after upload."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['Metadata'].get('file_hash') == expected_hash
    except Exception as e:
        logging.error(f"Error validating file integrity: {e}")
        return False

def safe_upload_file(s3_client, file_path: str, bucket: str, key: str, metadata: dict) -> bool:
    """Safely upload file with versioning and validation."""
    try:
        # Upload to temporary location first
        temp_key = f"{key}.temp"
        s3_client.upload_file(file_path, bucket, temp_key, ExtraArgs={'Metadata': metadata})

        # Validate upload
        if validate_file_integrity(s3_client, bucket, temp_key, metadata['file_hash']):
            # Move to final location
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': temp_key},
                Key=key
            )
            s3_client.delete_object(Bucket=bucket, Key=temp_key)
            return True
        return False
    except Exception as e:
        logging.error(f"Error in safe file upload: {e}")
        return False

def cleanup_old_versions(s3_client, bucket: str, prefix: str, keep_versions: int = 3):
    """Clean up old versions of files, keeping only the specified number of most recent versions."""
    try:
        files = []
        page_iterator = s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/',
            PaginationConfig={'PageSize': 1000}
        )

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    filename = obj['Key'].split('/')[-1]
                    if filename.startswith('aircrafts_database'):
                        version = filename.split('_v')[1].split('.')[0] if '_v' in filename else '0'
                        files.append((int(version), obj['Key']))

        # Sort by version and keep only the most recent ones
        files.sort(reverse=True)
        for _, key in files[keep_versions:]:
            s3_client.delete_object(Bucket=bucket, Key=key)
            logging.info(f"Deleted old version: {key}")
    except Exception as e:
        logging.error(f"Error cleaning up old versions: {e}")

def process_json_records(gz_file, logger):
    """Process JSON records from gzipped file."""
    records = []
    with gzip.GzipFile(gz_file, 'r') as gz_file:
        for line in gz_file:
            try:
                record = json.loads(line.decode('utf-8'))
                processed_record = {
                    'icao': record.get('icao', ''),
                    'reg': record.get('reg', ''),
                    'icaotype': record.get('icaotype', ''),
                    'year': record.get('year', ''),
                    'manufacturer': record.get('manufacturer', ''),
                    'model': record.get('model', ''),
                    'owner': record.get('ownop', ''),
                    'faa_pia': record.get('faa_pia', False),
                    'faa_ladd': record.get('faa_ladd', False),
                    'short_type': record.get('short_type', ''),
                    'mil': record.get('mil', False)
                }
                records.append(processed_record)
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line: {e}")
                continue
    return records

def optimize_dataframe(df):
    """Optimize DataFrame data types."""
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df['faa_pia'] = df['faa_pia'].astype(bool)
    df['faa_ladd'] = df['faa_ladd'].astype(bool)
    df['mil'] = df['mil'].astype(bool)
    # Rename ownop to owner
    if 'ownop' in df.columns:
        df['owner'] = df['ownop']
        df = df.drop('ownop', axis=1)
    for col in ['icao', 'reg', 'icaotype', 'manufacturer', 'model', 'owner', 'short_type']:
        df[col] = df[col].astype('string')
    return df

def create_parquet_schema():
    """Create PyArrow schema for Parquet file."""
    return pa.schema([
        ('icao', pa.string()),
        ('reg', pa.string()),
        ('icaotype', pa.string()),
        ('year', pa.int64()),
        ('manufacturer', pa.string()),
        ('model', pa.string()),
        ('owner', pa.string()),
        ('faa_pia', pa.bool_()),
        ('faa_ladd', pa.bool_()),
        ('short_type', pa.string()),
        ('mil', pa.bool_())
    ])

@task(retries=3, retry_delay=60)
def download_to_bronze():
    """Download and store in bronze layer with versioning and date partitioning."""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting download to bronze layer")

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            output_file = Path(temp_dir) / "basic-ac-db.json.gz"
            session = create_session()
            response = session.get(SOURCE_URL)
            response.raise_for_status()

            with open(output_file, 'wb') as f:
                f.write(response.content)

            # Calculate file hash
            file_hash = calculate_file_hash(output_file)

            # Get current date for partitioning
            current_date = datetime.now(timezone.utc)
            date_str = current_date.strftime("%Y%m%d")

            # Get next version number
            s3_client = boto3.client("s3")
            version = get_file_version(s3_client, S3_BUCKET, f"bronze/aircrafts_database/_in_date={date_str}/")

            # Prepare metadata
            metadata = {
                'file_hash': file_hash,
                'source_url': SOURCE_URL,
                'download_timestamp': current_date.isoformat(),
                'version': version
            }

            # Upload with versioning
            key = f"bronze/aircrafts_database/_in_date={date_str}/aircrafts_database_v{version}.json.gz"
            if not safe_upload_file(s3_client, output_file, S3_BUCKET, key, metadata):
                raise Exception("Failed to upload file with validation")

            # Clean up old versions
            cleanup_old_versions(s3_client, S3_BUCKET, f"bronze/aircrafts_database/_in_date={date_str}/")

            logger.info(f"Successfully downloaded and stored in bronze layer (date={date_str}, version={version})")
            return date_str
        except Exception as e:
            logger.error(f"Failed to download aircraft database: {e}")
            return False

@task(retries=3, retry_delay=60)
def transform_to_silver(bronze_data: str):
    """Transform to silver layer with versioning and date partitioning."""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting transformation to silver layer")

    date_str = bronze_data

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            bronze_file = Path(temp_dir) / "basic-ac-db.json.gz"
            parquet_file = Path(temp_dir) / "aircraft_db.parquet"

            s3_client = boto3.client("s3")

            # Get the latest version of the bronze file using pagination
            latest_bronze = None
            latest_version = -1
            page_iterator = s3_client.get_paginator('list_objects_v2').paginate(
                Bucket=S3_BUCKET,
                Prefix=f"bronze/aircrafts_database/_in_date={date_str}/aircrafts_database_v",
                Delimiter='/',
                PaginationConfig={'PageSize': 1000}
            )

            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        version = int(obj['Key'].split('_v')[1].split('.')[0])
                        if version > latest_version:
                            latest_version = version
                            latest_bronze = obj['Key']

            if not latest_bronze:
                raise Exception(f"No bronze files found for date {date_str}")

            s3_client.download_file(S3_BUCKET, latest_bronze, bronze_file)

            # Process data
            records = process_json_records(bronze_file, logger)
            df = pd.DataFrame(records)
            df = optimize_dataframe(df)

            # Write to parquet
            schema = create_parquet_schema()
            table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(
                table,
                parquet_file,
                compression=PARQUET_COMPRESSION,
                version='2.6',
                row_group_size=100000
            )

            # Calculate parquet file hash
            parquet_hash = calculate_file_hash(parquet_file)

            # Get next version number
            version = get_file_version(s3_client, S3_BUCKET, f"silver/aircrafts_database/_in_date={date_str}/")

            # Prepare metadata
            metadata = {
                'file_hash': parquet_hash,
                'bronze_version': latest_bronze.split('_v')[1].split('.')[0],
                'transform_timestamp': datetime.now(timezone.utc).isoformat(),
                'version': version
            }

            # Upload with versioning
            key = f"silver/aircrafts_database/_in_date={date_str}/aircrafts_database_v{version}.parquet"
            if not safe_upload_file(s3_client, parquet_file, S3_BUCKET, key, metadata):
                raise Exception("Failed to upload file with validation")

            # Clean up old versions
            cleanup_old_versions(s3_client, S3_BUCKET, f"silver/aircrafts_database/_in_date={date_str}/")

            logger.info(
                f"Successfully transformed and stored in silver layer "
                f"(date={date_str}, version={version})"
            )
            return date_str
        except Exception as e:
            logger.error(f"Failed to transform aircraft database: {e}")
            return False

def create_database_table(cur):
    """Create database table and indexes if they don't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircrafts_database_table (
            icao VARCHAR(6),
            reg VARCHAR(20),
            icaotype VARCHAR(50),
            year INTEGER,
            manufacturer VARCHAR(255),
            model VARCHAR(255),
            owner VARCHAR(255),
            faa_pia BOOLEAN,
            faa_ladd BOOLEAN,
            short_type VARCHAR(50),
            mil BOOLEAN,
            valid_from TIMESTAMP NOT NULL,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE,
            version INTEGER NOT NULL,
            source_date VARCHAR(8) NOT NULL,
            _loaded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _file_location TEXT,
            _systems_passed TEXT[],
            PRIMARY KEY (icao, valid_from)
        );

        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_icao
            ON aircrafts_database_table(icao);
        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_type
            ON aircrafts_database_table(icaotype);
        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_manufacturer
            ON aircrafts_database_table(manufacturer);
        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_valid
            ON aircrafts_database_table(valid_from, valid_to);
        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_source_date
            ON aircrafts_database_table(source_date);
        CREATE INDEX IF NOT EXISTS idx_aircrafts_database_table_version
            ON aircrafts_database_table(version);
    """)

def get_existing_records(cur):
    """Get existing current records from database."""
    cur.execute("""
        SELECT icao, reg, icaotype, year, manufacturer, model,
               owner, faa_pia, faa_ladd, short_type, mil
        FROM aircrafts_database_table
        WHERE is_current = TRUE
    """)
    return {row[0]: row[1:] for row in cur.fetchall()}

def prepare_new_records(data, existing_records, current_timestamp, next_version, date_str, logger):
    """Prepare new records for insertion."""
    new_records = []
    for row in data:
        icao = row[0]
        row_data = tuple(
            str(x).strip().replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')
                .replace('\\\\', '\\').strip('"\' \t\n\r') if isinstance(x, str) else
            int(x) if isinstance(x, (int, float)) and not pd.isna(x) else
            bool(x) if isinstance(x, bool) else
            None if pd.isna(x) else x
            for x in row[1:]
        )

        if icao not in existing_records:
            logger.info(f"New aircraft record: {icao}")
            new_records.append((*row, current_timestamp, None, True, next_version, date_str))
        else:
            existing_data = tuple(
                str(x).strip().replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')
                    .replace('\\\\', '\\').strip('"\' \t\n\r') if isinstance(x, str) else
                int(x) if isinstance(x, (int, float)) and not pd.isna(x) else
                bool(x) if isinstance(x, bool) else
                None if pd.isna(x) else x
                for x in existing_records[icao]
            )

            if existing_data != row_data:
                logger.info(f"Data changed for aircraft {icao}")
                new_records.append((*row, current_timestamp, None, True, next_version, date_str))
    return new_records

def log_database_stats(cur, new_records, logger):
    """Log database statistics."""
    cur.execute("""
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_current = TRUE THEN 1 END) as current_records,
            COUNT(DISTINCT version) as versions,
            COUNT(DISTINCT icao) as unique_aircraft,
            COUNT(DISTINCT source_date) as unique_dates,
            MAX(version) as latest_version
        FROM aircrafts_database_table
    """)
    stats = cur.fetchone()
    logger.info(f"""
        Database Statistics:
        - Total Records: {stats[0]}
        - Current Records: {stats[1]}
        - Versions: {stats[2]}
        - Unique Aircraft: {stats[3]}
        - Unique Dates: {stats[4]}
        - Latest Version: {stats[5]}
        - New Records Inserted: {len(new_records)}
    """)

def handle_silver_file(s3_client, date_str, temp_dir):
    """Handle silver file download and processing."""
    parquet_file = Path(temp_dir) / "aircraft_db.parquet"

    # Get the latest version of the silver file using pagination
    latest_silver = None
    latest_version = -1
    page_iterator = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=S3_BUCKET,
        Prefix=f"silver/aircrafts_database/_in_date={date_str}/aircrafts_database_v",
        Delimiter='/',
        PaginationConfig={'PageSize': 1000}
    )

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                version = int(obj['Key'].split('_v')[1].split('.')[0])
                if version > latest_version:
                    latest_version = version
                    latest_silver = obj['Key']

    if not latest_silver:
        raise Exception(f"No silver files found for date {date_str}")

    s3_client.download_file(S3_BUCKET, latest_silver, parquet_file)
    return parquet_file, latest_silver

@task(retries=3, retry_delay=60)
def upload_to_postgres(silver_data: str):
    """Upload to PostgreSQL with versioning and change tracking."""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting upload to PostgreSQL")

    date_str = silver_data

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            s3_client = boto3.client("s3")
            parquet_file, latest_silver = handle_silver_file(s3_client, date_str, temp_dir)

            df = pd.read_parquet(parquet_file)
            initialize_connection_pool()

            with get_db_cursor() as cur:
                create_database_table(cur)
                current_timestamp = datetime.now(timezone.utc)

                cur.execute("""
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM aircrafts_database_table
                """)
                next_version = cur.fetchone()[0]

                chunks = np.array_split(df, len(df) // CHUNK_SIZE + 1)
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=MAX_CONNECTIONS
                ) as executor:
                    processed_chunks = list(executor.map(process_chunk, chunks))

                data = [item for chunk in processed_chunks for item in chunk]

                if data:
                    existing_records = get_existing_records(cur)
                    new_records = prepare_new_records(
                        data, existing_records, current_timestamp,
                        next_version, date_str, logger
                    )

                    if new_records:
                        cur.execute("""
                            UPDATE aircrafts_database_table
                            SET valid_to = %s, is_current = FALSE
                            WHERE is_current = TRUE AND icao = ANY(%s)
                        """, (current_timestamp, [r[0] for r in new_records]))

                        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                            for row in new_records:
                                escaped_row = [
                                    str(x).replace('\t', '\\t') if x is not None else '\\N'
                                    for x in row
                                ]
                                file_location = latest_silver
                                systems_passed = ['bronze', 'silver', 'postgres']
                                f.write('\t'.join(escaped_row) + '\t' +
                                       file_location + '\t' +
                                       '{' + ','.join(systems_passed) + '}' + '\n')

                        with open(f.name) as f:
                            cur.copy_from(
                                f,
                                'aircrafts_database_table',
                                columns=(
                                    'icao', 'reg', 'icaotype', 'year', 'manufacturer', 'model',
                                    'owner', 'faa_pia', 'faa_ladd', 'short_type', 'mil',
                                    'valid_from', 'valid_to', 'is_current', 'version', 'source_date',
                                    '_file_location', '_systems_passed'
                                )
                            )

                        os.unlink(f.name)
                        log_database_stats(cur, new_records, logger)
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

@dag(
    schedule="@daily",
    start_date=datetime(2023, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aircraft", "ingest", "database"],
)
def aircrafts_database_ingest():
    # Define tasks
    bronze_task = download_to_bronze()
    silver_task = transform_to_silver(bronze_task)
    postgres_task = upload_to_postgres(silver_task)

    # Set task dependencies
    bronze_task >> silver_task >> postgres_task

aircrafts_database_ingest = aircrafts_database_ingest()
