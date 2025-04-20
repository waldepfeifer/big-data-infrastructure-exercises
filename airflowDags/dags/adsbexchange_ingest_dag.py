# Standard library imports
import gc
import gzip
import json
import logging
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import cpu_count
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
MAX_FILES = 100  # Reduced to prevent overwhelming S3
MAX_WORKERS = min(8, cpu_count())  # Match M3 core count
CHUNK_SIZE = 512 * 1024  # Increased to 512KB for better throughput
BATCH_SIZE = 250  # Reduced to prevent S3 throttling
PARQUET_COMPRESSION = 'SNAPPY'

# S3 Configuration
S3_MAX_RETRIES = 3  # Reduced retries to fail faster
S3_BACKOFF_FACTOR = 2.0  # More aggressive backoff
S3_TIMEOUT = 60  # Increased timeout for larger files
S3_CONNECT_TIMEOUT = 15  # Increased connect timeout
S3_READ_TIMEOUT = 60  # Increased read timeout
S3_MAX_POOL_CONNECTIONS = 10  # Reduced to prevent connection pool exhaustion

# Database settings
MIN_CONNECTIONS, MAX_CONNECTIONS = 5, 20
connection_pool = None

# Dates to process
URL_DATES = [
    "2023-11-01", "2023-12-01", "2024-01-01", "2024-02-01",
    "2024-03-01", "2024-04-01", "2024-05-01", "2024-06-01",
    "2024-07-01", "2024-08-01", "2024-09-01", "2024-10-01",
    "2024-11-01"
]

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
        'application_name': 'adsb_monthly_ingest',
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

def create_session():
    """Create an optimized requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=['GET', 'HEAD']
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
        pool_block=False
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def download_file(session, file_url, temp_file):
    """Download a file to temporary storage."""
    try:
        with session.get(file_url, stream=True) as response:
            response.raise_for_status()
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        logging.error(f"❌ Failed to download file: {file_url} - Error: {e}")
        return False

def download_bronze_file(s3_client, s3_bucket, bronze_key, temp_file):
    """Download a single file from bronze layer with streaming."""
    try:
        logging.info(f"  📥 Downloading from S3 Bronze: {Path(bronze_key).name}")
        logging.info(f"    Source: s3://{s3_bucket}/{bronze_key}")
        logging.info(f"    Temp: {temp_file}")
        with open(temp_file, 'wb') as f:
            s3_client.download_fileobj(s3_bucket, bronze_key, f)
        logging.info(f"  ✅ Successfully downloaded from S3 Bronze: {Path(bronze_key).name}")
        return bronze_key, True
    except Exception as e:
        logging.error(f"  ❌ Failed to download {bronze_key}: {e}")
        return bronze_key, False

def upload_to_silver(s3_client, s3_bucket, silver_key, temp_parquet):
    """Upload parquet file to silver layer with streaming."""
    try:
        logging.info("  📤 Uploading to S3 Silver: data.parquet")
        logging.info(f"    Source: {temp_parquet}")
        logging.info(f"    Destination: s3://{s3_bucket}/{silver_key}")
        with open(temp_parquet, 'rb') as f:
            s3_client.upload_fileobj(
                f,
                s3_bucket,
                silver_key,
                ExtraArgs={'ContentType': 'application/parquet'}
            )
        logging.info("  ✅ Successfully uploaded to S3 Silver: data.parquet")
        return True
    except Exception as e:
        logging.error(f"  ❌ Failed to upload to silver: {e}")
        return False

def process_date_files(session, s3_client, date_str, source_url, s3_bucket, temp_dir):
    """Process all files for a single date with parallel downloads and uploads."""
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
        base_url = f"{source_url}/{year}/{month}/{day}/"
        bronze_prefix = f"bronze/adsbexchange/_in_date={year}{month}{day}/"

        # Get list of files
        response = session.get(base_url)
        response.raise_for_status()
        gz_files = sorted(re.findall(r'href="(.*?\.gz)"', response.text))[:MAX_FILES]

        if not gz_files:
            logging.warning(f"⚠️ No files found for {date_str}")
            return False

        logging.info(f"📥 Found {len(gz_files)} files to process for {date_str}")

        # Process files in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Download and upload each file
            futures = []
            for filename in gz_files:
                file_url = f"{base_url}{filename}"
                temp_file = temp_dir / filename
                s3_key = f"{bronze_prefix}{filename}"

                logging.info(f"  📥 Queueing download: {filename}")
                logging.info(f"    Source: {file_url}")
                logging.info(f"    Temp: {temp_file}")

                # Submit download and upload task
                futures.append(executor.submit(
                    process_single_file,
                    session, s3_client, file_url, temp_file, s3_bucket, s3_key
                ))

            # Wait for all tasks to complete
            success_count = 0
            for future in as_completed(futures):
                if future.result():
                    success_count += 1

            if success_count == 0:
                raise ValueError(f"No files successfully processed for {date_str}")

            logging.info(f"✅ Successfully processed {success_count}/{len(gz_files)} files for {date_str}")
            return True

    except Exception as e:
        logging.error(f"❌ Failed to process date {date_str}: {e}")
        return False

def process_single_file(session, s3_client, file_url, temp_file, s3_bucket, s3_key):
    """Process a single file (download and upload)."""
    try:
        # Download file
        with session.get(file_url, stream=True) as response:
            response.raise_for_status()
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

        # Upload to S3 Bronze
        logging.info(f"  📤 Uploading to S3 Bronze: {Path(s3_key).name}")
        logging.info(f"    Source: {temp_file}")
        logging.info(f"    Destination: s3://{s3_bucket}/{s3_key}")
        with open(temp_file, 'rb') as f:
            s3_client.upload_fileobj(
                f,
                s3_bucket,
                s3_key,
                ExtraArgs={'ContentType': 'application/gzip'}
            )

        logging.info(f"  ✅ Successfully uploaded to S3 Bronze: {Path(s3_key).name}")
        return True
    except Exception as e:
        logging.error(f"  ❌ Failed to process {Path(s3_key).name}: {e}")
        return False

def is_valid_gzip(file_path):
    """Check if a file is a valid gzip file by checking magic number."""
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(2)
            return magic == b'\x1f\x8b'
    except Exception:
        return False

def process_bronze_file(temp_file):
    """Process a single bronze file and return its records."""
    try:
        records = []
        # First try to read as regular JSON file
        try:
            with open(temp_file, encoding='utf-8', errors='replace') as f:
                content = f.read()
                data = json.loads(content)
                if 'aircraft' in data:
                    records.extend(data['aircraft'])
                    logging.info(f"  ✅ Successfully processed {len(data['aircraft'])} records from {temp_file}")
                else:
                    logging.warning(f"  ⚠️ No aircraft data found in {temp_file}")
                return records
        except json.JSONDecodeError:
            # If regular JSON fails, try gzip
            try:
                with gzip.open(temp_file, 'rt', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                    data = json.loads(content)
                    if 'aircraft' in data:
                        records.extend(data['aircraft'])
                        logging.info(f"  ✅ Successfully processed {len(data['aircraft'])} records from {temp_file}")
                    else:
                        logging.warning(f"  ⚠️ No aircraft data found in {temp_file}")
                return records
            except Exception as e:
                logging.error(f"  ❌ Error processing {temp_file} as gzip: {e}")
                return []
    except Exception as e:
        logging.error(f"  ❌ Error processing {temp_file}: {e}")
        return []

def process_batch_records(batch_records, schema, parquet_chunks_dir, parquet_chunk_count, chunk_size):
    """Process a batch of records and write to parquet chunks."""
    total_records = 0
    for j in range(0, len(batch_records), chunk_size):
        chunk = batch_records[j:j + chunk_size]
        df = pd.DataFrame(chunk)

        # Convert numeric columns
        numeric_columns = ['lat', 'lon', 'ground_speed']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)

        # Convert alt_baro to integer
        if 'alt_baro' in df.columns:
            df['alt_baro'] = df['alt_baro'].replace('ground', 0)
            df['alt_baro'] = pd.to_numeric(df['alt_baro'], errors='coerce')
            df['alt_baro'] = df['alt_baro'].fillna(0).astype('int64')

        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Convert to table with enforced schema
        table = pa.Table.from_pandas(df, schema=schema)
        table = table.sort_by('icao')

        chunk_path = parquet_chunks_dir / f"chunk_{parquet_chunk_count}.parquet"
        pq.write_table(
            table,
            chunk_path,
            compression=PARQUET_COMPRESSION,
            version='2.6',
            write_statistics=True,
            row_group_size=100000
        )

        total_records += len(chunk)
        parquet_chunk_count += 1
        logging.info(f"  📊 Processed {total_records} records so far")

        # Clear memory
        del df
        del table
        del chunk
        gc.collect()

    return total_records, parquet_chunk_count

def combine_and_upload_parquet_chunks(chunk_paths, s3_client, s3_bucket, silver_key):
    """Combine parquet chunks and upload to S3."""
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_parquet:
        tables = []
        for chunk_path in chunk_paths:
            table = pq.read_table(chunk_path)
            tables.append(table)
            chunk_path.unlink()

        combined_table = pa.concat_tables(tables)
        combined_table = combined_table.sort_by('icao')

        pq.write_table(
            combined_table,
            temp_parquet.name,
            compression=PARQUET_COMPRESSION,
            version='2.6',
            write_statistics=True,
            row_group_size=100000
        )

        del tables
        del combined_table
        gc.collect()

        return upload_to_silver(s3_client, s3_bucket, silver_key, temp_parquet.name)

def list_bronze_files(s3_client, s3_bucket, bronze_prefix):
    """List all bronze files for a given prefix."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=s3_bucket,
        Prefix=bronze_prefix,
        PaginationConfig={'MaxItems': 20000, 'PageSize': 1000}
    )
    for page in page_iterator:
        if 'Contents' in page:
            objects.extend(page['Contents'])
    return [obj['Key'] for obj in objects if obj['Key'].endswith('.gz')]

def process_batch_files(
    bronze_files, s3_client, s3_bucket, temp_dir,
    schema, parquet_chunks_dir, batch_size, chunk_size
):
    """Process files in batches with parallel processing."""
    total_records = 0
    parquet_chunk_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(bronze_files), batch_size):
            batch_files = bronze_files[i:i + batch_size]
            batch_records = []

            # Process files in parallel
            futures = {
                executor.submit(
                    download_and_process_bronze_file,
                    s3_client, s3_bucket, bronze_key, temp_dir / Path(bronze_key).name
                ): bronze_key
                for bronze_key in batch_files
            }

            for future in as_completed(futures):
                records = future.result()
                if records:
                    batch_records.extend(records)
                temp_file = temp_dir / Path(futures[future]).name
                if temp_file.exists():
                    temp_file.unlink()

            # Process records in parallel chunks
            if batch_records:
                chunk_futures = []
                for j in range(0, len(batch_records), chunk_size):
                    chunk = batch_records[j:j + chunk_size]
                    chunk_futures.append(
                        executor.submit(
                            process_batch_records,
                            chunk, schema, parquet_chunks_dir,
                            parquet_chunk_count, chunk_size
                        )
                    )
                    parquet_chunk_count += 1

                for future in as_completed(chunk_futures):
                    chunk_total, _ = future.result()
                    total_records += chunk_total

            batch_records.clear()
            gc.collect()

    return total_records, parquet_chunk_count

def prepare_date_parquet(s3_client, s3_bucket, date_str, temp_dir):
    """Convert all files for a date into a single parquet file with parallel processing."""
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
        bronze_prefix = f"bronze/adsbexchange/_in_date={year}{month}{day}/"
        silver_prefix = f"silver/adsbexchange/_in_date={year}{month}{day}/"

        schema = pa.schema([
            ('icao', pa.string()),
            ('registration', pa.string()),
            ('type', pa.string()),
            ('lat', pa.float64()),
            ('lon', pa.float64()),
            ('alt_baro', pa.int64()),
            ('timestamp', pa.timestamp('us')),
            ('ground_speed', pa.float64()),
            ('emergency', pa.string())
        ])

        logging.info(f"📥 Listing files in S3 Bronze for {date_str}")
        bronze_files = list_bronze_files(s3_client, s3_bucket, bronze_prefix)
        if not bronze_files:
            logging.warning(f"⚠️ No files found in bronze for {date_str}")
            return False

        parquet_chunks_dir = temp_dir / "parquet_chunks"
        parquet_chunks_dir.mkdir(exist_ok=True)
        batch_size = 3
        chunk_size = 5000

        total_records, _ = process_batch_files(
            bronze_files, s3_client, s3_bucket, temp_dir,
            schema, parquet_chunks_dir, batch_size, chunk_size
        )

        if total_records == 0:
            raise ValueError(f"No valid data found for {date_str}")

        chunk_paths = sorted(parquet_chunks_dir.glob("*.parquet"))
        silver_key = f"{silver_prefix}aircraft_data.parquet"
        return combine_and_upload_parquet_chunks(chunk_paths, s3_client, s3_bucket, silver_key)

    except Exception as e:
        logging.error(f"❌ Failed to prepare parquet for {date_str}: {e}")
        return False
    finally:
        if os.path.exists(parquet_chunks_dir):
            for file in parquet_chunks_dir.glob("*"):
                file.unlink()
            parquet_chunks_dir.rmdir()

def download_and_process_bronze_file(s3_client, s3_bucket, bronze_key, temp_file):
    """Download and process a single bronze file."""
    try:
        # Download file
        logging.info(f"  📥 Downloading from S3 Bronze: {Path(bronze_key).name}")
        with open(temp_file, 'wb') as f:
            s3_client.download_fileobj(s3_bucket, bronze_key, f)

        # Process file
        records = []

        # First try to read as regular JSON file
        try:
            with open(temp_file, encoding='utf-8', errors='replace') as f:
                content = f.read()
                data = json.loads(content)
                logging.info(f"  ✅ Successfully read as JSON file: {Path(bronze_key).name}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            # If regular JSON fails, try gzip
            try:
                with gzip.open(temp_file, 'rt', encoding='utf-8', errors='replace') as f:
                    content = f.read()
                    data = json.loads(content)
                logging.info(f"  ✅ Successfully read as gzip file: {Path(bronze_key).name}")
            except Exception as e:
                logging.error(f"  ❌ Failed to read file as either JSON or gzip: {Path(bronze_key).name} - {e}")
                return []

        # Convert timestamp
        timestamp = datetime.fromtimestamp(data.get("now", 0)).isoformat()

        # Process aircraft records
        for aircraft in data.get("aircraft", []):
            # Process alt_baro
            alt_baro_raw = aircraft.get("alt_baro")
            try:
                alt_baro_val = float(alt_baro_raw) if alt_baro_raw != 'ground' else 0
            except (ValueError, TypeError):
                alt_baro_val = 0

            record = {
                "icao": aircraft.get("hex"),
                "registration": aircraft.get("r"),
                "type": aircraft.get("t"),
                "lat": aircraft.get("lat"),
                "lon": aircraft.get("lon"),
                "alt_baro": alt_baro_val,
                "timestamp": timestamp,
                "ground_speed": aircraft.get("gs"),
                "emergency": aircraft.get("emergency")
            }
            records.append(record)

        logging.info(f"  ✅ Successfully processed {len(records)} records from {Path(bronze_key).name}")
        return records
    except Exception as e:
        logging.error(f"  ❌ Error processing {Path(bronze_key).name}: {e}")
        return []

def process_json_line(line):
    """Process a single JSON line with better error handling."""
    try:
        # Try to parse as regular JSON
        return json.loads(line)
    except json.JSONDecodeError:
        try:
            # Try to fix common JSON issues
            # Remove any trailing commas
            line = re.sub(r',(\s*[}\]])', r'\1', line)
            # Ensure property names are quoted
            line = re.sub(r'([{,]\s*)(\w+)(\s*:)', r'\1"\2"\3', line)
            return json.loads(line)
        except json.JSONDecodeError as e:
            logging.warning(f"  ⚠️ Could not parse JSON line: {str(e)}")
            return None

def process_chunk(chunk: pd.DataFrame) -> list:
    """Process a chunk of records for database insertion."""
    return [
        (
            str(row['icao'])[:6] if pd.notna(row['icao']) else '',  # Truncate to 6 chars
            str(row['registration']) if pd.notna(row['registration']) else '',
            str(row['type']) if pd.notna(row['type']) else '',
            float(row['lat']) if pd.notna(row['lat']) else 0.0,
            float(row['lon']) if pd.notna(row['lon']) else 0.0,
            int(row['alt_baro']) if pd.notna(row['alt_baro']) else 0,
            row['timestamp'] if pd.notna(row['timestamp']) else None,
            float(row['ground_speed']) if pd.notna(row['ground_speed']) else 0.0,
            str(row['emergency']) if pd.notna(row['emergency']) else ''
        )
        for _, row in chunk.iterrows()
    ]

@task(retries=3, retry_delay=60)
def upload_to_postgres():
    """Upload to PostgreSQL with parallel processing and optimized connection handling."""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting upload to PostgreSQL")

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            s3_client = boto3.client("s3")
            s3_bucket = Variable.get("S3_BUCKET", default_var="bdi-aircraft-waldepfeifer")
            initialize_connection_pool()

            # Process dates sequentially to avoid connection pool exhaustion
            for date_str in URL_DATES:
                date = datetime.strptime(date_str, "%Y-%m-%d")
                year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
                silver_prefix = f"silver/adsbexchange/_in_date={year}{month}{day}/"
                source_date = f"{year}{month}{day}"

                # Download parquet file
                parquet_file = Path(temp_dir) / f"aircraft_data_{date_str}.parquet"
                s3_client.download_file(
                    s3_bucket,
                    f"{silver_prefix}aircraft_data.parquet",
                    parquet_file
                )

                # Process data in smaller chunks to avoid memory issues
                df = pd.read_parquet(parquet_file)
                chunk_size = 10000  # Process 10k records at a time
                chunks = np.array_split(df, len(df) // chunk_size + 1)

                # Process chunks sequentially to avoid connection pool exhaustion
                for chunk in chunks:
                    process_chunk_to_postgres(chunk, source_date, silver_prefix)

            logger.info("Successfully uploaded all data to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to upload data to PostgreSQL: {e}")
            return False
        finally:
            if connection_pool is not None:
                connection_pool.closeall()

def process_chunk_to_postgres(chunk: pd.DataFrame, source_date: str, silver_prefix: str):
    """Process a single chunk of data and upload to PostgreSQL."""
    try:
        with get_db_cursor() as cur:
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS adsbexchange_table (
                    id SERIAL PRIMARY KEY,
                    icao VARCHAR(6),
                    registration VARCHAR(20),
                    type VARCHAR(50),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    alt_baro INTEGER,
                    timestamp TIMESTAMP,
                    ground_speed DOUBLE PRECISION,
                    emergency TEXT,
                    source_date VARCHAR(8) NOT NULL,
                    _loaded_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    _file_location TEXT,
                    _systems_passed TEXT[]
                );

                CREATE INDEX IF NOT EXISTS idx_adsbexchange_table_icao
                    ON adsbexchange_table(icao);
                CREATE INDEX IF NOT EXISTS idx_adsbexchange_table_type
                    ON adsbexchange_table(type);
                CREATE INDEX IF NOT EXISTS idx_adsbexchange_table_timestamp
                    ON adsbexchange_table(timestamp);
                CREATE INDEX IF NOT EXISTS idx_adsbexchange_table_source_date
                    ON adsbexchange_table(source_date);
                CREATE INDEX IF NOT EXISTS idx_adsbexchange_table_loaded_time
                    ON adsbexchange_table(_loaded_time);
            """)

            # Process the chunk
            data = process_chunk(chunk)
            if data:
                # Get existing records
                cur.execute("""
                    SELECT icao, registration, type, lat, lon, alt_baro,
                           timestamp, ground_speed, emergency, source_date
                    FROM adsbexchange_table
                """)
                existing_records = {row[0]: row[1:] for row in cur.fetchall()}

                # Prepare new records
                new_records = []
                for row in data:
                    icao = row[0]
                    # Convert row to tuple for comparison, handling NULL values
                    row_data = tuple(
                        str(x).strip() if isinstance(x, str) else x
                        for x in row[1:]
                    )

                    # Only create new record if:
                    # 1. Aircraft doesn't exist in database, or
                    # 2. Data has actually changed, or
                    # 3. Source date is different
                    if (icao not in existing_records or
                        existing_records[icao][:-1] != row_data or  # Compare all fields except source_date
                        existing_records[icao][-1] != source_date):  # Compare source_date
                        new_records.append(row)

                if new_records:
                    # Use COPY for bulk insert
                    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                        for row in new_records:
                            escaped_row = [str(x).replace('\t', '\\t') if x is not None else '\\N' for x in row]
                            file_location = f"{silver_prefix}aircraft_data.parquet"
                            systems_passed = ['bronze', 'silver', 'postgres']
                            f.write(
                                '\t'.join(escaped_row) + '\t' + source_date + '\t' +
                                file_location + '\t' + '{' + ','.join(systems_passed) + '}' + '\n'
                            )

                    with open(f.name) as f:
                        cur.copy_expert("""
                            COPY adsbexchange_table
                            (icao, registration, type, lat, lon, alt_baro,
                             timestamp, ground_speed, emergency, source_date,
                             _file_location, _systems_passed)
                            FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N')
                        """, f)

                    os.unlink(f.name)

                    # Log statistics
                    cur.execute("""
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(DISTINCT icao) as unique_aircraft,
                            COUNT(DISTINCT source_date) as unique_dates
                        FROM adsbexchange_table
                    """)
                    stats = cur.fetchone()
                    logger = logging.getLogger("airflow.task")
                    logger.info(f"""
                        Database Statistics:
                        - Total Records: {stats[0]}
                        - Unique Aircraft: {stats[1]}
                        - Unique Dates: {stats[2]}
                        - New Records Inserted: {len(new_records)}
                    """)
    except Exception as e:
        logger = logging.getLogger("airflow.task")
        logger.error(f"Error processing chunk: {str(e)}")
        raise

@task(retries=3, retry_delay=60)
def download_and_upload():
    logger = logging.getLogger("airflow.task")
    source_url = Variable.get("SOURCE_URL", default_var="https://samples.adsbexchange.com/readsb-hist")
    s3_bucket = Variable.get("S3_BUCKET", default_var="bdi-aircraft-waldepfeifer")

    logger.info("="*80)
    logger.info("Starting download and upload task")
    logger.info(f"  Source URL: {source_url}")
    logger.info(f"  S3 Bucket: {s3_bucket}")
    logger.info("="*80)

    s3_client = boto3.client("s3")
    http_session = create_session()
    temp_dir = Path(tempfile.mkdtemp())

    try:
        for date_str in URL_DATES:
            logger.info(f"Processing date: {date_str}")
            success = process_date_files(http_session, s3_client, date_str, source_url, s3_bucket, temp_dir)
            if success:
                logger.info(f"✅ Successfully processed {date_str}")
            else:
                logger.error(f"❌ Failed to process {date_str}")
    finally:
        if temp_dir.exists():
            for file in temp_dir.glob("*"):
                file.unlink()
            temp_dir.rmdir()
        logger.info("="*80)
        logger.info("Download and upload task completed")
        logger.info("="*80)

@task(retries=3, retry_delay=60)
def prepare():
    logger = logging.getLogger("airflow.task")
    s3_bucket = Variable.get("S3_BUCKET", default_var="bdi-aircraft-waldepfeifer")

    logger.info("="*80)
    logger.info("Starting preparation task")
    logger.info(f"  S3 Bucket: {s3_bucket}")
    logger.info("="*80)

    s3_client = boto3.client("s3")
    temp_dir = Path(tempfile.mkdtemp())

    try:
        for date_str in URL_DATES:
            logger.info(f"Preparing parquet for: {date_str}")
            success = prepare_date_parquet(s3_client, s3_bucket, date_str, temp_dir)
            if success:
                logger.info(f"✅ Successfully prepared parquet for {date_str}")
            else:
                logger.error(f"❌ Failed to prepare parquet for {date_str}")
    finally:
        if temp_dir.exists():
            for file in temp_dir.glob("*"):
                file.unlink()
            temp_dir.rmdir()
        logger.info("="*80)
        logger.info("Preparation task completed")
        logger.info("="*80)

@dag(
    schedule="@daily",
    start_date=datetime(2023, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["adsbexchange", "ingest"],
)
def adsbexchange_ingest():
    download_and_upload() >> prepare() >> upload_to_postgres()

adsbexchange_ingest = adsbexchange_ingest()
