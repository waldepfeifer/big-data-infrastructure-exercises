import csv
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import orjson
import psycopg2
from fastapi import APIRouter, HTTPException, status

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()
s3_bucket = settings.s3_bucket
s3_prefix_path = "raw/day=20231101/"
s3_client = boto3.client("s3")

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

def create_global_connection():
    """
    Create a global DB connection to minimize connection overhead.
    This connection is created once at module load and reused for every request.
    Autocommit is enabled to avoid unnecessary transaction overhead for read-only queries.
    """
    try:
        # Get credentials from our DBCredentials class which now uses Secrets Manager
        db_credentials = DBCredentials()
        print(f"Creating global DB connection to {db_credentials.host}:{db_credentials.port} "
              f"as {db_credentials.username}...")
        conn = psycopg2.connect(
            host=db_credentials.host,
            port=db_credentials.port,
            user=db_credentials.username,
            password=db_credentials.password,
            dbname="postgres"  # Adjust if your DB name is different.
        )
        conn.autocommit = True
        print("Global DB connection established successfully.")
        return conn
    except Exception as e:
        print(f"Failed to establish global DB connection: {e}")
        return None

# Create the global DB connection on module load.
global_conn = create_global_connection()


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    print("Starting aircraft data preparation...")
    print(f"Using DB credentials: host={db_credentials.host}, user={db_credentials.username}")

    # Get all data from S3
    all_data = fetch_data_from_s3()
    # Insert data into RDS
    return insert_data_into_rds(all_data)


def fetch_data_from_s3():
    """Fetch and process aircraft data from S3"""
    # Initialize S3 client and list objects.
    s3_client = boto3.client("s3")
    print(f"Listing objects in S3 bucket '{s3_bucket}' with prefix '{s3_prefix_path}'...")
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=s3_bucket,
        Prefix=s3_prefix_path,
        PaginationConfig={
            'MaxItems': 20000,  # Adjust based on expected maximum files
            'PageSize': 1000    # S3 API maximum per page
        }
    )

    # Track pagination progress
    page_count = 0
    for page in page_iterator:
        page_count += 1
        if 'Contents' in page:
            page_objects = page['Contents']
            objects.extend(page_objects)
            print(f"Retrieved page {page_count} with {len(page_objects)} objects. Total so far: {len(objects)}")

        # Check if there's a continuation token (next page marker)
        if page_iterator.resume_token:
            print(f"Using continuation token for next page: {page_iterator.resume_token}")

    print(f"Total objects found across {page_count} pages: {len(objects)}")

    # Filter for JSON files.
    s3_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".json")]
    print(f"JSON files to process: {len(s3_keys)}")

    # Process files concurrently and aggregate all records.
    all_data = []
    print("Processing files concurrently...")

    # Process files in batches to avoid memory issues with very large buckets
    batch_size = 100  # Adjust based on file sizes and available memory
    for i in range(0, len(s3_keys), batch_size):
        batch = s3_keys[i:i+batch_size]
        # Calculate batch numbers before printing to avoid long line
        current_batch = i//batch_size + 1
        total_batches = (len(s3_keys) + batch_size - 1)//batch_size
        print(f"Processing batch {current_batch}/{total_batches} ({len(batch)} files)")

        with ThreadPoolExecutor(max_workers=10) as executor:  # Limit concurrent workers
            batch_results = list(executor.map(process_file, batch))
            for result in batch_results:
                all_data.extend(result)

    print(f"Total records processed: {len(all_data)}")
    return all_data


def process_file(s3_key):
    """Process a single file from S3"""
    print(f"Processing file: {s3_key}")
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        file_content = response["Body"].read()
        file_data = orjson.loads(file_content)
        # Convert the "now" field to an ISO timestamp.
        timestamp = datetime.fromtimestamp(file_data.get("now", 0)).isoformat()
        records = []
        for aircraft_data in file_data.get("aircraft", []):
            # Process alt_baro: if not numeric, set to None.
            alt_baro_raw = aircraft_data.get("alt_baro")
            try:
                alt_baro_val = int(alt_baro_raw)
            except (ValueError, TypeError):
                alt_baro_val = None

            # Retain emergency field as text.
            emergency_val = aircraft_data.get("emergency")

            records.append({
                "icao": aircraft_data.get("hex"),
                "registration": aircraft_data.get("r"),
                "type": aircraft_data.get("t"),
                "lat": aircraft_data.get("lat"),
                "lon": aircraft_data.get("lon"),
                "alt_baro": alt_baro_val,
                "timestamp": timestamp,
                "ground_speed": aircraft_data.get("gs"),
                "emergency": emergency_val,
            })
        print(f"Completed file: {s3_key} with {len(records)} records.")
        return records
    except Exception as e:
        print(f"Error: Processing file {s3_key} failed: {e}")
        return []


def insert_data_into_rds(all_data):
    """Insert processed data into RDS database"""
    # Perform bulk insert into RDS using COPY for optimal performance.
    if global_conn is None:
        print("Error: Global DB connection is not available.")
        raise HTTPException(status_code=500, detail="Database connection error")
    try:
        print("Using global DB connection for bulk insert...")
        with global_conn.cursor() as cursor:
            # Create the target table if it doesn't exist.
            print("Ensuring target table 'aircraft_data' exists...")
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS aircraft_data (
                    id SERIAL PRIMARY KEY,
                    icao VARCHAR(20),
                    registration VARCHAR(50),
                    type VARCHAR(50),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    alt_baro INTEGER,
                    timestamp TIMESTAMP,
                    ground_speed DOUBLE PRECISION,
                    emergency TEXT
                );

                -- Create an index on the icao column for faster lookups
                CREATE INDEX IF NOT EXISTS idx_aircraft_data_icao ON aircraft_data(icao);
            """
            cursor.execute(create_table_sql)

            # Truncate the table for a clean slate.
            print("Truncating table 'aircraft_data' to remove old data...")
            cursor.execute("TRUNCATE TABLE aircraft_data;")
            print("Table truncated successfully.")

            # If there are records, perform bulk insert using COPY.
            if all_data:
                print(f"Preparing to bulk insert {len(all_data)} records using COPY...")
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                for row in all_data:
                    writer.writerow([
                        row["icao"],
                        row["registration"],
                        row["type"],
                        row["lat"],
                        row["lon"],
                        row["alt_baro"],
                        row["timestamp"],
                        row["ground_speed"],
                        row["emergency"],
                    ])
                csv_buffer.seek(0)
                copy_sql = """
                    COPY aircraft_data
                    (icao, registration, type, lat, lon, alt_baro, timestamp, ground_speed, emergency)
                    FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '')
                """
                print("Starting COPY command for bulk insert...")
                cursor.copy_expert(copy_sql, csv_buffer)
                print("Bulk insert via COPY completed successfully.")
            else:
                print("No records available for insert.")
        global_conn.commit()
        print("Transaction committed successfully.")
    except Exception as e:
        print(f"Error: Failed to upload data to RDS: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e

    print("=== Aircraft data preparation and upload completed successfully ===")
    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # Ensure page is not negative
    page = max(0, page)

    print(f"Listing aircraft: num_results={num_results}, page={page}")
    offset = page * num_results

    try:
        with global_conn.cursor() as cursor:
            sql = """
                SELECT DISTINCT icao, registration, type
                FROM aircraft_data
                WHERE icao IS NOT NULL
                  AND registration IS NOT NULL
                  AND type IS NOT NULL
                ORDER BY icao ASC
                LIMIT %s OFFSET %s;
            """
            print(f"Executing SQL query with LIMIT={num_results} OFFSET={offset}...")
            cursor.execute(sql, (num_results, offset))
            rows = cursor.fetchall()
            print(f"Retrieved {len(rows)} records from DB.")
            result = [{"icao": row[0], "registration": row[1], "type": row[2]} for row in rows]
            print("Aircraft listing completed successfully.")
            return result

    except Exception as e:
        print(f"Error: Failed to list aircraft from DB: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # Ensure page is not negative
    page = max(0, page)

    # For security, escape the ICAO parameter
    icao = icao.replace("'", "").replace(";", "").replace("--", "")

    print(f"Retrieving positions for aircraft ICAO: {icao} (Page: {page}, Results per page: {num_results})")
    offset = page * num_results

    try:
        with global_conn.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT timestamp, lat, lon
                FROM aircraft_data
                WHERE icao = %s
                ORDER BY timestamp ASC
                LIMIT %s OFFSET %s;
            """
            print(f"Executing SQL query with LIMIT={num_results} OFFSET={offset} for ICAO {icao}...")
            cursor.execute(sql, (icao, num_results, offset))
            rows = cursor.fetchall()
            print(f"Retrieved {len(rows)} records for aircraft {icao}.")
            result = [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in rows]
            return result
    except Exception as e:
        print(f"Failed to retrieve positions for aircraft {icao}: {e}")
        # Return the default data instead of exposing the error
        return []


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    print(f"Retrieving statistics for aircraft with ICAO: {icao}")

    # For security, escape the ICAO parameter
    icao = icao.replace("'", "").replace(";", "").replace("--", "")

    if global_conn is None:
        print("Error: Global DB connection is not available.")
        # Return default values instead of raising an exception for more graceful degradation
        return {
            "max_altitude_baro": 0,
            "max_ground_speed": 0,
            "had_emergency": False
        }

    try:
        with global_conn.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT
                    COALESCE(MAX(alt_baro), 0) AS max_altitude_baro,
                    COALESCE(MAX(ground_speed), 0) AS max_ground_speed,
                    MAX(CASE WHEN emergency IS NOT NULL AND emergency <> '' THEN 1 ELSE 0 END) AS had_emergency
                FROM aircraft_data
                WHERE icao = %s;
            """
            print(f"Executing query for ICAO: {icao}")
            cursor.execute(sql, (icao,))
            result = cursor.fetchone()
            if result is None:
                print(f"No records found for aircraft {icao}")
                raise HTTPException(status_code=404, detail="Aircraft not found")

            max_altitude_baro, max_ground_speed, had_emergency_val = result
            had_emergency = bool(had_emergency_val)
            print(f"Statistics for aircraft {icao}: "
                  f"max_altitude_baro={max_altitude_baro}, "
                  f"max_ground_speed={max_ground_speed}, "
                  f"had_emergency={had_emergency}")
            return {
                "max_altitude_baro": max_altitude_baro,
                "max_ground_speed": max_ground_speed,
                "had_emergency": had_emergency
            }
    except Exception as e:
        print(f"Failed to retrieve statistics for aircraft {icao}: {e}")
        # For security reasons, don't expose the actual error
        # Return 404 instead of 500 for potential SQL injection attempts
        raise HTTPException(status_code=404, detail="Aircraft not found") from None
