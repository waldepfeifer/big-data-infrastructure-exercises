import csv
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import boto3
import orjson
import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()


base_url = settings.source_url + "/2023/11/01/"
s3_bucket = settings.s3_bucket
s3_prefix_path = "raw/day=20231101/"
s3_client = boto3.client("s3")

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

def clear_s3_directory(prefix: str):
    objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    if "Contents" in objects:
        delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
        s3_client.delete_objects(Bucket=s3_bucket, Delete={"Objects": delete_keys})
        print(f"Cleared S3 directory: {prefix}")

def upload_to_s3(file_path: Path, s3_key: str):
    try:
        s3_client.upload_file(str(file_path), s3_bucket, s3_key)
        print(f"Uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"Failed to upload {s3_key} to S3: {e}")


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download. The default is 100.""",
        ),
    ] = 100,
) -> str:
    base_date = datetime(2023, 11, 1, 0, 0, 0)
    local_download_dir = Path("/tmp") / "aircraft_data"

    if os.path.exists(local_download_dir):
        shutil.rmtree(local_download_dir)
    os.makedirs(local_download_dir, exist_ok=True)

    clear_s3_directory(s3_prefix_path)

    interval_seconds = 5
    max_retries = 3

    for i in range(file_limit):
        seconds_offset = i * interval_seconds
        file_time_gz = (base_date + timedelta(seconds=seconds_offset)).strftime("%H%M%SZ.json.gz")
        file_time = file_time_gz[:-3]  # Remove .gz extension
        file_url = base_url + file_time_gz
        local_file_path = local_download_dir / Path(file_time)

        for attempt in range(max_retries):
            try:
                response = requests.get(file_url, timeout=10)
                response.raise_for_status()
                with local_file_path.open("wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_time}")
                upload_to_s3(local_file_path, s3_prefix_path + file_time)
                break
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed to download {file_time}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"Skipping {file_time} after {max_retries} attempts.")
    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory."""
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    # Clean up prepared directory if it exists
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    s3_client = boto3.client("s3")

    # List all objects in the S3 raw directory for the specified day
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix_path):
        objects.extend(page.get("Contents", []))
    # Filter for JSON files
    s3_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".json")]

    def process_file(s3_key):
        try:
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            file_content = response["Body"].read()
            file_data = orjson.loads(file_content)
            # Extract a timestamp based on the "now" field
            timestamp = datetime.fromtimestamp(file_data.get("now", 0)).isoformat()
            return [
                {
                    "icao": aircraft_data.get("hex"),
                    "registration": aircraft_data.get("r"),
                    "type": aircraft_data.get("t"),
                    "lat": aircraft_data.get("lat"),
                    "lon": aircraft_data.get("lon"),
                    "alt_baro": aircraft_data.get("alt_baro"),
                    "timestamp": timestamp,
                    "ground_speed": aircraft_data.get("gs"),
                    "emergency": aircraft_data.get("emergency"),
                }
                for aircraft_data in file_data.get("aircraft", [])
            ]
        except Exception as e:
            print(f"Error processing {s3_key}: {e}")
            return []

    # Process files in parallel and flatten the list of results
    all_data = []
    with ThreadPoolExecutor() as executor:
        for result in executor.map(process_file, s3_keys):
            all_data.extend(result)

    # Write the consolidated data to CSV in the prepared directory
    prepared_file_path = os.path.join(prepared_dir, "aircraft_data.csv")
    with open(prepared_file_path, mode="w", newline="") as csv_file:
        fieldnames = [
            "icao",
            "registration",
            "type",
            "lat",
            "lon",
            "alt_baro",
            "timestamp",
            "ground_speed",
            "emergency",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)

    return "OK"
