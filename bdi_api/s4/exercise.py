import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import boto3
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
        file_time = (base_date + timedelta(seconds=seconds_offset)).strftime("%H%M%SZ.json.gz")
        file_url = base_url + file_time
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
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    return "OK"
