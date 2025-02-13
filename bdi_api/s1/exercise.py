import json
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import requests
from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()



s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    """
    base_date = datetime(2023, 11, 1, 0, 0, 0)
    download_dir = Path(settings.raw_dir) / "day=20231101"
    base_url = settings.source_url + "/2023/11/01/"

    # Clean the download directory
    if os.path.exists(download_dir): #If directory exists
        shutil.rmtree(download_dir) #Delete directory
    os.makedirs(download_dir, exist_ok=True) #Create directory

    # Sequential download logic based on 5-second intervals for time format "%H%M%SZ.json.gz"
    interval_seconds = 5
    max_retries = 3

    for i in range(file_limit):
        seconds_offset = i * interval_seconds
        file_time = (base_date + timedelta(seconds=seconds_offset)).strftime("%H%M%SZ.json.gz")
        file_url = base_url + file_time
        target_path = download_dir / Path(file_time)

        for attempt in range(max_retries):
            try:
                response = requests.get(file_url, timeout=10)
                response.raise_for_status()
                with target_path.open("wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_time}")
                break
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed to download {file_time}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Skipping {file_time} after {max_retries} attempts.")

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    """
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    file_paths = [
        os.path.join(raw_dir, file_name)
        for file_name in os.listdir(raw_dir)
        if os.path.isfile(os.path.join(raw_dir, file_name))
    ]

    def process_file(file_path):
        try:
            with open(file_path) as file:
                file_data = json.load(file)
                timestamp = datetime.fromtimestamp(file_data.get("now", 0)).isoformat()
                return [
                    {
                    "icao": aircraft_data.get("hex", None),
                    "registration": aircraft_data.get("r", None),
                    "type": aircraft_data.get("t", None),
                    "lat": aircraft_data.get("lat", None),
                    "lon": aircraft_data.get("lon", None),
                    "alt_baro": aircraft_data.get("alt_baro", None),
                    "timestamp": timestamp,
                    "ground_speed": aircraft_data.get("gs", None),
                    "emergency": aircraft_data.get("emergency", None),
                    }
                    for aircraft_data in file_data.get("aircraft", [])
                ]
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    # Process files in parallel and flatten the results
    with ThreadPoolExecutor() as executor:
        all_data = [entry for result in executor.map(process_file, file_paths) for entry in result]

    prepared_file_path = os.path.join(prepared_dir, "aircraft_data.json")
    with open(prepared_file_path, "w") as prepared_file:
        json.dump(all_data, prepared_file, indent=4)

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    prepared_file_path = os.path.join(settings.prepared_dir, "day=20231101", "aircraft_data.json")

    with open(prepared_file_path) as file:
        data = json.load(file)

    # Extract relevant fields and filter out entries without required fields
    filtered_data = [
        {"icao": record.get("icao"), "registration": record.get("registration"), "type": record.get("type")}
        for record in data
        if record.get("icao") and record.get("registration") and record.get("type")
    ]

    # Remove duplicates by creating a dictionary keyed by `icao`
    unique_data = {entry["icao"]: entry for entry in filtered_data}.values()

    # Sort by icao field
    sorted_data = sorted(unique_data, key=lambda x: x["icao"])

    # Paginate results
    start = page * num_results
    end = start + num_results

    return sorted_data[start:end]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    prepared_file_path = os.path.join(settings.prepared_dir, "day=20231101", "aircraft_data.json")

    with open(prepared_file_path) as file:
        data = json.load(file)

    # Filter records by ICAO
    try:
        aircraft_positions = [
            {"timestamp": record["timestamp"], "lat": record["lat"], "lon": record["lon"]}
            for record in data if record.get("icao") == icao
        ]

        # If no positions are found, raise 404
        if not aircraft_positions:
            raise ValueError

    except ValueError:
        raise HTTPException(status_code=404, detail="Aircraft not found") from None

    # Sort positions by timestamp
    sorted_positions = sorted(aircraft_positions, key=lambda x: x["timestamp"])
    start = page * num_results
    end = start + num_results

    return sorted_positions[start:end]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    prepared_file_path = os.path.join(settings.prepared_dir, "day=20231101", "aircraft_data.json")

    with open(prepared_file_path) as file:
        data = json.load(file)

    # Try to get aircraft data
    try:
        aircraft_data = [record for record in data if record.get("icao") == icao]

        # If no data is found, raise 404
        if not aircraft_data:
            raise ValueError

    except ValueError:
        raise HTTPException(status_code=404, detail="Aircraft not found") from None

    # Compute statistics
    max_altitude_baro = max(
        (float(record.get("alt_baro", 0)) if isinstance(record.get("alt_baro", 0), (int, float))
         else 0 for record in aircraft_data), default=0
    )
    max_ground_speed = max(
        (float(record.get("ground_speed", 0)) if isinstance(record.get("ground_speed", 0), (int, float))
         else 0 for record in aircraft_data), default=0
    )
    had_emergency = any(record.get("emergency", False) for record in aircraft_data)

    return {
        "max_altitude_baro": max_altitude_baro,
        "max_ground_speed": max_ground_speed,
        "had_emergency": had_emergency
    }
