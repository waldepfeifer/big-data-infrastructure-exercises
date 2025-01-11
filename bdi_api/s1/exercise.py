import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated

import requests
from fastapi import APIRouter, status
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
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Clean the download directory
    if os.path.exists(download_dir): #If directory exists
        shutil.rmtree(download_dir) #Delete directory
    os.makedirs(download_dir, exist_ok=True) #Create directory

    i = 0
    p = 0
    while i < file_limit:
        file_name = "Z.json.gz"
        file_url = f"{base_url}{p:06d}{file_name}"  # Construct file url
        local_file_path = os.path.join(download_dir, f"{p:06d}{file_name}")  # Construct file path
        i += 1
        p += 5

        try:
            # First check if the file exists using a HEAD request (fast)
            requests.head(file_url, timeout=2)

            # File exists, perform GET request to download the file
            response = requests.get(file_url, timeout=2)
            response.raise_for_status()

            # Write file locally
            with open(local_file_path, "w") as file:
                file.write(response.text)
                print(f"Download successful {file_url}")

        except requests.RequestException as e:
            file_limit = file_limit +1
            print(f"Failed to download {file_url}. Error: {str(e)}")  # Log failure with error message

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    all_data = []

    # Use ThreadPoolExecutor to process files in parallel
    file_paths = [os.path.join(raw_dir, file_name) for file_name in os.listdir(raw_dir)]
    with ThreadPoolExecutor() as executor:
        results = executor.map(
            lambda file_path: json.load(open(file_path)).get("aircraft", [])
            if os.path.isfile(file_path) else [],
            file_paths
        )
        for aircraft_data in results:
            all_data.extend(aircraft_data)

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
        {"icao": record.get("hex"), "registration": record.get("r"), "type": record.get("t")}
        for record in data
        if record.get("hex") and record.get("r") and record.get("t")
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

    # Filter records by `hex` field, which corresponds to `icao`
    aircraft_positions = [
        {"timestamp": record["seen"], "lat": record["lat"], "lon": record["lon"]}
        for record in data if record.get("hex") == icao
    ]

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

    # Filter by `hex` instead of `icao`
    aircraft_data = [record for record in data if record.get("hex") == icao]

    if not aircraft_data:
        return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

    # Compute statistics
    max_altitude_baro = max(
        (float(record.get("alt_baro", 0)) if isinstance(record.get("alt_baro", 0), (int, float))
         else 0 for record in aircraft_data), default=0
    )
    max_ground_speed = max(
        (float(record.get("gs", 0)) if isinstance(record.get("gs", 0), (int, float))
         else 0 for record in aircraft_data), default=0
    )
    had_emergency = any(record.get("emergency", False) for record in aircraft_data)

    return {
        "max_altitude_baro": max_altitude_baro,
        "max_ground_speed": max_ground_speed,
        "had_emergency": had_emergency,
    }
