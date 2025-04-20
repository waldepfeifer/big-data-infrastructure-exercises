import logging
from contextlib import contextmanager
from typing import Optional
from datetime import datetime

import psycopg2
from fastapi import APIRouter, HTTPException, status, Query
from psycopg2 import pool
from pydantic import BaseModel

from bdi_api.settings import DBCredentials, Settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

# Connection pool configuration
MIN_CONNECTIONS = 1
MAX_CONNECTIONS = 10

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

# Create connection pool
connection_pool = None

def initialize_connection_pool():
    """
    Initialize the connection pool using the database credentials.
    Returns the created connection pool.
    """
    global connection_pool
    try:
        logger.info(f"Creating connection pool to {db_credentials.host}:{db_credentials.port} "
                   f"as {db_credentials.username}...")

        connection_pool = pool.ThreadedConnectionPool(
            MIN_CONNECTIONS,
            MAX_CONNECTIONS,
            host=db_credentials.host,
            port=db_credentials.port,
            user=db_credentials.username,
            password=db_credentials.password,
            dbname="postgres"  # Adjust if your DB name is different
        )
        logger.info("Connection pool created successfully")
        return connection_pool
    except Exception as e:
        logger.error(f"Failed to establish connection pool: {e}")
        raise HTTPException(status_code=500, detail="Database connection error") from e

# Initialize the connection pool on module load
try:
    connection_pool = initialize_connection_pool()
except Exception as e:
    logger.error(f"Failed to initialize connection pool: {e}")
    connection_pool = None

@contextmanager
def get_db_connection():
    """
    Context manager to get a connection from the pool and ensure it's returned.
    If the pool is not available, raises an appropriate exception.
    """
    connection = None
    try:
        if connection_pool is None:
            logger.error("Connection pool is not available")
            raise HTTPException(status_code=500, detail="Database connection error")

        connection = connection_pool.getconn()
        connection.autocommit = True
        yield connection
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection error") from e
    finally:
        if connection is not None:
            connection_pool.putconn(connection)

@contextmanager
def get_db_cursor():
    """
    Context manager to get a cursor from a connection and ensure proper cleanup.
    """
    with get_db_connection() as connection:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


class AircraftReturn(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    ADDITIONS:
    * Instead of passing a JSON, use pydantic to return the correct schema
       See: https://fastapi.tiangolo.com/tutorial/response-model/
    * Enrich it with information from the aircrafts database (see README for link)
      * `owner`  (`ownop` field in the aircrafts DB)
      * `manufacturer` and `model`


    IMPORTANT: Only return the aircraft that we have seen and not the entire list in the aircrafts database

    """
    # Ensure page is not negative
    page = max(0, page)
    offset = page * num_results

    logger.info(f"Listing aircraft: num_results={num_results}, page={page}")
    
    try:
        with get_db_cursor() as cursor:
            sql = """
                SELECT DISTINCT 
                    a.icao, 
                    a.registration, 
                    a.type,
                    NULLIF(ad.owner, '') as owner,
                    NULLIF(ad.manufacturer, '') as manufacturer,
                    NULLIF(ad.model, '') as model
                FROM adsbexchange_table a
                LEFT JOIN aircrafts_database_table ad ON 
                    a.icao = ad.icao AND 
                    ad.is_current = TRUE
                WHERE (a.icao IS NOT NULL AND a.icao != '')
                   OR (a.registration IS NOT NULL AND a.registration != '')
                   OR (a.type IS NOT NULL AND a.type != '')
                ORDER BY a.icao ASC
                LIMIT %s OFFSET %s;
            """
            logger.info(f"Executing SQL query with LIMIT={num_results} OFFSET={offset}...")
            cursor.execute(sql, (num_results, offset))
            rows = cursor.fetchall()
            logger.info(f"Retrieved {len(rows)} records from DB.")

            # Convert database rows to Pydantic models using parse_obj
            aircraft_list = []
            for row in rows:
                aircraft = AircraftReturn.parse_obj({
                    "icao": row[0],
                    "registration": row[1],
                    "type": row[2],
                    "owner": row[3],
                    "manufacturer": row[4],
                    "model": row[5]
                })
                aircraft_list.append(aircraft)

            logger.info("Aircraft listing completed successfully.")
            return aircraft_list

    except psycopg2.Error as e:
        logger.error(f"Error: Failed to list aircraft from DB: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error") from e


class AircraftCO2(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    hours_flown: float
    """Co2 tons generated"""
    co2: Optional[float]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(
    icao: str,
    day: str = Query(
        ...,
        description="Date in YYYY-MM-DD format (e.g., 2023-11-01)",
        regex=r"^\d{4}-\d{2}-\d{2}$",
        example="2023-11-01"
    )
) -> AircraftCO2:
    """Returns the CO2 generated by the aircraft **in a given day**.

    The day parameter must be in YYYY-MM-DD format (e.g., 2023-11-01).
    Each record represents 5 seconds of flight time.

    Then, you can use these metrics:

    ```python
    fuel_used_kg = fuel_used_gal * 3.04
        c02_tons = (fuel_used_kg * 3.15 ) / 907.185
        ```

    Use the gallon per hour from https://github.com/martsec/flight_co2_analysis/blob/main/data/aircraft_type_fuel_consumption_rates.json
    The key is the `icaotype`

    ```json
    {
      ...,
      "GLF6": { "source":"https://github.com/Jxck-S/plane-notify",
        "name": "Gulfstream G650",
        "galph": 503,
        "category": "Ultra Long Range"
      },
    }

    If you don't have the fuel consumption rate, return `None` in the `co2` field
    """
    try:
        # Validate date format and convert to source_date format (YYYYMMDD)
        try:
            datetime.strptime(day, "%Y-%m-%d")
            source_date = day.replace("-", "")
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="Invalid date format. Must be YYYY-MM-DD (e.g., 2023-11-01)"
            )
            
        with get_db_cursor() as cursor:
            # Query to get flight records and fuel consumption data
            sql = """
                WITH flight_records AS (
                    SELECT 
                        COUNT(*) as record_count,
                        a.type as aircraft_type
                    FROM adsbexchange_table a
                    WHERE a.icao = %s
                    AND a.source_date = %s
                    GROUP BY a.type
                )
                SELECT 
                    fr.record_count,
                    fr.aircraft_type,
                    fc.galph
                FROM flight_records fr
                LEFT JOIN fuel_consumption_table fc ON 
                    fr.aircraft_type = fc.aircraft_type
            """
            
            cursor.execute(sql, (icao, source_date))
            result = cursor.fetchone()
            
            if not result:
                logger.warning(f"No flight records found for ICAO {icao} on {day}")
                return AircraftCO2(icao=icao, hours_flown=0.0, co2=None)
            
            record_count, aircraft_type, galph = result
            
            # Calculate hours flown (each record is 5 seconds)
            hours_flown = round((record_count * 5) / 3600, 6)  # Convert seconds to hours and round to 6 decimal places
            
            # Calculate CO2 if we have fuel consumption data
            co2 = None
            if galph is not None:
                # Calculate fuel used in gallons
                fuel_used_gal = hours_flown * galph
                
                # Convert to kg and then to tons
                fuel_used_kg = fuel_used_gal * 3.04
                co2 = round((fuel_used_kg * 3.15) / 907.185, 6)  # Round to 6 decimal places
            
            logger.info(f"Calculated CO2 for {icao}: hours_flown={hours_flown:.6f}, co2={co2}")
            return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=co2)
            
    except psycopg2.Error as e:
        logger.error(f"Database error calculating CO2 for {icao}: {e}")
        raise HTTPException(status_code=500, detail="Database error") from e
    except Exception as e:
        logger.error(f"Error calculating CO2 for {icao}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e
