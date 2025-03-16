import json
from os.path import dirname, join

import boto3
from botocore.exceptions import ClientError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))


def get_secret():
    """
    Retrieve database credentials from AWS Secrets Manager.
    Returns a dictionary with host, port, username, and password.
    Falls back to default values if unable to retrieve secrets.
    """
    secret_name = "rds!db-91487eb7-3193-475e-8b9a-258a7b778a9e"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_string = get_secret_value_response['SecretString']
        # Parse the JSON string into a Python dictionary
        return json.loads(secret_string)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        # Return default values if unable to retrieve secrets
        return {
            "host": "bds.cvnjj5hrh7vc.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "username": "my_bds_user",
            "password": "placeholder_password"
        }
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error parsing secret: {e}")
        # Return default values if unable to parse secrets
        return {
            "host": "bds.cvnjj5hrh7vc.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "username": "my_bds_user",
            "password": "placeholder_password"
        }


class DBCredentials(BaseSettings):
    """Use env variables prefixed with BDI_DB_ or values from Secrets Manager"""

    # Load secret values upon instantiation
    def __init__(self, **data):
        # First try to get credentials from Secrets Manager
        secrets = get_secret()

        # Create default values from secrets but allow environment variables to override
        defaults = {
            "host": secrets.get("host", "bds.cvnjj5hrh7vc.us-east-1.rds.amazonaws.com"),
            "port": int(secrets.get("port", 5432)),
            "username": secrets.get("username", "my_bds_user"),
            "password": secrets.get("password", "placeholder_password"),
        }

        # Update with any provided data (e.g., from environment variables)
        defaults.update(data)

        # Initialize with the combined values
        super().__init__(**defaults)

    host: str
    port: int
    username: str
    password: str
    model_config = SettingsConfigDict(env_prefix="bdi_db_")


class Settings(BaseSettings):
    source_url: str = Field(
        default="https://samples.adsbexchange.com/readsb-hist",
        description="Base URL to the website used to download the data.",
    )
    local_dir: str = Field(
        default=join(PROJECT_DIR, "data"),
        description="For any other value set env variable 'BDI_LOCAL_DIR'",
    )
    s3_bucket: str = Field(
        default="bdi-aircraft-waldepfeifer",
        description="Call the api like `BDI_S3_BUCKET=yourbucket poetry run uvicorn...`",
    )


    telemetry: bool = False
    telemetry_dsn: str = "http://project2_secret_token@uptrace:14317/2"

    model_config = SettingsConfigDict(env_prefix="bdi_")

    @property
    def raw_dir(self) -> str:
        """Store inside all the raw jsons"""
        return join(self.local_dir, "raw")

    @property
    def prepared_dir(self) -> str:
        return join(self.local_dir, "prepared")
