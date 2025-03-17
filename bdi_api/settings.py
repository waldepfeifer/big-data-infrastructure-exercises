import os
from os.path import dirname, join

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import bdi_api

PROJECT_DIR = dirname(dirname(bdi_api.__file__))

load_dotenv()
print("DB Host:", os.getenv("bdi_db_host"))

class DBCredentials(BaseSettings):
    """Use env variables prefixed with BDI_DB_"""

    host: str = Field(..., env="bdi_db_host")
    port: int = Field(..., env="bdi_db_port")
    username: str = Field(..., env="bdi_db_username")
    password: str = Field(..., env="bdi_db_password")
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
