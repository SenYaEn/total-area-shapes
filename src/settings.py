"""
@name: src\settings.py
@author: jan.strocki@hotmail.co.uk

This module settings loaders using Pydantic BaseSettings classes.
"""

# Specific Imports

from pydantic_settings import BaseSettings, SettingsConfigDict

##############################################################################################


class Settings(BaseSettings):
    """
    Class representing app settings.
    """

    # Environment specific variables
    AWS_ACCESS_KEY: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = ""

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
