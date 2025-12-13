"""Configuration for Streamlit app."""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Streamlit settings."""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/tg_filter"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

