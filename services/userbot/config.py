"""Configuration for Userbot service."""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Userbot settings."""
    
    # Telegram API credentials
    api_id: int = 0
    api_hash: str = ""
    session_string: str = ""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/tg_filter"
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    
    # Service
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

