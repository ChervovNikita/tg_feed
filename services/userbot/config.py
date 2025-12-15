"""Configuration for Userbot service."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Userbot settings."""
    
    # Encryption key for session strings in DB (required)
    userbot_encryption_key: str = ""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/tg_filter"
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    
    # Rate limiting
    join_cooldown_seconds: int = 60  # Min seconds between joins per account
    poll_interval_seconds: int = 15  # Seconds between polling cycles
    max_channels_per_account: int = 50
    
    # Service
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

