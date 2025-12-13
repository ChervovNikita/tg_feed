"""Configuration for Bot service."""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Bot settings."""
    
    # Telegram Bot
    bot_token: str = ""
    
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

