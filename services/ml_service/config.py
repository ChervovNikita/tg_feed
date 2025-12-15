"""Configuration for ML Service."""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/tg_filter"
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "ml-service-group"
    
    # OpenAI
    openai_api_key: str = ""
    embedding_model: str = "text-embedding-3-small"
    embedding_dimension: int = 1536
    
    # Model settings
    default_threshold: float = 0.5
    min_samples_for_training: int = 20
    
    # Userbot management
    userbot_encryption_key: str = ""
    
    # Service
    service_name: str = "ml-service"
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

