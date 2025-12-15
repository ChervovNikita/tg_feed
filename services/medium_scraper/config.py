"""Configuration for Medium Scraper service."""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Medium scraper settings."""
    
    # Medium settings
    medium_tags: str = "programming,technology,software-development"  # comma-separated
    medium_cookie: str = ""  # For GraphQL API access
    poll_interval_seconds: int = 60  # How often to check for new articles
    min_article_length: int = 500  # Minimum article length in characters
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/tg_filter"
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    
    # Service
    log_level: str = "INFO"
    
    @property
    def tags_list(self) -> List[str]:
        return [t.strip() for t in self.medium_tags.split(",") if t.strip()]
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

