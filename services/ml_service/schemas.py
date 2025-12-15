"""Pydantic schemas for ML Service."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class PostMessage(BaseModel):
    """Message schema for posts/articles from Kafka."""
    source: str = "medium"  # 'medium', 'telegram', etc.
    source_id: Optional[str] = None  # article_id for Medium
    source_url: Optional[str] = None  # URL to original
    title: Optional[str] = None
    text: Optional[str] = None
    author: Optional[str] = None
    tag: Optional[str] = None  # Medium tag
    media_urls: list[str] = Field(default_factory=list)
    timestamp: Optional[str] = None  # Accept any string format


class FilteredPost(BaseModel):
    """Message schema for filtered posts to Kafka."""
    user_id: int
    post_db_id: int
    title: Optional[str] = None
    text: Optional[str] = None
    author: Optional[str] = None
    tag: Optional[str] = None
    source_url: Optional[str] = None
    media_urls: list[str] = Field(default_factory=list)
    score: float
    timestamp: Optional[str] = None


class ReactionMessage(BaseModel):
    """Message schema for reactions from Kafka."""
    user_id: int
    post_id: int
    reaction: int  # 1=like, -1=dislike


class EmbeddingRequest(BaseModel):
    """Request schema for embedding endpoint."""
    text: Optional[str] = None
    image_url: Optional[str] = None


class EmbeddingResponse(BaseModel):
    """Response schema for embedding endpoint."""
    text_embedding: Optional[list[float]] = None
    image_embedding: Optional[list[float]] = None
    dimension: int


class PredictRequest(BaseModel):
    """Request schema for prediction endpoint."""
    user_id: int
    text: Optional[str] = None
    media_urls: list[str] = Field(default_factory=list)


class PredictResponse(BaseModel):
    """Response schema for prediction endpoint."""
    user_id: int
    score: float
    should_send: bool
    threshold: float


class ModelInfo(BaseModel):
    """Model information response."""
    user_id: int
    has_model: bool
    threshold: float
    accuracy: Optional[float] = None
    num_samples: int
    updated_at: Optional[datetime] = None


class RetrainRequest(BaseModel):
    """Request to trigger model retraining."""
    user_id: int


class RetrainResponse(BaseModel):
    """Response for retraining request."""
    user_id: int
    success: bool
    message: str
    accuracy: Optional[float] = None
    num_samples: int = 0


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str = "1.0.0"
    kafka_connected: bool
    db_connected: bool
