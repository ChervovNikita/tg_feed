"""FastAPI ML Service main application."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app, REGISTRY

from config import settings
from database import db
from embeddings import embedding_service
from model import model_manager
from consumer import kafka_service
from schemas import (
    EmbeddingRequest,
    EmbeddingResponse,
    PredictRequest,
    PredictResponse,
    ModelInfo,
    RetrainRequest,
    RetrainResponse,
    HealthResponse,
    RecommendResponse,
    PostSentRequest,
    PostSentResponse,
)
from metrics import models_trained_total, predictions_total, predictions_sent_total
from admin import router as admin_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting ML Service...")
    
    # Connect to database
    await db.connect()
    logger.info("Database connected")
    
    # Connect Kafka producer
    kafka_service.connect_producer()
    
    # Start Kafka consumers
    await kafka_service.start_consumers()
    
    yield
    
    # Shutdown
    logger.info("Shutting down ML Service...")
    kafka_service.stop()
    await db.disconnect()


# Create FastAPI app
app = FastAPI(
    title="TG Channel Filter ML Service",
    description="ML Service for filtering Telegram channel posts based on user preferences",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include admin router
app.include_router(admin_router)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    db_connected = db.pool is not None
    kafka_connected = kafka_service.is_connected()
    
    return HealthResponse(
        status="healthy" if db_connected and kafka_connected else "degraded",
        kafka_connected=kafka_connected,
        db_connected=db_connected
    )


@app.post("/embeddings", response_model=EmbeddingResponse)
async def get_embeddings(request: EmbeddingRequest):
    """
    Get embeddings for text and/or image.
    
    - **text**: Text to embed
    - **image_url**: URL of image to embed
    """
    text_emb, image_emb = await embedding_service.get_combined_embedding(
        text=request.text,
        image_urls=[request.image_url] if request.image_url else None
    )
    
    return EmbeddingResponse(
        text_embedding=text_emb.tolist() if text_emb is not None else None,
        image_embedding=image_emb.tolist() if image_emb is not None else None,
        dimension=settings.embedding_dimension
    )


@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """
    Predict relevance score for a post.
    
    - **user_id**: User ID to predict for
    - **text**: Post text
    - **media_urls**: List of media URLs
    """
    logger.info(
        "predict endpoint: user_id=%s text_len=%s media_count=%s",
        request.user_id,
        len(request.text) if request.text else 0,
        len(request.media_urls),
    )

    # Get embeddings
    text_emb, image_emb = await embedding_service.get_combined_embedding(
        text=request.text,
        image_urls=request.media_urls
    )
    
    # Predict
    score, threshold = await model_manager.predict(
        user_id=request.user_id,
        text_embedding=text_emb,
        image_embedding=image_emb
    )
    
    logger.info(
        "predict endpoint result: user_id=%s score=%.4f threshold=%.4f should_send=%s",
        request.user_id,
        score,
        threshold,
        score >= threshold,
    )
    return PredictResponse(
        user_id=request.user_id,
        score=score,
        should_send=score >= threshold,
        threshold=threshold
    )


@app.get("/model/{user_id}", response_model=ModelInfo)
async def get_model_info(user_id: int):
    """
    Get information about user's model.
    
    - **user_id**: User ID
    """
    model_data = await db.get_user_model(user_id)
    
    if model_data is None:
        return ModelInfo(
            user_id=user_id,
            has_model=False,
            threshold=settings.default_threshold,
            num_samples=0
        )
    
    return ModelInfo(
        user_id=user_id,
        has_model=model_data.get('model_weights') is not None,
        threshold=model_data.get('threshold', settings.default_threshold),
        accuracy=model_data.get('accuracy'),
        num_samples=model_data.get('num_samples', 0),
        updated_at=model_data.get('updated_at')
    )


@app.post("/retrain/{user_id}", response_model=RetrainResponse)
async def retrain_model(user_id: int):
    """
    Trigger model retraining for a user.
    
    - **user_id**: User ID to retrain model for
    """
    success, accuracy, num_samples = await model_manager.train_model(user_id)
    
    if success:
        models_trained_total.inc()
    
    return RetrainResponse(
        user_id=user_id,
        success=success,
        message="Model trained successfully" if success else "Not enough samples for training",
        accuracy=accuracy,
        num_samples=num_samples
    )


@app.get("/recommend/{user_id}", response_model=RecommendResponse)
async def recommend_posts(user_id: int, limit: int = 10, track: bool = False):
    """
    Get recommended posts for a user using a two-tower style model.

    - **user_id**: User ID to get recommendations for
    - **limit**: Max number of posts to return
    """
    logger.info(
        "recommend endpoint: user_id=%s limit=%s",
        user_id,
        limit,
    )

    # Use database helper that implements two-tower logic on top of pgvector
    recs = await db.get_recommended_posts_for_user(
        user_id=user_id,
        limit=limit,
    )

    if track and recs:
        inserted = await db.log_recommendations_as_predictions(
            user_id=user_id,
            recommendations=recs,
            sent=False,
        )
        if inserted:
            predictions_total.inc(inserted)

    logger.info(
        "recommend endpoint result: user_id=%s returned=%s",
        user_id,
        len(recs),
    )

    return RecommendResponse(
        user_id=user_id,
        recommendations=recs,
    )


@app.post("/events/post_sent", response_model=PostSentResponse)
async def post_sent_event(request: PostSentRequest):
    """
    Called by the bot after it successfully sent a post to the user.
    Updates predictions history and increments monitoring counters.
    """
    updated = await db.mark_latest_prediction_sent(
        user_id=request.user_id,
        post_id=request.post_id,
    )
    if updated:
        predictions_sent_total.inc()
    return PostSentResponse(success=updated)


@app.post("/reload/{user_id}")
async def reload_model(user_id: int):
    """
    Reload user's model from database (hot reload).
    
    - **user_id**: User ID
    """
    model_manager.reload_model(user_id)
    return {"status": "ok", "message": f"Model cache invalidated for user {user_id}"}


@app.get("/users/trainable")
async def get_trainable_users():
    """Get list of users who have enough data for training."""
    users = await db.get_users_needing_retraining(
        min_samples=settings.min_samples_for_training
    )
    return {"users": users, "min_samples": settings.min_samples_for_training}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

