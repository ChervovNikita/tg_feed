"""Database operations for ML Service."""
import pickle
from datetime import datetime
from typing import Optional
import asyncpg
import numpy as np
from pgvector.asyncpg import register_vector

from config import settings


class Database:
    """Async database operations."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            init=self._init_connection
        )
    
    async def _init_connection(self, conn):
        """Initialize connection with pgvector."""
        await register_vector(conn)
    
    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
    
    async def get_or_create_user(self, user_id: int, username: str = None) -> int:
        """Get or create user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (id, username) 
                VALUES ($1, $2) 
                ON CONFLICT (id) DO UPDATE SET username = COALESCE($2, users.username)
                """,
                user_id, username
            )
            return user_id
    
    async def save_post(
        self,
        channel_id: int,
        message_id: int,
        text: Optional[str],
        media_urls: list[str],
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray]
    ) -> int:
        """Save post to database, return post id."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO posts (channel_id, message_id, text, media_urls, text_embedding, image_embedding)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_id, message_id) 
                DO UPDATE SET 
                    text = COALESCE($3, posts.text),
                    media_urls = COALESCE($4, posts.media_urls),
                    text_embedding = COALESCE($5, posts.text_embedding),
                    image_embedding = COALESCE($6, posts.image_embedding)
                RETURNING id
                """,
                channel_id, message_id, text, media_urls,
                text_embedding, image_embedding
            )
            return row['id']
    
    async def get_post_by_id(self, post_id: int) -> Optional[dict]:
        """Get post by id."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM posts WHERE id = $1",
                post_id
            )
            return dict(row) if row else None
    
    async def save_prediction(
        self,
        user_id: int,
        post_id: int,
        score: float,
        sent: bool
    ) -> int:
        """Save prediction to database."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO predictions (user_id, post_id, score, sent)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                user_id, post_id, score, sent
            )
            return row['id']
    
    async def save_reaction(
        self,
        user_id: int,
        post_id: int,
        reaction: int
    ):
        """Save user reaction."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO reactions (user_id, post_id, reaction)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, post_id) 
                DO UPDATE SET reaction = $3, created_at = NOW()
                """,
                user_id, post_id, reaction
            )
    
    async def get_user_model(self, user_id: int) -> Optional[dict]:
        """Get user model from database."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM user_models WHERE user_id = $1",
                user_id
            )
            return dict(row) if row else None
    
    async def save_user_model(
        self,
        user_id: int,
        model_weights: bytes,
        threshold: float,
        accuracy: float,
        num_samples: int
    ):
        """Save user model to database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_models (user_id, model_weights, threshold, accuracy, num_samples, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    model_weights = $2,
                    threshold = $3,
                    accuracy = $4,
                    num_samples = $5,
                    updated_at = NOW()
                """,
                user_id, model_weights, threshold, accuracy, num_samples
            )
    
    async def get_training_data(self, user_id: int) -> list[dict]:
        """Get training data for user (posts with reactions)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    p.id,
                    p.text_embedding,
                    p.image_embedding,
                    r.reaction
                FROM reactions r
                JOIN posts p ON r.post_id = p.id
                WHERE r.user_id = $1
                AND p.text_embedding IS NOT NULL
                ORDER BY r.created_at DESC
                LIMIT 1000
                """,
                user_id
            )
            return [dict(row) for row in rows]
    
    async def get_user_subscriptions(self, user_id: int) -> list[int]:
        """Get list of channel IDs user is subscribed to."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT channel_id FROM subscriptions 
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id
            )
            return [row['channel_id'] for row in rows]
    
    async def get_all_active_subscriptions(self) -> list[dict]:
        """Get all active subscriptions."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, channel_id FROM subscriptions 
                WHERE is_active = TRUE
                """
            )
            return [dict(row) for row in rows]
    
    async def get_users_for_channel(self, channel_id: int) -> list[int]:
        """Get all users subscribed to a channel."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id FROM subscriptions 
                WHERE channel_id = $1 AND is_active = TRUE
                """,
                channel_id
            )
            return [row['user_id'] for row in rows]
    
    async def get_users_needing_retraining(self, min_samples: int = 20) -> list[int]:
        """Get users who have enough reactions for training."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id, COUNT(*) as cnt
                FROM reactions
                GROUP BY user_id
                HAVING COUNT(*) >= $1
                """,
                min_samples
            )
            return [row['user_id'] for row in rows]


# Global database instance
db = Database()

