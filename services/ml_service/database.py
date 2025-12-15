"""Database operations for ML Service."""
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
        source: str,
        source_id: Optional[str],
        source_url: Optional[str],
        title: Optional[str],
        text: Optional[str],
        author: Optional[str],
        tag: Optional[str],
        media_urls: list[str],
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray]
    ) -> int:
        """Save post/article to database, return post id."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO posts (source, source_id, source_url, title, text, author, tag, 
                                   media_urls, text_embedding, image_embedding)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (source_url) 
                DO UPDATE SET 
                    text = COALESCE($5, posts.text),
                    text_embedding = COALESCE($9, posts.text_embedding),
                    image_embedding = COALESCE($10, posts.image_embedding)
                RETURNING id
                """,
                source, source_id, source_url, title, text, author, tag,
                media_urls, text_embedding, image_embedding
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
    
    async def is_post_exists(self, source_url: str) -> bool:
        """Check if post already exists."""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM posts WHERE source_url = $1)",
                source_url
            )
            return result
    
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
    
    async def get_user_tags(self, user_id: int) -> list[str]:
        """Get list of tags user is subscribed to."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tag FROM tag_subscriptions 
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id
            )
            return [row['tag'] for row in rows]
    
    async def get_users_for_tag(self, tag: str) -> list[int]:
        """Get all users subscribed to a tag."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT user_id FROM tag_subscriptions 
                WHERE tag = $1 AND is_active = TRUE
                """,
                tag
            )
            return [row['user_id'] for row in rows]
    
    async def get_all_subscribed_tags(self) -> list[str]:
        """Get all tags that at least one user is subscribed to."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT tag FROM tag_subscriptions 
                WHERE is_active = TRUE
                """
            )
            return [row['tag'] for row in rows]
    
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
