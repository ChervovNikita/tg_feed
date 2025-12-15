"""Database operations for Bot service."""
from typing import Optional, List
import asyncpg

from config import settings


class Database:
    """Async database operations for bot."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10
        )
    
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
    
    # ============ Tag Subscriptions ============
    
    async def add_tag_subscription(self, user_id: int, tag: str):
        """Add tag subscription for user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tag_subscriptions (user_id, tag, is_active)
                VALUES ($1, $2, TRUE)
                ON CONFLICT (user_id, tag) DO UPDATE SET is_active = TRUE
                """,
                user_id, tag.lower()
            )
    
    async def remove_tag_subscription(self, user_id: int, tag: str):
        """Remove tag subscription for user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tag_subscriptions SET is_active = FALSE 
                WHERE user_id = $1 AND tag = $2
                """,
                user_id, tag.lower()
            )
    
    async def get_user_tags(self, user_id: int) -> List[str]:
        """Get all active tag subscriptions for user."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tag FROM tag_subscriptions 
                WHERE user_id = $1 AND is_active = TRUE
                ORDER BY tag
                """,
                user_id
            )
            return [row['tag'] for row in rows]
    
    # ============ Posts/Articles ============
    
    async def get_post(self, post_id: int) -> Optional[dict]:
        """Get post by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, source, source_id, source_url, title, text, author, tag
                FROM posts WHERE id = $1
                """,
                post_id
            )
            return dict(row) if row else None
    
    async def get_post_url(self, post_id: int) -> Optional[str]:
        """Get source URL for a post."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT source_url FROM posts WHERE id = $1",
                post_id
            )
    
    # ============ User Stats ============
    
    async def get_user_stats(self, user_id: int) -> dict:
        """Get user statistics."""
        async with self.pool.acquire() as conn:
            # Count tag subscriptions
            tags_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tag_subscriptions WHERE user_id = $1 AND is_active = TRUE",
                user_id
            )
            
            # Count predictions
            preds_count = await conn.fetchval(
                "SELECT COUNT(*) FROM predictions WHERE user_id = $1",
                user_id
            )
            
            # Count reactions
            reactions = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) FILTER (WHERE reaction > 0) as likes,
                    COUNT(*) FILTER (WHERE reaction < 0) as dislikes
                FROM reactions WHERE user_id = $1
                """,
                user_id
            )
            
            # Get model info
            model = await conn.fetchrow(
                "SELECT accuracy, num_samples, updated_at FROM user_models WHERE user_id = $1",
                user_id
            )
            
            return {
                'tags': tags_count or 0,
                'predictions': preds_count or 0,
                'likes': reactions['likes'] if reactions else 0,
                'dislikes': reactions['dislikes'] if reactions else 0,
                'model_accuracy': model['accuracy'] if model else None,
                'model_samples': model['num_samples'] if model else 0,
                'model_updated': model['updated_at'] if model else None
            }


# Global database instance
db = Database()
