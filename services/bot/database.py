"""Database operations for Bot service."""
from typing import Optional
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
    
    async def add_subscription(
        self,
        user_id: int,
        channel_id: int,
        channel_name: str = None,
        channel_username: str = None
    ):
        """Add channel subscription for user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO subscriptions (user_id, channel_id, channel_name, channel_username, is_active)
                VALUES ($1, $2, $3, $4, TRUE)
                ON CONFLICT (user_id, channel_id) 
                DO UPDATE SET 
                    is_active = TRUE,
                    channel_name = COALESCE($3, subscriptions.channel_name),
                    channel_username = COALESCE($4, subscriptions.channel_username)
                """,
                user_id, channel_id, channel_name, channel_username
            )
    
    async def remove_subscription(self, user_id: int, channel_id: int):
        """Remove channel subscription for user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE subscriptions 
                SET is_active = FALSE 
                WHERE user_id = $1 AND channel_id = $2
                """,
                user_id, channel_id
            )
    
    async def get_subscriptions(self, user_id: int) -> list[dict]:
        """Get all active subscriptions for user."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT channel_id, channel_name, channel_username 
                FROM subscriptions 
                WHERE user_id = $1 AND is_active = TRUE
                ORDER BY created_at DESC
                """,
                user_id
            )
            return [dict(row) for row in rows]
    
    async def get_user_stats(self, user_id: int) -> dict:
        """Get user statistics."""
        async with self.pool.acquire() as conn:
            # Count subscriptions
            subs_count = await conn.fetchval(
                "SELECT COUNT(*) FROM subscriptions WHERE user_id = $1 AND is_active = TRUE",
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
                'subscriptions': subs_count or 0,
                'predictions': preds_count or 0,
                'likes': reactions['likes'] if reactions else 0,
                'dislikes': reactions['dislikes'] if reactions else 0,
                'model_accuracy': model['accuracy'] if model else None,
                'model_samples': model['num_samples'] if model else 0,
                'model_updated': model['updated_at'] if model else None
            }
    
    async def get_post_channel_id(self, post_id: int) -> Optional[int]:
        """Get channel_id for a post."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT channel_id FROM posts WHERE id = $1",
                post_id
            )
    
    async def mute_channel_for_user(self, user_id: int, channel_id: int):
        """Mute (unsubscribe from) a channel."""
        await self.remove_subscription(user_id, channel_id)


# Global database instance
db = Database()

