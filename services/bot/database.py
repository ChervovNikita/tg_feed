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
    
    # ============ Next Post Logic ============
    
    async def get_next_post_for_user(self, user_id: int, threshold: float = 0.5) -> Optional[dict]:
        """
        Get next unread post for user that passes threshold.
        Returns post with highest score that hasn't been sent yet.
        """
        async with self.pool.acquire() as conn:
            # Get user's model threshold if exists
            user_threshold = await conn.fetchval(
                "SELECT threshold FROM user_models WHERE user_id = $1",
                user_id
            )
            if user_threshold is not None:
                threshold = user_threshold
            
            # Find next post: not sent, score >= threshold, ordered by score desc
            row = await conn.fetchrow(
                """
                SELECT 
                    p.id as post_id,
                    p.title,
                    p.text,
                    p.author,
                    p.tag,
                    p.source_url,
                    pr.score,
                    pr.id as prediction_id
                FROM predictions pr
                JOIN posts p ON pr.post_id = p.id
                WHERE pr.user_id = $1 
                AND pr.sent = FALSE
                AND pr.score >= $2
                ORDER BY pr.score DESC, pr.created_at DESC
                LIMIT 1
                """,
                user_id, threshold
            )
            return dict(row) if row else None
    
    async def mark_prediction_sent(self, prediction_id: int):
        """Mark a prediction as sent to user."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE predictions SET sent = TRUE WHERE id = $1",
                prediction_id
            )
    
    async def skip_post(self, user_id: int, post_id: int):
        """Skip a post (mark as sent without reaction)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE predictions SET sent = TRUE WHERE user_id = $1 AND post_id = $2",
                user_id, post_id
            )
    
    async def set_user_waiting(self, user_id: int, waiting: bool):
        """Set user waiting status for new posts."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (id, waiting_for_posts) 
                VALUES ($1, $2)
                ON CONFLICT (id) DO UPDATE SET waiting_for_posts = $2
                """,
                user_id, waiting
            )
    
    async def get_users_waiting_for_posts(self) -> List[int]:
        """Get all users waiting for new posts."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM users WHERE waiting_for_posts = TRUE"
            )
            return [row['id'] for row in rows]
    
    async def has_new_posts_for_user(self, user_id: int, threshold: float = 0.5) -> bool:
        """Check if user has any unread posts above threshold."""
        async with self.pool.acquire() as conn:
            user_threshold = await conn.fetchval(
                "SELECT threshold FROM user_models WHERE user_id = $1",
                user_id
            )
            if user_threshold is not None:
                threshold = user_threshold
            
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM predictions 
                WHERE user_id = $1 AND sent = FALSE AND score >= $2
                """,
                user_id, threshold
            )
            return count > 0
    
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
