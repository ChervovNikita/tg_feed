"""Database operations for ML Service."""
from typing import Optional, List, Dict
import asyncio
import logging
import time

import asyncpg
import numpy as np
from pgvector.asyncpg import register_vector

from config import settings

logger = logging.getLogger(__name__)


class Database:
    """Async database operations."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        # Cache for recommendations: user_id -> (timestamp, recommendations)
        self._recommendation_cache: Dict[int, tuple[float, list[dict]]] = {}
        # Cache for user embeddings: user_id -> (timestamp, embedding)
        self._user_embedding_cache: Dict[int, tuple[float, Optional[np.ndarray]]] = {}
        # Cache for user tags: user_id -> (timestamp, tags)
        self._user_tags_cache: Dict[int, tuple[float, Optional[List[str]]]] = {}
        # Locks per user to prevent concurrent requests for same user
        self._user_locks: Dict[int, asyncio.Lock] = {}
        self._cache_ttl: float = 5.0  # Cache for 5 seconds
        self._embedding_cache_ttl: float = 30.0  # Cache user embeddings for 30 seconds
        self._tags_cache_ttl: float = 60.0  # Cache user tags for 60 seconds (tags change rarely)
    
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=10,
            max_size=50,  # Much larger pool for high concurrency
            init=self._init_connection,
            command_timeout=120,  # Even longer timeout for slow queries
            max_queries=50000,
            max_inactive_connection_lifetime=300,
        )
    
    async def _init_connection(self, conn):
        """Initialize connection with pgvector."""
        await register_vector(conn)
    
    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
    
    async def _acquire_connection(self, timeout: float = 5.0):
        """
        Acquire connection from pool with timeout and logging.
        Helps identify connection pool exhaustion issues.
        """
        import time
        start = time.time()
        try:
            conn = await asyncio.wait_for(
                self.pool.acquire(),
                timeout=timeout
            )
            elapsed = time.time() - start
            if elapsed > 0.1:  # Log if waited more than 100ms
                logger.warning(
                    "Connection pool: waited %.2fs to acquire connection (pool may be exhausted)",
                    elapsed
                )
            return conn
        except asyncio.TimeoutError:
            elapsed = time.time() - start
            logger.error(
                "Connection pool: timeout after %.2fs waiting for connection (pool size: %s, max: %s)",
                elapsed,
                self.pool.get_size() if self.pool else 0,
                self.pool.get_max_size() if self.pool else 0,
            )
            raise
    
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
        """Save user reaction and invalidate recommendation cache."""
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
        # Invalidate caches so recommendations reflect new reaction
        self._invalidate_user_cache(user_id)
    
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
        conn = await self._acquire_connection()
        try:
            await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            rows = await conn.fetch(
                """
                SELECT tag FROM tag_subscriptions 
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id
            )
            return [row['tag'] for row in rows]
        finally:
            await self.pool.release(conn)
    
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

    # ============ Two-Tower Recommendations ============

    async def get_user_embedding(
        self,
        user_id: int,
        max_positive_reactions: int = 100,
    ) -> Optional[np.ndarray]:
        """
        Build user embedding as an average of embeddings of positively reacted posts.
        This keeps the user profile purely "what user likes".
        """
        conn = await self._acquire_connection()
        try:
            # Use READ COMMITTED for faster reads (no blocking)
            await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            rows = await conn.fetch(
                """
                SELECT 
                    p.text_embedding
                FROM reactions r
                JOIN posts p ON r.post_id = p.id
                WHERE r.user_id = $1
                  AND r.reaction > 0
                  AND p.text_embedding IS NOT NULL
                ORDER BY r.created_at DESC
                LIMIT $2
                """,
                user_id,
                max_positive_reactions,
            )
        finally:
            await self.pool.release(conn)

        if not rows:
            logger.debug("get_user_embedding: no positive reactions for user_id=%s", user_id)
            return None

        embs = [np.array(row["text_embedding"], dtype=np.float32) for row in rows]
        if not embs:
            logger.debug(
                "get_user_embedding: no valid embeddings for user_id=%s (rows=%s)",
                user_id,
                len(rows),
            )
            return None

        user_emb = np.mean(embs, axis=0)
        logger.debug(
            "get_user_embedding: built user embedding for user_id=%s from %s positives",
            user_id,
            len(embs),
        )
        return user_emb

    async def get_negative_embedding(
        self,
        user_id: int,
        max_negative_reactions: int = 100,
    ) -> Optional[np.ndarray]:
        """
        Build a single embedding that represents "disliked" content for the user.
        Used as an item-level penalty, not mixed into the main user profile.
        """
        conn = await self._acquire_connection()
        try:
            await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            rows = await conn.fetch(
                """
                SELECT 
                    p.text_embedding
                FROM reactions r
                JOIN posts p ON r.post_id = p.id
                WHERE r.user_id = $1
                  AND r.reaction < 0
                  AND p.text_embedding IS NOT NULL
                ORDER BY r.created_at DESC
                LIMIT $2
                """,
                user_id,
                max_negative_reactions,
            )
        finally:
            await self.pool.release(conn)

        if not rows:
            logger.debug("get_negative_embedding: no negative reactions for user_id=%s", user_id)
            return None

        embs = [np.array(row["text_embedding"], dtype=np.float32) for row in rows]
        if not embs:
            logger.debug(
                "get_negative_embedding: no valid embeddings for user_id=%s (rows=%s)",
                user_id,
                len(rows),
            )
            return None

        neg_emb = np.mean(embs, axis=0)
        # Normalize for stable cosine similarity later
        norm = float(np.linalg.norm(neg_emb))
        if norm > 0:
            neg_emb = neg_emb / norm

        logger.debug(
            "get_negative_embedding: built negative embedding for user_id=%s from %s negatives",
            user_id,
            len(embs),
        )
        return neg_emb

    def _get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Get or create a lock for a user."""
        if user_id not in self._user_locks:
            self._user_locks[user_id] = asyncio.Lock()
        return self._user_locks[user_id]
    
    def _invalidate_user_cache(self, user_id: int):
        """Invalidate caches for a user (called when user reacts to a post)."""
        if user_id in self._recommendation_cache:
            del self._recommendation_cache[user_id]
        if user_id in self._user_embedding_cache:
            del self._user_embedding_cache[user_id]
        # Don't invalidate tags cache - tags don't change when user reacts
        logger.debug("Invalidated caches for user_id=%s", user_id)

    async def get_recommended_posts_for_user(
        self,
        user_id: int,
        limit: int = 10,
        max_positive_reactions: int = 100,
        only_subscribed_tags: bool = True,
    ) -> list[dict]:
        """
        Get top-N recommended posts for a user using a two-tower style approach:
        - User tower: average of embeddings of liked posts.
        - Item tower: text embeddings of posts stored in pgvector.
        Posts already reacted to by user are excluded.
        """
        import time
        start_time = time.time()
        
        # Use a single connection for all operations to avoid pool exhaustion
        conn = await self._acquire_connection()
        try:
            # Don't set isolation level - use default (may be faster)
            
            # 1) Build user embedding (within same connection)
            # Check cache first to avoid expensive query
            t0 = time.time()
            current_time = time.time()
            user_emb = None
            
            if user_id in self._user_embedding_cache:
                cached_time, cached_emb = self._user_embedding_cache[user_id]
                if current_time - cached_time < self._embedding_cache_ttl:
                    user_emb = cached_emb
                    logger.debug(
                        "get_recommended_posts_for_user: using cached user embedding for user_id=%s (age=%.2fs)",
                        user_id,
                        current_time - cached_time,
                    )
            
            if user_emb is None:
                # Fetch from DB
                query_start = time.time()
                rows_pos = await conn.fetch(
                    """
                    SELECT 
                        p.text_embedding
                    FROM reactions r
                    INNER JOIN posts p ON r.post_id = p.id
                    WHERE r.user_id = $1
                      AND r.reaction > 0
                      AND p.text_embedding IS NOT NULL
                    ORDER BY r.created_at DESC
                    LIMIT $2
                    """,
                    user_id,
                    max_positive_reactions,
                )
                query_time = time.time() - query_start
                logger.debug(
                    "get_recommended_posts_for_user: user embedding query took %.2fs, returned %s rows for user_id=%s",
                    query_time,
                    len(rows_pos),
                    user_id,
                )
                
                if rows_pos:
                    embs = [np.array(row["text_embedding"], dtype=np.float32) for row in rows_pos]
                    if embs:
                        user_emb = np.mean(embs, axis=0)
                        # Cache the result
                        self._user_embedding_cache[user_id] = (time.time(), user_emb)
            
            t1 = time.time()
            logger.info(
                "get_recommended_posts_for_user: get_user_embedding took %.2fs for user_id=%s",
                t1 - t0,
                user_id,
            )
            
            # Fallback: if no user embedding (cold start), return fresh posts by subscribed tags
            if user_emb is None:
                logger.info(
                    "get_recommended_posts_for_user: no user embedding for user_id=%s, using fallback (fresh posts by tags)",
                    user_id,
                )
                # Get tags and fallback posts using same connection
                tags_rows = await conn.fetch(
                    "SELECT tag FROM tag_subscriptions WHERE user_id = $1 AND is_active = TRUE",
                    user_id
                )
                tags_filter = [row['tag'] for row in tags_rows] if tags_rows else None
                fallback_recs = await self._get_fallback_recommendations_with_conn(conn, user_id, tags_filter or None, limit)
                logger.info(
                    "get_recommended_posts_for_user: fallback returned %s recommendations for user_id=%s",
                    len(fallback_recs),
                    user_id,
                )
                return fallback_recs

            # 2) Get user's subscribed tags (check cache first)
            t2 = time.time()
            tags_filter: Optional[List[str]] = None
            if only_subscribed_tags:
                current_time = time.time()
                cached_tags = None
                if user_id in self._user_tags_cache:
                    cached_time, cached_tags = self._user_tags_cache[user_id]
                    if current_time - cached_time < self._tags_cache_ttl:
                        tags_filter = cached_tags
                        logger.debug(
                            "get_recommended_posts_for_user: using cached tags for user_id=%s",
                            user_id,
                        )
                
                if tags_filter is None:
                    tags_rows = await conn.fetch(
                        "SELECT tag FROM tag_subscriptions WHERE user_id = $1 AND is_active = TRUE",
                        user_id
                    )
                    tags_filter = [row['tag'] for row in tags_rows] if tags_rows else None
                    # Cache the result (even if empty list)
                    self._user_tags_cache[user_id] = (time.time(), tags_filter)
            t3 = time.time()
            logger.info(
                "get_recommended_posts_for_user: get_user_tags took %.2fs for user_id=%s",
                t3 - t2,
                user_id,
            )

            # 3) Check for negative embedding (within same connection)
            t4 = time.time()
            rows_neg = await conn.fetch(
                """
                SELECT 
                    p.text_embedding
                FROM reactions r
                INNER JOIN posts p ON r.post_id = p.id
                WHERE r.user_id = $1
                  AND r.reaction < 0
                  AND p.text_embedding IS NOT NULL
                ORDER BY r.created_at DESC
                LIMIT $2
                """,
                user_id,
                100,  # max_negative_reactions
            )
            
            neg_emb = None
            if rows_neg:
                neg_embs = [np.array(row["text_embedding"], dtype=np.float32) for row in rows_neg]
                if neg_embs:
                    neg_emb = np.mean(neg_embs, axis=0)
                    norm = float(np.linalg.norm(neg_emb))
                    if norm > 0:
                        neg_emb = neg_emb / norm
            
            need_embeddings = neg_emb is not None
            t5 = time.time()
            logger.info(
                "get_recommended_posts_for_user: get_negative_embedding took %.2fs for user_id=%s",
                t5 - t4,
                user_id,
            )

            # 4) Vector search query (within same connection)
            t6 = time.time()
            if tags_filter:
                if need_embeddings:
                    # Include text_embedding for penalty calculation
                    rows = await conn.fetch(
                        """
                        SELECT 
                            p.id,
                            p.title,
                            p.text,
                            p.author,
                            p.tag,
                            p.source_url,
                            p.media_urls,
                            p.text_embedding,
                            (p.text_embedding <-> $1) AS distance
                        FROM posts p
                        WHERE p.text_embedding IS NOT NULL
                          AND p.tag = ANY($3::text[])
                          AND NOT EXISTS (
                              SELECT 1 FROM reactions r 
                              WHERE r.user_id = $2 AND r.post_id = p.id
                          )
                        ORDER BY p.text_embedding <-> $1
                        LIMIT $4
                        """,
                        user_emb,
                        user_id,
                        tags_filter,
                        limit,
                    )
                else:
                    # Don't fetch text_embedding - faster query
                    # Use NOT EXISTS instead of LEFT JOIN for better performance
                    rows = await conn.fetch(
                        """
                        SELECT 
                            p.id,
                            p.title,
                            p.text,
                            p.author,
                            p.tag,
                            p.source_url,
                            p.media_urls,
                            (p.text_embedding <-> $1) AS distance
                        FROM posts p
                        WHERE p.text_embedding IS NOT NULL
                          AND p.tag = ANY($3::text[])
                          AND NOT EXISTS (
                              SELECT 1 FROM reactions r 
                              WHERE r.user_id = $2 AND r.post_id = p.id
                          )
                        ORDER BY p.text_embedding <-> $1
                        LIMIT $4
                        """,
                        user_emb,
                        user_id,
                        tags_filter,
                        limit,
                    )
            else:
                if need_embeddings:
                    rows = await conn.fetch(
                        """
                        SELECT 
                            p.id,
                            p.title,
                            p.text,
                            p.author,
                            p.tag,
                            p.source_url,
                            p.media_urls,
                            p.text_embedding,
                            (p.text_embedding <-> $1) AS distance
                        FROM posts p
                        WHERE p.text_embedding IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM reactions r 
                              WHERE r.user_id = $2 AND r.post_id = p.id
                          )
                        ORDER BY p.text_embedding <-> $1
                        LIMIT $3
                        """,
                        user_emb,
                        user_id,
                        limit,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT 
                            p.id,
                            p.title,
                            p.text,
                            p.author,
                            p.tag,
                            p.source_url,
                            p.media_urls,
                            (p.text_embedding <-> $1) AS distance
                        FROM posts p
                        WHERE p.text_embedding IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM reactions r 
                              WHERE r.user_id = $2 AND r.post_id = p.id
                          )
                        ORDER BY p.text_embedding <-> $1
                        LIMIT $3
                        """,
                        user_emb,
                        user_id,
                    limit,
                )
            t7 = time.time()
            logger.info(
                "get_recommended_posts_for_user: vector search query took %.2fs, fetched %s candidates for user_id=%s",
                t7 - t6,
                len(rows),
                user_id,
            )

            # 5) Apply penalty and build recommendations (within same connection)
            recommendations: list[dict] = []
            t8 = time.time()
            for row in rows:
                distance = float(row["distance"])
                # Convert distance to a similarity-like score in [0, 1)
                base_score = 1.0 / (1.0 + distance)

                # Apply penalty for similarity to disliked content (if we have such a profile)
                penalty = 0.0
                sim_neg = None
                if neg_emb is not None and need_embeddings:
                    item_emb_raw = row.get("text_embedding")
                    if item_emb_raw is not None:
                        item_emb = np.array(item_emb_raw, dtype=np.float32)
                        item_norm = float(np.linalg.norm(item_emb))
                        if item_norm > 0:
                            item_emb = item_emb / item_norm
                            # cosine similarity in [-1, 1]; we only penalize for positive similarity
                            sim_neg = float(np.dot(item_emb, neg_emb))
                            if sim_neg > 0:
                                penalty = 0.3 * sim_neg  # 0.3 is a conservative penalty factor

                score = max(0.0, base_score - penalty)
                recommendations.append(
                    {
                        "post_id": row["id"],
                        "title": row["title"],
                        "text": row["text"],
                        "author": row["author"],
                        "tag": row["tag"],
                        "source_url": row["source_url"],
                        "media_urls": row["media_urls"],
                        "score": score,
                    }
                )

                logger.debug(
                    "get_recommended_posts_for_user: user_id=%s post_id=%s base_score=%.4f penalty=%.4f sim_neg=%s final_score=%.4f",
                    user_id,
                    row["id"],
                    base_score,
                    penalty,
                    f"{sim_neg:.4f}" if sim_neg is not None else None,
                    score,
                )
            t9 = time.time()
            logger.info(
                "get_recommended_posts_for_user: penalty calculation took %.2fs for user_id=%s",
                t9 - t8,
                user_id,
            )
        finally:
            await self.pool.release(conn)

            elapsed = time.time() - start_time
            logger.info(
                "get_recommended_posts_for_user: returning %s recommendations for user_id=%s (took %.2fs)",
                len(recommendations),
                user_id,
                elapsed,
            )
            
            # Cache the result
            self._recommendation_cache[user_id] = (time.time(), recommendations)
            
            return recommendations

    async def _get_fallback_recommendations(
        self, user_id: int, limit: int = 10
    ) -> list[dict]:
        """
        Fallback recommendations for cold-start users (no likes yet).
        Returns fresh posts from user's subscribed tags, ordered by creation date.
        """
        # Get user's subscribed tags
        tags_filter = await self.get_user_tags(user_id)

        conn = await self._acquire_connection()
        try:
            await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            if tags_filter:
                logger.debug(
                    "_get_fallback_recommendations: searching posts for user_id=%s with tags=%s",
                    user_id,
                    tags_filter,
                )
                rows = await conn.fetch(
                    """
                    SELECT 
                        p.id,
                        p.title,
                        p.text,
                        p.author,
                        p.tag,
                        p.source_url,
                        p.media_urls
                    FROM posts p
                    LEFT JOIN reactions r 
                        ON r.user_id = $1 AND r.post_id = p.id
                    WHERE r.post_id IS NULL  -- not yet reacted
                      AND p.tag = ANY($2::text[])
                    ORDER BY p.created_at DESC
                    LIMIT $3
                    """,
                    user_id,
                    tags_filter,
                    limit,
                )
            else:
                # No tags subscribed - return any fresh posts
                logger.debug(
                    "_get_fallback_recommendations: searching any posts for user_id=%s (no tags)",
                    user_id,
                )
                rows = await conn.fetch(
                    """
                    SELECT 
                        p.id,
                        p.title,
                        p.text,
                        p.author,
                        p.tag,
                        p.source_url,
                        p.media_urls
                    FROM posts p
                    LEFT JOIN reactions r 
                        ON r.user_id = $1 AND r.post_id = p.id
                    WHERE r.post_id IS NULL  -- not yet reacted
                    ORDER BY p.created_at DESC
                    LIMIT $2
                    """,
                    user_id,
                    limit,
                )
        finally:
            await self.pool.release(conn)
            
        logger.info(
            "_get_fallback_recommendations: found %s posts in DB for user_id=%s",
            len(rows),
            user_id,
        )

        recommendations: list[dict] = []
        for row in rows:
            # Use a default score for fallback (0.5 = neutral)
            recommendations.append(
                {
                    "post_id": row["id"],
                    "title": row["title"],
                    "text": row["text"],
                    "author": row["author"],
                    "tag": row["tag"],
                    "source_url": row["source_url"],
                    "media_urls": row["media_urls"],
                    "score": 0.5,  # Neutral score for cold-start
                }
            )

        logger.info(
            "get_recommended_posts_for_user: fallback returned %s recommendations for user_id=%s (tags=%s)",
            len(recommendations),
            user_id,
            tags_filter,
        )

        return recommendations

    async def _get_fallback_recommendations_with_conn(
        self, conn, user_id: int, tags_filter: Optional[List[str]], limit: int = 10
    ) -> list[dict]:
        """
        Fallback recommendations for cold-start users (no likes yet).
        Uses provided connection instead of creating a new one.
        Returns fresh posts from user's subscribed tags, ordered by creation date.
        """
        if tags_filter:
            logger.debug(
                "_get_fallback_recommendations_with_conn: searching posts for user_id=%s with tags=%s",
                user_id,
                tags_filter,
            )
            rows = await conn.fetch(
                """
                SELECT 
                    p.id,
                    p.title,
                    p.text,
                    p.author,
                    p.tag,
                    p.source_url,
                    p.media_urls
                FROM posts p
                WHERE NOT EXISTS (
                    SELECT 1 FROM reactions r 
                    WHERE r.user_id = $1 AND r.post_id = p.id
                )
                  AND p.tag = ANY($2::text[])
                ORDER BY p.created_at DESC
                LIMIT $3
                """,
                user_id,
                tags_filter,
                limit,
            )
        else:
            # No tags subscribed - return any fresh posts
            logger.debug(
                "_get_fallback_recommendations_with_conn: searching any posts for user_id=%s (no tags)",
                user_id,
            )
            rows = await conn.fetch(
                """
                SELECT 
                    p.id,
                    p.title,
                    p.text,
                    p.author,
                    p.tag,
                    p.source_url,
                    p.media_urls
                FROM posts p
                WHERE NOT EXISTS (
                    SELECT 1 FROM reactions r 
                    WHERE r.user_id = $1 AND r.post_id = p.id
                )
                ORDER BY p.created_at DESC
                LIMIT $2
                """,
                user_id,
                limit,
            )
            
        logger.info(
            "_get_fallback_recommendations_with_conn: found %s posts in DB for user_id=%s",
            len(rows),
            user_id,
        )

        recommendations: list[dict] = []
        for row in rows:
            # Use a default score for fallback (0.5 = neutral)
            recommendations.append(
                {
                    "post_id": row["id"],
                    "title": row["title"],
                    "text": row["text"],
                    "author": row["author"],
                    "tag": row["tag"],
                    "source_url": row["source_url"],
                    "media_urls": row["media_urls"],
                    "score": 0.5,  # Neutral score for cold-start
                }
            )

        return recommendations


# Global database instance
db = Database()
