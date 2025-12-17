"""Two-tower style recommender: user and item in a shared embedding space."""
import logging
from typing import Optional

import numpy as np

from config import settings
from database import db

logger = logging.getLogger(__name__)


class UserModelManager:
    """
    Two-tower style scoring instead of per-user logistic regression.

    - User tower: average of embeddings of positively reacted posts
      (implemented in `db.get_user_embedding`).
    - Item tower: embedding of current post (text/image).
    - Score: cosine similarity mapped to [0, 1].
    """

    async def _get_item_embedding(
        self,
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray],
    ) -> Optional[np.ndarray]:
        """
        Build a single item embedding from available modalities.
        For now we prefer text; if нет текста, используем картинку.
        """
        if text_embedding is not None:
            return np.array(text_embedding, dtype=np.float32)
        if image_embedding is not None:
            return np.array(image_embedding, dtype=np.float32)
        return None

    async def predict(
        self,
        user_id: int,
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray],
    ) -> tuple[float, float]:
        """
        Predict relevance score for a post using two-tower embeddings.
        Returns (score, threshold).
        """
        try:
            item_emb = await self._get_item_embedding(text_embedding, image_embedding)
            if item_emb is None:
                logger.info(
                    "predict: no item embedding for user_id=%s (text=%s, image=%s)",
                    user_id,
                    text_embedding is not None,
                    image_embedding is not None,
                )
                return 0.5, settings.default_threshold

            user_emb = await db.get_user_embedding(user_id=user_id)
            if user_emb is None:
                # Нет пользовательского профиля — нейтральный скор
                logger.info(
                    "predict: no user embedding for user_id=%s, returning neutral score",
                    user_id,
                )
                return 0.5, settings.default_threshold

            # Cosine similarity
            num = float(np.dot(user_emb, item_emb))
            denom = float(np.linalg.norm(user_emb) * np.linalg.norm(item_emb))
            if denom == 0:
                logger.warning(
                    "predict: zero norm encountered for user_id=%s, returning neutral score",
                    user_id,
                )
                return 0.5, settings.default_threshold

            sim = num / denom  # в [-1, 1]
            # Маппим в [0, 1]
            score = 0.5 * (sim + 1.0)
            logger.info(
                "predict: user_id=%s sim=%.4f score=%.4f",
                user_id,
                sim,
                score,
            )
            return score, settings.default_threshold

        except Exception as e:
            logger.error(f"Error in two-tower predict for user_id={user_id}: {e}")
            return 0.5, settings.default_threshold

    async def train_model(self, user_id: int) -> tuple[bool, Optional[float], int]:
        """
        Stub to keep API compatible with old training endpoints.
        Two-tower вариант не требует отдельного обучения per-user модели.
        """
        logger.info(
            f"train_model called for user {user_id}, "
            "but logistic regression training is disabled in two-tower setup."
        )
        # num_samples можно оценить по количеству реакций, если понадобится
        return False, None, 0

    def invalidate_cache(self, user_id: int):
        """No-op for compatibility; two-tower variant не кэширует per-user модели."""
        return None

    def reload_model(self, user_id: int):
        """No-op for compatibility."""
        return None


# Global model manager (now two-tower based)
model_manager = UserModelManager()

