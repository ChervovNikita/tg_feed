"""ML Model for post classification."""
import logging
import pickle
from typing import Optional
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from config import settings
from database import db

logger = logging.getLogger(__name__)


class UserModelManager:
    """Manages ML models for users."""
    
    def __init__(self):
        # Cache of loaded models {user_id: (model, threshold)}
        self._models_cache: dict[int, tuple[LogisticRegression, float]] = {}
    
    def _prepare_features(
        self,
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray]
    ) -> np.ndarray:
        """Prepare feature vector from embeddings."""
        dim = settings.embedding_dimension
        
        # Default to zeros if embeddings are missing
        if text_embedding is None:
            text_embedding = np.zeros(dim, dtype=np.float32)
        if image_embedding is None:
            image_embedding = np.zeros(dim, dtype=np.float32)
        
        # Concatenate embeddings
        features = np.concatenate([text_embedding, image_embedding])
        return features
    
    async def predict(
        self,
        user_id: int,
        text_embedding: Optional[np.ndarray],
        image_embedding: Optional[np.ndarray]
    ) -> tuple[float, float]:
        """
        Predict relevance score for a post.
        Returns (score, threshold).
        """
        # Get or load model
        model, threshold = await self._get_model(user_id)
        
        if model is None:
            # No model yet, use default threshold
            return 0.5, settings.default_threshold
        
        # Prepare features
        features = self._prepare_features(text_embedding, image_embedding)
        
        # Predict probability
        try:
            proba = model.predict_proba(features.reshape(1, -1))[0]
            # Get probability of positive class
            score = float(proba[1]) if len(proba) > 1 else float(proba[0])
            return score, threshold
        except Exception as e:
            logger.error(f"Error predicting: {e}")
            return 0.5, threshold
    
    async def _get_model(self, user_id: int) -> tuple[Optional[LogisticRegression], float]:
        """Get model from cache or database."""
        # Check cache first
        if user_id in self._models_cache:
            return self._models_cache[user_id]
        
        # Load from database
        model_data = await db.get_user_model(user_id)
        
        if model_data is None or model_data.get('model_weights') is None:
            return None, settings.default_threshold
        
        try:
            model = pickle.loads(model_data['model_weights'])
            threshold = model_data.get('threshold', settings.default_threshold)
            
            # Cache it
            self._models_cache[user_id] = (model, threshold)
            
            return model, threshold
        except Exception as e:
            logger.error(f"Error loading model for user {user_id}: {e}")
            return None, settings.default_threshold
    
    async def train_model(self, user_id: int) -> tuple[bool, Optional[float], int]:
        """
        Train a new model for user.
        Returns (success, accuracy, num_samples).
        """
        # Get training data
        training_data = await db.get_training_data(user_id)
        
        if len(training_data) < settings.min_samples_for_training:
            logger.info(f"Not enough samples for user {user_id}: {len(training_data)}")
            return False, None, len(training_data)
        
        # Prepare features and labels
        X = []
        y = []
        
        for row in training_data:
            text_emb = row.get('text_embedding')
            image_emb = row.get('image_embedding')
            reaction = row['reaction']
            
            # Convert numpy arrays
            if text_emb is not None:
                text_emb = np.array(text_emb, dtype=np.float32)
            if image_emb is not None:
                image_emb = np.array(image_emb, dtype=np.float32)
            
            features = self._prepare_features(text_emb, image_emb)
            X.append(features)
            
            # Convert reaction to binary: 1 (like) -> 1, -1 (dislike) -> 0
            y.append(1 if reaction > 0 else 0)
        
        X = np.array(X)
        y = np.array(y)
        
        # Train/test split
        if len(X) >= 10:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
        else:
            X_train, X_test, y_train, y_test = X, X, y, y
        
        # Train model
        try:
            model = LogisticRegression(
                max_iter=1000,
                class_weight='balanced',
                random_state=42
            )
            model.fit(X_train, y_train)
            
            # Calculate accuracy
            y_pred = model.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            # Serialize model
            model_bytes = pickle.dumps(model)
            
            # Save to database
            await db.save_user_model(
                user_id=user_id,
                model_weights=model_bytes,
                threshold=settings.default_threshold,
                accuracy=accuracy,
                num_samples=len(training_data)
            )
            
            # Update cache
            self._models_cache[user_id] = (model, settings.default_threshold)
            
            logger.info(f"Trained model for user {user_id}: accuracy={accuracy:.3f}, samples={len(training_data)}")
            
            return True, accuracy, len(training_data)
            
        except Exception as e:
            logger.error(f"Error training model for user {user_id}: {e}")
            return False, None, len(training_data)
    
    def invalidate_cache(self, user_id: int):
        """Remove user model from cache."""
        if user_id in self._models_cache:
            del self._models_cache[user_id]
    
    def reload_model(self, user_id: int):
        """Force reload model from database."""
        self.invalidate_cache(user_id)


# Global model manager
model_manager = UserModelManager()

