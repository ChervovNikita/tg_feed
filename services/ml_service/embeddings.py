"""OpenAI embeddings service."""
import logging
from typing import Optional, List
import numpy as np
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings using OpenAI API."""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.embedding_model
        self.dimension = settings.embedding_dimension
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    async def get_text_embedding(self, text: str) -> Optional[np.ndarray]:
        """Get embedding for text using OpenAI API."""
        if not text or not text.strip():
            return None
        
        try:
            # Truncate long text (OpenAI limit is ~8k tokens)
            text = text[:8000]
            
            response = await self.client.embeddings.create(
                model=self.model,
                input=text,
                dimensions=self.dimension
            )
            
            embedding = np.array(response.data[0].embedding, dtype=np.float32)
            return embedding
            
        except Exception as e:
            logger.error(f"Error getting text embedding: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    async def get_image_embedding(self, image_urls: List[str]) -> Optional[np.ndarray]:
        """
        Get embedding for image.
        Uses GPT-4 Vision to describe the image, then embeds the description.
        """
        if not image_urls:
            return None
        
        descriptions = []

        for image_url in image_urls:
            if not image_url:
                continue
            try:
                # First, get image description using GPT-4 Vision
                vision_response = await self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Describe this image in detail for content classification. Focus on: main subject, visual style, mood, any text visible, and key objects."
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {"url": image_url}
                                }
                            ]
                        }
                    ],
                    max_tokens=300
                )

                description = vision_response.choices[0].message.content
                descriptions.append(description)
            except Exception as e:
                logger.error(f"Error getting image embedding: {e}")
                return None

        if not descriptions:
            return None
        return await self.get_text_embedding('\n'.join(descriptions))

    async def get_combined_embedding(
        self,
        text: Optional[str] = None,
        image_urls: Optional[List[str]] = None
    ) -> tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Get combined embeddings for text and images.
        Returns (text_embedding, image_embedding).
        """
        text_emb = None
        image_emb = None
        
        # Get text embedding
        if text:
            text_emb = await self.get_text_embedding(text)
        
        # Get image embedding (use first image if multiple)
        if image_urls and len(image_urls) > 0:
            image_emb = await self.get_image_embedding(image_urls)
        
        return text_emb, image_emb


# Global embedding service
embedding_service = EmbeddingService()

