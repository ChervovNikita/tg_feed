"""Kafka consumers for ML Service."""
import asyncio
import json
import logging
from typing import Optional
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import time

from config import settings
from schemas import PostMessage, FilteredPost, ReactionMessage
from database import db
from embeddings import embedding_service
from model import model_manager
from metrics import (
    predictions_total,
    predictions_sent_total,
    reactions_total,
    inference_latency
)

logger = logging.getLogger(__name__)


class KafkaService:
    """Kafka producer/consumer service."""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.consumers: list[KafkaConsumer] = []
        self._running = False
    
    def connect_producer(self):
        """Connect Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("Kafka producer connected")
    
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self.producer is not None
    
    def send_filtered_post(self, post: FilteredPost):
        """Send filtered post to Kafka."""
        if self.producer:
            try:
                self.producer.send(
                    'filtered_posts',
                    value=post.model_dump()
                )
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error sending filtered post: {e}")
    
    async def process_raw_post(self, message: PostMessage):
        """Process a raw post/article from Kafka."""
        start_time = time.time()
        
        try:
            # Get embeddings
            text_emb, image_emb = await embedding_service.get_combined_embedding(
                text=message.text,
                image_urls=message.media_urls
            )
            
            # Save post to database (embeddings will be used later via vector search)
            post_id = await db.save_post(
                source=message.source,
                source_id=message.source_id,
                source_url=message.source_url,
                title=message.title,
                text=message.text,
                author=message.author,
                tag=message.tag,
                media_urls=message.media_urls,
                text_embedding=text_emb,
                image_embedding=image_emb
            )
            
            # Record latency for embedding + write path
            latency = time.time() - start_time
            inference_latency.observe(latency)
            
            logger.info(
                "Processed article id=%s title=%s tag=%s",
                post_id,
                (message.title[:50] if message.title else "No title"),
                message.tag,
            )
            
        except Exception as e:
            logger.error(f"Error processing post: {e}")
    
    async def process_reaction(self, message: ReactionMessage):
        """Process a reaction from Kafka."""
        try:
            # Save reaction
            await db.save_reaction(
                user_id=message.user_id,
                post_id=message.post_id,
                reaction=message.reaction
            )
            
            # Update metrics
            if message.reaction > 0:
                reactions_total.labels(reaction="positive").inc()
            elif message.reaction < 0:
                reactions_total.labels(reaction="negative").inc()
            elif message.reaction == 0:
                reactions_total.labels(reaction="skip").inc()
            
            logger.info(f"Saved reaction from user {message.user_id} for post {message.post_id}")
            
        except Exception as e:
            logger.error(f"Error processing reaction: {e}")
    
    async def start_consumers(self):
        """Start Kafka consumers in background."""
        self._running = True
        
        # Start raw_posts consumer
        asyncio.create_task(self._consume_raw_posts())
        
        # Start reactions consumer
        asyncio.create_task(self._consume_reactions())
        
        logger.info("Kafka consumers started")
    
    async def _consume_raw_posts(self):
        """Consume from raw_posts topic."""
        try:
            consumer = KafkaConsumer(
                'raw_posts',
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=f"{settings.kafka_consumer_group}-posts",
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            self.consumers.append(consumer)
            
            logger.info("Started raw_posts consumer")
            
            while self._running:
                messages = consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        try:
                            post = PostMessage(**msg.value)
                            await self.process_raw_post(post)
                        except Exception as e:
                            logger.error(f"Error parsing post message: {e}")
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in raw_posts consumer: {e}")
    
    async def _consume_reactions(self):
        """Consume from reactions topic."""
        try:
            consumer = KafkaConsumer(
                'reactions',
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=f"{settings.kafka_consumer_group}-reactions",
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            self.consumers.append(consumer)
            
            logger.info("Started reactions consumer")
            
            while self._running:
                messages = consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        try:
                            reaction = ReactionMessage(**msg.value)
                            await self.process_reaction(reaction)
                        except Exception as e:
                            logger.error(f"Error parsing reaction message: {e}")
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in reactions consumer: {e}")
    
    def stop(self):
        """Stop all consumers."""
        self._running = False
        for consumer in self.consumers:
            consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("Kafka service stopped")


# Global Kafka service
kafka_service = KafkaService()
