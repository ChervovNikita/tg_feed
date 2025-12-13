"""Pyrogram Userbot for reading channel posts."""
import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

import asyncpg
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ChatType
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ChannelListener:
    """Listens to Telegram channels and forwards posts to Kafka."""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self.producer: Optional[KafkaProducer] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.subscribed_channels: set[int] = set()
    
    async def start(self):
        """Start the userbot."""
        # Connect to database
        await self._connect_db()
        
        # Connect Kafka producer
        self._connect_kafka()
        
        # Load subscribed channels
        await self._load_subscriptions()
        
        # Create Pyrogram client
        self.client = Client(
            name="channel_listener",
            api_id=settings.api_id,
            api_hash=settings.api_hash,
            session_string=settings.session_string,
            in_memory=True
        )
        
        # Register message handler
        @self.client.on_message(filters.channel)
        async def handle_channel_message(client: Client, message: Message):
            await self._handle_message(message)
        
        logger.info("Starting userbot...")
        await self.client.start()
        logger.info("Userbot started successfully")
        
        # Keep running and periodically reload subscriptions
        while True:
            await asyncio.sleep(60)  # Reload every minute
            await self._load_subscriptions()
    
    async def _connect_db(self):
        """Connect to database."""
        self.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=5
        )
        logger.info("Database connected")
    
    def _connect_kafka(self):
        """Connect Kafka producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka producer connected")
        except KafkaError as e:
            logger.error(f"Failed to connect Kafka: {e}")
            self.producer = None
    
    async def _load_subscriptions(self):
        """Load all subscribed channels from database."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT channel_id FROM subscriptions 
                WHERE is_active = TRUE
                """
            )
            new_channels = {row['channel_id'] for row in rows}
            
            if new_channels != self.subscribed_channels:
                added = new_channels - self.subscribed_channels
                removed = self.subscribed_channels - new_channels
                
                if added:
                    logger.info(f"New channel subscriptions: {added}")
                if removed:
                    logger.info(f"Removed channel subscriptions: {removed}")
                
                self.subscribed_channels = new_channels
    
    async def _handle_message(self, message: Message):
        """Handle incoming channel message."""
        try:
            channel_id = message.chat.id
            
            # Check if anyone is subscribed to this channel
            if channel_id not in self.subscribed_channels:
                return
            
            # Extract media URLs
            media_urls = []
            if message.photo:
                # For photos, we need to download or get file_id
                # Using file_unique_id as reference
                media_urls.append(f"photo:{message.photo.file_id}")
            elif message.video:
                media_urls.append(f"video:{message.video.file_id}")
            elif message.document:
                if message.document.mime_type and message.document.mime_type.startswith('image'):
                    media_urls.append(f"document:{message.document.file_id}")
            
            # Prepare message data
            post_data = {
                'user_id': 0,  # Will be filled per-user by ML service
                'channel_id': channel_id,
                'message_id': message.id,
                'text': message.text or message.caption or '',
                'media_urls': media_urls,
                'timestamp': message.date.isoformat() if message.date else datetime.now().isoformat()
            }
            
            # Send to Kafka
            if self.producer:
                self.producer.send('raw_posts', value=post_data)
                self.producer.flush()
                logger.info(f"Forwarded post {message.id} from channel {channel_id}")
            else:
                logger.warning("Kafka producer not available")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def stop(self):
        """Stop the userbot."""
        if self.client:
            await self.client.stop()
        if self.producer:
            self.producer.close()
        if self.db_pool:
            await self.db_pool.close()
        logger.info("Userbot stopped")


async def main():
    """Main entry point."""
    listener = ChannelListener()
    
    try:
        await listener.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await listener.stop()


if __name__ == "__main__":
    asyncio.run(main())

