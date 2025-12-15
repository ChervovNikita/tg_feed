"""Main bot application."""
import asyncio
import json
import logging
from typing import Optional

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import settings
from database import db
from keyboards import get_reaction_keyboard
from handlers import start, tags, reactions

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_article_keyboard(post_id: int, url: str) -> InlineKeyboardMarkup:
    """Create keyboard for article with reactions and link."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👍", callback_data=f"react:like:{post_id}"),
            InlineKeyboardButton(text="👎", callback_data=f"react:dislike:{post_id}"),
        ],
        [
            InlineKeyboardButton(text="📖 Читать на Medium", url=url)
        ]
    ])


class FilteredPostsConsumer:
    """Consumes filtered posts and sends them to users."""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.consumer: Optional[KafkaConsumer] = None
        self._running = False
    
    async def start(self):
        """Start consuming filtered posts."""
        self._running = True
        
        try:
            self.consumer = KafkaConsumer(
                'filtered_posts',
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id='bot-consumer-group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            logger.info("Started filtered_posts consumer")
            
            while self._running:
                messages = self.consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        try:
                            await self._send_article_to_user(msg.value)
                        except Exception as e:
                            logger.error(f"Error sending article to user: {e}")
                
                await asyncio.sleep(0.1)
                
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
    
    async def _send_article_to_user(self, post_data: dict):
        """Send a filtered article to user."""
        user_id = post_data.get('user_id')
        post_id = post_data.get('post_db_id')
        title = post_data.get('title', '')
        text = post_data.get('text', '')
        author = post_data.get('author', '')
        tag = post_data.get('tag', '')
        url = post_data.get('source_url', '')
        score = post_data.get('score', 0)
        
        if not user_id or not post_id:
            return
        
        # Build message
        message_parts = []
        
        if title:
            message_parts.append(f"<b>{title}</b>")
        
        if author:
            message_parts.append(f"✍️ @{author}")
        
        if tag:
            message_parts.append(f"🏷️ #{tag}")
        
        message_parts.append("")  # Empty line
        
        # Add text preview
        if text:
            preview = text[:500] + "..." if len(text) > 500 else text
            message_parts.append(preview)
        
        message_text = "\n".join(message_parts)
        
        try:
            keyboard = get_article_keyboard(post_id, url) if url else get_reaction_keyboard(post_id)
            
            await self.bot.send_message(
                chat_id=user_id,
                text=message_text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            logger.info(f"Sent article {post_id} to user {user_id} (score: {score:.2f})")
        except Exception as e:
            logger.error(f"Failed to send message to user {user_id}: {e}")
    
    def stop(self):
        """Stop the consumer."""
        self._running = False
        if self.consumer:
            self.consumer.close()


async def main():
    """Main entry point."""
    # Create bot and dispatcher
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    
    # Register routers
    dp.include_router(start.router)
    dp.include_router(tags.router)
    dp.include_router(reactions.router)
    
    # Connect to database
    await db.connect()
    logger.info("Database connected")
    
    # Create filtered posts consumer
    consumer = FilteredPostsConsumer(bot)
    
    # Start consumer in background
    consumer_task = asyncio.create_task(consumer.start())
    
    try:
        logger.info("Starting bot...")
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down...")
        consumer.stop()
        consumer_task.cancel()
        await db.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
