"""Main bot application."""
import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import settings
from database import db
from bot_instance import set_bot
from next_post import check_waiting_users
from handlers import start, tags, reactions

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point."""
    # Create bot and dispatcher
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    set_bot(bot)  # Make bot available globally
    dp = Dispatcher()

    # Register routers
    dp.include_router(start.router)
    dp.include_router(tags.router)
    dp.include_router(reactions.router)

    # Connect to database
    await db.connect()
    logger.info("Database connected")

    # Start background checker
    checker_task = asyncio.create_task(check_waiting_users(bot))

    try:
        logger.info("Starting bot...")
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down...")
        checker_task.cancel()
        await db.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
