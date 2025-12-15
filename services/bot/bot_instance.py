"""Bot instance holder to avoid circular imports."""
from typing import Optional
from aiogram import Bot

_bot: Optional[Bot] = None


def set_bot(bot: Bot):
    """Set bot instance."""
    global _bot
    _bot = bot


def get_bot() -> Optional[Bot]:
    """Get bot instance."""
    return _bot

