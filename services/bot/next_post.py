"""Next-post logic and background checker (no circular imports)."""
import asyncio
import logging
from typing import Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database import db

logger = logging.getLogger(__name__)

# Check interval for waiting users (seconds)
CHECK_INTERVAL = 30


def get_article_keyboard(post_id: int, url: Optional[str]) -> InlineKeyboardMarkup:
    """Create keyboard for article with reactions and link."""
    buttons = [
        [
            InlineKeyboardButton(text="👍", callback_data=f"react:like:{post_id}"),
            InlineKeyboardButton(text="👎", callback_data=f"react:dislike:{post_id}"),
            InlineKeyboardButton(text="⏭️ Пропустить", callback_data=f"skip:{post_id}"),
        ]
    ]
    if url:
        buttons.append(
            [InlineKeyboardButton(text="📖 Читать на Medium", url=url)]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def send_next_post_to_user(bot: Bot, user_id: int) -> bool:
    """
    Send next available post to user.
    Returns True if post was sent, False if no posts available.
    """
    post = await db.get_next_post_for_user(user_id)

    if not post:
        return False

    # Build message
    message_parts = []

    if post["title"]:
        message_parts.append(f"<b>{post['title']}</b>")

    if post["author"]:
        message_parts.append(f"✍️ @{post['author']}")

    if post["tag"]:
        message_parts.append(f"🏷️ #{post['tag']}")

    # Score indicator
    score = post["score"]
    if score >= 0.8:
        message_parts.append(f"🔥 Релевантность: {score:.0%}")
    elif score >= 0.6:
        message_parts.append(f"⭐ Релевантность: {score:.0%}")

    message_parts.append("")  # Empty line

    # Add text preview
    if post["text"]:
        preview = post["text"][:500] + "..." if len(post["text"]) > 500 else post["text"]
        message_parts.append(preview)

    message_text = "\n".join(message_parts)

    try:
        keyboard = get_article_keyboard(post["post_id"], post["source_url"])

        await bot.send_message(
            chat_id=user_id,
            text=message_text,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )

        # Mark as sent
        await db.mark_prediction_sent(post["prediction_id"])

        # User is no longer waiting
        await db.set_user_waiting(user_id, False)

        logger.info(
            f"Sent post {post['post_id']} to user {user_id} (score: {score:.2f})"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to send message to user {user_id}: {e}")
        return False


async def check_waiting_users(bot: Bot):
    """Background task to check for new posts for waiting users."""
    logger.info("Started background checker for waiting users")

    while True:
        try:
            waiting_users = await db.get_users_waiting_for_posts()

            for user_id in waiting_users:
                has_posts = await db.has_new_posts_for_user(user_id)

                if has_posts:
                    logger.info(f"Found new posts for waiting user {user_id}")

                    # Notify user
                    try:
                        await bot.send_message(
                            chat_id=user_id,
                            text="🎉 Появились новые статьи! Нажми /next чтобы посмотреть.",
                        )
                        await db.set_user_waiting(user_id, False)
                    except Exception as e:
                        logger.error(f"Failed to notify user {user_id}: {e}")

        except Exception as e:
            logger.error(f"Error in waiting users checker: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


