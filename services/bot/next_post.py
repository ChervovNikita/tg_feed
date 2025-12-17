"""Next-post logic and background checker (no circular imports)."""
import asyncio
import logging
from typing import Optional

import aiohttp
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import settings
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
    post = await _fetch_next_post_from_ml(user_id)

    if not post:
        logger.info("send_next_post_to_user: no recommendations for user_id=%s", user_id)
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

        # Best-effort: notify ML service that this post was actually delivered
        try:
            await _notify_post_sent_to_ml(user_id=user_id, post_id=post["post_id"])
        except Exception as e:
            logger.warning(
                "_notify_post_sent_to_ml failed for user_id=%s post_id=%s: %s",
                user_id,
                post["post_id"],
                e,
            )

        # User is no longer waiting
        await db.set_user_waiting(user_id, False)

        logger.info(
            f"Sent post {post['post_id']} to user {user_id} (score: {score:.2f})"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to send message to user {user_id}: {e}")
        return False


async def _fetch_next_post_from_ml(user_id: int) -> Optional[dict]:
    """
    Fetch single recommended post for user from ML service (/recommend).
    """
    import time
    
    base_url = settings.ml_service_url.rstrip("/")
    url = f"{base_url}/recommend/{user_id}"

    logger.info("_fetch_next_post_from_ml: requesting recommendations for user_id=%s from %s", user_id, url)

    # Use longer timeout for first request (cold start can be slow)
    timeout = aiohttp.ClientTimeout(total=60, connect=15)
    
    for attempt in range(2):  # Retry once
        start_time = time.time()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params={"limit": 1, "track": "true"}) as resp:
                    elapsed = time.time() - start_time
                    logger.debug("_fetch_next_post_from_ml: got response in %.2fs for user_id=%s", elapsed, user_id)
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(
                            "Failed to get recommendation from ML service for user_id=%s: status=%s body=%s",
                            user_id,
                            resp.status,
                            text,
                        )
                        return None

                    data = await resp.json()
                    recs = data.get("recommendations") or []
                    logger.info(
                        "_fetch_next_post_from_ml: got %s recommendations for user_id=%s",
                        len(recs),
                        user_id,
                    )
                    
                    if not recs:
                        logger.info("_fetch_next_post_from_ml: empty recommendations list for user_id=%s", user_id)
                        return None

                    rec = recs[0]
                    logger.info(
                        "_fetch_next_post_from_ml: returning post_id=%s title=%s for user_id=%s",
                        rec.get("post_id"),
                        rec.get("title", "No title")[:50],
                        user_id,
                    )
                    
                    # Normalize field names to what send_next_post_to_user expects
                    return {
                        "post_id": rec.get("post_id"),
                        "title": rec.get("title"),
                        "text": rec.get("text"),
                        "author": rec.get("author"),
                        "tag": rec.get("tag"),
                        "source_url": rec.get("source_url"),
                        "media_urls": rec.get("media_urls") or [],
                        "score": rec.get("score", 0.0),
                    }

        except asyncio.TimeoutError:
            logger.warning(
                "Timeout fetching recommendation for user_id=%s (attempt %s/2)",
                user_id,
                attempt + 1,
            )
            if attempt == 0:
                await asyncio.sleep(1)  # Wait before retry
                continue
            return None
        except aiohttp.ClientConnectorError as e:
            logger.error(
                "Cannot connect to ML service for user_id=%s: %s. Is ml-service running?",
                user_id,
                e,
            )
            return None
        except aiohttp.ClientError as e:
            logger.error("Network error fetching recommendation for user_id=%s: %s", user_id, e)
            if attempt == 0:
                await asyncio.sleep(1)
                continue
            return None
        except Exception as e:
            logger.error("Unexpected error fetching recommendation for user_id=%s: %s", user_id, e, exc_info=True)
            return None
    
    return None


async def _notify_post_sent_to_ml(user_id: int, post_id: int) -> None:
    """
    Notify ML service that a post was successfully delivered to the user.
    This powers Grafana "sent" counters and Streamlit "sent" status.
    """
    base_url = settings.ml_service_url.rstrip("/")
    url = f"{base_url}/events/post_sent"
    timeout = aiohttp.ClientTimeout(total=5, connect=2)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json={"user_id": user_id, "post_id": post_id}) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"ml-service post_sent failed: status={resp.status} body={text}")


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


