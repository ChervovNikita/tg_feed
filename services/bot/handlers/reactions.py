"""Reaction and post navigation handlers."""
import json

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from kafka import KafkaProducer

from config import settings
from database import db
from bot_instance import get_bot
from next_post import send_next_post_to_user

router = Router()

# Kafka producer for reactions
_producer = None


def get_producer() -> KafkaProducer:
    """Get or create Kafka producer."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    return _producer


async def _send_next_post(callback: CallbackQuery):
    """Send next post to user after reaction."""
    user_id = callback.from_user.id
    bot = get_bot()

    if bot is None:
        await callback.message.answer("❌ Бот ещё не инициализирован, попробуй позже.")
        return

    sent = await send_next_post_to_user(bot, user_id)

    if not sent:
        # No more posts - mark user as waiting
        await db.set_user_waiting(user_id, True)
        await callback.message.answer(
            "📭 Пока новых статей нет.\n\n"
            "Я напишу тебе, когда появятся интересные материалы!\n"
            "Или используй /next чтобы проверить вручную."
        )


@router.message(Command("next"))
async def cmd_next(message: Message):
    """Handle /next command - show next post."""
    user_id = message.from_user.id
    bot = get_bot()

    if bot is None:
        await message.answer("❌ Бот ещё не инициализирован, попробуй позже.")
        return

    # Check if user has tag subscriptions
    tags = await db.get_user_tags(user_id)
    if not tags:
        await message.answer(
            "❌ Ты не подписан ни на один тег.\n\n"
            "Используй /tags чтобы выбрать интересующие темы."
        )
        return

    sent = await send_next_post_to_user(bot, user_id)

    if not sent:
        await db.set_user_waiting(user_id, True)
        await message.answer(
            "📭 Пока новых статей нет.\n\n"
            "Я напишу тебе, когда появятся интересные материалы!"
        )


@router.callback_query(F.data.startswith("react:"))
async def handle_reaction(callback: CallbackQuery):
    """Handle reaction callback."""
    user_id = callback.from_user.id

    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("❌ Ошибка")
        return

    reaction_type = parts[1]
    post_id = int(parts[2])

    # Map reaction type to value
    reaction_map = {"like": 1, "dislike": -1}

    reaction_value = reaction_map.get(reaction_type)
    if reaction_value is None:
        await callback.answer("❌ Неизвестная реакция")
        return

    # Send reaction to Kafka
    try:
        producer = get_producer()
        producer.send(
            "reactions",
            value={"user_id": user_id, "post_id": post_id, "reaction": reaction_value},
        )
        producer.flush()
    except Exception as e:
        print(f"Error sending reaction to Kafka: {e}")

    # Update UI
    if reaction_type == "like":
        await callback.answer("👍 Записал!")
    else:
        await callback.answer("👎 Понял!")

    # Remove reaction buttons
    await callback.message.edit_reply_markup(reply_markup=None)

    # Send next post
    await _send_next_post(callback)


@router.callback_query(F.data.startswith("skip:"))
async def handle_skip(callback: CallbackQuery):
    """Handle skip callback - show next post without reaction."""
    user_id = callback.from_user.id
    post_id = int(callback.data.split(":")[1])

    # Mark as skipped (sent but no reaction)
    await db.skip_post(user_id, post_id)

    await callback.answer("⏭️ Пропущено")

    # Remove buttons
    await callback.message.edit_reply_markup(reply_markup=None)

    # Send next post
    await _send_next_post(callback)


@router.callback_query(F.data.startswith("open_article:"))
async def handle_open_article(callback: CallbackQuery):
    """Handle open article callback."""
    post_id = int(callback.data.split(":")[1])
    url = await db.get_post_url(post_id)

    if url:
        await callback.answer(url=url)
    else:
        await callback.answer("❌ Статья не найдена")


@router.callback_query(F.data == "cancel")
async def handle_cancel(callback: CallbackQuery):
    """Handle cancel callback."""
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("Отменено")
