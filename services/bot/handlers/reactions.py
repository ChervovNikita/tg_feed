"""Reaction handlers."""
import json
from aiogram import Router, F
from aiogram.types import CallbackQuery
from kafka import KafkaProducer

from config import settings
from database import db

router = Router()

# Kafka producer for reactions
_producer = None


def get_producer() -> KafkaProducer:
    """Get or create Kafka producer."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
    return _producer


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
    reaction_map = {
        "like": 1,
        "dislike": -1
    }
    
    reaction_value = reaction_map.get(reaction_type)
    if reaction_value is None:
        await callback.answer("❌ Неизвестная реакция")
        return
    
    # Send reaction to Kafka
    try:
        producer = get_producer()
        producer.send('reactions', value={
            'user_id': user_id,
            'post_id': post_id,
            'reaction': reaction_value
        })
        producer.flush()
    except Exception as e:
        print(f"Error sending reaction to Kafka: {e}")
    
    # Update UI
    if reaction_type == "like":
        await callback.answer("👍 Записал! Буду показывать больше такого")
    else:
        await callback.answer("👎 Понял! Буду показывать меньше такого")
    
    # Remove reaction buttons
    await callback.message.edit_reply_markup(reply_markup=None)


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
