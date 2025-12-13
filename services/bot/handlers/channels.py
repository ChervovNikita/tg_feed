"""Channel management handlers."""
import re
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from database import db
from keyboards import get_channel_list_keyboard

router = Router()


def parse_channel_input(text: str) -> tuple[str | None, int | None]:
    """
    Parse channel input.
    Returns (username, channel_id) - one of them will be set.
    """
    text = text.strip()
    
    # Check if it's a channel ID (negative number)
    if text.lstrip('-').isdigit():
        return None, int(text)
    
    # Check if it's a username
    if text.startswith('@'):
        return text[1:], None
    
    # Check if it's a t.me link
    match = re.match(r'(?:https?://)?(?:t\.me|telegram\.me)/(\w+)', text)
    if match:
        return match.group(1), None
    
    # Assume it's a username without @
    if re.match(r'^[\w\d_]{5,}$', text):
        return text, None
    
    return None, None


@router.message(Command("add_channel"))
async def cmd_add_channel(message: Message):
    """Handle /add_channel command."""
    user_id = message.from_user.id
    
    # Get channel from command argument
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❌ Укажи канал для добавления.\n\n"
            "Примеры:\n"
            "/add_channel @durov\n"
            "/add_channel https://t.me/durov\n"
            "/add_channel -1001234567890"
        )
        return
    
    channel_input = args[1]
    username, channel_id = parse_channel_input(channel_input)
    
    if username is None and channel_id is None:
        await message.answer(
            "❌ Не удалось распознать канал.\n\n"
            "Укажи username канала (@channel) или его ID."
        )
        return
    
    # If we have username, we need to resolve it to ID
    # For now, we'll store the username and let userbot resolve it
    if username:
        # Use a placeholder ID based on username hash
        # In real scenario, userbot would resolve this
        channel_id = hash(username) % (10**12) * -1  # Make it negative (channel ID format)
        
        await db.add_subscription(
            user_id=user_id,
            channel_id=channel_id,
            channel_name=None,
            channel_username=username
        )
        
        await message.answer(
            f"✅ Канал @{username} добавлен!\n\n"
            f"Теперь я буду присылать тебе посты из этого канала.\n"
            f"Не забывай ставить реакции 👍👎 чтобы я лучше понимал твои интересы!"
        )
    else:
        await db.add_subscription(
            user_id=user_id,
            channel_id=channel_id,
            channel_name=None,
            channel_username=None
        )
        
        await message.answer(
            f"✅ Канал {channel_id} добавлен!\n\n"
            f"Теперь я буду присылать тебе посты из этого канала."
        )


@router.message(Command("list_channels"))
async def cmd_list_channels(message: Message):
    """Handle /list_channels command."""
    user_id = message.from_user.id
    
    channels = await db.get_subscriptions(user_id)
    
    if not channels:
        await message.answer(
            "📭 У тебя пока нет добавленных каналов.\n\n"
            "Добавь первый канал командой:\n"
            "/add_channel @channel_username"
        )
        return
    
    text = f"<b>📺 Твои каналы ({len(channels)}):</b>\n\n"
    for i, ch in enumerate(channels, 1):
        name = ch.get('channel_name') or f"@{ch.get('channel_username')}" or str(ch['channel_id'])
        text += f"{i}. {name}\n"
    
    text += "\nНажми на канал чтобы удалить его:"
    
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=get_channel_list_keyboard(channels)
    )


@router.message(Command("remove_channel"))
async def cmd_remove_channel(message: Message):
    """Handle /remove_channel command."""
    user_id = message.from_user.id
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❌ Укажи канал для удаления.\n\n"
            "Пример: /remove_channel @durov\n\n"
            "Или используй /list_channels для просмотра списка с кнопками удаления."
        )
        return
    
    channel_input = args[1]
    username, channel_id = parse_channel_input(channel_input)
    
    if username:
        channel_id = hash(username) % (10**12) * -1
    
    if channel_id is None:
        await message.answer("❌ Не удалось распознать канал.")
        return
    
    await db.remove_subscription(user_id, channel_id)
    
    await message.answer("✅ Канал удалён из твоего списка.")


@router.callback_query(F.data.startswith("remove_channel:"))
async def callback_remove_channel(callback: CallbackQuery):
    """Handle channel removal callback."""
    user_id = callback.from_user.id
    channel_id = int(callback.data.split(":")[1])
    
    await db.remove_subscription(user_id, channel_id)
    
    # Update message with new list
    channels = await db.get_subscriptions(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📭 Все каналы удалены.\n\n"
            "Добавь новый канал командой:\n"
            "/add_channel @channel_username"
        )
    else:
        text = f"<b>📺 Твои каналы ({len(channels)}):</b>\n\n"
        for i, ch in enumerate(channels, 1):
            name = ch.get('channel_name') or f"@{ch.get('channel_username')}" or str(ch['channel_id'])
            text += f"{i}. {name}\n"
        
        text += "\nНажми на канал чтобы удалить его:"
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=get_channel_list_keyboard(channels)
        )
    
    await callback.answer("✅ Канал удалён")

