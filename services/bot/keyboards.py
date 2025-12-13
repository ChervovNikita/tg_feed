"""Keyboard builders for the bot."""
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def get_reaction_keyboard(post_id: int) -> InlineKeyboardMarkup:
    """Build reaction keyboard for a post."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="👍", callback_data=f"react:like:{post_id}"),
        InlineKeyboardButton(text="👎", callback_data=f"react:dislike:{post_id}"),
        InlineKeyboardButton(text="🔇", callback_data=f"react:mute:{post_id}")
    )
    return builder.as_markup()


def get_confirm_keyboard(action: str, data: str) -> InlineKeyboardMarkup:
    """Build confirmation keyboard."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm:{action}:{data}"),
        InlineKeyboardButton(text="❌ Нет", callback_data="cancel")
    )
    return builder.as_markup()


def get_channel_list_keyboard(channels: list[dict]) -> InlineKeyboardMarkup:
    """Build channel list with remove buttons."""
    builder = InlineKeyboardBuilder()
    for channel in channels:
        channel_id = channel['channel_id']
        name = channel.get('channel_name') or channel.get('channel_username') or str(channel_id)
        builder.row(
            InlineKeyboardButton(
                text=f"❌ {name}",
                callback_data=f"remove_channel:{channel_id}"
            )
        )
    return builder.as_markup()

