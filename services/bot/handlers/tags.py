"""Tag subscription management handlers."""
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database import db

router = Router()

# Popular Medium tags
POPULAR_TAGS = [
    "programming", "technology", "software-development",
    "artificial-intelligence", "machine-learning", "data-science",
    "python", "javascript", "web-development",
    "startup", "entrepreneurship", "productivity",
    "science", "design", "ux"
]


def get_tags_keyboard(user_tags: list[str]) -> InlineKeyboardMarkup:
    """Create keyboard with popular tags."""
    buttons = []
    for tag in POPULAR_TAGS:
        is_subscribed = tag in user_tags
        emoji = "✅" if is_subscribed else "➕"
        buttons.append(
            InlineKeyboardButton(
                text=f"{emoji} {tag}",
                callback_data=f"toggle_tag:{tag}"
            )
        )
    
    # Arrange in rows of 2
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@router.message(Command("tags"))
async def cmd_tags(message: Message):
    """Show tag subscription menu."""
    user_id = message.from_user.id
    await db.get_or_create_user(user_id, message.from_user.username)
    
    user_tags = await db.get_user_tags(user_id)
    
    await message.answer(
        "📚 <b>Выбери интересующие тебя темы:</b>\n\n"
        "Нажми на тег чтобы подписаться/отписаться.\n"
        "Я буду присылать статьи с Medium по выбранным темам.",
        parse_mode="HTML",
        reply_markup=get_tags_keyboard(user_tags)
    )


@router.message(Command("add_tag"))
async def cmd_add_tag(message: Message):
    """Handle /add_tag command."""
    user_id = message.from_user.id
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "❌ Укажи тег для добавления.\n\n"
            "Пример: /add_tag programming\n\n"
            "Или используй /tags для выбора из списка."
        )
        return
    
    tag = args[1].strip().lower().replace(" ", "-")
    
    await db.add_tag_subscription(user_id, tag)
    
    await message.answer(
        f"✅ Тег <b>{tag}</b> добавлен!\n\n"
        f"Теперь я буду присылать тебе статьи по этой теме.",
        parse_mode="HTML"
    )


@router.message(Command("my_tags"))
async def cmd_my_tags(message: Message):
    """Show user's subscribed tags."""
    user_id = message.from_user.id
    
    tags = await db.get_user_tags(user_id)
    
    if not tags:
        await message.answer(
            "📭 У тебя пока нет подписок на теги.\n\n"
            "Используй /tags чтобы выбрать интересные темы."
        )
        return
    
    tags_list = "\n".join(f"• {tag}" for tag in tags)
    await message.answer(
        f"<b>📚 Твои теги ({len(tags)}):</b>\n\n{tags_list}\n\n"
        f"Используй /tags чтобы изменить подписки.",
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("toggle_tag:"))
async def callback_toggle_tag(callback: CallbackQuery):
    """Handle tag toggle callback."""
    user_id = callback.from_user.id
    tag = callback.data.split(":")[1]
    
    user_tags = await db.get_user_tags(user_id)
    
    if tag in user_tags:
        await db.remove_tag_subscription(user_id, tag)
        await callback.answer(f"❌ Отписался от {tag}")
    else:
        await db.add_tag_subscription(user_id, tag)
        await callback.answer(f"✅ Подписался на {tag}")
    
    # Update keyboard
    user_tags = await db.get_user_tags(user_id)
    await callback.message.edit_reply_markup(
        reply_markup=get_tags_keyboard(user_tags)
    )

