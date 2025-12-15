"""Start and basic command handlers."""
from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from database import db

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    """Handle /start command."""
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Create or update user
    await db.get_or_create_user(user_id, username)
    
    await message.answer(
        "👋 <b>Привет! Я бот-рекомендатель статей с Medium.</b>\n\n"
        "Я помогу тебе находить интересные статьи и отфильтровывать ненужное.\n\n"
        "<b>Как это работает:</b>\n"
        "1️⃣ Выбери интересующие темы командой /tags\n"
        "2️⃣ Я буду присылать тебе статьи по этим темам\n"
        "3️⃣ Ставь реакции 👍 или 👎 на статьи\n"
        "4️⃣ Со временем я научусь понимать твои предпочтения!\n\n"
        "<b>Команды:</b>\n"
        "/tags - выбрать темы\n"
        "/my_tags - мои подписки\n"
        "/stats - твоя статистика\n"
        "/help - помощь",
        parse_mode="HTML"
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Handle /help command."""
    await message.answer(
        "<b>📚 Помощь по командам:</b>\n\n"
        "<b>/tags</b>\n"
        "Открыть меню выбора тем. Нажимай на теги чтобы подписаться/отписаться.\n\n"
        "<b>/add_tag</b> tag_name\n"
        "Добавить произвольный тег.\n"
        "Пример: /add_tag machine-learning\n\n"
        "<b>/my_tags</b>\n"
        "Показать список твоих тегов.\n\n"
        "<b>/stats</b>\n"
        "Показать твою статистику: количество оценённых статей, точность модели.\n\n"
        "<b>Реакции на статьи:</b>\n"
        "👍 - нравится (такие статьи будут показываться чаще)\n"
        "👎 - не нравится (такие статьи будут показываться реже)\n\n"
        "Чем больше реакций ты ставишь, тем лучше я понимаю твои предпочтения!",
        parse_mode="HTML"
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Handle /stats command."""
    user_id = message.from_user.id
    
    stats = await db.get_user_stats(user_id)
    
    model_status = "🔴 Не обучена"
    if stats['model_accuracy'] is not None:
        accuracy_pct = stats['model_accuracy'] * 100
        model_status = f"🟢 Точность: {accuracy_pct:.1f}%"
    elif stats['likes'] + stats['dislikes'] >= 10:
        model_status = "🟡 Скоро будет обучена"
    
    reactions_total = stats['likes'] + stats['dislikes']
    
    await message.answer(
        f"<b>📊 Твоя статистика:</b>\n\n"
        f"🏷️ Тегов: {stats['tags']}\n"
        f"📨 Статей получено: {stats['predictions']}\n\n"
        f"<b>Реакции:</b>\n"
        f"👍 Лайков: {stats['likes']}\n"
        f"👎 Дизлайков: {stats['dislikes']}\n"
        f"📊 Всего: {reactions_total}\n\n"
        f"<b>Модель:</b>\n"
        f"{model_status}\n"
        f"Обучена на: {stats['model_samples']} примерах",
        parse_mode="HTML"
    )
