"""Start and basic command handlers."""
from aiogram import Router, F
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
        "👋 <b>Привет! Я бот для фильтрации постов из Telegram каналов.</b>\n\n"
        "Я помогу тебе не пропустить интересные посты и отфильтровать ненужное.\n\n"
        "<b>Как это работает:</b>\n"
        "1️⃣ Добавь каналы командой /add_channel\n"
        "2️⃣ Я буду присылать тебе посты из этих каналов\n"
        "3️⃣ Ставь реакции 👍 или 👎 на посты\n"
        "4️⃣ Со временем я научусь понимать твои предпочтения и буду показывать только интересное!\n\n"
        "<b>Команды:</b>\n"
        "/add_channel - добавить канал\n"
        "/list_channels - список каналов\n"
        "/stats - твоя статистика\n"
        "/help - помощь",
        parse_mode="HTML"
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Handle /help command."""
    await message.answer(
        "<b>📚 Помощь по командам:</b>\n\n"
        "<b>/add_channel</b> @channel_username\n"
        "Добавить канал для отслеживания. Укажи username канала.\n"
        "Пример: /add_channel @durov\n\n"
        "<b>/list_channels</b>\n"
        "Показать список твоих каналов с возможностью удаления.\n\n"
        "<b>/remove_channel</b> @channel_username\n"
        "Удалить канал из списка.\n\n"
        "<b>/stats</b>\n"
        "Показать твою статистику: количество оценённых постов, точность модели.\n\n"
        "<b>Реакции на посты:</b>\n"
        "👍 - нравится (такие посты будут показываться чаще)\n"
        "👎 - не нравится (такие посты будут показываться реже)\n"
        "🔇 - отписаться от канала\n\n"
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
        f"📺 Каналов: {stats['subscriptions']}\n"
        f"📨 Постов получено: {stats['predictions']}\n\n"
        f"<b>Реакции:</b>\n"
        f"👍 Лайков: {stats['likes']}\n"
        f"👎 Дизлайков: {stats['dislikes']}\n"
        f"📊 Всего: {reactions_total}\n\n"
        f"<b>Модель:</b>\n"
        f"{model_status}\n"
        f"Обучена на: {stats['model_samples']} примерах",
        parse_mode="HTML"
    )

