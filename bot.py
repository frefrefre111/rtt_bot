import asyncio
import logging
import re
import csv
import aiosqlite
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import os

# ===================================================
# НАСТРОЙКИ
# ===================================================
API_TOKEN = os.getenv('BOT_TOKEN', '8724924899:AAGxvwuLTx8NqWKMAJbRKjjBv0KrVhAeq2k')
ADMIN_IDS = [366181523]

# --- ПОДПИСЬ РАЗРАБОТЧИКА ---
DEVELOPER = "@barselonap1"
CHANNEL = "https://t.me/tennisnextgen"
SIGNATURE = f"\n\n---\n🤖 **Бот разработан:** {DEVELOPER}\n📢 **Наш канал:** {CHANNEL}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ===================================================
# ФИЛЬТР МАТА
# ===================================================
BAD_WORDS = ['хуй', 'пизд', 'бля', 'ебал', 'ебат', 'заеб', 'мудак', 'говно', 
             'срака', 'жопа', 'хер', 'нахер', 'похер', 'дроч', 'пидор', 'гандон']

def filter_bad_words(text: str) -> tuple:
    if not text:
        return text, False
    has_bad = False
    for word in BAD_WORDS:
        if re.search(word, text, re.IGNORECASE):
            has_bad = True
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            text = pattern.sub('***', text)
    return text, has_bad

# ===================================================
# БАЗА ДАННЫХ (АСИНХРОННАЯ)
# ===================================================
async def init_database():
    async with aiosqlite.connect('rtt_bot.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                city TEXT,
                date_start TEXT,
                date_end TEXT,
                venue TEXT,
                surface TEXT,
                court_type TEXT,
                category TEXT,
                gender TEXT,
                is_doubles INTEGER DEFAULT 0,
                added_by INTEGER,
                added_date TIMESTAMP,
                is_deleted INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                user_name TEXT,
                tournament_id INTEGER,
                tournament_name TEXT,
                venue_name TEXT,
                source_group_id INTEGER,
                source_thread_id INTEGER,
                entry_fee INTEGER,
                court_rating INTEGER,
                referee_rating INTEGER,
                light_rating INTEGER,
                transport_rating INTEGER,
                balls_name TEXT,
                balls_rating INTEGER,
                temperature TEXT,
                food_rating INTEGER,
                stringer_rating TEXT,
                toilet_rating INTEGER,
                warmup_rating INTEGER,
                warmup_time TEXT,
                match_viewing TEXT,
                court_availability TEXT,
                accommodation TEXT,
                weather TEXT,
                warmup_comment TEXT,
                general_comment TEXT,
                review_date TIMESTAMP,
                is_anonymous INTEGER DEFAULT 0,
                photo_id TEXT,
                is_deleted INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                group_id INTEGER,
                thread_id INTEGER,
                group_name TEXT,
                is_default INTEGER DEFAULT 0,
                created_at TIMESTAMP,
                UNIQUE(user_id, group_id, thread_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS venue_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venue_name TEXT UNIQUE,
                total_reviews INTEGER DEFAULT 0,
                total_score REAL DEFAULT 0,
                avg_court REAL DEFAULT 0,
                avg_referee REAL DEFAULT 0,
                avg_light REAL DEFAULT 0,
                avg_transport REAL DEFAULT 0,
                avg_balls REAL DEFAULT 0,
                avg_food REAL DEFAULT 0,
                avg_toilet REAL DEFAULT 0,
                avg_warmup REAL DEFAULT 0,
                last_updated TIMESTAMP
            )
        ''')
        await db.commit()
    logger.info("✅ База данных готова")

# ===================================================
# СОСТОЯНИЯ
# ===================================================
class TournamentReview(StatesGroup):
    choose_tournament = State()
    input_entry_fee = State()
    rate_court = State()
    rate_referee = State()
    rate_light = State()
    rate_transport = State()
    input_balls_name = State()
    rate_balls = State()
    rate_temperature = State()
    rate_temperature_outdoor = State()
    rate_food = State()
    rate_stringer = State()
    rate_toilet = State()
    rate_warmup = State()
    rate_warmup_time = State()
    rate_match_viewing = State()
    rate_court_availability = State()
    rate_accommodation = State()
    rate_weather = State()
    input_warmup_comment = State()
    add_comment = State()
    ask_anonymous = State()
    ask_photo = State()
    ask_group = State()
    filter_category = State()
    filter_gender = State()
    filter_doubles = State()
    search_query = State()

class AddTournament(StatesGroup):
    input_name = State()
    input_city = State()
    input_date_start = State()
    input_date_end = State()
    input_venue = State()
    input_surface = State()
    input_court_type = State()
    input_category = State()
    input_gender = State()
    input_doubles = State()

class EditReview(StatesGroup):
    choose_review = State()
    choose_field = State()
    edit_value = State()

# ===================================================
# КЛАВИАТУРЫ
# ===================================================
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🏆 Оценить турнир")],
        [KeyboardButton(text="🔍 Поиск турниров")],
        [KeyboardButton(text="📋 Мои оценки")],
        [KeyboardButton(text="➕ Добавить турнир")],
        [KeyboardButton(text="📅 Все турниры")],
        [KeyboardButton(text="🏟️ Рейтинг теннисных центров")],
        [KeyboardButton(text="🏆 Топ турниров (лучшие и худшие)")],
        [KeyboardButton(text="⚙️ Настройки")]
    ],
    resize_keyboard=True
)

admin_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🏆 Оценить турнир")],
        [KeyboardButton(text="🔍 Поиск турниров")],
        [KeyboardButton(text="📋 Мои оценки")],
        [KeyboardButton(text="➕ Добавить турнир")],
        [KeyboardButton(text="📅 Все турниры")],
        [KeyboardButton(text="🏟️ Рейтинг теннисных центров")],
        [KeyboardButton(text="🏆 Топ турниров (лучшие и худшие)")],
        [KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="👑 Админ-панель")]
    ],
    resize_keyboard=True
)

def rating_keyboard(action: str):
    kb = InlineKeyboardBuilder()
    for i in range(1, 6):
        kb.button(text=f"{i} ⭐", callback_data=f"{action}_{i}")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(5, 1)
    return kb.as_markup()

def anonymous_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔒 Анонимно (отзыв НЕ будет опубликован в группе)", callback_data="anonymous_yes")
    kb.button(text="👤 Публично (отзыв будет опубликован в группе)", callback_data="anonymous_no")
    kb.adjust(1)
    return kb.as_markup()

def photo_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="📸 Добавить фото (можно несколько)", callback_data="photo_add")
    kb.button(text="⏩ Пропустить (без фото)", callback_data="photo_skip")
    kb.adjust(2)
    return kb.as_markup()

def temperature_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🥶 Холодно (некомфортно, мёрзли)", callback_data="temp_cold")
    kb.button(text="🥵 Жарко/Душно (тяжело играть)", callback_data="temp_hot")
    kb.button(text="😎 Комфортно (идеальная температура)", callback_data="temp_good")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(3, 1)
    return kb.as_markup()

def outdoor_temperature_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🥶 Холодно (мёрзли, сложно играть)", callback_data="temp_cold")
    kb.button(text="🥵 Жарко (тяжело, не хватает воды)", callback_data="temp_hot")
    kb.button(text="😎 Комфортно (приятная погода)", callback_data="temp_good")
    kb.button(text="💨 Ветрено (ветер мешал игре)", callback_data="temp_windy")
    kb.button(text="☀️ Солнечно (солнце в глаза, слепит)", callback_data="temp_sunny")
    kb.button(text="🌧 Дождливо (шёл дождь, корт мокрый)", callback_data="temp_rain")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(3, 3, 1)
    return kb.as_markup()

def stringer_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Есть стрингер", callback_data="stringer_yes")
    kb.button(text="❌ Нет стрингера", callback_data="stringer_no")
    kb.button(text="🤷 Не пользовался / не знаю", callback_data="stringer_na")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(3, 1)
    return kb.as_markup()

def yes_no_keyboard(prefix: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да", callback_data=f"{prefix}_yes")
    kb.button(text="❌ Нет", callback_data=f"{prefix}_no")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 1)
    return kb.as_markup()

def match_viewing_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="👁️ Вживую (можно подойти к корту)", callback_data="match_live")
    kb.button(text="📺 Монитор (есть экран/трансляция)", callback_data="match_monitor")
    kb.button(text="📱 Оба варианта", callback_data="match_both")
    kb.button(text="🚫 Невозможно посмотреть", callback_data="match_none")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def court_availability_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🟢 Есть свободное время", callback_data="avail_free")
    kb.button(text="🔴 Нет свободного времени", callback_data="avail_busy")
    kb.button(text="⚪ Неизвестно", callback_data="avail_unknown")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def surface_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎾 Хард", callback_data="surface_hard")
    kb.button(text="🟤 Грунт", callback_data="surface_clay")
    kb.button(text="🔵 Терафлекс", callback_data="surface_teraflex")
    kb.button(text="🌿 Искусственная трава", callback_data="surface_grass")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def court_type_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Закрытый (в помещении)", callback_data="type_indoor")
    kb.button(text="🌳 Открытый (на улице)", callback_data="type_outdoor")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 1)
    return kb.as_markup()

def category_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="👶 9-10 лет", callback_data="cat_9_10")
    kb.button(text="🧒 до 13 лет", callback_data="cat_u13")
    kb.button(text="👦 до 15 лет", callback_data="cat_u15")
    kb.button(text="👨 до 17 лет", callback_data="cat_u17")
    kb.button(text="👨 до 19 лет", callback_data="cat_u19")
    kb.button(text="👨 Взрослые", callback_data="cat_adult")
    kb.button(text="📋 Все категории", callback_data="cat_all")
    kb.button(text="🔙 Назад", callback_data="back_to_filters")
    kb.adjust(2, 2, 2, 1, 1)
    return kb.as_markup()

def gender_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="👨 Мужской", callback_data="gender_male")
    kb.button(text="👩 Женский", callback_data="gender_female")
    kb.button(text="👥 Смешанный", callback_data="gender_mixed")
    kb.button(text="📋 Все", callback_data="gender_all")
    kb.button(text="🔙 Назад", callback_data="back_to_filters")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def doubles_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎾 Одиночные", callback_data="doubles_no")
    kb.button(text="🤝 Парные", callback_data="doubles_yes")
    kb.button(text="📋 Все", callback_data="doubles_all")
    kb.button(text="🔙 Назад", callback_data="back_to_filters")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def filter_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏷️ Категория", callback_data="filter_category")
    kb.button(text="⚥ Пол", callback_data="filter_gender")
    kb.button(text="🤝 Парные/Одиночные", callback_data="filter_doubles")
    kb.button(text="🔍 Поиск по названию", callback_data="filter_search")
    kb.button(text="🔄 Сбросить фильтры", callback_data="filter_reset")
    kb.button(text="✅ Применить", callback_data="filter_apply")
    kb.button(text="🔙 Назад", callback_data="back_to_menu")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()

def weather_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="☀️ Солнце в глаза на подаче", callback_data="weather_sun_eyes")
    kb.button(text="💨 Ветрено", callback_data="weather_windy")
    kb.button(text="🌧 Дождливо", callback_data="weather_rain")
    kb.button(text="☁️ Облачно", callback_data="weather_cloudy")
    kb.button(text="🌫 Туман", callback_data="weather_fog")
    kb.button(text="❄️ Снег/Холодно", callback_data="weather_snow")
    kb.button(text="✅ Готово", callback_data="weather_done")
    kb.button(text="🔙 Назад", callback_data="back_to_prev")
    kb.adjust(2, 2, 2, 1, 1)
    return kb.as_markup()

def skip_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏩ Пропустить", callback_data="skip_comment")
    return kb.as_markup()

def edit_review_keyboard(review_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить турнирный взнос", callback_data=f"edit_{review_id}_fee")
    kb.button(text="✏️ Изменить оценку корта", callback_data=f"edit_{review_id}_court")
    kb.button(text="✏️ Изменить оценку судей", callback_data=f"edit_{review_id}_referee")
    kb.button(text="✏️ Изменить оценку освещения", callback_data=f"edit_{review_id}_light")
    kb.button(text="✏️ Изменить оценку транспорта", callback_data=f"edit_{review_id}_transport")
    kb.button(text="✏️ Изменить оценку мячей", callback_data=f"edit_{review_id}_balls")
    kb.button(text="✏️ Изменить оценку питания", callback_data=f"edit_{review_id}_food")
    kb.button(text="✏️ Изменить оценку санузла", callback_data=f"edit_{review_id}_toilet")
    kb.button(text="✏️ Изменить оценку разминочной зоны", callback_data=f"edit_{review_id}_warmup")
    kb.button(text="✏️ Изменить общий комментарий", callback_data=f"edit_{review_id}_comment")
    kb.button(text="🔒 Изменить анонимность", callback_data=f"edit_{review_id}_anonymous")
    kb.button(text="🗑 Удалить этот отзыв", callback_data=f"delete_review_{review_id}")
    kb.button(text="🔙 Назад к моим оценкам", callback_data="back_to_reviews")
    kb.adjust(2, 2, 2, 2, 2, 1)
    return kb.as_markup()

def admin_panel_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Удалить турнир", callback_data="admin_delete_tournament")
    kb.button(text="📊 Все отзывы", callback_data="admin_all_reviews")
    kb.button(text="👥 Список пользователей", callback_data="admin_users")
    kb.button(text="📈 Статистика бота", callback_data="admin_stats")
    kb.button(text="📤 Экспорт данных", callback_data="admin_export")
    kb.button(text="🔙 Назад", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()

def settings_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="📌 Мои группы", callback_data="settings_groups")
    kb.button(text="⭐ Выбрать группу по умолчанию", callback_data="settings_default_group")
    kb.button(text="🗑 Удалить группу", callback_data="settings_delete_group")
    kb.button(text="🔙 Назад", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()

def add_signature(text: str) -> str:
    return text + SIGNATURE

# ===================================================
# ФУНКЦИИ РАБОТЫ С БД (АСИНХРОННЫЕ)
# ===================================================
async def update_venue_rating(venue_name: str):
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            stats = await db.execute('''
                SELECT COUNT(*), AVG(court_rating), AVG(referee_rating), AVG(light_rating),
                       AVG(transport_rating), AVG(balls_rating), AVG(food_rating),
                       AVG(toilet_rating), AVG(warmup_rating)
                FROM reviews 
                WHERE venue_name = ? AND is_deleted = 0
            ''', (venue_name,))
            stats = await stats.fetchone()
            if stats and stats[0] > 0:
                total_score = (stats[1] + stats[2] + stats[3] + stats[4] + stats[5] + stats[6] + stats[7] + stats[8]) / 8
                await db.execute('''
                    INSERT INTO venue_ratings 
                    (venue_name, total_reviews, total_score, avg_court, avg_referee,
                     avg_light, avg_transport, avg_balls, avg_food, avg_toilet, avg_warmup, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(venue_name) DO UPDATE SET
                    total_reviews = excluded.total_reviews,
                    total_score = excluded.total_score,
                    avg_court = excluded.avg_court,
                    avg_referee = excluded.avg_referee,
                    avg_light = excluded.avg_light,
                    avg_transport = excluded.avg_transport,
                    avg_balls = excluded.avg_balls,
                    avg_food = excluded.avg_food,
                    avg_toilet = excluded.avg_toilet,
                    avg_warmup = excluded.avg_warmup,
                    last_updated = excluded.last_updated
                ''', (venue_name, stats[0], total_score, stats[1], stats[2], stats[3],
                      stats[4], stats[5], stats[6], stats[7], stats[8], datetime.now()))
                await db.commit()
    except Exception as e:
        logger.error(f"Ошибка обновления рейтинга: {e}")

async def get_user_groups(user_id: int) -> List[tuple]:
    async with aiosqlite.connect('rtt_bot.db') as db:
        cursor = await db.execute(
            'SELECT group_id, thread_id, group_name, is_default FROM user_groups WHERE user_id = ? ORDER BY is_default DESC, created_at DESC',
            (user_id,)
        )
        return await cursor.fetchall()

async def add_user_group(user_id: int, group_id: int, thread_id: Optional[int], group_name: str, is_default: bool = False):
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            existing = await db.execute(
                'SELECT id FROM user_groups WHERE user_id = ? AND group_id = ? AND (thread_id = ? OR (thread_id IS NULL AND ? IS NULL))',
                (user_id, group_id, thread_id, thread_id)
            )
            if not await existing.fetchone():
                await db.execute('''
                    INSERT INTO user_groups (user_id, group_id, thread_id, group_name, created_at, is_default)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, group_id, thread_id, group_name, datetime.now(), 1 if is_default else 0))
            else:
                await db.execute('''
                    UPDATE user_groups 
                    SET group_name = ?, created_at = ?
                    WHERE user_id = ? AND group_id = ? AND (thread_id = ? OR (thread_id IS NULL AND ? IS NULL))
                ''', (group_name, datetime.now(), user_id, group_id, thread_id, thread_id))
            await db.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка добавления группы: {e}")
        return False

async def get_tournaments_last_14_days():
    today = datetime.now().date()
    start_date = today - timedelta(days=14)
    start_str = start_date.strftime('%d.%m.%Y')
    today_str = today.strftime('%d.%m.%Y')
    
    async with aiosqlite.connect('rtt_bot.db') as db:
        cursor = await db.execute('''
            SELECT id, name, city, date_start, surface, court_type 
            FROM tournaments 
            WHERE is_deleted = 0 
            AND date_start >= ? AND date_start <= ?
            ORDER BY date_start ASC
        ''', (start_str, today_str))
        return await cursor.fetchall()

async def get_all_tournaments():
    async with aiosqlite.connect('rtt_bot.db') as db:
        cursor = await db.execute('''
            SELECT id, name, city, date_start, surface, court_type 
            FROM tournaments 
            WHERE is_deleted = 0 
            ORDER BY date_start ASC
        ''')
        return await cursor.fetchall()

# ===================================================
# ОБРАБОТЧИКИ
# ===================================================

@dp.callback_query(lambda c: c.data == "back_to_prev")
async def go_back(callback: types.CallbackQuery, state: FSMContext):
    current = await state.get_state()
    back = {
        TournamentReview.rate_court: TournamentReview.input_entry_fee,
        TournamentReview.rate_referee: TournamentReview.rate_court,
        TournamentReview.rate_light: TournamentReview.rate_referee,
        TournamentReview.rate_transport: TournamentReview.rate_light,
        TournamentReview.input_balls_name: TournamentReview.rate_transport,
        TournamentReview.rate_balls: TournamentReview.input_balls_name,
        TournamentReview.rate_temperature: TournamentReview.rate_balls,
        TournamentReview.rate_temperature_outdoor: TournamentReview.rate_balls,
        TournamentReview.rate_food: TournamentReview.rate_temperature,
        TournamentReview.rate_stringer: TournamentReview.rate_food,
        TournamentReview.rate_toilet: TournamentReview.rate_stringer,
        TournamentReview.rate_warmup: TournamentReview.rate_toilet,
        TournamentReview.rate_warmup_time: TournamentReview.rate_warmup,
        TournamentReview.rate_match_viewing: TournamentReview.rate_warmup_time,
        TournamentReview.rate_court_availability: TournamentReview.rate_match_viewing,
        TournamentReview.rate_accommodation: TournamentReview.rate_court_availability,
        TournamentReview.rate_weather: TournamentReview.rate_accommodation,
        TournamentReview.ask_anonymous: TournamentReview.add_comment,
        AddTournament.input_surface: AddTournament.input_venue,
        AddTournament.input_court_type: AddTournament.input_surface,
        AddTournament.input_category: AddTournament.input_venue,
        AddTournament.input_gender: AddTournament.input_category,
        AddTournament.input_doubles: AddTournament.input_gender,
    }
    if current in back:
        await state.set_state(back[current])
        await callback.message.edit_text("🔙 Возврат к предыдущему шагу", reply_markup=None)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_reviews")
async def back_to_reviews(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await my_reviews(callback.message)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    menu = admin_menu if callback.from_user.id in ADMIN_IDS else main_menu
    await callback.message.answer(add_signature("Главное меню:"), reply_markup=menu)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "skip_comment")
async def skip_comment(callback: types.CallbackQuery, state: FSMContext):
    current = await state.get_state()
    if current == TournamentReview.input_balls_name:
        await state.update_data(balls_name="Не указано")
        await state.set_state(TournamentReview.rate_balls)
        await callback.message.edit_text("⚾ **Оцените качество мячей** (1-5):\n(новые/старые, износ, прыгучесть, хватает ли мячей на игру)", reply_markup=rating_keyboard("balls"))
    elif current == TournamentReview.input_warmup_comment:
        await state.update_data(warmup_comment="Комментарий отсутствует")
        await state.set_state(TournamentReview.add_comment)
        await callback.message.edit_text("📝 **Общий комментарий о турнире:**\n(впечатления, организация, что понравилось/не понравилось)", reply_markup=skip_keyboard())
    elif current == TournamentReview.add_comment:
        await state.update_data(general_comment="Комментарий отсутствует")
        await state.set_state(TournamentReview.ask_anonymous)
        await callback.message.edit_text("🔒 **Анонимность отзыва:**\n\nАнонимный отзыв НЕ будет опубликован в группе, но пойдёт в общую статистику турнира и рейтинг теннисного центра.\n\nВыберите вариант:", reply_markup=anonymous_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('temp_'))
async def process_temp(callback: types.CallbackQuery, state: FSMContext):
    temp_map = {'temp_cold': '🥶 Холодно', 'temp_hot': '🥵 Жарко/Душно', 'temp_good': '😎 Комфортно', 'temp_windy': '💨 Ветрено', 'temp_sunny': '☀️ Солнечно', 'temp_rain': '🌧 Дождливо'}
    await state.update_data(temperature=temp_map.get(callback.data, "Не указано"))
    await state.set_state(TournamentReview.rate_food)
    await callback.message.edit_text("🍔 **Оцените точку питания** (1-5):\n(наличие, ассортимент, цены, качество еды)", reply_markup=rating_keyboard("food"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('weather_'))
async def process_weather(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    weather_list = data.get('weather_list', [])
    if callback.data == "weather_done":
        await state.update_data(weather=", ".join(weather_list) if weather_list else "Не указано")
        await state.set_state(TournamentReview.input_warmup_comment)
        await callback.message.edit_text("📝 **Комментарий про разминочную зону:**\n(что есть, чего не хватает, пожелания)", reply_markup=skip_keyboard())
    elif callback.data == "back_to_prev":
        await state.set_state(TournamentReview.rate_accommodation)
        await callback.message.edit_text("🏨 **Возможно ли проживание на турнире:**\n(гостиница, хостел, размещение от организаторов)", reply_markup=yes_no_keyboard("accommodation"))
    else:
        w_map = {"weather_sun_eyes": "☀️ Солнце в глаза на подаче", "weather_windy": "💨 Ветрено", "weather_rain": "🌧 Дождливо", "weather_cloudy": "☁️ Облачно", "weather_fog": "🌫 Туман", "weather_snow": "❄️ Снег/Холодно"}
        wt = w_map.get(callback.data)
        if wt in weather_list:
            weather_list.remove(wt)
        else:
            weather_list.append(wt)
        await state.update_data(weather_list=weather_list)
        text = "Выбранные погодные условия:\n" + "\n".join(weather_list) if weather_list else "Пока ничего не выбрано"
        await callback.message.edit_text(f"🌦 {text}\n\nВыберите погодные условия (можно несколько):", reply_markup=weather_keyboard())
    await callback.answer()

# ===================================================
# СТАРТ И НАСТРОЙКИ
# ===================================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.chat.type in ['group', 'supergroup']:
        thread_id = message.message_thread_id
        group_id = message.chat.id
        group_name = message.chat.title
        user_id = message.from_user.id
        
        display_name = group_name
        if thread_id:
            display_name = f"{group_name} (тема #{thread_id})"
        
        try:
            user_groups = await get_user_groups(user_id)
            is_first_group = len(user_groups) == 0
            success = await add_user_group(user_id, group_id, thread_id, display_name, is_first_group)
            
            if success:
                topic_text = " в теме" if thread_id else ""
                await message.answer(
                    f"✅ **Группа сохранена{topic_text}!**\n\n"
                    f"📌 Название: {group_name}\n"
                    f"{'📎 ID темы: ' + str(thread_id) if thread_id else ''}\n\n"
                    f"Теперь вы можете оценивать турниры в личных сообщениях.\n"
                    f"После заполнения отзыва вы сможете выбрать эту группу{topic_text} для публикации.\n\n"
                    f"👉 Нажмите кнопку, чтобы начать:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🏆 ОЦЕНИТЬ ТУРНИР", 
                                            url=f"https://t.me/{(await bot.get_me()).username}?start=review")]
                    ])
                )
                
                try:
                    await bot.send_message(
                        user_id,
                        f"✅ **Группа успешно добавлена!**\n\n"
                        f"📌 Название: {group_name}\n"
                        f"🆔 ID: {group_id}\n"
                        f"{'📎 ID темы: ' + str(thread_id) if thread_id else ''}\n\n"
                        f"Теперь вы можете оценивать турниры."
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить личное сообщение: {e}")
        except Exception as e:
            logger.error(f"Ошибка сохранения группы: {e}")
        return
    
    menu = admin_menu if message.from_user.id in ADMIN_IDS else main_menu
    user_groups = await get_user_groups(message.from_user.id)
    
    groups_text = ""
    if user_groups:
        groups_count = len(user_groups)
        groups_text = f"\n\n📌 **У вас сохранено {groups_count} групп** для публикации отзывов."
    
    await message.answer(
        add_signature(
            "👋 **Привет! Я бот для сбора отзывов о теннисных турнирах!**\n\n"
            "❗️ **Как это работает:**\n"
            "1. Добавьте бота в группу, напишите /start\n"
            "2. Вернитесь сюда и нажмите **🏆 Оценить турнир**\n"
            "3. Заполните форму\n"
            "4. В конце выберите группу для публикации\n\n"
            "🔍 **Поиск турниров:**\n"
            "• По категории (возраст)\n"
            "• По полу (М/Ж/Смешанный)\n"
            "• Парные/одиночные\n"
            "• По первым буквам названия\n\n"
            "👇 **Выбери действие:**" + groups_text
        ),
        reply_markup=menu
    )

# ===================================================
# ОЦЕНКА ТУРНИРА
# ===================================================

@dp.message(F.text == "🏆 Оценить турнир")
async def start_review(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    
    user_groups = await get_user_groups(message.from_user.id)
    if not user_groups:
        await message.answer("⚠️ **Нет сохранённых групп!**\n\nСначала добавьте бота в группу и напишите там /start.")
        return
    
    await state.set_state(TournamentReview.choose_tournament)
    await show_tournaments_page(message, state, 0)

async def show_tournaments_page(message: types.Message, state: FSMContext, page: int):
    try:
        tournaments = await get_tournaments_last_14_days()
        
        if not tournaments:
            await message.answer("😕 Нет турниров за последние 14 дней.\n\nВы можете добавить турнир вручную через кнопку ➕ Добавить турнир.")
            await state.clear()
            return
        
        total = len(tournaments)
        per_page = 10
        total_pages = (total + per_page - 1) // per_page
        
        if page >= total_pages:
            page = total_pages - 1
        if page < 0:
            page = 0
        
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total)
        page_tournaments = tournaments[start_idx:end_idx]
        
        kb = InlineKeyboardBuilder()
        for t in page_tournaments:
            dates = t[3][:10] if t[3] else "дата?"
            surf_icon = {"Хард": "🎾", "Грунт": "🟤", "Терафлекс": "🔵", "Искусственная трава": "🌿"}.get(t[4], "🏟️")
            court_icon = "🏠" if t[5] == "Закрытый" else "🌳"
            kb.button(text=f"{dates} | {t[2]} | {surf_icon} {t[4]} {court_icon} | {t[1]}", callback_data=f"rate_{t[0]}")
        
        if page > 0:
            kb.button(text="◀️ Назад", callback_data=f"tournament_page_{page-1}")
        if page < total_pages - 1:
            kb.button(text="Вперед ▶️", callback_data=f"tournament_page_{page+1}")
        
        kb.button(text="🔍 Поиск", callback_data="go_to_search")
        kb.button(text="❌ Отмена", callback_data="cancel_review")
        kb.adjust(1)
        
        await message.answer(
            f"🏆 **Выберите турнир**\n\n📄 Страница {page + 1} из {total_pages}\n📊 Всего: {total}",
            reply_markup=kb.as_markup()
        )
    except Exception as e:
        logger.error(f"Ошибка загрузки турниров: {e}")
        await message.answer(f"❌ Ошибка загрузки: {str(e)[:100]}")
        await state.clear()

@dp.callback_query(lambda c: c.data.startswith('tournament_page_'))
async def change_tournament_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.split('_')[2])
    await show_tournaments_page(callback.message, state, page)
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "go_to_search")
async def go_to_search(callback: types.CallbackQuery, state: FSMContext):
    await search_tournaments_start(callback.message, state)
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_review")
async def cancel_review(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    menu = admin_menu if callback.from_user.id in ADMIN_IDS else main_menu
    await callback.message.edit_text("❌ Отменено")
    await callback.message.answer("Главное меню:", reply_markup=menu)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('rate_'))
async def rate_tournament(callback: types.CallbackQuery, state: FSMContext):
    tid = callback.data.split('_')[1]
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT name, court_type, venue FROM tournaments WHERE id = ? AND is_deleted = 0', (tid,))
            t = await cursor.fetchone()
            if not t:
                await callback.message.edit_text("❌ Турнир был удалён")
                await callback.answer()
                return
        await state.update_data(tournament_id=tid, tournament_name=t[0], court_type=t[1], venue_name=t[2])
        await state.set_state(TournamentReview.input_entry_fee)
        await callback.message.edit_text("💰 Введите **турнирный взнос** в рублях:\nНапример: 1500, 2000, 3500")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка выбора турнира: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.message(TournamentReview.input_entry_fee)
async def get_fee(message: types.Message, state: FSMContext):
    try:
        fee = int(message.text)
        if fee < 0:
            raise ValueError
        await state.update_data(entry_fee=fee)
        await state.set_state(TournamentReview.rate_court)
        await message.answer("🎾 **Оцените подготовку корта/площадки** (1-5 ⭐):\n(качество покрытия, ровность, разметка, состояние сеток)", reply_markup=rating_keyboard("court"))
    except ValueError:
        await message.answer("❌ Введите положительное число! Например: 1500")

@dp.callback_query(lambda c: c.data.startswith('court_'))
async def get_court(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(court_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_referee)
    await callback.message.edit_text("👨‍⚖️ **Оцените работу судей** (1-5 ⭐):\n(компетентность, внимательность, объективность, вежливость)", reply_markup=rating_keyboard("referee"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('referee_'))
async def get_referee(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(referee_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_light)
    await callback.message.edit_text("💡 **Оцените освещение корта** (1-5 ⭐):\n(достаточно ли света, нет ли слепящих зон, равномерность)", reply_markup=rating_keyboard("light"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('light_'))
async def get_light(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(light_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_transport)
    await callback.message.edit_text("🚗 **Оцените транспортную доступность** (1-5 ⭐):\n(удобно ли добираться, есть ли парковка, близость метро/ж/д)", reply_markup=rating_keyboard("transport"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('transport_'))
async def get_transport(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(transport_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.input_balls_name)
    await callback.message.edit_text("🏐 **Напишите, какими мячами играли:**\nНапример: Wilson US Open, Babolat, Head\n\nИли нажмите 'Пропустить'", reply_markup=skip_keyboard())
    await callback.answer()

@dp.message(TournamentReview.input_balls_name)
async def get_balls_name(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте.")
        return
    await state.update_data(balls_name=text if text.lower() != 'пропустить' else "Не указано")
    await state.set_state(TournamentReview.rate_balls)
    await message.answer("⚾ **Оцените качество мячей** (1-5 ⭐):\n(новые/старые, износ, прыгучесть, хватает ли мячей на игру)", reply_markup=rating_keyboard("balls"))

@dp.callback_query(lambda c: c.data.startswith('balls_'))
async def get_balls(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(balls_rating=int(callback.data.split('_')[1]))
    data = await state.get_data()
    if data['court_type'] == "Открытый":
        await state.set_state(TournamentReview.rate_temperature_outdoor)
        await callback.message.edit_text("🌡 **Погодные условия и ощущения на улице:**\n(температура, ветер, солнце, осадки)", reply_markup=outdoor_temperature_keyboard())
    else:
        await state.set_state(TournamentReview.rate_temperature)
        await callback.message.edit_text("🌡 **Температура и ощущения в зале:**\n(комфортно, душно, холодно)", reply_markup=temperature_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('food_'))
async def get_food(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(food_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_stringer)
    await callback.message.edit_text("🪡 **Стрингер на турнире:**\n(есть ли возможность натянуть ракетку на месте)", reply_markup=stringer_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('stringer_'))
async def get_stringer(callback: types.CallbackQuery, state: FSMContext):
    m = {'stringer_yes': '✅ Есть стрингер (можно натянуть ракетку на месте)', 'stringer_no': '❌ Нет стрингера (пришлось искать самому)', 'stringer_na': '🤷 Не пользовался / не знаю'}
    await state.update_data(stringer_rating=m[callback.data])
    await state.set_state(TournamentReview.rate_toilet)
    await callback.message.edit_text("🚻 **Оцените состояние санузла** (1-5 ⭐):\n(чистота, наличие бумаги, мыла, туалетной бумаги)", reply_markup=rating_keyboard("toilet"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('toilet_'))
async def get_toilet(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(toilet_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_warmup)
    await callback.message.edit_text("🏋️ **Оцените разминочную зону** (1-5 ⭐):\n(место для разминки, тренажёры, резина, пространство, велотренажёры)", reply_markup=rating_keyboard("warmup"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('warmup_'))
async def get_warmup(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(warmup_rating=int(callback.data.split('_')[1]))
    await state.set_state(TournamentReview.rate_warmup_time)
    await callback.message.edit_text("⏱️ **Специальное время для разминки на корте:**\n(дают ли время размяться непосредственно на корте до матча)", reply_markup=yes_no_keyboard("warmuptime"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('warmuptime_'))
async def get_warmup_time(callback: types.CallbackQuery, state: FSMContext):
    m = {'warmuptime_yes': '✅ Да (дают время размяться на корте)', 'warmuptime_no': '❌ Нет (разминка только за кортом)'}
    await state.update_data(warmup_time=m[callback.data])
    await state.set_state(TournamentReview.rate_match_viewing)
    await callback.message.edit_text("📺 **Просмотр матчей:**\n(можно ли посмотреть игры других участников)", reply_markup=match_viewing_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('match_'))
async def get_match(callback: types.CallbackQuery, state: FSMContext):
    m = {'match_live': '👁️ Вживую (можно подойти к корту и смотреть)', 'match_monitor': '📺 Монитор (есть экран/трансляция)', 'match_both': '📱 Оба варианта (и вживую, и монитор)', 'match_none': '🚫 Невозможно посмотреть матчи других участников'}
    await state.update_data(match_viewing=m[callback.data])
    await state.set_state(TournamentReview.rate_court_availability)
    await callback.message.edit_text("🎾 **Доступность кортов во внеигровое время (платно/бесплатно):**\n(можно ли поиграть после или до своих матчей)", reply_markup=court_availability_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('avail_'))
async def get_avail(callback: types.CallbackQuery, state: FSMContext):
    m = {'avail_free': '🟢 Есть свободное время (можно поиграть после/до матчей)', 'avail_busy': '🔴 Нет свободного времени (корты всегда заняты)', 'avail_unknown': '⚪ Неизвестно (не интересовался)'}
    await state.update_data(court_availability=m[callback.data])
    await state.set_state(TournamentReview.rate_accommodation)
    await callback.message.edit_text("🏨 **Возможно ли проживание на турнире:**\n(гостиница, хостел, размещение от организаторов)", reply_markup=yes_no_keyboard("accommodation"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('accommodation_'))
async def get_accom(callback: types.CallbackQuery, state: FSMContext):
    m = {'accommodation_yes': '✅ Да (есть возможность проживания)', 'accommodation_no': '❌ Нет (приходится искать самому)'}
    await state.update_data(accommodation=m[callback.data])
    data = await state.get_data()
    if data['court_type'] == "Открытый":
        await state.set_state(TournamentReview.rate_weather)
        await callback.message.edit_text("🌦 **Погодные условия:**\n(выберите, что мешало или помогало игре, можно несколько вариантов)", reply_markup=weather_keyboard())
    else:
        await state.update_data(weather="Не применимо (закрытый корт)")
        await state.set_state(TournamentReview.input_warmup_comment)
        await callback.message.edit_text("📝 **Комментарий про разминочную зону:**\n(что есть, чего не хватает, пожелания)", reply_markup=skip_keyboard())
    await callback.answer()

@dp.message(TournamentReview.input_warmup_comment)
async def get_warmup_comment(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте комментарий.")
        return
    await state.update_data(warmup_comment=text if text.lower() != 'пропустить' else "Комментарий отсутствует")
    await state.set_state(TournamentReview.add_comment)
    await message.answer("📝 **Общий комментарий о турнире:**\n(впечатления, организация, что понравилось/не понравилось, пожелания организаторам)", reply_markup=skip_keyboard())

@dp.message(TournamentReview.add_comment)
async def get_comment(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте комментарий.")
        return
    await state.update_data(general_comment=text if text.lower() != 'пропустить' else "Комментарий отсутствует")
    await state.set_state(TournamentReview.ask_anonymous)
    await message.answer("🔒 **Анонимность отзыва:**\n\nАнонимный отзыв НЕ будет опубликован в группе, но пойдёт в общую статистику турнира и рейтинг теннисного центра.\n\nВыберите вариант:", reply_markup=anonymous_keyboard())

@dp.callback_query(lambda c: c.data.startswith('anonymous_'))
async def get_anonymous(callback: types.CallbackQuery, state: FSMContext):
    is_anonymous = 1 if callback.data == "anonymous_yes" else 0
    await state.update_data(is_anonymous=is_anonymous)
    await state.set_state(TournamentReview.ask_photo)
    await callback.message.edit_text("📸 **Добавить фото к отзыву?**\n\nВы можете отправить фото корта, питания, разминочной зоны или других моментов.\n\nФото будут видны только в личном сообщении с подтверждением, в группу не публикуются.\n\nВыберите вариант:", reply_markup=photo_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "photo_add")
async def photo_add(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(photos=[])
    await callback.message.edit_text("📸 **Отправьте фото** (можно несколько)\n\n• Отправляйте фото по одному\n• После отправки всех фото нажмите кнопку '✅ Готово'\n\nОтправьте первое фото:")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "photo_skip")
async def photo_skip(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TournamentReview.ask_group)
    await show_group_selection(callback.message, state)
    await callback.message.delete()
    await callback.answer()

@dp.message(F.photo, TournamentReview.ask_photo)
async def handle_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get('photos', [])
    photo_id = message.photo[-1].file_id
    photos.append(photo_id)
    await state.update_data(photos=photos)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Готово (завершить отзыв)", callback_data="photo_done")]])
    await message.answer(f"📸 Фото №{len(photos)} добавлено!\n\nМожете отправить ещё фото или нажать 'Готово'", reply_markup=kb)

@dp.callback_query(lambda c: c.data == "photo_done")
async def photo_done(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TournamentReview.ask_group)
    await show_group_selection(callback.message, state)
    await callback.message.delete()
    await callback.answer()

async def show_group_selection(message: types.Message, state: FSMContext):
    user_groups = await get_user_groups(message.from_user.id)
    
    if not user_groups:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Как добавить группу?", callback_data="add_group_instructions")],
            [InlineKeyboardButton(text="🔒 Только в статистику (не публиковать)", callback_data="publish_none")],
            [InlineKeyboardButton(text="❌ Отменить публикацию", callback_data="cancel_publish")]
        ])
        await message.answer(
            "📌 **У вас нет сохранённых групп для публикации!**\n\n"
            "Чтобы добавить группу:\n"
            "1️⃣ Добавьте бота в группу\n"
            "2️⃣ Напишите в группе команду /start\n"
            "3️⃣ Если у вас включены темы, напишите /start в нужной теме\n"
            "4️⃣ Вернитесь сюда и выберите группу\n\n"
            "Или выберите вариант:",
            reply_markup=kb
        )
        return
    
    kb = InlineKeyboardBuilder()
    default_group = None
    
    for group in user_groups:
        group_id, thread_id, group_name, is_default = group
        if is_default:
            default_group = (group_id, thread_id, group_name)
        icon = "⭐" if is_default else ("📎" if thread_id else "💬")
        name = f"{icon} {group_name}"
        kb.button(text=name, callback_data=f"publish_group_{group_id}_{thread_id or 0}")
    
    kb.button(text="🆕 Добавить новую группу/тему", callback_data="add_group_instructions")
    kb.button(text="🔒 Только в статистику (не публиковать)", callback_data="publish_none")
    kb.button(text="❌ Отменить публикацию", callback_data="cancel_publish")
    kb.adjust(1)
    
    text = "📌 **Выберите группу или тему для публикации отзыва:**\n\n"
    text += "💬 - обычная группа\n"
    text += "📎 - тема в супергруппе\n"
    text += "⭐ - группа по умолчанию\n\n"
    
    if default_group:
        text += f"⭐ Группа по умолчанию: {default_group[2]}\n\n"
    
    text += "Нажмите на группу, чтобы опубликовать отзыв там."
    
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(lambda c: c.data == "add_group_instructions")
async def add_group_instructions(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "📌 **Как добавить группу:**\n\n"
        "1️⃣ Добавьте бота в группу (если ещё не добавили)\n"
        "2️⃣ Напишите в группе команду /start\n"
        "3️⃣ Если у вас включены темы, напишите /start в нужной теме\n"
        "4️⃣ Бот автоматически сохранит группу\n"
        "5️⃣ Вернитесь сюда и нажмите кнопку ниже\n\n"
        "⚠️ **Важно:** Бот не требует прав администратора!\n\n"
        "После добавления группы нажмите кнопку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Проверить группы", callback_data="refresh_groups")],
            [InlineKeyboardButton(text="🔙 Назад к выбору", callback_data="back_to_group_selection")]
        ])
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "refresh_groups")
async def refresh_groups(callback: types.CallbackQuery, state: FSMContext):
    await show_group_selection(callback.message, state)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_group_selection")
async def back_to_group_selection(callback: types.CallbackQuery, state: FSMContext):
    await show_group_selection(callback.message, state)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('publish_group_'))
async def publish_to_group(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split('_')
    group_id = int(parts[2])
    thread_id = int(parts[3]) if len(parts) > 3 and parts[3] != '0' else None
    
    data = await state.get_data()
    data['source_group_id'] = group_id
    data['source_thread_id'] = thread_id
    await finish_review(callback.message, state, data)
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "publish_none")
async def publish_none(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    data['source_group_id'] = None
    data['source_thread_id'] = None
    await finish_review(callback.message, state, data)
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_publish")
async def cancel_publish(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    menu = admin_menu if callback.from_user.id in ADMIN_IDS else main_menu
    await callback.message.edit_text("❌ Публикация отменена")
    await callback.message.answer(add_signature("Главное меню:"), reply_markup=menu)
    await callback.answer()

async def finish_review(message: types.Message, state: FSMContext, data: Dict[str, Any]):
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            await db.execute('''
                INSERT INTO reviews (user_id, user_name, tournament_id, tournament_name, venue_name, source_group_id, source_thread_id, entry_fee, court_rating, referee_rating, light_rating, transport_rating, balls_name, balls_rating, temperature, food_rating, stringer_rating, toilet_rating, warmup_rating, warmup_time, match_viewing, court_availability, accommodation, weather, warmup_comment, general_comment, review_date, is_anonymous, photo_id, is_deleted)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
            ''', (message.from_user.id, message.from_user.full_name, data['tournament_id'], data['tournament_name'], data['venue_name'], data.get('source_group_id'), data.get('source_thread_id'), data['entry_fee'], data['court_rating'], data['referee_rating'], data['light_rating'], data['transport_rating'], data['balls_name'], data['balls_rating'], data['temperature'], data['food_rating'], data['stringer_rating'], data['toilet_rating'], data['warmup_rating'], data['warmup_time'], data['match_viewing'], data['court_availability'], data['accommodation'], data['weather'], data['warmup_comment'], data['general_comment'], datetime.now(), data['is_anonymous'], data.get('photos', [None])[0] if data.get('photos') else None))
            await db.commit()
            await update_venue_rating(data['venue_name'])
            
            stats = await db.execute('''
                SELECT COUNT(*), AVG(court_rating), AVG(referee_rating), AVG(light_rating),
                       AVG(transport_rating), AVG(balls_rating), AVG(food_rating),
                       AVG(toilet_rating), AVG(warmup_rating)
                FROM reviews WHERE tournament_id = ? AND is_deleted = 0
            ''', (data['tournament_id'],))
            stats = await stats.fetchone()
            stats = stats if stats else (0, 0, 0, 0, 0, 0, 0, 0, 0)
            total_avg = (stats[1] + stats[2] + stats[3] + stats[4] + stats[5] + stats[6] + stats[7] + stats[8]) / 8 if stats[0] > 0 else 0
        
        if not data['is_anonymous'] and data.get('source_group_id'):
            try:
                group_msg = f"👤 **Оценка {message.from_user.first_name}**\n🏆 {data['tournament_name']}\n\n💰 Взнос: {data['entry_fee']} руб.\n🎾 Корт: {data['court_rating']}⭐\n👨‍⚖️ Судьи: {data['referee_rating']}⭐\n💡 Свет: {data['light_rating']}⭐\n🚗 Транспорт: {data['transport_rating']}⭐\n⚾ Мячи: {data['balls_rating']}⭐ ({data['balls_name']})\n🌡 {data['temperature']}\n🍔 Питание: {data['food_rating']}⭐\n🪡 Стрингер: {data['stringer_rating']}\n🚻 Санузел: {data['toilet_rating']}⭐\n🏋️ Разминка: {data['warmup_rating']}⭐\n⏱️ Разминка: {data['warmup_time']}\n📺 Просмотр: {data['match_viewing']}\n🎾 Доступность: {data['court_availability']}\n🏨 Проживание: {data['accommodation']}\n🌦 {data['weather']}\n"
                if data['warmup_comment'] != "Комментарий отсутствует":
                    group_msg += f"\n📝 {data['warmup_comment']}"
                if data['general_comment'] != "Комментарий отсутствует":
                    group_msg += f"\n📝 {data['general_comment']}"
                await bot.send_message(data['source_group_id'], group_msg, message_thread_id=data.get('source_thread_id'), parse_mode="Markdown")
                
                if stats[0] > 1:
                    avg_msg = f"📊 **СРЕДНИЕ ОЦЕНКИ**\n🏆 {data['tournament_name']}\n⭐ Общий: {total_avg:.1f} ({stats[0]} отзывов)\n\n🎾 Корт: {stats[1]:.1f}⭐\n👨‍⚖️ Судьи: {stats[2]:.1f}⭐\n💡 Свет: {stats[3]:.1f}⭐\n🚗 Транспорт: {stats[4]:.1f}⭐\n⚾ Мячи: {stats[5]:.1f}⭐\n🍔 Питание: {stats[6]:.1f}⭐\n🚻 Санузел: {stats[7]:.1f}⭐\n🏋️ Разминка: {stats[8]:.1f}⭐"
                    await bot.send_message(data['source_group_id'], avg_msg, message_thread_id=data.get('source_thread_id'), parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Ошибка отправки в группу: {e}")
        
        personal = f"✅ **Ваша оценка сохранена!**\n\n🏆 {data['tournament_name']}\n⭐ Общий рейтинг: {total_avg:.1f} ({stats[0]} отзывов)\n\n💰 {data['entry_fee']} руб.\n🎾 Корт: {data['court_rating']}⭐\n👨‍⚖️ Судьи: {data['referee_rating']}⭐\n💡 Свет: {data['light_rating']}⭐\n🚗 Транспорт: {data['transport_rating']}⭐\n⚾ Мячи: {data['balls_rating']}⭐ ({data['balls_name']})\n🌡 {data['temperature']}\n🍔 Питание: {data['food_rating']}⭐\n🪡 {data['stringer_rating']}\n🚻 {data['toilet_rating']}⭐\n🏋️ {data['warmup_rating']}⭐\n⏱️ {data['warmup_time']}\n📺 {data['match_viewing']}\n🎾 {data['court_availability']}\n🏨 {data['accommodation']}\n🌦 {data['weather']}\n"
        if data['warmup_comment'] != "Комментарий отсутствует":
            personal += f"\n📝 {data['warmup_comment']}"
        if data['general_comment'] != "Комментарий отсутствует":
            personal += f"\n📝 {data['general_comment']}"
        personal += f"\n\n🔒 {'Анонимно' if data['is_anonymous'] else 'Публично'}"
        if data.get('source_group_id'):
            personal += f"\n\n📌 Опубликовано в группе"
        else:
            personal += f"\n\n🔒 Отзыв добавлен только в статистику"
        
        menu = admin_menu if message.from_user.id in ADMIN_IDS else main_menu
        await message.answer(add_signature(personal), parse_mode="Markdown", reply_markup=menu)
        
        if data.get('photos'):
            media_group = []
            for photo_id in data['photos'][:10]:
                media_group.append(types.InputMediaPhoto(media=photo_id))
            if media_group:
                try:
                    await message.answer_media_group(media_group)
                except Exception as e:
                    logger.error(f"Ошибка отправки фото: {e}")
                    await message.answer("📸 Фото сохранены, но не могут быть показаны сейчас")
        
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка сохранения отзыва: {e}")
        await message.answer("❌ Произошла ошибка при сохранении отзыва. Пожалуйста, попробуйте позже.")
        await state.clear()

# ===================================================
# ПОИСК ТУРНИРОВ
# ===================================================

@dp.message(F.text == "🔍 Поиск турниров")
async def search_tournaments_start(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    await state.set_state(TournamentReview.filter_category)
    await state.update_data(filters={"category": None, "gender": None, "doubles": None, "search": None})
    await message.answer(
        "🔍 **Поиск турниров**\n\nВыберите параметры поиска:",
        reply_markup=filter_keyboard()
    )

@dp.callback_query(lambda c: c.data == "filter_category")
async def filter_category(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🏷️ **Выберите категорию (возраст):**", reply_markup=category_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('cat_'))
async def set_category(callback: types.CallbackQuery, state: FSMContext):
    cat_map = {
        'cat_9_10': '9-10 лет',
        'cat_u13': 'до 13 лет',
        'cat_u15': 'до 15 лет',
        'cat_u17': 'до 17 лет',
        'cat_u19': 'до 19 лет',
        'cat_adult': 'Взрослые',
        'cat_all': 'all'
    }
    category = cat_map.get(callback.data, 'all')
    
    data = await state.get_data()
    filters = data.get('filters', {})
    filters['category'] = category if category != 'all' else None
    await state.update_data(filters=filters)
    
    await callback.message.edit_text(
        f"✅ Категория: {category if category != 'all' else 'Все'}\n\nВыберите следующий параметр:",
        reply_markup=filter_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "filter_gender")
async def filter_gender(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⚥ **Выберите пол:**", reply_markup=gender_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('gender_'))
async def set_gender(callback: types.CallbackQuery, state: FSMContext):
    gender_map = {
        'gender_male': 'male',
        'gender_female': 'female',
        'gender_mixed': 'mixed',
        'gender_all': 'all'
    }
    gender = gender_map.get(callback.data, 'all')
    
    data = await state.get_data()
    filters = data.get('filters', {})
    filters['gender'] = gender if gender != 'all' else None
    await state.update_data(filters=filters)
    
    gender_text = 'Все'
    if gender == 'male':
        gender_text = 'Мужской'
    elif gender == 'female':
        gender_text = 'Женский'
    elif gender == 'mixed':
        gender_text = 'Смешанный'
    
    await callback.message.edit_text(
        f"✅ Пол: {gender_text}\n\nВыберите следующий параметр:",
        reply_markup=filter_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "filter_doubles")
async def filter_doubles(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🤝 **Выберите тип:**", reply_markup=doubles_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('doubles_'))
async def set_doubles(callback: types.CallbackQuery, state: FSMContext):
    doubles_map = {
        'doubles_no': 0,
        'doubles_yes': 1,
        'doubles_all': -1
    }
    doubles = doubles_map.get(callback.data, -1)
    
    data = await state.get_data()
    filters = data.get('filters', {})
    filters['doubles'] = doubles if doubles != -1 else None
    await state.update_data(filters=filters)
    
    doubles_text = 'Все'
    if doubles == 0:
        doubles_text = 'Одиночные'
    elif doubles == 1:
        doubles_text = 'Парные'
    
    await callback.message.edit_text(
        f"✅ Тип: {doubles_text}\n\nВыберите следующий параметр:",
        reply_markup=filter_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "filter_search")
async def filter_search(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(TournamentReview.search_query)
    await callback.message.edit_text(
        "🔍 **Введите первые буквы названия турнира:**\n\nНапример: Кубок, Первенство, Чемпионат\n\n(поиск по началу названия)"
    )
    await callback.answer()

@dp.message(TournamentReview.search_query)
async def set_search(message: types.Message, state: FSMContext):
    search = message.text.strip()
    
    data = await state.get_data()
    filters = data.get('filters', {})
    filters['search'] = search if search else None
    await state.update_data(filters=filters)
    
    await state.set_state(TournamentReview.filter_category)
    await message.answer(
        f"✅ Поиск по названию: {search}\n\nВыберите следующий параметр:",
        reply_markup=filter_keyboard()
    )

@dp.callback_query(lambda c: c.data == "filter_reset")
async def filter_reset(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(filters={"category": None, "gender": None, "doubles": None, "search": None})
    await callback.message.edit_text(
        "🔄 **Фильтры сброшены**\n\nВыберите параметры поиска:",
        reply_markup=filter_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "filter_apply")
async def filter_apply(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    filters = data.get('filters', {})
    
    query = 'SELECT id, name, city, date_start, category, gender, is_doubles FROM tournaments WHERE is_deleted = 0'
    params = []
    
    if filters.get('category'):
        query += ' AND category = ?'
        params.append(filters['category'])
    
    if filters.get('gender'):
        if filters['gender'] == 'male':
            query += ' AND gender = "Мужской"'
        elif filters['gender'] == 'female':
            query += ' AND gender = "Женский"'
        elif filters['gender'] == 'mixed':
            query += ' AND gender = "Смешанный"'
    
    if filters.get('doubles') is not None:
        query += ' AND is_doubles = ?'
        params.append(filters['doubles'])
    
    if filters.get('search'):
        query += ' AND name LIKE ?'
        params.append(filters['search'] + '%')
    
    query += ' ORDER BY date_start ASC'
    
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute(query, tuple(params))
            tournaments = await cursor.fetchall()
        
        if not tournaments:
            await callback.message.edit_text(
                "❌ **Турниры не найдены**\n\nПопробуйте изменить параметры поиска:",
                reply_markup=filter_keyboard()
            )
            await callback.answer()
            return
        
        await state.update_data(tournaments_list=tournaments, tournaments_page=0)
        await show_search_results(callback.message, state, 0)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        await callback.message.edit_text(f"❌ Ошибка поиска: {str(e)[:100]}")
        await callback.answer()

async def show_search_results(message: types.Message, state: FSMContext, page: int):
    data = await state.get_data()
    tournaments = data.get('tournaments_list', [])
    total_pages = (len(tournaments) + 9) // 10
    
    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0
    
    start_idx = page * 10
    end_idx = min(start_idx + 10, len(tournaments))
    page_tournaments = tournaments[start_idx:end_idx]
    
    kb = InlineKeyboardBuilder()
    for t in page_tournaments:
        date = t[3][:10] if t[3] else "дата?"
        cat = t[4] if t[4] else "?"
        gender_icon = "👨" if t[5] == "Мужской" else "👩" if t[5] == "Женский" else "👥"
        doubles_icon = "🤝" if t[6] else "🎾"
        kb.button(text=f"{date} | {t[2]} | {cat} {gender_icon}{doubles_icon} | {t[1]}", callback_data=f"rate_{t[0]}")
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"search_page_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"search_page_{page+1}"))
    
    if nav_buttons:
        kb.row(*nav_buttons)
    
    kb.button(text="🔍 Новый поиск", callback_data="new_search")
    kb.button(text="❌ Отмена", callback_data="cancel_review")
    kb.adjust(1)
    
    await message.answer(
        f"🔍 **Результаты поиска**\n\n📄 Страница {page + 1} из {total_pages}\n📊 Найдено: {len(tournaments)}",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data.startswith('search_page_'))
async def change_search_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.split('_')[2])
    await show_search_results(callback.message, state, page)
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "new_search")
async def new_search(callback: types.CallbackQuery, state: FSMContext):
    await search_tournaments_start(callback.message, state)
    await callback.message.delete()
    await callback.answer()

# ===================================================
# ДОБАВЛЕНИЕ ТУРНИРА (РУЧНОЙ ВВОД)
# ===================================================

@dp.message(F.text == "➕ Добавить турнир")
async def add_tournament_start(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    await state.set_state(AddTournament.input_name)
    await message.answer("🏆 **Добавление нового турнира**\n\nВведите **название турнира**:\nНапример: Кубок Одинцово 2026\n\n❗️ Рекомендуется указывать год в названии.")

@dp.message(AddTournament.input_name)
async def add_name(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте название.")
        return
    await state.update_data(name=text)
    await state.set_state(AddTournament.input_city)
    await message.answer("🌆 Введите **город**:\nНапример: Одинцово, Москва, Красногорск")

@dp.message(AddTournament.input_city)
async def add_city(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте.")
        return
    await state.update_data(city=text)
    await state.set_state(AddTournament.input_date_start)
    await message.answer("📅 Введите **дату начала** турнира в формате ДД.ММ.ГГГГ:\nНапример: 15.06.2026")

@dp.message(AddTournament.input_date_start)
async def add_date_start(message: types.Message, state: FSMContext):
    try:
        datetime.strptime(message.text, '%d.%m.%Y')
        await state.update_data(date_start=message.text)
        await state.set_state(AddTournament.input_date_end)
        await message.answer("📅 Введите **дату окончания** турнира в формате ДД.ММ.ГГГГ:\nНапример: 16.06.2026")
    except ValueError:
        await message.answer("❌ Неверный формат! Используйте ДД.ММ.ГГГГ, например: 15.06.2026")

@dp.message(AddTournament.input_date_end)
async def add_date_end(message: types.Message, state: FSMContext):
    try:
        datetime.strptime(message.text, '%d.%m.%Y')
        await state.update_data(date_end=message.text)
        await state.set_state(AddTournament.input_venue)
        await message.answer("📍 Введите **место проведения** (название теннисного клуба или центра):\nНапример: ТК Одинцово, Лужники, Кайман")
    except ValueError:
        await message.answer("❌ Неверный формат! Используйте ДД.ММ.ГГГГ")

@dp.message(AddTournament.input_venue)
async def add_venue(message: types.Message, state: FSMContext):
    text, bad = filter_bad_words(message.text)
    if bad:
        await message.answer("⚠️ Обнаружены недопустимые слова. Пожалуйста, переформулируйте.")
        return
    await state.update_data(venue=text)
    await state.set_state(AddTournament.input_category)
    await message.answer("🏷️ **Выберите возрастную категорию:**\n\n9-10 лет / до 13 лет / до 15 лет / до 17 лет / до 19 лет / Взрослые")

@dp.message(AddTournament.input_category)
async def add_category(message: types.Message, state: FSMContext):
    cat = message.text.lower()
    valid = ['9-10 лет', 'до 13 лет', 'до 15 лет', 'до 17 лет', 'до 19 лет', 'взрослые']
    if cat not in valid:
        await message.answer(f"❌ Неверная категория! Введите: {', '.join(valid)}")
        return
    await state.update_data(category=message.text)
    await state.set_state(AddTournament.input_gender)
    await message.answer("⚥ **Выберите пол:**\n\nМужской / Женский / Смешанный")

@dp.message(AddTournament.input_gender)
async def add_gender(message: types.Message, state: FSMContext):
    gender = message.text.lower()
    if gender not in ['мужской', 'женский', 'смешанный']:
        await message.answer("❌ Введите: Мужской / Женский / Смешанный")
        return
    await state.update_data(gender=message.text)
    await state.set_state(AddTournament.input_doubles)
    await message.answer("🤝 **Тип турнира:**\n\n0 - одиночные\n1 - парные\n\nВведите 0 или 1:")

@dp.message(AddTournament.input_doubles)
async def add_doubles(message: types.Message, state: FSMContext):
    try:
        is_doubles = int(message.text)
        if is_doubles not in [0, 1]:
            raise
        await state.update_data(is_doubles=is_doubles)
        await state.set_state(AddTournament.input_surface)
        await message.answer("🎾 **Выберите покрытие корта:**", reply_markup=surface_keyboard())
    except:
        await message.answer("❌ Введите 0 (одиночные) или 1 (парные)")

@dp.callback_query(lambda c: c.data.startswith('surface_'))
async def add_surface(callback: types.CallbackQuery, state: FSMContext):
    surf = {'surface_hard': 'Хард', 'surface_clay': 'Грунт', 'surface_teraflex': 'Терафлекс', 'surface_grass': 'Искусственная трава'}
    await state.update_data(surface=surf[callback.data])
    await state.set_state(AddTournament.input_court_type)
    await callback.message.edit_text("🏟️ **Выберите тип корта:**", reply_markup=court_type_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('type_'))
async def add_type(callback: types.CallbackQuery, state: FSMContext):
    t = {'type_indoor': 'Закрытый', 'type_outdoor': 'Открытый'}
    await state.update_data(court_type=t[callback.data])
    data = await state.get_data()
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            await db.execute('''
                INSERT INTO tournaments (name, city, date_start, date_end, venue, surface, court_type, 
                                         category, gender, is_doubles, added_by, added_date, is_deleted)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
            ''', (data['name'], data['city'], data['date_start'], data['date_end'], data['venue'],
                  data['surface'], data['court_type'], data['category'], data['gender'], data['is_doubles'],
                  callback.from_user.id, datetime.now()))
            await db.commit()
        await callback.message.edit_text(f"✅ **Турнир успешно добавлен!**\n\n🏆 {data['name']}\n📍 {data['city']}, {data['venue']}\n🎾 Покрытие: {data['surface']}\n🏟️ Тип корта: {data['court_type']}\n🏷️ Категория: {data['category']}\n⚥ Пол: {data['gender']}\n🤝 {'Парные' if data['is_doubles'] else 'Одиночные'}\n📅 {data['date_start']} - {data['date_end']}\n\nТеперь этот турнир доступен для оценки.")
    except Exception as e:
        logger.error(f"Ошибка добавления турнира: {e}")
        await callback.message.edit_text("❌ Произошла ошибка при добавлении турнира")
    menu = admin_menu if callback.from_user.id in ADMIN_IDS else main_menu
    await callback.message.answer("✅ Готово!", reply_markup=menu)
    await state.clear()
    await callback.answer()

# ===================================================
# ВСЕ ТУРНИРЫ
# ===================================================

@dp.message(F.text == "📅 Все турниры")
async def all_tournaments(message: types.Message):
    tournaments = await get_all_tournaments()
    if not tournaments:
        await message.answer("😕 Нет турниров. Нажмите '➕ Добавить турнир', чтобы добавить первый турнир.")
        return
    
    text = "📅 **Список всех турниров:**\n\n"
    for t in tournaments:
        text += f"🏆 {t[1]}\n📍 {t[2]}\n📅 {t[3][:10]}\n\n"
        if len(text) > 3500:
            await message.answer(text)
            text = ""
    if text:
        await message.answer(text)

# ===================================================
# РЕЙТИНГ ТЕННИСНЫХ ЦЕНТРОВ
# ===================================================

@dp.message(F.text == "🏟️ Рейтинг теннисных центров")
async def show_venue_ratings(message: types.Message):
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('''
                SELECT venue_name, total_reviews, total_score, avg_court, avg_referee, 
                       avg_light, avg_transport, avg_balls, avg_food, avg_toilet, avg_warmup 
                FROM venue_ratings 
                WHERE total_reviews > 0 
                ORDER BY total_score DESC 
                LIMIT 20
            ''')
            venues = await cursor.fetchall()
        
        if not venues:
            await message.answer("🏟️ **Рейтинг теннисных центров**\n\nПока нет оценок мест. Оцените турниры, и здесь появится статистика по каждому теннисному центру!")
            return
        
        text = "🏆 **РЕЙТИНГ ТЕННИСНЫХ ЦЕНТРОВ** 🏆\n\n"
        for i, v in enumerate(venues, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            ratings = {
                "🎾 Подготовка корта": v[3],
                "👨‍⚖️ Работа судей": v[4],
                "💡 Освещение": v[5],
                "🚗 Транспортная доступность": v[6],
                "⚾ Качество мячей": v[7],
                "🍔 Точка питания": v[8],
                "🚻 Состояние санузла": v[9],
                "🏋️ Разминочная зона": v[10]
            }
            best = max(ratings.items(), key=lambda x: x[1])
            worst = min(ratings.items(), key=lambda x: x[1])
            text += f"{medal} **{v[0]}**\n ⭐ **Общий рейтинг: {v[2]:.1f}**\n 👥 На основе {v[1]} отзывов\n 👍 Лучше всего: {best[0]} — {best[1]:.1f}⭐\n 👎 Хуже всего: {worst[0]} — {worst[1]:.1f}⭐\n\n"
        await message.answer(text)
    except Exception as e:
        logger.error(f"Ошибка показа рейтинга: {e}")
        await message.answer("❌ Произошла ошибка при загрузке рейтинга")

# ===================================================
# ТОП ТУРНИРОВ
# ===================================================

@dp.message(F.text == "🏆 Топ турниров (лучшие и худшие)")
async def show_top_tournaments(message: types.Message):
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('''
                SELECT tournament_id, tournament_name, venue_name, COUNT(*), 
                       AVG(court_rating), AVG(referee_rating), AVG(light_rating),
                       AVG(transport_rating), AVG(balls_rating), AVG(food_rating),
                       AVG(toilet_rating), AVG(warmup_rating)
                FROM reviews WHERE is_deleted = 0 
                GROUP BY tournament_id HAVING COUNT(*) >= 1
            ''')
            tournaments = await cursor.fetchall()
        
        if not tournaments:
            await message.answer("🏆 **Топ турниров**\n\nПока нет оценок турниров. Добавьте турнир и оцените его!")
            return
        
        results = []
        for t in tournaments:
            total = (t[4] + t[5] + t[6] + t[7] + t[8] + t[9] + t[10] + t[11]) / 8
            results.append((t[1], t[2], t[3], total))
        
        results.sort(key=lambda x: x[3], reverse=True)
        best = results[:10]
        worst = results[-10:][::-1]
        
        text = "🏆 **ТОП-10 ЛУЧШИХ ТУРНИРОВ** 🏆\n\n"
        for i, (name, venue, reviews, score) in enumerate(best, 1):
            text += f"{i}. {name}\n 📍 {venue}\n ⭐ Общий рейтинг: {score:.1f}\n 👥 {int(reviews)} отзывов\n\n"
        text += "\n📉 **ТОП-10 ХУДШИХ ТУРНИРОВ** 📉\n\n"
        for i, (name, venue, reviews, score) in enumerate(worst, 1):
            text += f"{i}. {name}\n 📍 {venue}\n ⭐ Общий рейтинг: {score:.1f}\n 👥 {int(reviews)} отзывов\n\n"
        await message.answer(text)
    except Exception as e:
        logger.error(f"Ошибка показа топа турниров: {e}")
        await message.answer("❌ Произошла ошибка при загрузке топа турниров")

# ===================================================
# МОИ ОЦЕНКИ
# ===================================================

@dp.message(F.text == "📋 Мои оценки")
async def my_reviews(message: types.Message):
    if message.chat.type != 'private':
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('''
                SELECT id, tournament_name, review_date, is_anonymous 
                FROM reviews 
                WHERE user_id = ? AND is_deleted = 0 
                ORDER BY review_date DESC 
                LIMIT 10
            ''', (message.from_user.id,))
            reviews = await cursor.fetchall()
        
        if not reviews:
            await message.answer("📋 У вас пока нет ни одной оценки. Нажмите '🏆 Оценить турнир', чтобы оставить первый отзыв.")
            return
        
        kb = InlineKeyboardBuilder()
        for r in reviews:
            anon = "🔒 Анонимный" if r[3] else "👤 Публичный"
            kb.button(text=f"{anon} | {r[1]} ({r[2][:10]})", callback_data=f"myreview_{r[0]}")
        kb.adjust(1)
        await message.answer("📋 **Ваши последние оценки:**\n\nНажмите на отзыв, чтобы редактировать или удалить.", reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Ошибка загрузки отзывов: {e}")
        await message.answer("❌ Произошла ошибка при загрузке отзывов")

@dp.callback_query(lambda c: c.data.startswith('myreview_'))
async def show_my_review(callback: types.CallbackQuery):
    rid = callback.data.split('_')[1]
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT * FROM reviews WHERE id = ? AND is_deleted = 0', (rid,))
            r = await cursor.fetchone()
            if not r:
                await callback.message.edit_text("❌ Отзыв не найден или был удалён")
                await callback.answer()
                return
        
        text = f"🏆 **{r[4]}**\n📍 Место: {r[5]}\n📅 {r[26][:10]}\n\n💰 Турнирный взнос: {r[7]} руб.\n🎾 Подготовка корта: {r[8]} ⭐\n👨‍⚖️ Работа судей: {r[9]} ⭐\n💡 Освещение корта: {r[10]} ⭐\n🚗 Транспортная доступность: {r[11]} ⭐\n⚾ Мячи: {r[13]} ⭐ ({r[12]})\n🌡 {r[14]}\n🍔 Точка питания: {r[15]} ⭐\n🪡 Стрингер: {r[16]}\n🚻 Состояние санузла: {r[17]} ⭐\n🏋️ Разминочная зона: {r[18]} ⭐\n⏱️ Специальное время для разминки: {r[19]}\n📺 Просмотр матчей: {r[20]}\n🎾 Доступность кортов во внеигровое время: {r[21]}\n🏨 Проживание: {r[22]}\n🌦 {r[23]}\n"
        if r[24] and r[24] != "Комментарий отсутствует":
            text += f"📝 Комментарий о разминке: {r[24]}\n"
        if r[25] and r[25] != "Комментарий отсутствует":
            text += f"📝 Общий комментарий: {r[25]}\n"
        text += f"\n🔒 Анонимность: {'Анонимно' if r[27] else 'Публично'}"
        await callback.message.edit_text(text, reply_markup=edit_review_keyboard(rid))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка показа отзыва: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('edit_'))
async def edit_review_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split('_')
    rid = parts[1]
    field = parts[2]
    await state.update_data(edit_review_id=int(rid), edit_field=field)
    if field == "comment":
        await state.set_state(EditReview.edit_value)
        await callback.message.edit_text("✏️ Введите **новый общий комментарий** к отзыву:")
    elif field == "anonymous":
        await state.update_data(edit_value=1)
        await finish_edit_review(callback.message, state)
    else:
        await state.set_state(EditReview.edit_value)
        await callback.message.edit_text(f"✏️ Введите **новую оценку** (от 1 до 5 ⭐):")

@dp.message(EditReview.edit_value)
async def edit_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    field = data['edit_field']
    value = message.text
    if field not in ["comment", "anonymous"]:
        try:
            val = int(value)
            if val < 1 or val > 5:
                raise ValueError
        except:
            await message.answer("❌ Оценка должна быть числом от 1 до 5! Попробуйте ещё раз.")
            return
    await state.update_data(edit_value=value)
    await finish_edit_review(message, state)

async def finish_edit_review(message: types.Message, state: FSMContext):
    data = await state.get_data()
    rid = data['edit_review_id']
    field = data['edit_field']
    value = data['edit_value']
    field_map = {"fee": "entry_fee", "court": "court_rating", "referee": "referee_rating", "light": "light_rating", "transport": "transport_rating", "balls": "balls_rating", "food": "food_rating", "toilet": "toilet_rating", "warmup": "warmup_rating", "comment": "general_comment", "anonymous": "is_anonymous"}
    db_field = field_map.get(field, field)
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            if field == "anonymous":
                await db.execute(f'UPDATE reviews SET {db_field} = 1 WHERE id = ?', (rid,))
            elif field == "comment":
                val, _ = filter_bad_words(str(value))
                await db.execute(f'UPDATE reviews SET {db_field} = ? WHERE id = ?', (val, rid))
            else:
                await db.execute(f'UPDATE reviews SET {db_field} = ? WHERE id = ?', (int(value), rid))
            await db.commit()
            
            cursor = await db.execute('SELECT venue_name FROM reviews WHERE id = ?', (rid,))
            venue = await cursor.fetchone()
            if venue:
                await update_venue_rating(venue[0])
        
        await message.answer("✅ Отзыв успешно обновлён!")
        await state.clear()
        await my_reviews(message)
    except Exception as e:
        logger.error(f"Ошибка редактирования отзыва: {e}")
        await message.answer("❌ Произошла ошибка при обновлении отзыва")

@dp.callback_query(lambda c: c.data.startswith('delete_review_'))
async def delete_review(callback: types.CallbackQuery):
    rid = callback.data.split('_')[2]
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT venue_name FROM reviews WHERE id = ?', (rid,))
            venue = await cursor.fetchone()
            await db.execute('UPDATE reviews SET is_deleted = 1 WHERE id = ?', (rid,))
            await db.commit()
            if venue:
                await update_venue_rating(venue[0])
        await callback.message.edit_text("✅ Отзыв удалён")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка удаления отзыва: {e}")
        await callback.message.edit_text("❌ Произошла ошибка при удалении")
        await callback.answer()

# ===================================================
# НАСТРОЙКИ
# ===================================================

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_groups = await get_user_groups(message.from_user.id)
    groups_info = ""
    if user_groups:
        groups_info = "\n\n📌 **Ваши группы:**\n"
        for group in user_groups:
            group_id, thread_id, group_name, is_default = group
            icon = "⭐" if is_default else ("📎" if thread_id else "💬")
            groups_info += f"{icon} {group_name}\n"
    await message.answer(f"⚙️ **Настройки**{groups_info}\n\nВыберите действие:", reply_markup=settings_keyboard())

@dp.callback_query(lambda c: c.data == "settings_groups")
async def settings_show_groups(callback: types.CallbackQuery):
    user_groups = await get_user_groups(callback.from_user.id)
    if not user_groups:
        await callback.message.edit_text("📌 **У вас пока нет сохранённых групп.**\n\nЧтобы добавить группу:\n1. Добавьте бота в группу\n2. Напишите в группе /start\n3. Бот автоматически сохранит группу")
        await callback.answer()
        return
    text = "📌 **Ваши группы:**\n\n"
    for i, group in enumerate(user_groups, 1):
        group_id, thread_id, group_name, is_default = group
        icon = "⭐" if is_default else ("📎" if thread_id else "💬")
        text += f"{icon} **{group_name}**\n"
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "settings_default_group")
async def settings_choose_default(callback: types.CallbackQuery):
    user_groups = await get_user_groups(callback.from_user.id)
    if not user_groups:
        await callback.message.edit_text("❌ У вас нет сохранённых групп.\n\nСначала добавьте группу, написав /start в ней.")
        await callback.answer()
        return
    kb = InlineKeyboardBuilder()
    for group in user_groups:
        group_id, thread_id, group_name, is_default = group
        icon = "⭐" if is_default else ("📎" if thread_id else "💬")
        kb.button(text=f"{icon} {group_name}", callback_data=f"set_default_{group_id}_{thread_id or 0}")
    kb.button(text="🔙 Назад", callback_data="back_to_settings")
    kb.adjust(1)
    await callback.message.edit_text("⭐ **Выберите группу по умолчанию:**\n\nЭта группа будет автоматически выбрана при публикации отзывов.", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('set_default_'))
async def set_default_group(callback: types.CallbackQuery):
    parts = callback.data.split('_')
    group_id = int(parts[2])
    thread_id = int(parts[3]) if len(parts) > 3 and parts[3] != '0' else None
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            await db.execute('UPDATE user_groups SET is_default = 0 WHERE user_id = ?', (callback.from_user.id,))
            await db.execute('UPDATE user_groups SET is_default = 1 WHERE user_id = ? AND group_id = ? AND (thread_id = ? OR (thread_id IS NULL AND ? IS NULL))', (callback.from_user.id, group_id, thread_id, thread_id))
            await db.commit()
        await callback.message.edit_text("✅ **Группа по умолчанию установлена!**")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка установки группы по умолчанию: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "settings_delete_group")
async def settings_delete_group(callback: types.CallbackQuery):
    user_groups = await get_user_groups(callback.from_user.id)
    if not user_groups:
        await callback.message.edit_text("❌ У вас нет сохранённых групп.")
        await callback.answer()
        return
    kb = InlineKeyboardBuilder()
    for group in user_groups:
        group_id, thread_id, group_name, is_default = group
        icon = "⭐" if is_default else ("📎" if thread_id else "💬")
        kb.button(text=f"{icon} {group_name}", callback_data=f"delete_group_{group_id}_{thread_id or 0}")
    kb.button(text="🔙 Назад", callback_data="back_to_settings")
    kb.adjust(1)
    await callback.message.edit_text("🗑 **Выберите группу для удаления:**\n\nВнимание! Группа будет удалена из списка, но уже опубликованные отзывы останутся.", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('delete_group_'))
async def delete_group(callback: types.CallbackQuery):
    parts = callback.data.split('_')
    group_id = int(parts[2])
    thread_id = int(parts[3]) if len(parts) > 3 and parts[3] != '0' else None
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            await db.execute('DELETE FROM user_groups WHERE user_id = ? AND group_id = ? AND (thread_id = ? OR (thread_id IS NULL AND ? IS NULL))', (callback.from_user.id, group_id, thread_id, thread_id))
            await db.commit()
        await callback.message.edit_text("✅ **Группа удалена!**")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка удаления группы: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_settings")
async def back_to_settings(callback: types.CallbackQuery):
    await callback.message.edit_text("⚙️ **Настройки**\n\nВыберите действие:", reply_markup=settings_keyboard())
    await callback.answer()

# ===================================================
# АДМИН-ПАНЕЛЬ
# ===================================================

@dp.message(F.text == "👑 Админ-панель")
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    await message.answer("👑 **Админ-панель**\n\nВыберите действие:", reply_markup=admin_panel_keyboard())

@dp.callback_query(lambda c: c.data == "admin_delete_tournament")
async def admin_delete_list(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT id, name, city FROM tournaments WHERE is_deleted = 0')
            tours = await cursor.fetchall()
        
        if not tours:
            await callback.message.edit_text("❌ Нет турниров")
            return
        
        kb = InlineKeyboardBuilder()
        for t in tours:
            kb.button(text=f"🗑 {t[1]} ({t[2]})", callback_data=f"admin_del_{t[0]}")
        kb.button(text="🔙 Назад", callback_data="back_to_menu")
        kb.adjust(1)
        await callback.message.edit_text("🗑 **Выберите турнир для удаления:**\n\n⚠️ ВНИМАНИЕ! Вместе с турниром будут удалены ВСЕ отзывы о нём.", reply_markup=kb.as_markup())
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка загрузки списка турниров: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_del_'))
async def admin_delete_tournament(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    tid = callback.data.split('_')[2]
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT name FROM tournaments WHERE id = ?', (tid,))
            tour = await cursor.fetchone()
            tour_name = tour[0] if tour else "Неизвестный"
            await db.execute('UPDATE tournaments SET is_deleted = 1 WHERE id = ?', (tid,))
            await db.execute('UPDATE reviews SET is_deleted = 1 WHERE tournament_id = ?', (tid,))
            await db.commit()
        await callback.message.edit_text(f"✅ Турнир **{tour_name}** и все его отзывы удалены.")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка удаления турнира: {e}")
        await callback.message.edit_text("❌ Произошла ошибка при удалении")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            tours = await db.execute('SELECT COUNT(*) FROM tournaments WHERE is_deleted = 0')
            revs = await db.execute('SELECT COUNT(*) FROM reviews WHERE is_deleted = 0')
            users = await db.execute('SELECT COUNT(DISTINCT user_id) FROM reviews')
            venues = await db.execute('SELECT COUNT(DISTINCT venue_name) FROM reviews WHERE venue_name IS NOT NULL AND is_deleted = 0')
            groups = await db.execute('SELECT COUNT(*) FROM user_groups')
            
            tours = await tours.fetchone()
            revs = await revs.fetchone()
            users = await users.fetchone()
            venues = await venues.fetchone()
            groups = await groups.fetchone()
        
        text = f"📊 **СТАТИСТИКА БОТА**\n\n🏆 Активных турниров: {tours[0]}\n📝 Всего отзывов: {revs[0]}\n👥 Пользователей: {users[0]}\n🏟️ Теннисных центров с оценками: {venues[0]}\n📌 Связок пользователь-группа: {groups[0]}"
        await callback.message.edit_text(text)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка загрузки статистики: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_all_reviews")
async def admin_all_reviews(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('''
                SELECT id, tournament_name, user_name, review_date 
                FROM reviews 
                WHERE is_deleted = 0 
                ORDER BY review_date DESC 
                LIMIT 20
            ''')
            reviews = await cursor.fetchall()
        
        if not reviews:
            await callback.message.edit_text("📊 Нет отзывов")
            return
        
        text = "📊 **ПОСЛЕДНИЕ 20 ОТЗЫВОВ**\n\n"
        for r in reviews:
            text += f"🆔 {r[0]} | {r[1]}\n👤 {r[2]} | {r[3][:10]}\n➖➖➖➖➖➖➖➖\n"
        await callback.message.edit_text(text)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка загрузки отзывов: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('''
                SELECT user_id, user_name, COUNT(*) 
                FROM reviews 
                WHERE is_deleted = 0 
                GROUP BY user_id 
                ORDER BY COUNT(*) DESC 
                LIMIT 20
            ''')
            users = await cursor.fetchall()
        
        if not users:
            await callback.message.edit_text("👥 Нет пользователей")
            return
        
        text = "👥 **ТОП-20 АКТИВНЫХ ПОЛЬЗОВАТЕЛЕЙ**\n\n"
        for i, u in enumerate(users, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            text += f"{medal} {u[1]} — {u[2]} отзывов\n"
        await callback.message.edit_text(text)
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка загрузки пользователей: {e}")
        await callback.message.edit_text("❌ Произошла ошибка")
        await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_export")
async def admin_export(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа")
        return
    try:
        async with aiosqlite.connect('rtt_bot.db') as db:
            cursor = await db.execute('SELECT * FROM tournaments WHERE is_deleted = 0')
            tournaments = await cursor.fetchall()
            cursor = await db.execute('SELECT * FROM reviews WHERE is_deleted = 0')
            reviews = await cursor.fetchall()
        
        tours_output = StringIO()
        tours_writer = csv.writer(tours_output)
        tours_writer.writerow(['id', 'name', 'city', 'date_start', 'date_end', 'venue', 'surface', 'court_type', 'category', 'gender', 'is_doubles', 'added_by', 'added_date'])
        tours_writer.writerows(tournaments)
        
        revs_output = StringIO()
        revs_writer = csv.writer(revs_output)
        revs_writer.writerow(['id', 'user_id', 'user_name', 'tournament_id', 'tournament_name', 'venue_name', 'entry_fee', 'court_rating', 'referee_rating', 'light_rating', 'transport_rating', 'balls_name', 'balls_rating', 'temperature', 'food_rating', 'stringer_rating', 'toilet_rating', 'warmup_rating', 'warmup_time', 'match_viewing', 'court_availability', 'accommodation', 'weather', 'warmup_comment', 'general_comment', 'review_date', 'is_anonymous', 'is_deleted'])
        revs_writer.writerows(reviews)
        
        await callback.message.edit_text("📤 **Экспорт данных:**\n\nФайлы готовятся...")
        await callback.message.answer_document(types.BufferedInputFile(tours_output.getvalue().encode('utf-8-sig'), filename='tournaments.csv'), caption="📊 Турниры")
        await callback.message.answer_document(types.BufferedInputFile(revs_output.getvalue().encode('utf-8-sig'), filename='reviews.csv'), caption="📝 Отзывы")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка экспорта: {e}")
        await callback.message.edit_text("❌ Произошла ошибка при экспорте")
        await callback.answer()

# ===================================================
# КОМАНДЫ В ГРУППЕ
# ===================================================

@dp.message(Command("rate"))
async def cmd_rate_group(message: types.Message):
    if message.chat.type in ['group', 'supergroup']:
        thread_id = message.message_thread_id
        group_name = message.chat.title
        await add_user_group(message.from_user.id, message.chat.id, thread_id, group_name)
        await message.answer(
            f"@{message.from_user.username}, нажми кнопку 👇\n\nПосле заполнения отзыва вы сможете выбрать эту группу для публикации.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏆 ОЦЕНИТЬ ТУРНИР", url=f"https://t.me/{(await bot.get_me()).username}?start=review")]
            ])
        )
        try:
            await bot.send_message(message.from_user.id, f"✅ Группа **{group_name}** сохранена!\n\nТеперь вы можете оценить турнир. После заполнения отзыва выберите эту группу для публикации.")
        except:
            pass

@dp.message(Command("all"))
async def cmd_all_group(message: types.Message):
    tournaments = await get_all_tournaments()
    if not tournaments:
        await message.answer("😕 Нет турниров")
        return
    text = "📅 **Последние турниры:**\n\n"
    for t in tournaments[:10]:
        text += f"🏆 {t[1]}\n📍 {t[2]}\n📅 {t[3][:10]}\n\n"
    await message.answer(text)

# ===================================================
# ОБРАБОТЧИК ОШИБОК
# ===================================================

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"Ошибка: {exception}\nUpdate: {update}")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"❌ **Ошибка в боте**\n\nТип: {type(exception).__name__}\nСообщение: {str(exception)[:200]}")
        except:
            pass
    return True

# ===================================================
# ЗАПУСК
# ===================================================

async def main():
    await init_database()
    # Удаляем вебхук перед запуском поллинга
    await bot.delete_webhook(drop_pending_updates=True)
    
    print("=" * 50)
    print("✅ БОТ УСПЕШНО ЗАПУЩЕН!")
    print("=" * 50)
    print("📌 ФУНКЦИИ БОТА:")
    print(" • Сбор отзывов по 15+ параметрам")
    print(" • Анонимные отзывы (не публикуются в группе)")
    print(" • Рейтинг теннисных центров")
    print(" • Топ-10 лучших и худших турниров")
    print(" • Фото к отзывам")
    print(" • Редактирование и удаление отзывов")
    print(" • Удаление турниров (только для админов)")
    print(" • Фильтр нецензурной лексики")
    print(" • Экспорт данных в CSV")
    print(" • Поддержка тем (форумов) в группах")
    print(" • Поиск турниров по категориям, полу, парности")
    print(" • Пагинация для списка турниров")
    print(" • Показ только турниров за последние 14 дней")
    print("=" * 50)
    
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())