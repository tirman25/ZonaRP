import asyncio
import logging
import json
import os
import re
import uuid
import html
import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InlineQueryResultArticle, InputTextMessageContent, ChosenInlineResult
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, BOT_NAME, ADMIN_IDS, MIN_NICKNAME_LENGTH, MAX_NICKNAME_LENGTH, MIN_COMMAND_NAME_LENGTH, MAX_COMMAND_NAME_LENGTH, REPORT_REASONS, PROXY_URL, DATABASE_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============= DATABASE =============

async def init_db():
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                nickname TEXT UNIQUE,
                is_blocked INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_id TEXT UNIQUE,
                name TEXT NOT NULL,
                description TEXT,
                creator_id INTEGER NOT NULL,
                command_type TEXT NOT NULL DEFAULT 'self',
                visibility TEXT NOT NULL DEFAULT 'public',
                text_before TEXT,
                buttons TEXT DEFAULT '[]',
                likes INTEGER DEFAULT 0,
                uses INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users(user_id)
            )
        """)
        try:
            await db.execute("ALTER TABLE commands ADD COLUMN public_id TEXT")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE commands ADD COLUMN visibility TEXT NOT NULL DEFAULT 'public'")
        except Exception:
            pass
        await db.execute("UPDATE commands SET public_id = lower(hex(randomblob(16))) WHERE public_id IS NULL OR public_id = ''")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS likes (
                user_id INTEGER NOT NULL,
                command_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, command_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id INTEGER NOT NULL,
                command_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, command_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id INTEGER PRIMARY KEY,
                blocked_by INTEGER,
                reason TEXT,
                blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def get_user(user_id: int) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def register_user(user_id: int, nickname: str) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("INSERT INTO users (user_id, nickname) VALUES (?, ?)", (user_id, nickname.lower()))
            await db.commit()
            return True
        except:
            return False

async def update_nickname(user_id: int, nickname: str) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("UPDATE users SET nickname = ? WHERE user_id = ?", (nickname.lower(), user_id))
            await db.commit()
            return True
        except:
            return False

async def is_nickname_taken(nickname: str) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM users WHERE nickname = ?", (nickname.lower(),))
        return await cursor.fetchone() is not None

async def is_user_blocked(user_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM blocked_users WHERE user_id = ?", (user_id,))
        return await cursor.fetchone() is not None

# ============= COMMANDS DB =============

async def create_command_db(user_id: int, name: str, description: str, command_type: str, visibility: str, text_before: str, buttons: list) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        public_id = uuid.uuid4().hex
        cursor = await db.execute(
            "INSERT INTO commands (public_id, name, description, creator_id, command_type, visibility, text_before, buttons) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (public_id, name.lower(), description, user_id, command_type, visibility, text_before, json.dumps(buttons, ensure_ascii=False))
        )
        await db.commit()
        return cursor.lastrowid

async def get_command(command_id: int) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT c.*, u.nickname as creator_nickname FROM commands c JOIN users u ON c.creator_id = u.user_id WHERE c.id = ?", 
            (command_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_command_by_public_id(public_id: str) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT c.*, u.nickname as creator_nickname FROM commands c JOIN users u ON c.creator_id = u.user_id WHERE c.public_id = ? AND c.is_active = 1",
            (public_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_command_by_name(name: str, user_id: int | None = None) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if user_id is None:
            cursor = await db.execute(
                """SELECT c.*, u.nickname as creator_nickname FROM commands c 
                   JOIN users u ON c.creator_id = u.user_id 
                   WHERE c.name = ? AND c.is_active = 1 AND c.visibility = 'public'
                   ORDER BY (c.likes + c.uses/10.0) DESC LIMIT 1""",
                (name.lower(),)
            )
        else:
            cursor = await db.execute(
                """SELECT c.*, u.nickname as creator_nickname FROM commands c 
                   JOIN users u ON c.creator_id = u.user_id 
                   WHERE c.name = ? AND c.is_active = 1
                     AND (c.visibility = 'public' OR c.creator_id = ?)
                   ORDER BY (c.likes + c.uses/10.0) DESC LIMIT 1""",
                (name.lower(), user_id)
            )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_user_commands(user_id: int, page: int = 0, per_page: int = 5) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            """SELECT c.*, u.nickname as creator_nickname FROM commands c 
               JOIN users u ON c.creator_id = u.user_id 
               WHERE c.creator_id = ? AND c.is_active = 1 ORDER BY c.created_at DESC LIMIT ? OFFSET ?""",
            (user_id, per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
async def get_user_commands_count(user_id: int) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE creator_id = ? AND is_active = 1", (user_id,))
        return (await cursor.fetchone())[0]

async def get_popular_commands(page: int = 0, per_page: int = 5) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            """SELECT c.*, u.nickname as creator_nickname FROM commands c 
               JOIN users u ON c.creator_id = u.user_id 
               WHERE c.is_active = 1 AND c.visibility = 'public'
               ORDER BY (c.likes + c.uses/10.0) DESC LIMIT ? OFFSET ?""",
            (per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_popular_commands_count() -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE is_active = 1 AND visibility = 'public'")
        return (await cursor.fetchone())[0]

async def search_commands(query: str, limit: int = 10, user_id: int | None = None) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if user_id is None:
            cursor = await db.execute(
                """SELECT c.*, u.nickname as creator_nickname FROM commands c 
                   JOIN users u ON c.creator_id = u.user_id 
                   WHERE c.name LIKE ? AND c.is_active = 1 AND c.visibility = 'public'
                   ORDER BY (c.likes + c.uses/10.0) DESC LIMIT ?""",
                (f"%{query.lower()}%", limit)
            )
        else:
            cursor = await db.execute(
                """SELECT c.*, u.nickname as creator_nickname FROM commands c 
                   JOIN users u ON c.creator_id = u.user_id 
                   WHERE c.name LIKE ? AND c.is_active = 1
                     AND (c.visibility = 'public' OR c.creator_id = ?)
                   ORDER BY (c.likes + c.uses/10.0) DESC LIMIT ?""",
                (f"%{query.lower()}%", user_id, limit)
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# ============= FAVORITES =============

async def add_to_favorites(user_id: int, command_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("INSERT INTO favorites (user_id, command_id) VALUES (?, ?)", (user_id, command_id))
            await db.commit()
            return True
        except:
            return False

async def remove_from_favorites(user_id: int, command_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("DELETE FROM favorites WHERE user_id = ? AND command_id = ?", (user_id, command_id))
            await db.commit()
            return True
        except:
            return False

async def is_in_favorites(user_id: int, command_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM favorites WHERE user_id = ? AND command_id = ?", (user_id, command_id))
        return await cursor.fetchone() is not None

async def get_user_favorites(user_id: int, page: int = 0, per_page: int = 5) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            """SELECT c.*, u.nickname as creator_nickname FROM favorites f
               JOIN commands c ON f.command_id = c.id
               JOIN users u ON c.creator_id = u.user_id
               WHERE f.user_id = ? AND c.is_active = 1
                 AND (c.visibility = 'public' OR c.creator_id = ?)
               ORDER BY f.rowid DESC LIMIT ? OFFSET ?""",
            (user_id, user_id, per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_user_favorites_count(user_id: int) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            """SELECT COUNT(*) FROM favorites f
               JOIN commands c ON f.command_id = c.id
               WHERE f.user_id = ? AND c.is_active = 1
                 AND (c.visibility = 'public' OR c.creator_id = ?)""",
            (user_id, user_id)
        )
        return (await cursor.fetchone())[0]

# ============= LIKES =============

async def toggle_like(user_id: int, command_id: int) -> tuple:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT creator_id FROM commands WHERE id = ?", (command_id,))
        row = await cursor.fetchone()
        if row and row[0] == user_id:
            return (None, 0)
        
        cursor = await db.execute("SELECT 1 FROM likes WHERE user_id = ? AND command_id = ?", (user_id, command_id))
        if await cursor.fetchone():
            await db.execute("DELETE FROM likes WHERE user_id = ? AND command_id = ?", (user_id, command_id))
            await db.execute("UPDATE commands SET likes = likes - 1 WHERE id = ?", (command_id,))
            await db.commit()
            cursor = await db.execute("SELECT likes FROM commands WHERE id = ?", (command_id,))
            likes = (await cursor.fetchone())[0]
            return (False, likes)
        else:
            await db.execute("INSERT INTO likes (user_id, command_id) VALUES (?, ?)", (user_id, command_id))
            await db.execute("UPDATE commands SET likes = likes + 1 WHERE id = ?", (command_id,))
            await db.commit()
            cursor = await db.execute("SELECT likes FROM commands WHERE id = ?", (command_id,))
            likes = (await cursor.fetchone())[0]
            return (True, likes)

async def has_liked(user_id: int, command_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM likes WHERE user_id = ? AND command_id = ?", (user_id, command_id))
        return await cursor.fetchone() is not None

async def increment_uses(command_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("UPDATE commands SET uses = uses + 1 WHERE id = ?", (command_id,))
        await db.commit()

async def delete_command(command_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("UPDATE commands SET is_active = 0 WHERE id = ?", (command_id,))
        await db.commit()


async def update_command_field(command_id: int, field: str, value: str) -> bool:
    allowed_fields = {"name", "description", "text_before"}
    if field not in allowed_fields:
        return False

    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute(
                f"UPDATE commands SET {field} = ? WHERE id = ?",
                (value, command_id)
            )
            await db.commit()
            return True
        except Exception:
            return False

# ============= REPORTS =============

async def create_report(command_id: int, reporter_id: int, reason: str) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("INSERT INTO reports (command_id, reporter_id, reason) VALUES (?, ?, ?)", (command_id, reporter_id, reason))
            await db.commit()
            return True
        except:
            return False

async def get_pending_reports(limit: int = 10, offset: int = 0) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT r.*, c.name as command_name, c.creator_id as command_creator_id,
                   c.visibility as command_visibility,
                   ru.nickname as reporter_nickname,
                   cu.nickname as creator_nickname
            FROM reports r 
            JOIN commands c ON r.command_id = c.id 
            JOIN users ru ON r.reporter_id = ru.user_id
            JOIN users cu ON c.creator_id = cu.user_id
            WHERE r.status = 'pending'
            ORDER BY r.created_at DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_report_by_id(report_id: int) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT r.*, c.name as command_name, c.creator_id as command_creator_id,
                   c.visibility as command_visibility, c.is_active as command_is_active,
                   ru.nickname as reporter_nickname,
                   cu.nickname as creator_nickname
            FROM reports r
            JOIN commands c ON r.command_id = c.id
            JOIN users ru ON r.reporter_id = ru.user_id
            JOIN users cu ON c.creator_id = cu.user_id
            WHERE r.id = ?
            LIMIT 1
        """, (report_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def resolve_report(report_id: int, action: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("UPDATE reports SET status = ? WHERE id = ?", (action, report_id))
        await db.commit()

async def get_report_count() -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM reports WHERE status = 'pending'")
        return (await cursor.fetchone())[0]

async def get_stats() -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        stats = {}
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        stats['users'] = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE is_active = 1")
        stats['commands'] = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT SUM(uses) FROM commands")
        stats['total_uses'] = (await cursor.fetchone())[0] or 0
        cursor = await db.execute("SELECT SUM(likes) FROM commands")
        stats['total_likes'] = (await cursor.fetchone())[0] or 0
        return stats

async def block_user(user_id: int, blocked_by: int, reason: str = ""):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("INSERT INTO blocked_users (user_id, blocked_by, reason) VALUES (?, ?, ?)", (user_id, blocked_by, reason))
        await db.execute("UPDATE commands SET is_active = 0 WHERE creator_id = ?", (user_id,))
        await db.commit()

async def unblock_user(user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM blocked_users WHERE user_id = ?", (user_id,))
        await db.commit()

async def get_blocked_users(limit: int = 20) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT b.*, u.nickname FROM blocked_users b 
            JOIN users u ON b.user_id = u.user_id 
            ORDER BY b.blocked_at DESC LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

# ============= STATES =============

class RegisterState(StatesGroup):
    waiting_nickname = State()

class EditNicknameState(StatesGroup):
    waiting_nickname = State()

class EditCommandState(StatesGroup):
    waiting_value = State()

class CreateCommandState(StatesGroup):
    waiting_name = State()
    waiting_description = State()
    waiting_visibility = State()
    waiting_type = State()
    waiting_text = State()
    waiting_text_with_buttons = State()
    waiting_button_name = State()
    waiting_button_result = State()
    waiting_more_buttons = State()

class SearchState(StatesGroup):
    waiting_query = State()

# ============= BOT INIT =============

if PROXY_URL and PROXY_URL.strip():
    logger.info(f"Using proxy: {PROXY_URL}")
    from aiohttp_socks import ProxyConnector
    from aiogram.client.session.aiohttp import AiohttpSession
    connector = ProxyConnector.from_url(PROXY_URL)
    session = AiohttpSession(connector=connector)
    bot = Bot(token=BOT_TOKEN, session=session)
else:
    bot = Bot(token=BOT_TOKEN)

dp = Dispatcher(storage=MemoryStorage())

# ============= KEYBOARDS =============

def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать команду", callback_data="menu_create")
    builder.button(text="🔎 Поиск", callback_data="menu_search")
    builder.button(text="🎯 Мои команды", callback_data="menu_my_commands")
    builder.button(text="⭐ Избранное", callback_data="menu_favorites")
    builder.button(text="🔥 Популярные", callback_data="menu_popular")
    builder.button(text="⚙️ Настройки", callback_data="menu_settings")
    if user_id in ADMIN_IDS:
        builder.button(text="👑 Админ-панель", callback_data="menu_admin")
    builder.adjust(2)
    return builder.as_markup()

def get_back_keyboard(callback_data: str = "back_main") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data=callback_data)
    return builder.as_markup()

def get_commands_keyboard(commands: list, page: int = 0, total_pages: int = 1, prefix: str = "cmd") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    for cmd in commands:
        builder.button(
            text=f"🎯 {cmd['name']}  •  {cmd['uses']}×  •  ❤️ {cmd['likes']}",
            callback_data=f"view_{prefix}_{cmd['id']}"
        )
    
    builder.adjust(1)
    
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"{prefix}_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"{prefix}_page_{page+1}"))
        builder.row(*nav_buttons)
    
    builder.button(text="🏠 В меню", callback_data="back_main")
    return builder.as_markup()

def get_command_view_keyboard(command_id: int, is_owner: bool = False, liked: bool = False, in_favorites: bool = False, visibility: str = 'public') -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    fav_text = "✅ В избранном" if in_favorites else "⭐ В избранное"

    if visibility == 'public':
        like_text = "💔 Убрать лайк" if liked else "❤️ Лайк"
        builder.button(text=like_text, callback_data=f"like_{command_id}")
        builder.button(text=fav_text, callback_data=f"fav_{command_id}")

        if is_owner:
            builder.button(text="📝 Изменить", callback_data=f"edit_{command_id}")
            builder.button(text="🗑️ Удалить", callback_data=f"delete_{command_id}")
            builder.adjust(2, 2)
        else:
            builder.button(text="🚩 Пожаловаться", callback_data=f"report_{command_id}")
            builder.adjust(2, 1)
    else:
        builder.button(text=fav_text, callback_data=f"fav_{command_id}")
        if is_owner:
            builder.button(text="📝 Изменить", callback_data=f"edit_{command_id}")
            builder.button(text="🗑️ Удалить", callback_data=f"delete_{command_id}")
            builder.adjust(1, 2)
        else:
            builder.adjust(1)

    builder.button(text="🏠 В меню", callback_data="back_main")
    return builder.as_markup()

def get_visibility_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🌐 Видна всем", callback_data="visibility_public")
    builder.button(text="🔒 Только для меня", callback_data="visibility_private")
    builder.button(text="◀️ Отмена", callback_data="back_main")
    builder.adjust(1)
    return builder.as_markup()
    

def get_type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👤 Для себя", callback_data="type_self")
    builder.button(text="👥 Для другого", callback_data="type_target")
    builder.button(text="◀️ Отмена", callback_data="back_main")
    builder.adjust(1)
    return builder.as_markup()

def get_more_buttons_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить ещё", callback_data="more_buttons_yes")
    builder.button(text="✅ Хватит", callback_data="more_buttons_no")
    builder.adjust(2)
    return builder.as_markup()

def get_report_keyboard(command_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for reason in REPORT_REASONS:
        builder.button(text=reason, callback_data=f"report_do_{command_id}_{reason}")
    builder.adjust(1)
    builder.button(text="◀️ Отмена", callback_data=f"view_pop_{command_id}")
    return builder.as_markup()


def get_admin_reports_keyboard(reports: list, page: int, total_pages: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for report in reports:
        reason_short = (report['reason'] or '')[:18]
        if len((report['reason'] or '')) > 18:
            reason_short += "…"
        builder.button(
            text=f"#{report['id']} · {report['command_name']} · {reason_short}",
            callback_data=f"admin_report_{report['id']}"
        )

    builder.adjust(1)
    
    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_reports_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_reports_page_{page+1}"))
        builder.row(*nav_buttons)

    builder.button(text="◀️ Назад", callback_data="menu_admin")
    return builder.as_markup()
    

def get_admin_report_actions_keyboard(report_id: int, command_id: int, creator_id: int, command_is_active: bool = True) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Отклонить жалобу", callback_data=f"resolve_{report_id}_reject")
    if command_is_active:
        builder.button(text="🗑️ Удалить команду", callback_data=f"delete_cmd_{command_id}")
    builder.button(text="🚫 Заблокировать автора", callback_data=f"admin_block_from_report_{report_id}_{creator_id}")
    builder.button(text="◀️ К списку жалоб", callback_data="admin_reports")
    builder.adjust(1)
    return builder.as_markup()

# ============= HANDLERS =============

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if await is_user_blocked(user_id):
        await message.answer("🚫 Вы заблокированы.")
        return
    
    if not user:
        await message.answer(
            f"👋 Добро пожаловать в RP Bot!\n\n"
            f"Для начала выберите никнейм.\n"
            f"📝 {MIN_NICKNAME_LENGTH}-{MAX_NICKNAME_LENGTH} символов, только английские буквы.\n\n"
            f"✍️ Введите ваш никнейм:"
        )
        await state.set_state(RegisterState.waiting_nickname)
        return
    
    start_arg = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            start_arg = parts[1].strip().lower()

    if start_arg == "create":
        await message.answer(
            f"➕ Создание команды\n\n"
            f"📝 Название ({MIN_COMMAND_NAME_LENGTH}-{MAX_COMMAND_NAME_LENGTH} символов):",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(CreateCommandState.waiting_name)
        return
    
    await message.answer(
        f"👋 <b>Привет, {html.escape(user['nickname'])}!</b>\n\n"
        f"Выбирай раздел ниже 👇",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message(RegisterState.waiting_nickname)
async def process_nickname(message: types.Message, state: FSMContext):
    nickname = message.text.strip().lower().replace("@", "")
    user_id = message.from_user.id
    
    if not nickname or len(nickname) < MIN_NICKNAME_LENGTH:
        await message.answer(f"❌ Минимум {MIN_NICKNAME_LENGTH} символов! Попробуйте снова:")
        return
    
    if len(nickname) > MAX_NICKNAME_LENGTH:
        await message.answer(f"❌ Максимум {MAX_NICKNAME_LENGTH} символов! Попробуйте снова:")
        return
    
    if ' ' in nickname or not nickname.isalpha() or not nickname.isascii():
        await message.answer("❌ Только английские буквы без пробелов! Попробуйте снова:")
        return
    
    if await is_nickname_taken(nickname):
        await message.answer("❌ Никнейм занят! Выберите другой:")
        return
    
    if await register_user(user_id, nickname):
        await message.answer(
            f"✅ Успешно!\n\n"
            f"🎉 Ваш никнейм: {nickname}\n\n"
            f"📋 Меню:",
            reply_markup=get_main_keyboard(user_id)
        )
    else:
        await message.answer("❌ Ошибка регистрации. Попробуйте позже.")
    await state.clear()

# ============= МЕНЮ =============

@dp.callback_query(F.data == "back_main")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await callback.message.edit_text("❌ Напишите /start", reply_markup=None)
        await callback.answer()
        return
    
    await callback.message.edit_text(
        f"👋 <b>Привет, {html.escape(user['nickname'])}!</b>\n\n"
        f"Выбирай раздел ниже 👇",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(user_id)
    )
    await callback.answer()

@dp.callback_query(F.data == "menu_settings")
async def menu_settings(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await callback.answer("❌ Напишите /start")
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Изменить ник", callback_data="change_nick")
    builder.button(text="◀️ Меню", callback_data="back_main")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"⚙️ Настройки\n\n👤 Ваш ник: {user['nickname']}",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "change_nick")
async def change_nick_start(callback: types.CallbackQuery, state: FSMContext):
    user = await get_user(callback.from_user.id)
    if not user:
        await callback.answer("❌ Напишите /start")
        return
    
    await callback.message.edit_text(
        f"✏️ Введите новый ник ({MIN_NICKNAME_LENGTH}-{MAX_NICKNAME_LENGTH} символов):",
        reply_markup=get_back_keyboard("menu_settings")
    )
    await state.set_state(EditNicknameState.waiting_nickname)
    await callback.answer()

@dp.message(EditNicknameState.waiting_nickname)
async def change_nick_process(message: types.Message, state: FSMContext):
    nickname = message.text.strip().lower().replace("@", "")
    user_id = message.from_user.id
    
    if len(nickname) < MIN_NICKNAME_LENGTH or len(nickname) > MAX_NICKNAME_LENGTH:
        await message.answer(f"❌ От {MIN_NICKNAME_LENGTH} до {MAX_NICKNAME_LENGTH} символов!")
        return
    
    if ' ' in nickname or not nickname.isalpha() or not nickname.isascii():
        await message.answer("❌ Только английские буквы без пробелов!")
        return
    
    if await is_nickname_taken(nickname):
        await message.answer("❌ Никнейм занят!")
        return
    
    if await update_nickname(user_id, nickname):
        await message.answer(f"✅ Ник изменён на: {nickname}", reply_markup=get_main_keyboard(user_id))
    else:
        await message.answer("❌ Ошибка")
    await state.clear()

@dp.callback_query(F.data == "menu_my_commands")
async def menu_my_commands(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    total = await get_user_commands_count(user_id)
    
    if total == 0:
        await callback.message.edit_text(
            "📭 У вас пока нет команд.\n\n💡 Хотите создать первую?",
            reply_markup=get_back_keyboard()
        )
        await callback.answer()
        return
    
    commands = await get_user_commands(user_id, page=0)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"🎯 Ваши команды:",
        reply_markup=get_commands_keyboard(commands, page=0, total_pages=total_pages, prefix="my")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("my_page_"))
async def my_commands_page(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split("_")[-1])
    
    total = await get_user_commands_count(user_id)
    commands = await get_user_commands(user_id, page=page)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"🎯 Ваши команды:",
        reply_markup=get_commands_keyboard(commands, page=page, total_pages=total_pages, prefix="my")
    )
    await callback.answer()

@dp.callback_query(F.data == "menu_favorites")
async def menu_favorites(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    total = await get_user_favorites_count(user_id)
    
    if total == 0:
        await callback.message.edit_text(
            "⭐ Избранное пусто.\n\n💡 Добавляйте команды в избранное для быстрого доступа!",
            reply_markup=get_back_keyboard()
        )
        await callback.answer()
        return
    
    commands = await get_user_favorites(user_id, page=0)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"⭐ Избранное:",
        reply_markup=get_commands_keyboard(commands, page=0, total_pages=total_pages, prefix="fav")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("fav_page_"))
async def favorites_page(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    page = int(callback.data.split("_")[-1])
    
    total = await get_user_favorites_count(user_id)
    commands = await get_user_favorites(user_id, page=page)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"⭐ Избранное:",
        reply_markup=get_commands_keyboard(commands, page=page, total_pages=total_pages, prefix="fav")
    )
    await callback.answer()

@dp.callback_query(F.data == "menu_popular")
async def menu_popular(callback: types.CallbackQuery):
    total = await get_popular_commands_count()
    
    if total == 0:
        await callback.message.edit_text(
            "📭 Пока нет команд.\n\n💡 Станьте первым!",
            reply_markup=get_back_keyboard()
        )
        await callback.answer()
        return
    
    commands = await get_popular_commands(page=0)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"🔥 Популярные:",
        reply_markup=get_commands_keyboard(commands, page=0, total_pages=total_pages, prefix="pop")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("pop_page_"))
async def popular_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[-1])
    
    total = await get_popular_commands_count()
    commands = await get_popular_commands(page=page)
    total_pages = (total + 4) // 5
    
    await callback.message.edit_text(
        f"🔥 Популярные:",
        reply_markup=get_commands_keyboard(commands, page=page, total_pages=total_pages, prefix="pop")
    )
    await callback.answer()

@dp.callback_query(F.data == "menu_search")
async def menu_search(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🔍 Введите название команды для поиска:",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(SearchState.waiting_query)
    await callback.answer()

@dp.message(SearchState.waiting_query)
async def process_search(message: types.Message, state: FSMContext):
    query = message.text.strip()
    commands = await search_commands(query, user_id=message.from_user.id)
    
    if not commands:
        await message.answer("❌ Ничего не найдено", reply_markup=get_back_keyboard())
    else:
        await message.answer(
            "🔍 Результаты поиска:",
            reply_markup=get_commands_keyboard(commands, prefix="pop")
        )
    await state.clear()

# ============= ПРОСМОТР КОМАНД =============

@dp.callback_query(F.data.startswith("view_my_"))
async def view_my_command(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    await show_command(callback, command_id)

@dp.callback_query(F.data.startswith("view_fav_"))
async def view_fav_command(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    await show_command(callback, command_id)

@dp.callback_query(F.data.startswith("view_pop_"))
async def view_pop_command(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    await show_command(callback, command_id)

async def show_command(callback: types.CallbackQuery, command_id: int):
    command = await get_command(command_id)
    if not command:
        await callback.answer("❌ Команда не найдена")
        return
    
    user_id = callback.from_user.id
    liked = await has_liked(user_id, command_id)
    in_favorites = await is_in_favorites(user_id, command_id)
    is_owner = command['creator_id'] == user_id
    
    if command['command_type'] == 'self':
        type_text = "👤 Для себя"
    else:
        type_text = "👥 Для другого"

    if command.get('visibility') == 'private' and command['creator_id'] != user_id:
        await callback.answer("🔒 Это приватная команда", show_alert=True)
        return
    
    visibility_text = "🔒 Только для меня" if command.get('visibility') == 'private' else "🌐 Видна всем"

    text = (
        f"🎯 <b>{html.escape(command['name'])}</b>\n\n"
        f"📝 {html.escape(command['description'] or 'Без описания')}\n"
        f"{type_text}\n"
        f"{visibility_text}\n"
        f"👤 Автор: <b>{html.escape(command['creator_nickname'])}</b>\n\n"
        f"📊 Использований: <b>{command['uses']}</b>\n"
        f"❤️ Лайков: <b>{command['likes']}</b>"
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=get_command_view_keyboard(
            command_id,
            is_owner=is_owner,
            liked=liked,
            in_favorites=in_favorites,
            visibility=command.get('visibility', 'public')
        )
    )
    await callback.answer()

# ============= ЛАЙКИ И ИЗБРАННОЕ =============

@dp.callback_query(F.data.startswith("like_"))
async def process_like(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id
    
    command = await get_command(command_id)
    if not command:
        await callback.answer("❌ Команда не найдена")
        return
    
    if command.get('visibility') == 'private':
        await callback.answer("🔒 На приватные команды нельзя ставить лайк", show_alert=True)
        return
    
    liked, likes = await toggle_like(user_id, command_id)
    
    if liked is None:
        await callback.answer("❌ Нельзя лайкать свою команду!", show_alert=True)
        return
    
    await show_command(callback, command_id)
    await callback.answer("❤️ Лайк!" if liked else "💔 Лайк убран!")

@dp.callback_query(F.data.startswith("fav_"))
async def process_favorite(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id
    
    in_favorites = await is_in_favorites(user_id, command_id)
    
    if in_favorites:
        await remove_from_favorites(user_id, command_id)
        await callback.answer("💔 Убрано из избранного!")
    else:
        await add_to_favorites(user_id, command_id)
        await callback.answer("⭐ Добавлено в избранное!")
    
    await show_command(callback, command_id)

# ============= УДАЛЕНИЕ =============

@dp.callback_query(F.data.startswith("edit_") & ~F.data.startswith("edit_field_"))
async def edit_command_menu(callback: types.CallbackQuery, state: FSMContext):
    command_id = int(callback.data.split("_")[-1])
    command = await get_command(command_id)
    
    if not command or command['creator_id'] != callback.from_user.id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Название", callback_data=f"edit_field_{command_id}_name")
    builder.button(text="📄 Описание", callback_data=f"edit_field_{command_id}_description")
    builder.button(text="✍️ Текст команды", callback_data=f"edit_field_{command_id}_text_before")
    builder.button(text="◀️ Назад", callback_data=f"view_my_{command_id}")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"🛠️ Редактирование команды <b>{html.escape(command['name'])}</b>\n\n"
        f"Выберите, что изменить:",
        parse_mode="HTML",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("edit_field_"))
async def edit_command_field_start(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    command_id = int(parts[2])
    field = "_".join(parts[3:])

    command = await get_command(command_id)
    if not command or command['creator_id'] != callback.from_user.id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    labels = {
        "name": "новое название",
        "description": "новое описание",
        "text_before": "новый текст команды"
    }

    if field not in labels:
        await callback.answer("❌ Поле не поддерживается", show_alert=True)
        return
    
    await state.update_data(edit_command_id=command_id, edit_field=field)
    await state.set_state(EditCommandState.waiting_value)

    await callback.message.edit_text(
        f"✏️ Введите {labels[field]}:",
        reply_markup=get_back_keyboard(f"edit_{command_id}")
    )
    await callback.answer()


@dp.message(EditCommandState.waiting_value)
async def edit_command_field_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    command_id = data.get("edit_command_id")
    field = data.get("edit_field")
    value = (message.text or "").strip()

    if not command_id or not field:
        await state.clear()
        await message.answer("❌ Ошибка редактирования", reply_markup=get_main_keyboard(message.from_user.id))
        return

    command = await get_command(command_id)
    if not command or command['creator_id'] != message.from_user.id:
        await state.clear()
        await message.answer("❌ Нет доступа", reply_markup=get_main_keyboard(message.from_user.id))
        return

    if field == "name":
        if len(value) < MIN_COMMAND_NAME_LENGTH or len(value) > MAX_COMMAND_NAME_LENGTH:
            await message.answer(f"❌ Название: от {MIN_COMMAND_NAME_LENGTH} до {MAX_COMMAND_NAME_LENGTH} символов")
            return
        if ' ' in value or not value.isalnum():
            await message.answer("❌ Название: только буквы и цифры без пробелов")
            return
        value = value.lower()

    if field == "text_before" and not value:
        await message.answer("❌ Текст команды не может быть пустым")
        return

    if not await update_command_field(command_id, field, value):
        await message.answer("❌ Не удалось сохранить изменения")
        return

    await state.clear()

    updated_command = await get_command(command_id)
    if not updated_command:
        await message.answer("✅ Изменения сохранены", reply_markup=get_main_keyboard(message.from_user.id))
        return
    
    if updated_command['command_type'] == 'self':
        type_text = "👤 Для себя"
    else:
        type_text = "👥 Для другого"

    visibility_text = "🔒 Только для меня" if updated_command.get('visibility') == 'private' else "🌐 Видна всем"

    text = (
        f"✅ <b>Изменения сохранены</b>\n\n"
        f"🎯 <b>{html.escape(updated_command['name'])}</b>\n\n"
        f"📝 {html.escape(updated_command['description'] or 'Без описания')}\n"
        f"{type_text}\n"
        f"{visibility_text}\n"
        f"👤 Автор: <b>{html.escape(updated_command['creator_nickname'])}</b>"
    )

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=get_main_keyboard(message.from_user.id)
    )


@dp.callback_query(F.data.startswith("delete_"))
async def delete_command_confirm(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    command = await get_command(command_id)
    
    if not command or command['creator_id'] != callback.from_user.id:
        await callback.answer("❌ Нет доступа")
        return
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"confirm_del_{command_id}")
    builder.button(text="❌ Отмена", callback_data=f"view_my_{command_id}")
    builder.adjust(2)
    
    await callback.message.edit_text(
        f"🗑️ Удалить команду \"{command['name']}\"?\n\n⚠️ Это действие нельзя отменить!",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_del_"))
async def delete_command_confirm_exec(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    await delete_command(command_id)
    await callback.answer("✅ Команда удалена!")
    await menu_my_commands(callback)

# ============= ЖАЛОБЫ =============

@dp.callback_query(F.data.startswith("report_") & ~F.data.startswith("report_do_"))
async def report_start(callback: types.CallbackQuery):
    command_id = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id
    
    command = await get_command(command_id)
    if not command:
        await callback.answer("❌ Команда не найдена")
        return
    
    if command.get('visibility') == 'private':
        await callback.answer("🔒 На приватные команды нельзя жаловаться", show_alert=True)
        return
    
    if command['creator_id'] == user_id:
        await callback.answer("❌ Нельзя жаловаться на свою команду!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "⚠️ Выберите причину жалобы:",
        reply_markup=get_report_keyboard(command_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("report_do_"))
async def report_process(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    command_id = int(parts[2])
    reason = "_".join(parts[3:])
    user_id = callback.from_user.id
    
    command = await get_command(command_id)
    if not command:
        await callback.answer("❌ Команда не найдена", show_alert=True)
        return
    
    if command.get('visibility') == 'private':
        await callback.answer("🔒 На приватные команды нельзя жаловаться", show_alert=True)
        return
    
    if command['creator_id'] == user_id:
        await callback.answer("❌ Нельзя жаловаться на свою команду!", show_alert=True)
        return
    
    if await create_report(command_id, user_id, reason):
        await callback.message.edit_text(
            "✅ Жалоба отправлена!\n\nАдминистрация рассмотрит её.",
            reply_markup=get_back_keyboard()
        )
    else:
        await callback.message.edit_text("❌ Ошибка отправки.", reply_markup=get_back_keyboard())
    
    await callback.answer()

# ============= СОЗДАНИЕ КОМАНД =============

@dp.callback_query(F.data == "menu_create")
async def menu_create(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await callback.answer("❌ Напишите /start")
        return
    
    if await is_user_blocked(user_id):
        await callback.answer("🚫 Вы заблокированы!")
        return
    
    await callback.message.edit_text(
        f"➕ Создание команды\n\n"
        f"📝 Название ({MIN_COMMAND_NAME_LENGTH}-{MAX_COMMAND_NAME_LENGTH} символов):",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_name)
    await callback.answer()

@dp.message(CreateCommandState.waiting_name)
async def create_name(message: types.Message, state: FSMContext):
    name = message.text.strip().lower()
    
    if len(name) < MIN_COMMAND_NAME_LENGTH or len(name) > MAX_COMMAND_NAME_LENGTH:
        await message.answer(f"❌ От {MIN_COMMAND_NAME_LENGTH} до {MAX_COMMAND_NAME_LENGTH} символов!")
        return
    
    if ' ' in name or not name.isalnum():
        await message.answer("❌ Только буквы и цифры без пробелов!")
        return
    
    await state.update_data(name=name)
    await message.answer("📝 Описание команды:", reply_markup=get_back_keyboard())
    await state.set_state(CreateCommandState.waiting_description)

@dp.message(CreateCommandState.waiting_description)
async def create_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await message.answer(
        "👁️ Кто будет видеть эту команду?",
        reply_markup=get_visibility_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_visibility)


@dp.callback_query(CreateCommandState.waiting_visibility, F.data.startswith("visibility_"))
async def create_visibility(callback: types.CallbackQuery, state: FSMContext):
    visibility = "private" if callback.data == "visibility_private" else "public"
    await state.update_data(visibility=visibility)

    await callback.message.edit_text(
        "🎭 Выберите тип команды:\n\n"
        "👤 Для себя — действие от вашего имени\n"
        "👥 Для другого — действие с выбором ответа кнопкой",
        reply_markup=get_type_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_type)
    await callback.answer()

@dp.callback_query(CreateCommandState.waiting_type, F.data.startswith("type_"))
async def create_type(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "type_self":
        command_type = "self"
    else:
        command_type = "target"

    await state.update_data(command_type=command_type)

    if command_type == "self":
        await callback.message.edit_text(
            "📄 Введите текст команды.\n\n"
            "Пример: «упал(а)»\n"
            "Результат: Иван упал(а)",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(CreateCommandState.waiting_text)
    else:
        await state.update_data(has_buttons=True, buttons=[])
        await callback.message.edit_text(
            "📄 Введите текст команды (до выбора кнопки).\n\n"
            "Пример: «хочет поцеловать»\n"
            "Результат: Иван хочет поцеловать Машу\n\n"
            "Далее вы добавите кнопки ответа.",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(CreateCommandState.waiting_text_with_buttons)

    await callback.answer()


@dp.message(CreateCommandState.waiting_text)
async def create_text(message: types.Message, state: FSMContext):
    await state.update_data(text_before=message.text.strip())
    await finish_create(message, state)

@dp.message(CreateCommandState.waiting_text_with_buttons)
async def create_text_with_buttons(message: types.Message, state: FSMContext):
    await state.update_data(text_before=message.text.strip(), buttons=[])
    await message.answer(
        "🔘 Кнопка 1/5\n\n"
        "Введите название кнопки:\n"
        "Пример: Принять, Да, Ок",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_button_name)

@dp.message(CreateCommandState.waiting_button_name)
async def create_button_name(message: types.Message, state: FSMContext):
    await state.update_data(current_button_name=message.text.strip())
    
    await message.answer(
        "📄 Введите результат при нажатии:\n"
        "Пример: обнял(а) → Иван обнял(а) Машу",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_button_result)

@dp.message(CreateCommandState.waiting_button_result)
async def create_button_result(message: types.Message, state: FSMContext):
    data = await state.get_data()
    buttons = data.get('buttons', [])
    current_name = data.get('current_button_name')
    
    buttons.append({
        "name": current_name,
        "result": message.text.strip()
    })
    
    await state.update_data(buttons=buttons)
    
    if len(buttons) >= 5:
        await finish_create(message, state)
    else:
        await message.answer(
            f"✅ Кнопка «{current_name}» добавлена!\n\n"
            f"📊 Кнопок: {len(buttons)}/5",
            reply_markup=get_more_buttons_keyboard()
        )
        await state.set_state(CreateCommandState.waiting_more_buttons)

@dp.callback_query(CreateCommandState.waiting_more_buttons, F.data == "more_buttons_yes")
async def more_buttons_yes(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    count = len(data.get('buttons', []))
    
    await callback.message.edit_text(
        f"🔘 Кнопка {count + 1}/5\n\n"
        "Введите название кнопки:",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(CreateCommandState.waiting_button_name)
    await callback.answer()

@dp.callback_query(CreateCommandState.waiting_more_buttons, F.data == "more_buttons_no")
async def more_buttons_no(callback: types.CallbackQuery, state: FSMContext):
    await finish_create(callback.message, state)
    await callback.answer()

async def finish_create(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.chat.id
    
    name = data.get('name')
    description = data.get('description', '')
    command_type = data.get('command_type', 'self')
    visibility = data.get('visibility', 'public')
    text_before = data.get('text_before', '')
    buttons = data.get('buttons', [])
    
    try:
        command_id = await create_command_db(user_id, name, description, command_type, visibility, text_before, buttons)
        
        if command_id:
            await message.answer(
                f"✅ Команда «{name}» создана!\n\n"
                f"🎮 Используйте: @{BOT_NAME} {name}"
            )
            await message.answer(
                "📋 Меню:",
                reply_markup=get_main_keyboard(user_id)
            )
        else:
            await message.answer("❌ Ошибка создания", reply_markup=get_main_keyboard(user_id))
    except Exception as e:
        logger.error(f"Error creating command: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=get_main_keyboard(user_id))
    
    await state.clear()

# ============= INLINE ЗАПРОС =============

@dp.inline_query()
async def inline_query(query: types.InlineQuery):
    user_id = query.from_user.id
    user = await get_user(user_id)

    raw_query = query.query.strip()
    query_text = raw_query.lower()

    # Можно передать цель прямо в inline-запросе: "команда @username"
    mention_match = re.search(r"@([a-zA-Z0-9_]{5,32})", raw_query)
    target_mention = f"@{mention_match.group(1)}" if mention_match else ""

    # Пытаемся красиво резолвить цель: имя + кликабельная ссылка
    target_link_html = ""
    if target_mention:
        try:
            target_chat = await bot.get_chat(target_mention)
            target_name = target_chat.first_name or target_mention
            target_username = getattr(target_chat, "username", None)
            if target_username:
                target_link_html = f"<a href=\"https://t.me/{html.escape(target_username)}\">{html.escape(target_name)}</a>"
            else:
                target_link_html = f"<a href=\"tg://user?id={target_chat.id}\">{html.escape(target_name)}</a>"
        except:
            target_link_html = html.escape(target_mention)

    # Для поиска команды убираем @mention, чтобы поиск не ломался
    search_query = re.sub(r"@([a-zA-Z0-9_]{5,32})", "", query_text).strip()

    results = []
    
    if not user:
        open_bot_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📝 Открыть бота", url=f"https://t.me/{BOT_NAME}?start=register")
        ]])
        results.append(
            InlineQueryResultArticle(
                id="register",
                title="📝 Регистрация",
                description="Нажмите для регистрации в боте",
                input_message_content=InputTextMessageContent(
                    message_text="📝 Для использования команд необходимо зарегистрироваться"
                ),
                reply_markup=open_bot_kb
            )
        )
        await query.answer(
            results,
            cache_time=0,
            button=types.InlineQueryResultsButton(
                text="📝 Открыть бота",
                start_parameter="register"
            )
        )
        return
    
    def build_inline_article(cmd: dict, title_prefix: str = "🎯") -> InlineQueryResultArticle:
        initiator_name = query.from_user.first_name or "Игрок"
        initiator_username = query.from_user.username
        if initiator_username:
            initiator_link = f"<a href=\"https://t.me/{html.escape(initiator_username)}\">{html.escape(initiator_name)}</a>"
        else:
            initiator_link = f"<a href=\"tg://user?id={user_id}\">{html.escape(initiator_name)}</a>"

        if cmd['command_type'] == 'self':
            message_text = f"{initiator_link} {html.escape(cmd['text_before'])}"
            markup = None
        else:
            message_text = f"{initiator_link} {html.escape(cmd['text_before'])}"
            try:
                buttons = json.loads(cmd['buttons']) if cmd['buttons'] else []
            except:
                buttons = []

            if buttons:
                kb = InlineKeyboardBuilder()
                for i, btn in enumerate(buttons):
                    kb.button(text=btn['name'], callback_data=f"rp_{cmd['public_id']}_{user_id}_0_{i}")
                kb.adjust(2)
                markup = kb.as_markup()
            else:
                if target_link_html:
                    message_text = f"{message_text} {target_link_html}"
                elif target_mention:
                    message_text = f"{message_text} {html.escape(target_mention)}"
                markup = None

        visibility_mark = "🔒 " if cmd.get('visibility') == 'private' else ""

        return InlineQueryResultArticle(
            id=f"cmdv3_{cmd['public_id']}",
            title=f"{title_prefix} {visibility_mark}{cmd['name']}",
            description=f"{(cmd['description'] or '')[:40]} | {cmd['uses']}🎯 {cmd['likes']}❤️",
            input_message_content=InputTextMessageContent(
                message_text=message_text,
                parse_mode="HTML",
                disable_web_page_preview=True
            ),
            reply_markup=markup
        )

    # Если пустой запрос:
    # 1) есть избранные -> показываем только избранные
    # 2) нет избранных -> показываем популярные (сверху вниз)
    if not search_query:
        favorites = await get_user_favorites(user_id, page=0, per_page=25)

        if favorites:
            for cmd in favorites:
                results.append(build_inline_article(cmd, title_prefix="⭐"))
        else:
            popular = await get_popular_commands(page=0, per_page=25)
            for cmd in popular:
                results.append(build_inline_article(cmd, title_prefix="🎯"))
    else:
        # Ищем по названию
        commands = await search_commands(search_query, limit=25, user_id=user_id)
        for cmd in commands:
            results.append(build_inline_article(cmd, title_prefix="🎯"))

    await query.answer(
        results,
        cache_time=0,
        button=types.InlineQueryResultsButton(
            text="➕ Создать команду",
            start_parameter="create"
        )
    )


@dp.chosen_inline_result()
async def chosen_inline_result_handler(chosen: ChosenInlineResult):
    # Считаем использование команды при выборе результата в inline
    if not (
        chosen.result_id.startswith("cmd_")
        or chosen.result_id.startswith("cmdv2_")
        or chosen.result_id.startswith("cmdv3_")
    ):
        return
    
    if chosen.result_id.startswith("cmdv3_"):
        public_id = chosen.result_id[6:]
    elif chosen.result_id.startswith("cmdv2_"):
        public_id = chosen.result_id[6:]
    else:
        public_id = chosen.result_id[4:]

    command = await get_command_by_public_id(public_id)
    if not command:
        return
    
    await increment_uses(command['id'])

# ============= ВЫПОЛНЕНИЕ RP КОМАНД =============

@dp.message(F.text.startswith("!rp_"))
async def execute_inline_command(message: types.Message):
    """Выполнение RP команды из inline"""
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await message.answer("❌ Напишите /start в бота!")
        return
    
    if await is_user_blocked(user_id):
        await message.answer("🚫 Вы заблокированы!")
        return
    
    command_token = message.text[4:].strip()
    if not command_token:
        return
    
    # Новый формат: !rp_<public_id> (уникальный UUID hex)
    command = await get_command_by_public_id(command_token)

    # Обратная совместимость: старый формат !rp_<numeric_id>
    if not command and command_token.isdigit():
        command = await get_command(int(command_token))

    if not command or not command['is_active']:
        await message.answer("❌ Команда не найдена!")
        return
    
    if command.get('visibility') == 'private' and command['creator_id'] != user_id:
        await message.answer("🔒 Эта команда приватная")
        return
    
    await execute_command(message, command, user)

@dp.message(F.text)
async def execute_text_command(message: types.Message):
    """Выполнение RP команды по названию"""
    if message.text.startswith('/'):
        return
    
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user:
        return
    
    if await is_user_blocked(user_id):
        return
    
    command_name = message.text.strip().lower()
    
    if not command_name:
        return
    
    command = await get_command_by_name(command_name, user_id=user_id)
    
    if not command or not command['is_active']:
        return
    
    await execute_command(message, command, user)

async def execute_command(message: types.Message, command: dict, user: dict):
    """Выполнение RP команды"""
    user_id = user['user_id']
    initiator = message.from_user
    initiator_id = initiator.id
    initiator_name = initiator.first_name
    
    # Создаём кликабельную ссылку на профиль
    initiator_link = f"[{initiator_name}](tg://user?id={initiator_id})"
    
    if command['command_type'] == 'self':
        text = f"{initiator_link} {command['text_before']}"
        await message.answer(text, parse_mode="Markdown")
        await increment_uses(command['id'])
        return
    
    target = None
    if message.reply_to_message:
        target = message.reply_to_message.from_user
    
    if not target:
        await message.answer("❌ Ответьте на сообщение игрока!")
        return
    
    target_name = target.first_name
    target_id = target.id
    target_link = f"[{target_name}](tg://user?id={target_id})"
    
    try:
        buttons = json.loads(command['buttons']) if command['buttons'] else []
    except:
        buttons = []

    # Для типа "Для другого" без кнопок создание запрещено,
    # но для старых записей даём понятную диагностику.
    if not buttons:
        await message.answer("❌ Для команды типа «Для другого» нужны кнопки. Пересоздайте команду.")
        return
    
    text = f"{initiator_link} {command['text_before']} {target_link}"
    
    builder = InlineKeyboardBuilder()
    for i, btn in enumerate(buttons):
        builder.button(text=btn['name'], callback_data=f"rp_{command['public_id']}_{initiator_id}_{target_id}_{i}")
    builder.adjust(1)
    
    await message.answer(text, parse_mode="Markdown", reply_markup=builder.as_markup())
    await increment_uses(command['id'])

@dp.callback_query(F.data.startswith("rp_"))
async def rp_button_press(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    command_token = parts[1]
    initiator_id = int(parts[2])
    target_id = int(parts[3])
    button_index = int(parts[4])
    
    # Для inline-режима target_id=0: цель определяется первым нажавшим (кроме инициатора)
    if target_id == 0:
        if callback.from_user.id == initiator_id:
            await callback.answer("❌ Нельзя нажать на свою же кнопку!", show_alert=True)
            return
        target_id = callback.from_user.id
    elif callback.from_user.id != target_id:
        await callback.answer("❌ Только адресат может нажать!", show_alert=True)
        return
    
    command = await get_command_by_public_id(command_token)
    if not command and command_token.isdigit():
        command = await get_command(int(command_token))
    if not command:
        await callback.answer("❌ Команда не найдена!")
        return
    
    if command.get('visibility') == 'private' and callback.from_user.id != command['creator_id']:
        await callback.answer("🔒 Это приватная команда", show_alert=True)
        return
    
    try:
        buttons = json.loads(command['buttons']) if command['buttons'] else []
    except:
        buttons = []
    
    if button_index >= len(buttons):
        await callback.answer("❌ Ошибка!")
        return
    button = buttons[button_index]
    
    # Обычный callback (не inline)
    if callback.message:
        initiator_name = "Игрок"
        initiator_username = None
        try:
            initiator_chat = await bot.get_chat(initiator_id)
            initiator_name = initiator_chat.first_name or "Игрок"
            initiator_username = initiator_chat.username
        except:
            entities = callback.message.entities or []
            initiator_name = entities[0].text if len(entities) > 0 else "Игрок"

        target_name = callback.from_user.first_name or "Игрок"
        target_username = callback.from_user.username

        if initiator_username:
            initiator_link = f"<a href=\"https://t.me/{html.escape(initiator_username)}\">{html.escape(initiator_name)}</a>"
        else:
            initiator_link = f"<a href=\"tg://user?id={initiator_id}\">{html.escape(initiator_name)}</a>"

        if target_username:
            target_link = f"<a href=\"https://t.me/{html.escape(target_username)}\">{html.escape(target_name)}</a>"
        else:
            target_link = f"<a href=\"tg://user?id={target_id}\">{html.escape(target_name)}</a>"

        result_text = f"{initiator_link} {html.escape(button['result'])} {target_link}"

        await callback.message.edit_text(result_text, parse_mode="HTML", disable_web_page_preview=True)
        await callback.answer()
        return
    
    # Inline callback
    inline_message_id = callback.inline_message_id
    if not inline_message_id:
        await callback.answer("❌ Не удалось обработать кнопку", show_alert=True)
        return
    
    initiator_name = "Игрок"
    initiator_username = None
    try:
        initiator_chat = await bot.get_chat(initiator_id)
        initiator_name = initiator_chat.first_name or "Игрок"
        initiator_username = initiator_chat.username
    except:
        initiator_user = await get_user(initiator_id)
        initiator_name = initiator_user['nickname'] if initiator_user else "Игрок"

    target_name = callback.from_user.first_name or "Игрок"
    target_username = callback.from_user.username

    if initiator_username:
        initiator_link = f"<a href=\"https://t.me/{html.escape(initiator_username)}\">{html.escape(initiator_name)}</a>"
    else:
        initiator_link = f"<a href=\"tg://user?id={initiator_id}\">{html.escape(initiator_name)}</a>"

    if target_username:
        target_link = f"<a href=\"https://t.me/{html.escape(target_username)}\">{html.escape(target_name)}</a>"
    else:
        target_link = f"<a href=\"tg://user?id={target_id}\">{html.escape(target_name)}</a>"

    result_text = f"{initiator_link} {html.escape(button['result'])} {target_link}"

    await bot.edit_message_text(
        text=result_text,
        inline_message_id=inline_message_id,
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    await callback.answer()

# ============= АДМИН ПАНЕЛЬ =============

@dp.callback_query(F.data == "menu_admin")
async def menu_admin(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    stats = await get_stats()
    report_count = await get_report_count()
    
    builder = InlineKeyboardBuilder()
    builder.button(text=f"📋 Жалобы ({report_count})", callback_data="admin_reports")
    builder.button(text="🚫 Заблокированные", callback_data="admin_blocked")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="◀️ Меню", callback_data="back_main")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"👑 Админ панель\n\n"
        f"👥 Пользователей: {stats['users']}\n"
        f"🎯 Команд: {stats['commands']}\n"
        f"🎯 Использований: {stats['total_uses']}\n"
        f"❤️ Лайков: {stats['total_likes']}",
        reply_markup=builder.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_reports")
async def admin_reports(callback: types.CallbackQuery):
    await admin_reports_page_render(callback, page=0)


@dp.callback_query(F.data.startswith("admin_reports_page_"))
async def admin_reports_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[-1])
    await admin_reports_page_render(callback, page=page)


async def admin_reports_page_render(callback: types.CallbackQuery, page: int = 0):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    per_page = 5
    total = await get_report_count()

    if total == 0:
        await callback.message.edit_text(
            "✅ Нет жалоб",
            reply_markup=get_back_keyboard("menu_admin")
        )
        await callback.answer()
        return
    
    total_pages = (total + per_page - 1) // per_page
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1

    reports = await get_pending_reports(limit=per_page, offset=page * per_page)

    lines = [
        f"📋 <b>Жалобы</b> ({total})",
        "",
        "Выберите жалобу из списка ниже:"
    ]

    for idx, report in enumerate(reports, start=1 + page * per_page):
        lines.append(
            f"{idx}. #{report['id']} | {html.escape(report['command_name'])} | "
            f"{html.escape(report['reporter_nickname'])}"
        )

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=get_admin_reports_keyboard(reports, page=page, total_pages=total_pages)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_report_"))
async def admin_report_view(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    report_id = int(callback.data.split("_")[-1])
    report = await get_report_by_id(report_id)

    if not report or report.get('status') != 'pending':
        await callback.answer("❌ Жалоба не найдена или уже обработана", show_alert=True)
        await admin_reports_page_render(callback, page=0)
        return
    
    visibility_text = "🔒 Приватная" if report.get('command_visibility') == 'private' else "🌐 Публичная"
    active_text = "✅ Активна" if report.get('command_is_active') else "🗑️ Уже удалена"

    text = (
        f"📄 <b>Жалоба #{report['id']}</b>\n\n"
        f"🎯 Команда: <b>{html.escape(report['command_name'])}</b>\n"
        f"👤 Автор команды: <b>{html.escape(report['creator_nickname'])}</b>\n"
        f"📝 Репорт от: <b>{html.escape(report['reporter_nickname'])}</b>\n"
        f"⚠️ Причина: <b>{html.escape(report['reason'])}</b>\n"
        f"📌 Видимость: {visibility_text}\n"
        f"📦 Статус команды: {active_text}\n"
        f"🕒 Дата: {report['created_at']}"
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=get_admin_report_actions_keyboard(
            report_id=report['id'],
            command_id=report['command_id'],
            creator_id=report['command_creator_id'],
            command_is_active=bool(report.get('command_is_active', 1))
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("resolve_"))
async def resolve_report_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    report_id = int(parts[1])
    action = parts[2]

    await resolve_report(report_id, action)
    await callback.answer("✅ Жалоба обработана")
    await admin_reports_page_render(callback, page=0)

@dp.callback_query(F.data.startswith("delete_cmd_"))
async def delete_command_admin(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    command_id = int(callback.data.split("_")[-1])
    await delete_command(command_id)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE reports SET status = 'approved' WHERE command_id = ? AND status = 'pending'",
            (command_id,)
        )
        await db.commit()

    await callback.answer("✅ Команда удалена")
    await admin_reports_page_render(callback, page=0)


@dp.callback_query(F.data.startswith("admin_block_from_report_"))
async def admin_block_from_report(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    parts = callback.data.split("_")
    report_id = int(parts[4])
    target_user_id = int(parts[5])

    await block_user(target_user_id, callback.from_user.id, reason=f"Report #{report_id}")
    await resolve_report(report_id, "approved")

    await callback.answer("🚫 Автор заблокирован")
    await admin_reports_page_render(callback, page=0)

@dp.callback_query(F.data == "admin_blocked")
async def admin_blocked(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    blocked = await get_blocked_users(20)
    
    if not blocked:
        await callback.message.edit_text("✅ Нет заблокированных", reply_markup=get_back_keyboard("menu_admin"))
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for user in blocked:
        builder.button(text=f"🔓 {user['nickname']}", callback_data=f"unblock_{user['user_id']}")
    
    builder.adjust(1)
    builder.button(text="◀️ Назад", callback_data="menu_admin")
    
    await callback.message.edit_text("🚫 Заблокированные:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("unblock_"))
async def unblock_user_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    target_id = int(callback.data.split("_")[-1])
    await unblock_user(target_id)
    await callback.answer("✅ Разблокирован!")
    await admin_blocked(callback)

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    stats = await get_stats()
    
    await callback.message.edit_text(
        f"📊 Статистика\n\n"
        f"👥 Пользователей: {stats['users']}\n"
        f"🎯 Команд: {stats['commands']}\n"
        f"🎯 Использований: {stats['total_uses']}\n"
        f"❤️ Лайков: {stats['total_likes']}",
        reply_markup=get_back_keyboard("menu_admin")
    )
    await callback.answer()

# ============= ЗАПУСК =============

async def main():
    await init_db()
    logger.info("Database initialized")
    logger.info(f"Bot starting: @{BOT_NAME}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
