import asyncio
import logging
import json
import os
import re
import uuid
import html
import calendar
import aiosqlite
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InlineQueryResultArticle, InputTextMessageContent, ChosenInlineResult
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

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
            cursor = await db.execute(
                "SELECT 1 FROM reports WHERE command_id = ? AND reporter_id = ? LIMIT 1",
                (command_id, reporter_id)
            )
            if await cursor.fetchone():
                return False

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


async def get_users_count(status_filter: str = "all") -> int:
    where_parts = []
    if status_filter == "blk":
        where_parts.append("b.user_id IS NOT NULL")
    elif status_filter == "act":
        where_parts.append("b.user_id IS NULL")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            f"""
            SELECT COUNT(*)
            FROM users u
            LEFT JOIN blocked_users b ON b.user_id = u.user_id
            {where_sql}
            """
        )
        return (await cursor.fetchone())[0]


async def get_admin_users_page(page: int = 0, per_page: int = 8, status_filter: str = "all", sort_by: str = "use") -> list:
    where_parts = []
    if status_filter == "blk":
        where_parts.append("b.user_id IS NOT NULL")
    elif status_filter == "act":
        where_parts.append("b.user_id IS NULL")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    order_map = {
        "use": "total_uses DESC, active_commands DESC, u.created_at DESC",
        "new": "u.created_at DESC",
        "cmd": "active_commands DESC, total_uses DESC, u.created_at DESC"
    }
    order_sql = order_map.get(sort_by, order_map["use"])

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            f"""
            SELECT
                u.user_id,
                u.nickname,
                COALESCE(SUM(CASE WHEN c.is_active = 1 THEN 1 ELSE 0 END), 0) AS active_commands,
                COALESCE(SUM(c.uses), 0) AS total_uses,
                COALESCE(SUM(c.likes), 0) AS total_likes,
                CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_blocked
            FROM users u
            LEFT JOIN commands c ON c.creator_id = u.user_id
            LEFT JOIN blocked_users b ON b.user_id = u.user_id
            {where_sql}
            GROUP BY u.user_id, u.nickname, b.user_id
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            (per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_admin_user_profile(user_id: int) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT
                u.user_id,
                u.nickname,
                u.created_at,
                CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_blocked,
                b.blocked_at,
                b.reason AS block_reason,
                COALESCE(COUNT(c.id), 0) AS commands_total,
                COALESCE(SUM(CASE WHEN c.is_active = 1 THEN 1 ELSE 0 END), 0) AS commands_active,
                COALESCE(SUM(CASE WHEN c.is_active = 0 THEN 1 ELSE 0 END), 0) AS commands_inactive,
                COALESCE(SUM(CASE WHEN c.visibility = 'public' AND c.is_active = 1 THEN 1 ELSE 0 END), 0) AS commands_public,
                COALESCE(SUM(CASE WHEN c.visibility = 'private' AND c.is_active = 1 THEN 1 ELSE 0 END), 0) AS commands_private,
                COALESCE(SUM(c.uses), 0) AS total_uses,
                COALESCE(SUM(c.likes), 0) AS total_likes
            FROM users u
            LEFT JOIN commands c ON c.creator_id = u.user_id
            LEFT JOIN blocked_users b ON b.user_id = u.user_id
            WHERE u.user_id = ?
            GROUP BY u.user_id, u.nickname, u.created_at, b.user_id, b.blocked_at, b.reason
            LIMIT 1
            """,
            (user_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_admin_user_commands_count(user_id: int) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE creator_id = ?", (user_id,))
        return (await cursor.fetchone())[0]


async def get_admin_user_commands(user_id: int, page: int = 0, per_page: int = 6) -> list:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            """
            SELECT c.*, u.nickname AS creator_nickname
            FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            WHERE c.creator_id = ?
            ORDER BY c.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    

async def get_admin_commands_count(status_filter: str = "all", visibility_filter: str = "all") -> int:
    where_parts = []
    if status_filter == "act":
        where_parts.append("c.is_active = 1")
    elif status_filter == "del":
        where_parts.append("c.is_active = 0")

    if visibility_filter == "pub":
        where_parts.append("c.visibility = 'public'")
    elif visibility_filter == "prv":
        where_parts.append("c.visibility = 'private'")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(f"SELECT COUNT(*) FROM commands c {where_sql}")
        return (await cursor.fetchone())[0]


async def get_admin_commands_page(page: int = 0, per_page: int = 8, status_filter: str = "all", visibility_filter: str = "all", sort_by: str = "new") -> list:
    where_parts = []
    if status_filter == "act":
        where_parts.append("c.is_active = 1")
    elif status_filter == "del":
        where_parts.append("c.is_active = 0")

    if visibility_filter == "pub":
        where_parts.append("c.visibility = 'public'")
    elif visibility_filter == "prv":
        where_parts.append("c.visibility = 'private'")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    order_map = {
        "new": "c.created_at DESC",
        "old": "c.created_at ASC",
        "use": "c.uses DESC, c.created_at DESC",
        "like": "c.likes DESC, c.created_at DESC"
    }
    order_sql = order_map.get(sort_by, order_map["new"])

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        offset = page * per_page
        cursor = await db.execute(
            f"""
            SELECT c.*, u.nickname AS creator_nickname
            FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            (per_page, offset)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def restore_command(command_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("UPDATE commands SET is_active = 1 WHERE id = ?", (command_id,))
        await db.commit()


async def resolve_pending_reports_for_command(command_id: int, status: str = "approved"):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE reports SET status = ? WHERE command_id = ? AND status = 'pending'",
            (status, command_id)
        )
        await db.commit()


async def get_stats_by_period(period: str = "all") -> dict:
    period_map = {
        "today": "date('now', 'localtime')",
        "week": "date('now', '-6 days', 'localtime')",
        "month": "date('now', 'start of month', 'localtime')",
        "all": None
    }
    start_expr = period_map.get(period, None)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        stats = {}

        users_q = "SELECT COUNT(*) FROM users"
        commands_q = "SELECT COUNT(*) FROM commands"
        uses_q = "SELECT COALESCE(SUM(uses), 0) FROM commands"
        likes_q = "SELECT COALESCE(SUM(likes), 0) FROM commands"
        reports_q = "SELECT COUNT(*) FROM reports"

        if start_expr:
            users_q += f" WHERE date(created_at) >= {start_expr}"
            commands_q += f" WHERE date(created_at) >= {start_expr}"
            uses_q += f" WHERE date(created_at) >= {start_expr}"
            likes_q += f" WHERE date(created_at) >= {start_expr}"
            reports_q += f" WHERE date(created_at) >= {start_expr}"

        cursor = await db.execute(users_q)
        stats['users'] = (await cursor.fetchone())[0]

        cursor = await db.execute(commands_q)
        stats['commands'] = (await cursor.fetchone())[0]

        cursor = await db.execute(uses_q)
        stats['total_uses'] = (await cursor.fetchone())[0] or 0

        cursor = await db.execute(likes_q)
        stats['total_likes'] = (await cursor.fetchone())[0] or 0

        cursor = await db.execute(reports_q)
        stats['reports'] = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE is_active = 1")
        stats['active_commands_total'] = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM blocked_users")
        stats['blocked_users_total'] = (await cursor.fetchone())[0]

        return stats


async def get_stats_by_date_range(date_from: str, date_to: str) -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        stats = {}

        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE date(created_at) BETWEEN date(?) AND date(?)",
            (date_from, date_to)
        )
        stats['users'] = (await cursor.fetchone())[0]

        cursor = await db.execute(
            "SELECT COUNT(*) FROM commands WHERE date(created_at) BETWEEN date(?) AND date(?)",
            (date_from, date_to)
        )
        stats['commands'] = (await cursor.fetchone())[0]
    
        cursor = await db.execute(
            "SELECT COALESCE(SUM(uses), 0) FROM commands WHERE date(created_at) BETWEEN date(?) AND date(?)",
            (date_from, date_to)
        )
        stats['total_uses'] = (await cursor.fetchone())[0] or 0

        cursor = await db.execute(
            "SELECT COALESCE(SUM(likes), 0) FROM commands WHERE date(created_at) BETWEEN date(?) AND date(?)",
            (date_from, date_to)
        )
        stats['total_likes'] = (await cursor.fetchone())[0] or 0

        cursor = await db.execute(
            "SELECT COUNT(*) FROM reports WHERE date(created_at) BETWEEN date(?) AND date(?)",
            (date_from, date_to)
        )
        stats['reports'] = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM commands WHERE is_active = 1")
        stats['active_commands_total'] = (await cursor.fetchone())[0]

        cursor = await db.execute("SELECT COUNT(*) FROM blocked_users")
        stats['blocked_users_total'] = (await cursor.fetchone())[0]

        return stats

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
    

def get_main_menu_text(nickname: str) -> str:
    return (
        f"👋 <b>Привет, {html.escape(nickname)}!</b>\n\n"
        f"Выбирай раздел ниже 👇"
    )

def get_back_keyboard(callback_data: str = "back_main") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="◀️ Назад", callback_data=callback_data)
    return builder.as_markup()


def get_page_picker_keyboard(base_callback: str, current_page: int, total_pages: int, back_callback: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    
    total_pages = max(1, total_pages)
    current_page = max(0, min(current_page, total_pages - 1))

    if total_pages <= 15:
        pages = list(range(total_pages))
    else:
        start = max(0, current_page - 4)
        end = min(total_pages, start + 9)
        if end - start < 9:
            start = max(0, end - 9)
        pages = list(range(start, end))

    for p in pages:
        text = f"·{p+1}·" if p == current_page else str(p + 1)
        builder.button(text=text, callback_data=f"{base_callback}_{p}")

    builder.adjust(5)
    builder.button(text="◀️ Назад", callback_data=back_callback)
    return builder.as_markup()


async def safe_edit_message(callback: types.CallbackQuery, text: str, *, parse_mode: str | None = None, reply_markup: InlineKeyboardMarkup | None = None, disable_web_page_preview: bool | None = None):
    try:
        await callback.message.edit_text(
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


def format_db_datetime(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt_utc = datetime.fromisoformat(str(value).replace(" ", "T")).replace(tzinfo=timezone.utc)
        return dt_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


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
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data=f"admin_reports_pick_{page}_{total_pages}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_reports_page_{page+1}"))
        builder.row(*nav_buttons)

    builder.button(text="◀️ Назад", callback_data="menu_admin")
    return builder.as_markup()


def get_admin_report_actions_keyboard(report_id: int, command_id: int, creator_id: int, command_is_active: bool = True) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔎 Открыть команду", callback_data=f"admin_cmd_{command_id}")
    builder.button(text="👤 Открыть профиль автора", callback_data=f"admin_user_{creator_id}")
    builder.button(text="✅ Отклонить жалобу", callback_data=f"resolve_{report_id}_reject")
    if command_is_active:
        builder.button(text="🗑️ Удалить команду", callback_data=f"delete_cmd_{command_id}")
    builder.button(text="🚫 Заблокировать автора", callback_data=f"admin_block_from_report_{report_id}_{creator_id}")
    builder.button(text="◀️ К списку жалоб", callback_data="admin_reports")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_main_keyboard(report_count: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=f"📋 Жалобы ({report_count})", callback_data="admin_reports")
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🎯 Команды", callback_data="admin_commands")
    builder.button(text="🚫 Заблокированные", callback_data="admin_blocked")
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="◀️ Меню", callback_data="back_main")
    builder.adjust(1)
    return builder.as_markup()
    

def get_admin_users_keyboard(users: list, page: int, total_pages: int, status_filter: str = "all", sort_by: str = "use") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for u in users:
        blocked_mark = "🚫 " if u.get('is_blocked') else ""
        builder.button(
            text=f"{blocked_mark}{u['nickname']} · cmd {u['active_commands']} · use {u['total_uses']}",
            callback_data=f"admin_user_{u['user_id']}"
        )

    builder.adjust(1)

    status_label = {
        "all": "Все",
        "act": "Активные",
        "blk": "Блок"
    }.get(status_filter, "Все")

    sort_label = {
        "use": "Использования",
        "new": "Новые",
        "cmd": "Команды"
    }.get(sort_by, "Использования")

    builder.row(
        InlineKeyboardButton(text=f"👤 {status_label}", callback_data=f"admin_users_filter_menu_{status_filter}_{sort_by}"),
        InlineKeyboardButton(text=f"↕️ {sort_label}", callback_data=f"admin_users_sort_menu_{status_filter}_{sort_by}")
    )

    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_page_{status_filter}_{sort_by}_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data=f"admin_users_pick_{status_filter}_{sort_by}_{page}_{total_pages}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_page_{status_filter}_{sort_by}_{page+1}"))
        builder.row(*nav_buttons)

    builder.button(text="◀️ Назад", callback_data="menu_admin")
    return builder.as_markup()


def get_admin_user_profile_keyboard(user_id: int, is_blocked: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🎯 Команды пользователя", callback_data=f"admin_user_cmds_{user_id}")
    if is_blocked:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_user_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_user_{user_id}")
    builder.button(text="◀️ К пользователям", callback_data="admin_users")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_user_commands_keyboard(commands: list, user_id: int, page: int, total_pages: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for cmd in commands:
        status = "✅" if cmd.get('is_active') else "🗑️"
        visibility = "🔒" if cmd.get('visibility') == 'private' else "🌐"
        cmd_type = "👤" if cmd.get('command_type') == 'self' else "👥"
        builder.button(
            text=f"{status} {visibility} {cmd_type} {cmd['name']} · {cmd['uses']}× · ❤️ {cmd['likes']}",
            callback_data=f"admin_user_cmd_{cmd['id']}_{user_id}"
        )

    builder.adjust(1)

    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_user_cmds_page_{user_id}_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data=f"admin_user_cmds_pick_{user_id}_{page}_{total_pages}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_user_cmds_page_{user_id}_{page+1}"))
        builder.row(*nav_buttons)

    builder.button(text="◀️ В профиль", callback_data=f"admin_user_{user_id}")
    return builder.as_markup()


def get_admin_commands_keyboard(commands: list, page: int, total_pages: int, status_filter: str = "all", visibility_filter: str = "all", sort_by: str = "new") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for cmd in commands:
        status = "✅" if cmd.get('is_active') else "🗑️"
        visibility = "🔒" if cmd.get('visibility') == 'private' else "🌐"
        builder.button(
            text=f"{status} {visibility} {cmd['name']} · @{cmd['creator_nickname']}",
            callback_data=f"admin_cmd_{cmd['id']}"
        )

    builder.adjust(1)
    
    status_label = {
        "all": "Все",
        "act": "Активные",
        "del": "Удалённые"
    }.get(status_filter, "Все")

    visibility_label = {
        "all": "Все",
        "pub": "Публичные",
        "prv": "Приватные"
    }.get(visibility_filter, "Все")

    sort_label = {
        "new": "Новые",
        "old": "Старые",
        "use": "Использования",
        "like": "Лайки"
    }.get(sort_by, "Новые")

    builder.row(
        InlineKeyboardButton(text=f"📦 {status_label}", callback_data=f"admin_commands_filter_status_menu_{status_filter}_{visibility_filter}_{sort_by}"),
        InlineKeyboardButton(text=f"👁️ {visibility_label}", callback_data=f"admin_commands_filter_visibility_menu_{status_filter}_{visibility_filter}_{sort_by}")
    )
    builder.row(
        InlineKeyboardButton(text=f"↕️ {sort_label}", callback_data=f"admin_commands_sort_menu_{status_filter}_{visibility_filter}_{sort_by}")
    )

    if total_pages > 1:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_commands_page_{status_filter}_{visibility_filter}_{sort_by}_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data=f"admin_commands_pick_{status_filter}_{visibility_filter}_{sort_by}_{page}_{total_pages}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_commands_page_{status_filter}_{visibility_filter}_{sort_by}_{page+1}"))
        builder.row(*nav_buttons)

    builder.button(text="◀️ Назад", callback_data="menu_admin")
    return builder.as_markup()


def get_admin_command_actions_keyboard(command_id: int, creator_id: int, is_active: bool, back_callback: str = "admin_commands") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👤 Профиль автора", callback_data=f"admin_user_{creator_id}")
    if is_active:
        builder.button(text="🗑️ Удалить команду", callback_data=f"admin_delete_cmd_{command_id}")
    else:
        builder.button(text="♻️ Восстановить команду", callback_data=f"admin_restore_cmd_{command_id}")
    builder.button(text="◀️ Назад", callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()


def get_admin_stats_keyboard(
    period: str = "all",
    filter_type: str | None = None,
    selected_day: str | None = None,
    selected_from: str | None = None,
    selected_to: str | None = None
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    period_label = {
        "today": "Сегодня",
        "week": "7 дней",
        "month": "Месяц",
        "all": "Всё время"
    }.get(period, "Всё время")

    day_text = "🗓️ Конкретный день"
    day_cb = "admin_stats_cal_day_now"
    if filter_type == "day" and selected_day:
        day_text = f"🗓️ День: {format_human_date_with_weekday(selected_day)}"
        day_cb = f"admin_stats_cal_day_sel_{selected_day}"

    range_text = "📆 Диапазон дат"
    range_cb = "admin_stats_cal_range_from_now"
    if filter_type == "range" and selected_from and selected_to:
        range_text = f"📆 Период: {format_human_date(selected_from)} → {format_human_date(selected_to)}"
        range_cb = f"admin_stats_cal_range_show_{selected_from}_{selected_to}"
    elif filter_type == "range" and selected_from:
        range_text = f"📆 От: {format_human_date_with_weekday(selected_from)}"
        range_cb = f"admin_stats_cal_range_from_sel_{selected_from}"

    builder.button(text=f"📅 Период: {period_label}", callback_data="admin_stats_period_menu")
    builder.button(text=day_text, callback_data=day_cb)
    builder.button(text=range_text, callback_data=range_cb)

    if filter_type:
        builder.button(text="♻️ Сбросить фильтр", callback_data="admin_stats_reset_dates")

    builder.button(text="🔄 Обновить", callback_data=f"admin_stats_{period}")
    builder.button(text="◀️ Назад", callback_data="menu_admin")
    builder.adjust(1)
    return builder.as_markup()


def build_calendar_month(year: int, month: int) -> list[list[int]]:
    return calendar.monthcalendar(year, month)


def format_month_title(year: int, month: int) -> str:
    month_names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
    ]
    return f"🗓️ {month_names[month - 1]} {year}"


def format_human_date(date_text: str | None) -> str:
    if not date_text:
        return "-"
    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return date_text


def format_human_date_with_weekday(date_text: str | None) -> str:
    if not date_text:
        return "-"

    weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
        weekday = weekdays[dt.weekday()]
        return f"{weekday}, {dt.strftime('%d.%m.%Y')}"
    except Exception:
        return date_text


def format_human_date_full_ru(date_text: str | None) -> str:
    if not date_text:
        return "-"

    weekdays = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    months = [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря"
    ]

    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
        return f"{weekdays[dt.weekday()]}, {dt.day} {months[dt.month - 1]} {dt.year}"
    except Exception:
        return date_text


def get_stats_calendar_keyboard(
    mode: str,
    year: int,
    month: int,
    selected_from: str | None = None,
    selected_to: str | None = None,
    back_callback: str | None = None
) -> InlineKeyboardMarkup:
    month_name = format_month_title(year, month)

    if mode == "day":
        title = "📅 Выберите день"
    elif mode == "range_from":
        title = "📆 Выберите начало периода"
    else:
        title = "📆 Выберите конец периода"

    prev_y, prev_m = (year - 1, 12) if month == 1 else (year, month - 1)
    next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)

    if mode == "day":
        prev_cb = f"admin_stats_cal_day_{prev_y}_{prev_m}"
        next_cb = f"admin_stats_cal_day_{next_y}_{next_m}"
    elif mode == "range_from":
        prev_cb = f"admin_stats_cal_range_from_{prev_y}_{prev_m}"
        next_cb = f"admin_stats_cal_range_from_{next_y}_{next_m}"
    else:
        prev_cb = f"admin_stats_cal_range_to_{selected_from}_{prev_y}_{prev_m}"
        next_cb = f"admin_stats_cal_range_to_{selected_from}_{next_y}_{next_m}"

    rows = [
        [InlineKeyboardButton(text=title, callback_data="noop")],
        [InlineKeyboardButton(text=f"════ {month_name} ════", callback_data="noop")],
        [
            InlineKeyboardButton(text="◀️", callback_data=prev_cb),
            InlineKeyboardButton(text="▶️", callback_data=next_cb)
        ],
        [
            InlineKeyboardButton(text="Пн", callback_data="noop"),
            InlineKeyboardButton(text="Вт", callback_data="noop"),
            InlineKeyboardButton(text="Ср", callback_data="noop"),
            InlineKeyboardButton(text="Чт", callback_data="noop"),
            InlineKeyboardButton(text="Пт", callback_data="noop"),
            InlineKeyboardButton(text="Сб", callback_data="noop"),
            InlineKeyboardButton(text="Вс", callback_data="noop")
        ]
    ]

    current_local_date = datetime.now().astimezone().date()

    for week in build_calendar_month(year, month):
        week_row = []
        for day in week:
            if day == 0:
                week_row.append(InlineKeyboardButton(text=" ", callback_data="noop"))
                continue

            btn_date = f"{year:04d}-{month:02d}-{day:02d}"
            btn_day_date = datetime(year, month, day).date()

            is_future = btn_day_date > current_local_date
            is_before_from = mode == "range_to" and selected_from and btn_date < selected_from

            day_plain = f"{day:02d}"
            day_text = day_plain
            if mode == "day":
                if selected_from and selected_to:
                    if selected_from == selected_to and btn_date == selected_from:
                        day_text = f"•{day_plain}•"
                    elif btn_date == selected_from:
                        day_text = f"•{day_plain}"
                    elif btn_date == selected_to:
                        day_text = f"{day_plain}•"
                elif selected_from and btn_date == selected_from:
                    day_text = f"•{day_plain}•"
                elif not selected_from and btn_day_date == current_local_date:
                    day_text = f"•{day_plain}•"
            else:
                if selected_from and btn_date == selected_from:
                    day_text = f"•{day_plain}"
                if selected_to and btn_date == selected_to:
                    day_text = f"{day_plain}•"

            if is_future or is_before_from:
                week_row.append(InlineKeyboardButton(text=day_text, callback_data="noop"))
                continue

            if mode == "day":
                cb = f"admin_stats_day_{btn_date}"
            elif mode == "range_from":
                cb = f"admin_stats_range_from_{btn_date}"
            else:
                cb = f"admin_stats_range_to_{selected_from}_{btn_date}"

            week_row.append(InlineKeyboardButton(text=day_text, callback_data=cb))

        rows.append(week_row)

    if mode == "range_to" and selected_from and selected_to:
        rows.append([
            InlineKeyboardButton(
                text="✅ Показать статистику",
                callback_data=f"admin_stats_range_apply_{selected_from}_{selected_to}"
            )
        ])

    # Кнопка сброса диапазона
    if mode in {"range_from", "range_to"} and (selected_from or selected_to):
        rows.append([InlineKeyboardButton(text="♻️ Сбросить", callback_data="admin_stats_cal_range_reset")])

    back_cb = back_callback or ("admin_stats" if mode in {"day", "range_from"} else "admin_stats_cal_range_from_now")
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_cb)])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_admin_users_filter_menu_keyboard(current_status: str, current_sort: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Все", callback_data=f"admin_users_apply_status_all_{current_sort}")
    builder.button(text="✅ Активные", callback_data=f"admin_users_apply_status_act_{current_sort}")
    builder.button(text="🚫 Заблокированные", callback_data=f"admin_users_apply_status_blk_{current_sort}")
    builder.button(text="◀️ Назад", callback_data=f"admin_users_page_{current_status}_{current_sort}_0")
    builder.adjust(1)
    return builder.as_markup()
    

def get_admin_users_sort_menu_keyboard(current_status: str, current_sort: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🎯 По использованиям", callback_data=f"admin_users_apply_sort_use_{current_status}")
    builder.button(text="🆕 Сначала новые", callback_data=f"admin_users_apply_sort_new_{current_status}")
    builder.button(text="🧱 По числу команд", callback_data=f"admin_users_apply_sort_cmd_{current_status}")
    builder.button(text="◀️ Назад", callback_data=f"admin_users_page_{current_status}_{current_sort}_0")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_commands_status_filter_menu_keyboard(current_status: str, current_visibility: str, current_sort: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Все", callback_data=f"admin_commands_apply_status_all_{current_visibility}_{current_sort}")
    builder.button(text="✅ Активные", callback_data=f"admin_commands_apply_status_act_{current_visibility}_{current_sort}")
    builder.button(text="🗑️ Удалённые", callback_data=f"admin_commands_apply_status_del_{current_visibility}_{current_sort}")
    builder.button(text="◀️ Назад", callback_data=f"admin_commands_page_{current_status}_{current_visibility}_{current_sort}_0")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_commands_visibility_filter_menu_keyboard(current_status: str, current_visibility: str, current_sort: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👁️ Все", callback_data=f"admin_commands_apply_visibility_all_{current_status}_{current_sort}")
    builder.button(text="🌐 Публичные", callback_data=f"admin_commands_apply_visibility_pub_{current_status}_{current_sort}")
    builder.button(text="🔒 Приватные", callback_data=f"admin_commands_apply_visibility_prv_{current_status}_{current_sort}")
    builder.button(text="◀️ Назад", callback_data=f"admin_commands_page_{current_status}_{current_visibility}_{current_sort}_0")
    builder.adjust(1)
    return builder.as_markup()


def get_admin_commands_sort_menu_keyboard(current_status: str, current_visibility: str, current_sort: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🆕 Сначала новые", callback_data=f"admin_commands_apply_sort_new_{current_status}_{current_visibility}")
    builder.button(text="📜 Сначала старые", callback_data=f"admin_commands_apply_sort_old_{current_status}_{current_visibility}")
    builder.button(text="🎯 По использованиям", callback_data=f"admin_commands_apply_sort_use_{current_status}_{current_visibility}")
    builder.button(text="❤️ По лайкам", callback_data=f"admin_commands_apply_sort_like_{current_status}_{current_visibility}")
    builder.button(text="◀️ Назад", callback_data=f"admin_commands_page_{current_status}_{current_visibility}_{current_sort}_0")
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
        get_main_menu_text(user['nickname']),
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
            get_main_menu_text(nickname),
            parse_mode="HTML",
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
        get_main_menu_text(user['nickname']),
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
        await message.answer(f"✅ Ник изменён на: {nickname}")
        await message.answer(
            get_main_menu_text(nickname),
            parse_mode="HTML",
            reply_markup=get_main_keyboard(user_id)
        )
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
        user = await get_user(message.from_user.id)
        await message.answer("✅ Изменения сохранены")
        await message.answer(
            get_main_menu_text(user['nickname'] if user else str(message.from_user.id)),
            parse_mode="HTML",
            reply_markup=get_main_keyboard(message.from_user.id)
        )
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

    await message.answer(text, parse_mode="HTML")

    user = await get_user(message.from_user.id)
    await message.answer(
        get_main_menu_text(user['nickname'] if user else str(message.from_user.id)),
        parse_mode="HTML",
        reply_markup=get_main_keyboard(message.from_user.id)
    )


@dp.callback_query(F.data.startswith("delete_") & ~F.data.startswith("delete_cmd_"))
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
        await callback.message.edit_text(
            "⚠️ Вы уже отправляли жалобу на эту команду.",
            reply_markup=get_back_keyboard()
        )
    
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
            user = await get_user(user_id)
            await message.answer(
                get_main_menu_text(user['nickname'] if user else str(user_id)),
                parse_mode="HTML",
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
    
    report_count = await get_report_count()
    
    await callback.message.edit_text(
        "👑 Админ панель\n\nВыберите раздел:",
        reply_markup=get_admin_main_keyboard(report_count)
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_reports")
async def admin_reports(callback: types.CallbackQuery):
    await admin_reports_page_render(callback, page=0)


@dp.callback_query(F.data.startswith("admin_reports_page_"))
async def admin_reports_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[-1])
    await admin_reports_page_render(callback, page=page)


@dp.callback_query(F.data.startswith("admin_reports_pick_"))
async def admin_reports_pick(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    parts = callback.data.split("_")
    current_page = int(parts[3]) if len(parts) > 3 else 0
    total_pages = int(parts[4]) if len(parts) > 4 else 1

    await callback.message.edit_text(
        "📄 Выберите страницу жалоб:",
        reply_markup=get_page_picker_keyboard(
            base_callback="admin_reports_page",
            current_page=current_page,
            total_pages=total_pages,
            back_callback=f"admin_reports_page_{current_page}"
        )
    )
    await callback.answer()


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

    command_full = await get_command(report['command_id'])
    command_text_preview = html.escape((command_full or {}).get('text_before') or '-')
    command_buttons_preview = "-"
    if command_full and command_full.get('command_type') == 'target':
        try:
            btns = json.loads(command_full.get('buttons') or '[]')
            command_buttons_preview = "\n".join([f"• {html.escape(b.get('name', ''))} → {html.escape(b.get('result', ''))}" for b in btns[:5]]) if btns else "(кнопок нет)"
        except Exception:
            command_buttons_preview = "(ошибка чтения кнопок)"

    text = (
        f"📄 <b>Жалоба #{report['id']}</b>\n\n"
        f"🎯 Команда: <b>{html.escape(report['command_name'])}</b>\n"
        f"✍️ Текст команды: {command_text_preview}\n"
        f"🔘 Кнопки:\n{command_buttons_preview}\n"
        f"👤 Автор команды: <b>{html.escape(report['creator_nickname'])}</b>\n"
        f"📝 Репорт от: <b>{html.escape(report['reporter_nickname'])}</b>\n"
        f"⚠️ Причина: <b>{html.escape(report['reason'])}</b>\n"
        f"📌 Видимость: {visibility_text}\n"
        f"📦 Статус команды: {active_text}\n"
        f"🕒 Дата: {format_db_datetime(report.get('created_at'))}"
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
async def delete_command_admin_from_report(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return
    
    command_id = int(callback.data.split("_")[-1])
    await delete_command(command_id)
    await resolve_pending_reports_for_command(command_id, status="approved")

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


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    await admin_users_page_render(callback, page=0, status_filter="all", sort_by="use")


@dp.callback_query(F.data.startswith("admin_users_filter_menu_"))
async def admin_users_filter_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    current_status = parts[4] if len(parts) > 4 else "all"
    current_sort = parts[5] if len(parts) > 5 else "use"

    await callback.message.edit_text(
        "👥 Фильтр пользователей по статусу:",
        reply_markup=get_admin_users_filter_menu_keyboard(current_status=current_status, current_sort=current_sort)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_users_sort_menu_"))
async def admin_users_sort_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    current_status = parts[4] if len(parts) > 4 else "all"
    current_sort = parts[5] if len(parts) > 5 else "use"

    await callback.message.edit_text(
        "↕️ Сортировка пользователей:",
        reply_markup=get_admin_users_sort_menu_keyboard(current_status=current_status, current_sort=current_sort)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_users_apply_status_"))
async def admin_users_apply_status(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[4] if len(parts) > 4 else "all"
    sort_by = parts[5] if len(parts) > 5 else "use"
    await admin_users_page_render(callback, page=0, status_filter=status_filter, sort_by=sort_by)


@dp.callback_query(F.data.startswith("admin_users_apply_sort_"))
async def admin_users_apply_sort(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    sort_by = parts[4] if len(parts) > 4 else "use"
    status_filter = parts[5] if len(parts) > 5 else "all"
    await admin_users_page_render(callback, page=0, status_filter=status_filter, sort_by=sort_by)


@dp.callback_query(F.data.startswith("admin_users_page_"))
async def admin_users_page(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    status_filter = parts[3] if len(parts) > 3 else "all"
    sort_by = parts[4] if len(parts) > 4 else "use"
    page = int(parts[5]) if len(parts) > 5 else 0
    await admin_users_page_render(callback, page=page, status_filter=status_filter, sort_by=sort_by)


@dp.callback_query(F.data.startswith("admin_users_pick_"))
async def admin_users_pick(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[3] if len(parts) > 3 else "all"
    sort_by = parts[4] if len(parts) > 4 else "use"
    current_page = int(parts[5]) if len(parts) > 5 else 0
    total_pages = int(parts[6]) if len(parts) > 6 else 1

    await callback.message.edit_text(
        "📄 Выберите страницу пользователей:",
        reply_markup=get_page_picker_keyboard(
            base_callback=f"admin_users_page_{status_filter}_{sort_by}",
            current_page=current_page,
            total_pages=total_pages,
            back_callback=f"admin_users_page_{status_filter}_{sort_by}_{current_page}"
        )
    )
    await callback.answer()


async def admin_users_page_render(callback: types.CallbackQuery, page: int = 0, status_filter: str = "all", sort_by: str = "use"):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    per_page = 8
    total = await get_users_count(status_filter=status_filter)

    if total == 0:
        await callback.message.edit_text(
            "👥 Пользователи\n\nСписок пуст по выбранному фильтру.",
            reply_markup=get_back_keyboard("menu_admin")
        )
        await callback.answer()
        return

    total_pages = (total + per_page - 1) // per_page
    page = max(0, min(page, total_pages - 1))

    users = await get_admin_users_page(page=page, per_page=per_page, status_filter=status_filter, sort_by=sort_by)

    await callback.message.edit_text(
        f"👥 Пользователи\n\nВсего: {total}\nВыберите пользователя:",
        reply_markup=get_admin_users_keyboard(users, page=page, total_pages=total_pages, status_filter=status_filter, sort_by=sort_by)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_user_") & ~F.data.startswith("admin_user_cmds_") & ~F.data.startswith("admin_user_cmd_"))
async def admin_user_profile(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    user_id = int(callback.data.split("_")[-1])
    profile = await get_admin_user_profile(user_id)

    if not profile:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    status_text = "🚫 Заблокирован" if profile.get('is_blocked') else "✅ Активен"
    block_info = ""
    if profile.get('is_blocked'):
        block_info = (
            f"\n⛔ Причина блока: {html.escape(profile.get('block_reason') or 'Не указана')}"
            f"\n🕒 Блок с: {format_db_datetime(profile.get('blocked_at'))}"
        )

    text = (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"ID: <code>{profile['user_id']}</code>\n"
        f"Ник: <b>{html.escape(profile['nickname'])}</b>\n"
        f"Статус: {status_text}\n"
        f"Регистрация: {format_db_datetime(profile.get('created_at'))}\n\n"
        f"🎯 Команд всего: <b>{profile['commands_total']}</b>\n"
        f"✅ Активных: <b>{profile['commands_active']}</b>\n"
        f"🗑️ Неактивных: <b>{profile['commands_inactive']}</b>\n"
        f"🌐 Публичных: <b>{profile['commands_public']}</b>\n"
        f"🔒 Приватных: <b>{profile['commands_private']}</b>\n"
        f"📊 Использований: <b>{profile['total_uses']}</b>\n"
        f"❤️ Лайков: <b>{profile['total_likes']}</b>"
        f"{block_info}"
    )

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=get_admin_user_profile_keyboard(user_id=user_id, is_blocked=bool(profile.get('is_blocked')))
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_user_cmds_") & ~F.data.startswith("admin_user_cmds_page_"))
async def admin_user_commands(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    user_id = int(callback.data.split("_")[-1])
    await admin_user_commands_page_render(callback, user_id=user_id, page=0)


@dp.callback_query(F.data.startswith("admin_user_cmds_page_"))
async def admin_user_commands_page(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    user_id = int(parts[4])
    page = int(parts[5])
    await admin_user_commands_page_render(callback, user_id=user_id, page=page)


@dp.callback_query(F.data.startswith("admin_user_cmds_pick_"))
async def admin_user_commands_pick(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    user_id = int(parts[4]) if len(parts) > 4 else 0
    current_page = int(parts[5]) if len(parts) > 5 else 0
    total_pages = int(parts[6]) if len(parts) > 6 else 1

    await callback.message.edit_text(
        "📄 Выберите страницу команд пользователя:",
        reply_markup=get_page_picker_keyboard(
            base_callback=f"admin_user_cmds_page_{user_id}",
            current_page=current_page,
            total_pages=total_pages,
            back_callback=f"admin_user_cmds_page_{user_id}_{current_page}"
        )
    )
    await callback.answer()


async def admin_user_commands_page_render(callback: types.CallbackQuery, user_id: int, page: int = 0):
    per_page = 6
    total = await get_admin_user_commands_count(user_id)
    user = await get_user(user_id)

    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return

    if total == 0:
        await callback.message.edit_text(
            f"🎯 Команды пользователя <b>{html.escape(user['nickname'])}</b>\n\n"
            f"Команд пока нет.",
            parse_mode="HTML",
            reply_markup=get_back_keyboard(f"admin_user_{user_id}")
        )
        await callback.answer()
        return

    total_pages = (total + per_page - 1) // per_page
    page = max(0, min(page, total_pages - 1))
    commands = await get_admin_user_commands(user_id=user_id, page=page, per_page=per_page)

    await callback.message.edit_text(
        f"🎯 Команды пользователя <b>{html.escape(user['nickname'])}</b>\n\n"
        f"Всего: {total}",
        parse_mode="HTML",
        reply_markup=get_admin_user_commands_keyboard(commands, user_id=user_id, page=page, total_pages=total_pages)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_user_cmd_"))
async def admin_user_command_open(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    command_id = int(parts[3])
    user_id = int(parts[4])
    await admin_command_view_render(callback, command_id=command_id, back_callback=f"admin_user_cmds_{user_id}")


@dp.callback_query(F.data == "admin_commands")
async def admin_commands(callback: types.CallbackQuery):
    await admin_commands_page_render(callback, page=0, status_filter="all", visibility_filter="all", sort_by="new")


@dp.callback_query(F.data.startswith("admin_commands_filter_status_menu_"))
async def admin_commands_filter_status_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[5] if len(parts) > 5 else "all"
    visibility_filter = parts[6] if len(parts) > 6 else "all"
    sort_by = parts[7] if len(parts) > 7 else "new"

    await callback.message.edit_text(
        "📦 Фильтр команд по статусу:",
        reply_markup=get_admin_commands_status_filter_menu_keyboard(
            current_status=status_filter,
            current_visibility=visibility_filter,
            current_sort=sort_by
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_commands_filter_visibility_menu_"))
async def admin_commands_filter_visibility_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[5] if len(parts) > 5 else "all"
    visibility_filter = parts[6] if len(parts) > 6 else "all"
    sort_by = parts[7] if len(parts) > 7 else "new"

    await callback.message.edit_text(
        "👁️ Фильтр команд по видимости:",
        reply_markup=get_admin_commands_visibility_filter_menu_keyboard(
            current_status=status_filter,
            current_visibility=visibility_filter,
            current_sort=sort_by
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_commands_sort_menu_"))
async def admin_commands_sort_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[4] if len(parts) > 4 else "all"
    visibility_filter = parts[5] if len(parts) > 5 else "all"
    sort_by = parts[6] if len(parts) > 6 else "new"

    await callback.message.edit_text(
        "↕️ Сортировка команд:",
        reply_markup=get_admin_commands_sort_menu_keyboard(
            current_status=status_filter,
            current_visibility=visibility_filter,
            current_sort=sort_by
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_commands_apply_status_"))
async def admin_commands_apply_status(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[4] if len(parts) > 4 else "all"
    visibility_filter = parts[5] if len(parts) > 5 else "all"
    sort_by = parts[6] if len(parts) > 6 else "new"

    await admin_commands_page_render(
        callback,
        page=0,
        status_filter=status_filter,
        visibility_filter=visibility_filter,
        sort_by=sort_by
    )


@dp.callback_query(F.data.startswith("admin_commands_apply_visibility_"))
async def admin_commands_apply_visibility(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    visibility_filter = parts[4] if len(parts) > 4 else "all"
    status_filter = parts[5] if len(parts) > 5 else "all"
    sort_by = parts[6] if len(parts) > 6 else "new"

    await admin_commands_page_render(
        callback,
        page=0,
        status_filter=status_filter,
        visibility_filter=visibility_filter,
        sort_by=sort_by
    )


@dp.callback_query(F.data.startswith("admin_commands_apply_sort_"))
async def admin_commands_apply_sort(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    sort_by = parts[4] if len(parts) > 4 else "new"
    status_filter = parts[5] if len(parts) > 5 else "all"
    visibility_filter = parts[6] if len(parts) > 6 else "all"

    await admin_commands_page_render(
        callback,
        page=0,
        status_filter=status_filter,
        visibility_filter=visibility_filter,
        sort_by=sort_by
    )


@dp.callback_query(F.data.startswith("admin_commands_page_"))
async def admin_commands_page(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    status_filter = parts[3] if len(parts) > 3 else "all"
    visibility_filter = parts[4] if len(parts) > 4 else "all"
    sort_by = parts[5] if len(parts) > 5 else "new"
    page = int(parts[6]) if len(parts) > 6 else 0
    await admin_commands_page_render(
        callback,
        page=page,
        status_filter=status_filter,
        visibility_filter=visibility_filter,
        sort_by=sort_by
    )


@dp.callback_query(F.data.startswith("admin_commands_pick_"))
async def admin_commands_pick(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    status_filter = parts[3] if len(parts) > 3 else "all"
    visibility_filter = parts[4] if len(parts) > 4 else "all"
    sort_by = parts[5] if len(parts) > 5 else "new"
    current_page = int(parts[6]) if len(parts) > 6 else 0
    total_pages = int(parts[7]) if len(parts) > 7 else 1

    await callback.message.edit_text(
        "📄 Выберите страницу команд:",
        reply_markup=get_page_picker_keyboard(
            base_callback=f"admin_commands_page_{status_filter}_{visibility_filter}_{sort_by}",
            current_page=current_page,
            total_pages=total_pages,
            back_callback=f"admin_commands_page_{status_filter}_{visibility_filter}_{sort_by}_{current_page}"
        )
    )
    await callback.answer()


async def admin_commands_page_render(callback: types.CallbackQuery, page: int = 0, status_filter: str = "all", visibility_filter: str = "all", sort_by: str = "new"):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    per_page = 8
    total = await get_admin_commands_count(status_filter=status_filter, visibility_filter=visibility_filter)

    if total == 0:
        await callback.message.edit_text(
            "🎯 Команды\n\nСписок пуст по выбранным фильтрам.",
            reply_markup=get_back_keyboard("menu_admin")
        )
        await callback.answer()
        return

    total_pages = (total + per_page - 1) // per_page
    page = max(0, min(page, total_pages - 1))
    commands = await get_admin_commands_page(
        page=page,
        per_page=per_page,
        status_filter=status_filter,
        visibility_filter=visibility_filter,
        sort_by=sort_by
    )

    await callback.message.edit_text(
        f"🎯 Команды\n\nВсего: {total}\nВыберите команду:",
        reply_markup=get_admin_commands_keyboard(
            commands,
            page=page,
            total_pages=total_pages,
            status_filter=status_filter,
            visibility_filter=visibility_filter,
            sort_by=sort_by
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_cmd_"))
async def admin_command_view(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    command_id = int(callback.data.split("_")[-1])
    await admin_command_view_render(callback, command_id=command_id, back_callback="admin_commands")


async def admin_command_view_render(callback: types.CallbackQuery, command_id: int, back_callback: str = "admin_commands"):
    command = await get_command(command_id)
    if not command:
        await callback.answer("❌ Команда не найдена", show_alert=True)
        return

    type_text = "👤 Для себя" if command['command_type'] == 'self' else "👥 Для другого"
    visibility_text = "🔒 Приватная" if command.get('visibility') == 'private' else "🌐 Публичная"
    status_text = "✅ Активна" if command.get('is_active') else "🗑️ Удалена"

    created_human = format_db_datetime(command.get('created_at'))

    buttons_preview = "-"
    if command['command_type'] == 'target':
        try:
            btns = json.loads(command.get('buttons') or '[]')
            if btns:
                buttons_preview = "\n".join([f"• {html.escape(b.get('name', ''))} → {html.escape(b.get('result', ''))}" for b in btns[:5]])
            else:
                buttons_preview = "(кнопок нет)"
        except Exception:
            buttons_preview = "(ошибка чтения кнопок)"

    text = (
        f"🎯 <b>Команда #{command['id']}</b>\n\n"
        f"Название: <b>{html.escape(command['name'])}</b>\n"
        f"Описание: {html.escape(command['description'] or 'Без описания')}\n"
        f"Тип: {type_text}\n"
        f"Видимость: {visibility_text}\n"
        f"Статус: {status_text}\n"
        f"Текст: {html.escape(command.get('text_before') or '-')}\n"
        f"Кнопки:\n{buttons_preview}\n"
        f"Автор: <b>{html.escape(command['creator_nickname'])}</b> (<code>{command['creator_id']}</code>)\n"
        f"Public ID: <code>{command.get('public_id') or '-'}</code>\n"
        f"Использований: <b>{command['uses']}</b>\n"
        f"Лайков: <b>{command['likes']}</b>\n"
        f"Создана: {created_human}"
    )

    kb = get_admin_command_actions_keyboard(
        command_id=command['id'],
        creator_id=command['creator_id'],
        is_active=bool(command.get('is_active', 1)),
        back_callback=back_callback
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_delete_cmd_"))
async def admin_delete_command(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    command_id = int(callback.data.split("_")[-1])
    command = await get_command(command_id)
    await delete_command(command_id)
    await resolve_pending_reports_for_command(command_id, status="approved")

    await callback.answer("✅ Команда деактивирована")
    back_cb = f"admin_user_cmds_{command['creator_id']}" if command else "admin_commands"
    await admin_command_view_render(callback, command_id=command_id, back_callback=back_cb)


@dp.callback_query(F.data.startswith("admin_restore_cmd_"))
async def admin_restore_command(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    command_id = int(callback.data.split("_")[-1])
    command = await get_command(command_id)
    await restore_command(command_id)

    await callback.answer("♻️ Команда восстановлена")
    back_cb = f"admin_user_cmds_{command['creator_id']}" if command else "admin_commands"
    await admin_command_view_render(callback, command_id=command_id, back_callback=back_cb)


@dp.callback_query(F.data.startswith("admin_block_user_"))
async def admin_block_user_direct(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    target_id = int(callback.data.split("_")[-1])
    await block_user(target_id, callback.from_user.id, reason="Manual admin block")
    await callback.answer("🚫 Пользователь заблокирован")
    await admin_user_profile(callback)


@dp.callback_query(F.data.startswith("admin_unblock_user_"))
async def admin_unblock_user_direct(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    target_id = int(callback.data.split("_")[-1])
    await unblock_user(target_id)
    await callback.answer("✅ Пользователь разблокирован")
    await admin_user_profile(callback)


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
        builder.button(text=f"🔓 {user['nickname']}", callback_data=f"admin_unblock_user_{user['user_id']}")

    builder.adjust(1)
    builder.button(text="◀️ Назад", callback_data="menu_admin")

    await callback.message.edit_text("🚫 Заблокированные:", reply_markup=builder.as_markup())
    await callback.answer()


async def get_admin_stats_selection(state: FSMContext) -> tuple[str | None, str | None, str | None, str | None]:
    data = await state.get_data()
    return (
        data.get("admin_stats_filter_type"),  # "day" или "range"
        data.get("admin_stats_selected_day"),
        data.get("admin_stats_selected_from"),
        data.get("admin_stats_selected_to")
    )


async def set_admin_stats_day_filter(state: FSMContext, date_text: str):
    await state.update_data(
        admin_stats_filter_type="day",
        admin_stats_selected_day=date_text,
        admin_stats_selected_from=None,
        admin_stats_selected_to=None
    )


async def set_admin_stats_range_filter(state: FSMContext, date_from: str, date_to: str):
    await state.update_data(
        admin_stats_filter_type="range",
        admin_stats_selected_day=None,
        admin_stats_selected_from=date_from,
        admin_stats_selected_to=date_to
    )


async def reset_admin_stats_filter(state: FSMContext):
    await state.update_data(
        admin_stats_filter_type=None,
        admin_stats_selected_day=None,
        admin_stats_selected_from=None,
        admin_stats_selected_to=None
    )


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery, state: FSMContext):
    period = (await state.get_data()).get("admin_stats_period", "all")

    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)
    if filter_type == "day" and selected_day:
        stats = await get_stats_by_date_range(selected_day, selected_day)
        await safe_edit_message(
            callback,
            f"📊 <b>Статистика за {format_human_date_with_weekday(selected_day)}</b>\n"
            f"<i>{format_human_date_full_ru(selected_day)}</i>\n\n"
            f"👥 Новых пользователей: <b>{stats['users']}</b>\n"
            f"🎯 Создано команд: <b>{stats['commands']}</b>\n"
            f"🔥 Использований (по созданным в дне): <b>{stats['total_uses']}</b>\n"
            f"❤️ Лайков (по созданным в дне): <b>{stats['total_likes']}</b>\n"
            f"🚩 Жалоб создано: <b>{stats['reports']}</b>\n\n"
            f"📦 Активных команд (текущее): <b>{stats['active_commands_total']}</b>\n"
            f"🚫 Заблокированных пользователей (текущее): <b>{stats['blocked_users_total']}</b>",
            parse_mode="HTML",
            reply_markup=get_admin_stats_keyboard(period, filter_type="day", selected_day=selected_day)
        )
        await callback.answer()
        return

    if filter_type == "range" and selected_from and selected_to:
        stats = await get_stats_by_date_range(selected_from, selected_to)
        await safe_edit_message(
            callback,
            f"📊 <b>Статистика за период</b>\n"
            f"• От: <b>{format_human_date_with_weekday(selected_from)}</b>\n"
            f"• До: <b>{format_human_date_with_weekday(selected_to)}</b>\n"
            f"<i>{format_human_date_full_ru(selected_from)} → {format_human_date_full_ru(selected_to)}</i>\n\n"
            f"👥 Новых пользователей: <b>{stats['users']}</b>\n"
            f"🎯 Создано команд: <b>{stats['commands']}</b>\n"
            f"🔥 Использований (по созданным в периоде): <b>{stats['total_uses']}</b>\n"
            f"❤️ Лайков (по созданным в периоде): <b>{stats['total_likes']}</b>\n"
            f"🚩 Жалоб создано: <b>{stats['reports']}</b>\n\n"
            f"📦 Активных команд (текущее): <b>{stats['active_commands_total']}</b>\n"
            f"🚫 Заблокированных пользователей (текущее): <b>{stats['blocked_users_total']}</b>",
            parse_mode="HTML",
            reply_markup=get_admin_stats_keyboard(period, filter_type="range", selected_from=selected_from, selected_to=selected_to)
        )
        await callback.answer()
        return

    await admin_stats_render(callback, state=state, period=period)


@dp.callback_query(F.data == "admin_stats_period_menu")
async def admin_stats_period_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    builder = InlineKeyboardBuilder()
    builder.button(text="📅 Сегодня", callback_data="admin_stats_today")
    builder.button(text="🗓️ 7 дней", callback_data="admin_stats_week")
    builder.button(text="🗓️ Месяц", callback_data="admin_stats_month")
    builder.button(text="♾️ Всё время", callback_data="admin_stats_all")
    builder.button(text="◀️ Назад", callback_data="admin_stats")
    builder.adjust(1)

    await safe_edit_message(
        callback,
        "📊 Выберите период статистики:",
        reply_markup=builder.as_markup()
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_reset_dates")
async def admin_stats_reset_dates(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    await reset_admin_stats_filter(state)
    await admin_stats(callback, state)
    await callback.answer("✅ Фильтр сброшен")


@dp.callback_query(F.data == "admin_stats_cal_day_now")
async def admin_stats_calendar_day_now(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)
    now = datetime.now().astimezone()
    year = now.year
    month = now.month

    cal_from = selected_day
    cal_to = None
    if filter_type == "range" and selected_from and selected_to:
        cal_from = selected_from
        cal_to = selected_to

    if cal_from:
        y, m, _ = cal_from.split("-")
        year = int(y)
        month = int(m)

    await state.update_data(admin_stats_cal_mode="day")
    await safe_edit_message(
        callback,
        "📅 Выберите день:",
        reply_markup=get_stats_calendar_keyboard("day", year, month, selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()



@dp.callback_query(F.data.startswith("admin_stats_cal_day_sel_"))
async def admin_stats_calendar_day_selected(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    selected_day = callback.data.removeprefix("admin_stats_cal_day_sel_")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_day):
        await callback.answer("❌ Неверная дата", show_alert=True)
        return

    filter_type, _, selected_from, selected_to = await get_admin_stats_selection(state)
    cal_from = selected_day
    cal_to = None
    if filter_type == "range" and selected_from and selected_to:
        cal_from = selected_from
        cal_to = selected_to

    year, month, _ = cal_from.split("-")
    await state.update_data(admin_stats_cal_mode="day")
    await safe_edit_message(
        callback,
        "📅 Выберите день:",
        reply_markup=get_stats_calendar_keyboard("day", int(year), int(month), selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def noop_callback(callback: types.CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_cal_day_"))
async def admin_stats_calendar_day_month(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    if len(parts) < 6 or not parts[4].isdigit() or not parts[5].isdigit():
        await callback.answer()
        return

    year = int(parts[4])
    month = int(parts[5])
    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)

    cal_from = selected_day
    cal_to = None
    if filter_type == "range" and selected_from and selected_to:
        cal_from = selected_from
        cal_to = selected_to

    await safe_edit_message(
        callback,
        "📅 Выберите день:",
        reply_markup=get_stats_calendar_keyboard("day", year, month, selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_day_"))
async def admin_stats_calendar_day_select(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    date_text = callback.data.removeprefix("admin_stats_day_")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
        await callback.answer("❌ Неверная дата", show_alert=True)
        return

    await set_admin_stats_day_filter(state, date_text)
    stats = await get_stats_by_date_range(date_text, date_text)

    await safe_edit_message(
        callback,
        f"📊 <b>Статистика за {format_human_date_with_weekday(date_text)}</b>\n"
        f"<i>{format_human_date_full_ru(date_text)}</i>\n\n"
        f"👥 Новых пользователей: <b>{stats['users']}</b>\n"
        f"🎯 Создано команд: <b>{stats['commands']}</b>\n"
        f"🔥 Использований (по созданным в дне): <b>{stats['total_uses']}</b>\n"
        f"❤️ Лайков (по созданным в дне): <b>{stats['total_likes']}</b>\n"
        f"🚩 Жалоб создано: <b>{stats['reports']}</b>\n\n"
        f"📦 Активных команд (текущее): <b>{stats['active_commands_total']}</b>\n"
        f"🚫 Заблокированных пользователей (текущее): <b>{stats['blocked_users_total']}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_stats_keyboard("all", filter_type="day", selected_day=date_text)
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_cal_range_from_now")
async def admin_stats_calendar_range_from_now(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)
    now = datetime.now().astimezone()
    year = now.year
    month = now.month

    cal_from = selected_from
    cal_to = selected_to
    if filter_type == "day" and selected_day:
        cal_from = selected_day
        cal_to = selected_day

    if cal_from:
        y, m, _ = cal_from.split("-")
        year = int(y)
        month = int(m)

    await state.update_data(admin_stats_cal_mode="range_from")
    await safe_edit_message(
        callback,
        "📆 Выберите начальную дату диапазона:",
        reply_markup=get_stats_calendar_keyboard("range_from", year, month, selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()



@dp.callback_query(F.data.startswith("admin_stats_cal_range_from_sel_"))
async def admin_stats_calendar_range_from_selected(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    selected_from = callback.data.removeprefix("admin_stats_cal_range_from_sel_")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_from):
        await callback.answer("❌ Неверная дата", show_alert=True)
        return

    filter_type, selected_day, _, selected_to = await get_admin_stats_selection(state)
    cal_from = selected_from
    cal_to = selected_to
    if filter_type == "day" and selected_day:
        cal_from = selected_day
        cal_to = selected_day

    year, month, _ = cal_from.split("-")
    await state.update_data(admin_stats_cal_mode="range_from")
    await safe_edit_message(
        callback,
        "📆 Выберите начальную дату диапазона:",
        reply_markup=get_stats_calendar_keyboard("range_from", int(year), int(month), selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_cal_range_from_"))
async def admin_stats_calendar_range_from_month(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    parts = callback.data.split("_")
    if len(parts) < 7 or not parts[5].isdigit() or not parts[6].isdigit():
        await callback.answer()
        return

    year = int(parts[5])
    month = int(parts[6])

    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)
    cal_from = selected_from
    cal_to = selected_to
    if filter_type == "day" and selected_day:
        cal_from = selected_day
        cal_to = selected_day

    await state.update_data(admin_stats_cal_mode="range_from")
    await safe_edit_message(
        callback,
        "📆 Выберите начальную дату диапазона:",
        reply_markup=get_stats_calendar_keyboard("range_from", year, month, selected_from=cal_from, selected_to=cal_to, back_callback="admin_stats")
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_range_from_"))
async def admin_stats_calendar_range_from_select(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    date_from = callback.data.removeprefix("admin_stats_range_from_")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from):
        await callback.answer("❌ Неверная дата", show_alert=True)
        return

    _, _, _, selected_to = await get_admin_stats_selection(state)
    await state.update_data(admin_stats_selected_from=date_from)
    year, month, _ = date_from.split("-")

    await state.update_data(admin_stats_cal_mode="range_to")
    await safe_edit_message(
        callback,
        f"📆 Начало периода: <b>{format_human_date_with_weekday(date_from)}</b>\n"
        f"<i>{format_human_date_full_ru(date_from)}</i>\n\n"
        f"Выберите конечную дату:",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to", int(year), int(month),
            selected_from=date_from,
            selected_to=selected_to,
            back_callback="admin_stats"
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_cal_range_to_from_"))
async def admin_stats_calendar_range_to_from(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    selected_from = callback.data.removeprefix("admin_stats_cal_range_to_from_")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_from):
        await callback.answer("❌ Неверная дата", show_alert=True)
        return

    _, _, _, selected_to = await get_admin_stats_selection(state)
    year, month, _ = selected_from.split("-")
    
    await state.update_data(admin_stats_cal_mode="range_to")
    await safe_edit_message(
        callback,
        f"📆 Начало периода: <b>{format_human_date_with_weekday(selected_from)}</b>\n"
        f"<i>{format_human_date_full_ru(selected_from)}</i>\n\n"
        f"Выберите конечную дату:",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to", int(year), int(month),
            selected_from=selected_from,
            selected_to=selected_to,
            back_callback="admin_stats"
        )
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_cal_range_to_open")
async def admin_stats_calendar_range_to_open(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    _, _, selected_from, _ = await get_admin_stats_selection(state)
    now = datetime.now().astimezone()
    
    if not selected_from:
        await callback.answer("❌ Сначала выберите дату «От»", show_alert=True)
        return

    year, month, _ = selected_from.split("-")
    await state.update_data(admin_stats_cal_mode="range_to")
    await safe_edit_message(
        callback,
        f"📆 Начало периода: <b>{format_human_date_with_weekday(selected_from)}</b>\n"
        f"<i>{format_human_date_full_ru(selected_from)}</i>\n\n"
        f"Выберите конечную дату:",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to", int(year), int(month),
            selected_from=selected_from,
            back_callback="admin_stats"
        )
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_stats_cal_range_reset")
async def admin_stats_calendar_range_reset(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    await reset_admin_stats_filter(state)
    
    now = datetime.now().astimezone()
    await state.update_data(admin_stats_cal_mode="range_from")
    await safe_edit_message(
        callback,
        "📆 Выберите начальную дату диапазона:",
        reply_markup=get_stats_calendar_keyboard("range_from", now.year, now.month, selected_from=None, back_callback="admin_stats")
    )
    await callback.answer("✅ Сброшено")


@dp.callback_query(F.data.startswith("admin_stats_cal_range_show_"))
async def admin_stats_calendar_range_show_selected(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    payload = callback.data.removeprefix("admin_stats_cal_range_show_")
    parts = payload.split("_", 1)
    if len(parts) != 2:
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    selected_from, selected_to = parts
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_from) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", selected_to):
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    year, month, _ = selected_to.split("-")
    await safe_edit_message(
        callback,
        f"📆 Период:\n"
        f"• От: <b>{format_human_date_with_weekday(selected_from)}</b>\n"
        f"• До: <b>{format_human_date_with_weekday(selected_to)}</b>\n"
        f"<i>{format_human_date_full_ru(selected_from)} → {format_human_date_full_ru(selected_to)}</i>\n\n"
        f"Обозначения в календаре: •DD — начало, DD• — конец",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to",
            int(year),
            int(month),
            selected_from=selected_from,
            selected_to=selected_to,
            back_callback="admin_stats"
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_cal_range_to_"))
async def admin_stats_calendar_range_to_month(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    payload = callback.data.removeprefix("admin_stats_cal_range_to_")
    parts = payload.split("_")
    if len(parts) < 3:
        await callback.answer()
        return

    selected_from = parts[0]
    year = int(parts[1])
    month = int(parts[2])

    _, _, _, selected_to = await get_admin_stats_selection(state)
    await state.update_data(admin_stats_cal_mode="range_to")
    await safe_edit_message(
        callback,
        f"📆 Начало периода: <b>{format_human_date_with_weekday(selected_from)}</b>\n"
        f"<i>{format_human_date_full_ru(selected_from)}</i>\n\n"
        f"Выберите конечную дату:",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to", year, month,
            selected_from=selected_from,
            selected_to=selected_to,
            back_callback="admin_stats"
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_stats_range_to_"))
async def admin_stats_calendar_range_to_select(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    payload = callback.data.removeprefix("admin_stats_range_to_")
    parts = payload.split("_", 1)
    if len(parts) != 2:
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    date_from, date_to = parts
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_to):
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    if date_from > date_to:
        await callback.answer("❌ Дата 'по' раньше даты 'с'", show_alert=True)
        return

    await set_admin_stats_range_filter(state, date_from, date_to)
    year, month, _ = date_to.split("-")

    await safe_edit_message(
        callback,
        f"📆 Период:\n"
        f"• От: <b>{format_human_date_with_weekday(date_from)}</b>\n"
        f"• До: <b>{format_human_date_with_weekday(date_to)}</b>\n"
        f"<i>{format_human_date_full_ru(date_from)} → {format_human_date_full_ru(date_to)}</i>\n\n"
        f"Обозначения в календаре: •DD — начало, DD• — конец",
        parse_mode="HTML",
        reply_markup=get_stats_calendar_keyboard(
            "range_to",
            int(year),
            int(month),
            selected_from=date_from,
            selected_to=date_to,
            back_callback="admin_stats"
        )
    )
    await callback.answer("Диапазон выбран")


@dp.callback_query(F.data.startswith("admin_stats_range_apply_"))
async def admin_stats_calendar_range_apply(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    payload = callback.data.removeprefix("admin_stats_range_apply_")
    parts = payload.split("_", 1)
    if len(parts) != 2:
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    date_from, date_to = parts
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_to):
        await callback.answer("❌ Неверный диапазон", show_alert=True)
        return

    if date_from > date_to:
        await callback.answer("❌ Дата 'по' раньше даты 'с'", show_alert=True)
        return

    await set_admin_stats_range_filter(state, date_from, date_to)
    stats = await get_stats_by_date_range(date_from, date_to)

    await safe_edit_message(
        callback,
        f"📊 <b>Статистика за период</b>\n"
        f"• От: <b>{format_human_date_with_weekday(date_from)}</b>\n"
        f"• До: <b>{format_human_date_with_weekday(date_to)}</b>\n"
        f"<i>{format_human_date_full_ru(date_from)} → {format_human_date_full_ru(date_to)}</i>\n\n"
        f"👥 Новых пользователей: <b>{stats['users']}</b>\n"
        f"🎯 Создано команд: <b>{stats['commands']}</b>\n"
        f"🔥 Использований (по созданным в периоде): <b>{stats['total_uses']}</b>\n"
        f"❤️ Лайков (по созданным в периоде): <b>{stats['total_likes']}</b>\n"
        f"🚩 Жалоб создано: <b>{stats['reports']}</b>\n\n"
        f"📦 Активных команд (текущее): <b>{stats['active_commands_total']}</b>\n"
        f"🚫 Заблокированных пользователей (текущее): <b>{stats['blocked_users_total']}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_stats_keyboard("all", filter_type="range", selected_from=date_from, selected_to=date_to)
    )
    await callback.answer()


@dp.callback_query(
    F.data.startswith("admin_stats_")
    & ~F.data.startswith("admin_stats_period_")
    & ~F.data.startswith("admin_stats_pick_")
    & ~F.data.startswith("admin_stats_cal_")
    & ~F.data.startswith("admin_stats_day_")
    & ~F.data.startswith("admin_stats_range_from_")
    & ~F.data.startswith("admin_stats_range_to_")
    & ~F.data.startswith("admin_stats_reset_")
)
async def admin_stats_period(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    period = callback.data.split("_")[-1]
    await state.update_data(admin_stats_period=period)
    await admin_stats_render(callback, state=state, period=period)


async def admin_stats_render(callback: types.CallbackQuery, state: FSMContext, period: str = "all"):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!")
        return

    filter_type, selected_day, selected_from, selected_to = await get_admin_stats_selection(state)

    stats = await get_stats_by_period(period)

    period_title = {
        "today": "за сегодня",
        "week": "за 7 дней",
        "month": "за месяц",
        "all": "за всё время"
    }.get(period, "за всё время")

    await safe_edit_message(
        callback,
        f"📊 <b>Статистика {period_title}</b>\n\n"
        f"👥 Новых пользователей: <b>{stats['users']}</b>\n"
        f"🎯 Создано команд: <b>{stats['commands']}</b>\n"
        f"🔥 Использований (по созданным в периоде): <b>{stats['total_uses']}</b>\n"
        f"❤️ Лайков (по созданным в периоде): <b>{stats['total_likes']}</b>\n"
        f"🚩 Жалоб создано: <b>{stats['reports']}</b>\n\n"
        f"📦 Активных команд (текущее): <b>{stats['active_commands_total']}</b>\n"
        f"🚫 Заблокированных пользователей (текущее): <b>{stats['blocked_users_total']}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_stats_keyboard(period, filter_type=filter_type, selected_day=selected_day, selected_from=selected_from, selected_to=selected_to)
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
