import aiosqlite
import os
from config import DATABASE_PATH

async def init_db():
    """Initialize database and create tables"""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                nickname TEXT UNIQUE,
                is_blocked INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Commands table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                creator_id INTEGER NOT NULL,
                template_type TEXT NOT NULL,
                template_text TEXT NOT NULL,
                buttons TEXT DEFAULT '[]',
                likes INTEGER DEFAULT 0,
                uses INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users(user_id)
            )
        """)
        
        # Likes table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS likes (
                user_id INTEGER NOT NULL,
                command_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, command_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (command_id) REFERENCES commands(id)
            )
        """)
        
        # Reports table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (command_id) REFERENCES commands(id),
                FOREIGN KEY (reporter_id) REFERENCES users(user_id)
            )
        """)
        
        # Blocked users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id INTEGER PRIMARY KEY,
                blocked_by INTEGER,
                reason TEXT,
                blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.commit()

# User operations
async def register_user(user_id: int, nickname: str) -> bool:
    """Register user with nickname"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO users (user_id, nickname) VALUES (?, ?)",
                (user_id, nickname.lower())
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def get_user(user_id: int) -> dict | None:
    """Get user info"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def is_nickname_taken(nickname: str) -> bool:
    """Check if nickname is already taken"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM users WHERE nickname = ?", (nickname.lower(),)
        )
        return await cursor.fetchone() is not None

async def is_user_blocked(user_id: int) -> bool:
    """Check if user is blocked"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM blocked_users WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone() is not None

async def block_user(user_id: int, blocked_by: int, reason: str = "") -> bool:
    """Block user and delete all their commands"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO blocked_users (user_id, blocked_by, reason) VALUES (?, ?, ?)",
                (user_id, blocked_by, reason)
            )
            await db.execute(
                "UPDATE commands SET is_active = 0 WHERE creator_id = ?", (user_id,)
            )
            await db.commit()
            return True
        except:
            return False

async def unblock_user(user_id: int) -> bool:
    """Unblock user"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM blocked_users WHERE user_id = ?", (user_id,))
        await db.commit()
        return True

# Command operations
async def create_command(user_id: int, name: str, description: str, 
                         template_type: str, template_text: str,
                         buttons: list = None) -> int | None:
    """Create new RP command
    
    buttons format: [{"name": "Принять", "result": "согласился"}, ...]
    """
    import json
    if buttons is None:
        buttons = []
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            cursor = await db.execute("""
                INSERT INTO commands 
                (name, description, creator_id, template_type, template_text, buttons)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name.lower(), description, user_id, template_type, template_text, json.dumps(buttons, ensure_ascii=False)))
            await db.commit()
            return cursor.lastrowid
        except:
            return None

async def get_command(command_id: int) -> dict | None:
    """Get command by ID"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT c.*, u.nickname as creator_nickname FROM commands c "
            "JOIN users u ON c.creator_id = u.user_id WHERE c.id = ?", (command_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_command_by_name(name: str) -> dict | None:
    """Get command by name (returns first match)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT c.*, u.nickname as creator_nickname FROM commands c "
            "JOIN users u ON c.creator_id = u.user_id "
            "WHERE c.name = ? AND c.is_active = 1 ORDER BY c.likes DESC LIMIT 1",
            (name.lower(),)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def search_commands(query: str, limit: int = 10) -> list:
    """Search commands by name"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT c.*, u.nickname as creator_nickname FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            WHERE c.name LIKE ? AND c.is_active = 1
            ORDER BY c.likes DESC LIMIT ?
        """, (f"%{query.lower()}%", limit))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def search_by_creator(nickname: str, limit: int = 10) -> list:
    """Search commands by creator nickname"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT c.*, u.nickname as creator_nickname FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            WHERE u.nickname LIKE ? AND c.is_active = 1
            ORDER BY c.likes DESC LIMIT ?
        """, (f"%{nickname.lower()}%", limit))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_popular_commands(limit: int = 10) -> list:
    """Get popular commands"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT c.*, u.nickname as creator_nickname FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            WHERE c.is_active = 1
            ORDER BY c.likes DESC, c.uses DESC LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_user_commands(user_id: int, limit: int = 10) -> list:
    """Get user's commands"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT c.*, u.nickname as creator_nickname FROM commands c
            JOIN users u ON c.creator_id = u.user_id
            WHERE c.creator_id = ?
            ORDER BY c.created_at DESC LIMIT ?
        """, (user_id, limit))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def delete_command(command_id: int) -> bool:
    """Delete command (soft delete)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE commands SET is_active = 0 WHERE id = ?", (command_id,)
        )
        await db.commit()
        return True

# Like operations
async def toggle_like(user_id: int, command_id: int) -> bool:
    """Toggle like on command"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM likes WHERE user_id = ? AND command_id = ?",
            (user_id, command_id)
        )
        if await cursor.fetchone():
            await db.execute(
                "DELETE FROM likes WHERE user_id = ? AND command_id = ?",
                (user_id, command_id)
            )
            await db.execute(
                "UPDATE commands SET likes = likes - 1 WHERE id = ?",
                (command_id,)
            )
        else:
            await db.execute(
                "INSERT INTO likes (user_id, command_id) VALUES (?, ?)",
                (user_id, command_id)
            )
            await db.execute(
                "UPDATE commands SET likes = likes + 1 WHERE id = ?",
                (command_id,)
            )
        await db.commit()
        return True

async def has_liked(user_id: int, command_id: int) -> bool:
    """Check if user has liked command"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM likes WHERE user_id = ? AND command_id = ?",
            (user_id, command_id)
        )
        return await cursor.fetchone() is not None

async def increment_uses(command_id: int):
    """Increment command usage count"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE commands SET uses = uses + 1 WHERE id = ?", (command_id,)
        )
        await db.commit()

# Report operations
async def create_report(command_id: int, reporter_id: int, reason: str) -> bool:
    """Create report for command"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("""
                INSERT INTO reports (command_id, reporter_id, reason)
                VALUES (?, ?, ?)
            """, (command_id, reporter_id, reason))
            await db.commit()
            return True
        except:
            return False

async def get_pending_reports(limit: int = 10) -> list:
    """Get pending reports"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT r.*, c.name as command_name, c.template_text,
                   u1.nickname as reporter_nickname, u2.nickname as creator_nickname
            FROM reports r
            JOIN commands c ON r.command_id = c.id
            JOIN users u1 ON r.reporter_id = u1.user_id
            JOIN users u2 ON c.creator_id = u2.user_id
            WHERE r.status = 'pending'
            ORDER BY r.created_at DESC LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def resolve_report(report_id: int, action: str):
    """Resolve report (approve/reject)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE reports SET status = ? WHERE id = ?", (action, report_id)
        )
        await db.commit()

async def get_report_count() -> int:
    """Get count of pending reports"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM reports WHERE status = 'pending'"
        )
        return (await cursor.fetchone())[0]

# Admin operations
async def get_blocked_users(limit: int = 20) -> list:
    """Get blocked users"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT b.*, u.nickname 
            FROM blocked_users b
            JOIN users u ON b.user_id = u.user_id
            ORDER BY b.blocked_at DESC LIMIT ?
        """, (limit,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_stats() -> dict:
    """Get bot statistics"""
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