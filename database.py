import aiosqlite
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import os
import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class Database:
    """Управление базой данных SQLite - ОПТИМИЗИРОВАННО ДЛЯ RENDER"""
    
    def __init__(self, db_path: str = "data/users.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = None
        self._lock = asyncio.Lock()
    
    async def connect(self):
        """Подключение к базе данных"""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            self.conn.row_factory = aiosqlite.Row
            # Оптимизации для Render
            await self.conn.execute("PRAGMA foreign_keys = ON")
            await self.conn.execute("PRAGMA journal_mode = WAL")
            await self.conn.execute("PRAGMA synchronous = NORMAL")
            await self.conn.execute("PRAGMA cache_size = 10000")
            
            await self.create_tables()
            await self.create_indexes()
            logger.info(f"✅ База данных подключена: {self.db_path}")
            
            # Проверяем наличие admin
            await self._ensure_admin_exists()
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise
    
    async def _ensure_admin_exists(self):
        """Создаем администратора, если нет"""
        admin_ids = os.environ.get('ADMIN_IDS', '').split(',')
        for admin_id_str in admin_ids:
            if admin_id_str.strip():
                try:
                    admin_id = int(admin_id_str.strip())
                    user = await self.get_user(admin_id)
                    if not user:
                        # Создаем пользователя
                        await self.conn.execute('''
                            INSERT INTO users (user_id, username, first_name, last_name, is_admin)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (admin_id, 'admin', 'Admin', 'Bot', 1))
                        
                        # Создаем подписку
                        expires_at = datetime.now() + timedelta(days=3650)
                        await self.conn.execute('''
                            INSERT INTO subscriptions 
                            (user_id, plan_type, status, expires_at, price, currency)
                            VALUES (?, 'lifetime', 'active', ?, 0.0, 'RUB')
                        ''', (admin_id, expires_at.isoformat()))
                        
                        await self.conn.commit()
                        logger.info(f"✅ Создан администратор: {admin_id}")
                except Exception as e:
                    logger.error(f"Ошибка создания администратора {admin_id_str}: {e}")
                    continue
    
    async def create_tables(self):
        """Создание таблиц с упрощенной структурой"""
        try:
            # Таблица пользователей
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT NOT NULL,
                    last_name TEXT,
                    language_code TEXT DEFAULT 'ru',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_admin BOOLEAN DEFAULT 0,
                    is_banned BOOLEAN DEFAULT 0,
                    ban_reason TEXT
                )
            ''')
            
            # Таблица подписок
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    plan_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    starts_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    price REAL DEFAULT 0.0,
                    currency TEXT DEFAULT 'RUB',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Таблица сессий парсинга
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS parsing_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    session_uid TEXT UNIQUE NOT NULL,
                    channel_url TEXT NOT NULL,
                    parsing_type TEXT NOT NULL DEFAULT 'members',
                    status TEXT NOT NULL DEFAULT 'pending',
                    total_items INTEGER DEFAULT 0,
                    parsed_items INTEGER DEFAULT 0,
                    result_file_path TEXT,
                    error_message TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Таблица статистики
            await self.conn.execute('''
                CREATE TABLE IF NOT EXISTS usage_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    date DATE DEFAULT CURRENT_DATE,
                    parsing_count INTEGER DEFAULT 0,
                    members_parsed INTEGER DEFAULT 0,
                    total_requests INTEGER DEFAULT 0,
                    UNIQUE(user_id, date),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            await self.conn.commit()
            logger.info("✅ Таблицы созданы")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise
    
    # ... остальные существующие методы класса ...
    
    async def create_user(self, user_id, username, first_name, last_name):
        """Создать пользователя (для совместимости с bot.py)"""
        return await self.get_or_create_user(user_id, username, first_name, last_name)
    
    async def get_subscription_plans(self):
        """Получить список тарифных планов"""
        return [
            {
                'code': 'trial',
                'name': 'Пробная',
                'days': 3,
                'price': 0.0,
                'currency': 'RUB',
                'description': 'Бесплатный доступ на 3 дня'
            },
            {
                'code': 'daily',
                'name': 'Дневная',
                'days': 1,
                'price': 50.0,
                'currency': 'RUB',
                'description': 'Доступ на 1 день'
            },
            {
                'code': 'weekly',
                'name': 'Недельная',
                'days': 7,
                'price': 250.0,
                'currency': 'RUB',
                'description': 'Доступ на 7 дней'
            },
            {
                'code': 'monthly',
                'name': 'Месячная',
                'days': 30,
                'price': 800.0,
                'currency': 'RUB',
                'description': 'Доступ на 30 дней'
            },
            {
                'code': 'yearly',
                'name': 'Годовая',
                'days': 365,
                'price': 5000.0,
                'currency': 'RUB',
                'description': 'Доступ на 365 дней'
            }
        ]
    
    async def create_indexes(self):
        """Создание индексов для производительности"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_admin ON users(is_admin)",
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_expires ON subscriptions(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON parsing_sessions(user_id, started_at)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_uid ON parsing_sessions(session_uid)",
            "CREATE INDEX IF NOT EXISTS idx_stats_user_date ON usage_stats(user_id, date)"
        ]
        
        for index_sql in indexes:
            try:
                await self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        
        await self.conn.commit()
    
    # ==================== ОСНОВНЫЕ МЕТОДЫ ====================
    
    async def get_or_create_user(self, user_id: int, username: str = None, 
                                first_name: str = None, last_name: str = None) -> Dict:
        """Атомарное получение или создание пользователя"""
        try:
            # Проверяем существование
            user = await self.get_user(user_id)
            if user:
                # Обновляем активность
                await self.conn.execute(
                    "UPDATE users SET last_activity = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (user_id,)
                )
                await self.conn.commit()
                return dict(user)
            
            # Создаем нового пользователя
            await self.conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, created_at, last_activity)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (user_id, username, first_name, last_name))
            
            # Создаем пробную подписку (3 дня)
            expires_at = datetime.now() + timedelta(days=3)
            await self.conn.execute('''
                INSERT INTO subscriptions 
                (user_id, plan_type, status, expires_at, price, currency)
                VALUES (?, 'trial', 'active', ?, 0.0, 'RUB')
            ''', (user_id, expires_at.isoformat()))
            
            # Добавляем начальную статистику
            await self.conn.execute('''
                INSERT INTO usage_stats (user_id, date, parsing_count, members_parsed, total_requests)
                VALUES (?, CURRENT_DATE, 0, 0, 0)
            ''', (user_id,))
            
            await self.conn.commit()
            logger.info(f"✅ Создан пользователь: {user_id}")
            
            return await self.get_user(user_id)
            
        except Exception as e:
            await self.conn.rollback()
            logger.error(f"❌ Ошибка создания пользователя {user_id}: {e}")
            raise
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Получить пользователя"""
        cursor = await self.conn.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None
    
    async def set_admin(self, user_id: int, is_admin: bool = True):
        """Назначить администратора"""
        await self.conn.execute(
            "UPDATE users SET is_admin = ? WHERE user_id = ?",
            (1 if is_admin else 0, user_id)
        )
        await self.conn.commit()
        logger.info(f"Пользователь {user_id} {'назначен администратором' if is_admin else 'снят с админки'}")
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверить администратора"""
        cursor = await self.conn.execute(
            "SELECT is_admin FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return bool(row['is_admin']) if row else False
    
    async def get_user_subscription(self, user_id: int) -> Optional[Dict]:
        """Получить активную подписку"""
        cursor = await self.conn.execute('''
            SELECT * FROM subscriptions 
            WHERE user_id = ? AND status = 'active' AND expires_at > CURRENT_TIMESTAMP
            ORDER BY expires_at DESC LIMIT 1
        ''', (user_id,))
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None
    
    async def create_subscription(self, user_id: int, plan_type: str, 
                                days: int, price: float = 0.0, 
                                currency: str = 'RUB') -> int:
        """Создать подписку"""
        starts_at = datetime.now()
        expires_at = starts_at + timedelta(days=days)
        
        cursor = await self.conn.execute('''
            INSERT INTO subscriptions 
            (user_id, plan_type, status, starts_at, expires_at, price, currency)
            VALUES (?, ?, 'active', ?, ?, ?, ?)
        ''', (user_id, plan_type, starts_at.isoformat(), 
              expires_at.isoformat(), price, currency))
        
        sub_id = cursor.lastrowid
        await self.conn.commit()
        logger.info(f"Создана подписка {sub_id} для {user_id}")
        return sub_id
    
    async def create_parsing_session(self, user_id: int, channel_url: str, 
                                   parsing_type: str = 'members') -> str:
        """Создать сессию парсинга"""
        session_uid = str(uuid.uuid4())
        
        await self.conn.execute('''
            INSERT INTO parsing_sessions 
            (user_id, session_uid, channel_url, parsing_type, status)
            VALUES (?, ?, ?, ?, 'pending')
        ''', (user_id, session_uid, channel_url, parsing_type))
        
        await self.conn.commit()
        logger.info(f"Создана сессия парсинга {session_uid} для {user_id}")
        return session_uid
    
    async def update_parsing_session(self, session_uid: str, **kwargs):
        """Обновить сессию парсинга"""
        allowed = ['status', 'total_items', 'parsed_items', 
                  'result_file_path', 'error_message']
        
        updates = []
        params = []
        
        for key, value in kwargs.items():
            if key in allowed and value is not None:
                updates.append(f"{key} = ?")
                params.append(value)
        
        if kwargs.get('status') == 'completed':
            updates.append("completed_at = CURRENT_TIMESTAMP")
        
        if updates:
            query = f"UPDATE parsing_sessions SET {', '.join(updates)} WHERE session_uid = ?"
            params.append(session_uid)
            await self.conn.execute(query, params)
            await self.conn.commit()
    
    async def get_user_stats(self, user_id: int) -> Dict:
        """Статистика пользователя"""
        cursor = await self.conn.execute('''
            SELECT 
                COUNT(*) as total_sessions,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_sessions,
                COALESCE(SUM(parsed_items), 0) as total_members
            FROM parsing_sessions 
            WHERE user_id = ?
        ''', (user_id,))
        
        row = await cursor.fetchone()
        await cursor.close()
        
        total = row['total_sessions'] or 0
        completed = row['completed_sessions'] or 0
        
        return {
            'total_sessions': total,
            'completed_sessions': completed,
            'total_members': row['total_members'] or 0,
            'success_rate': (completed / total * 100) if total > 0 else 0
        }
    
    async def get_all_users(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Получить всех пользователей (админ)"""
        cursor = await self.conn.execute('''
            SELECT u.*, 
                   (SELECT COUNT(*) FROM parsing_sessions WHERE user_id = u.user_id) as total_sessions
            FROM users u
            ORDER BY u.created_at DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))
        
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]
    
    async def get_user_count(self) -> int:
        """Количество пользователей"""
        cursor = await self.conn.execute("SELECT COUNT(*) as count FROM users")
        row = await cursor.fetchone()
        await cursor.close()
        return row['count']
    
    async def get_active_subscriptions_count(self) -> int:
        """Количество активных подписок"""
        cursor = await self.conn.execute('''
            SELECT COUNT(DISTINCT user_id) as count FROM subscriptions 
            WHERE status = 'active' AND expires_at > CURRENT_TIMESTAMP
        ''')
        row = await cursor.fetchone()
        await cursor.close()
        return row['count']
    
    async def get_total_parsings(self) -> int:
        """Всего парсингов"""
        cursor = await self.conn.execute("SELECT COUNT(*) as count FROM parsing_sessions WHERE status = 'completed'")
        row = await cursor.fetchone()
        await cursor.close()
        return row['count']
    
    async def get_revenue_stats(self) -> Dict:
        """Статистика доходов"""
        cursor = await self.conn.execute('''
            SELECT 
                COALESCE(SUM(price), 0) as total_revenue,
                COUNT(*) as total_sales,
                COALESCE(AVG(price), 0) as avg_price
            FROM subscriptions 
            WHERE price > 0
        ''')
        
        row = await cursor.fetchone()
        await cursor.close()
        
        return {
            'total_revenue': row['total_revenue'] or 0,
            'total_sales': row['total_sales'] or 0,
            'avg_price': row['avg_price'] or 0,
            'plans': []
        }
    
    async def update_user_subscription(self, user_id: int, days_to_add: int):
        """Продлить подписку"""
        cursor = await self.conn.execute('''
            SELECT * FROM subscriptions 
            WHERE user_id = ? AND status = 'active'
            ORDER BY expires_at DESC LIMIT 1
        ''', (user_id,))
        
        row = await cursor.fetchone()
        await cursor.close()
        
        if row:
            sub = dict(row)
            expires_at = datetime.fromisoformat(sub['expires_at'])
            new_expires_at = expires_at + timedelta(days=days_to_add)
            
            await self.conn.execute('''
                UPDATE subscriptions 
                SET expires_at = ?
                WHERE id = ?
            ''', (new_expires_at.isoformat(), sub['id']))
        else:
            # Создаем новую
            await self.create_subscription(user_id, 'admin', days_to_add, 0.0, 'RUB')
        
        await self.conn.commit()
        logger.info(f"Подписка пользователя {user_id} продлена на {days_to_add} дней")
    
    async def cleanup_expired_sessions(self):
        """Очистка старых сессий"""
        await self.conn.execute('''
            DELETE FROM parsing_sessions 
            WHERE completed_at IS NOT NULL 
            AND datetime(completed_at) < datetime('now', '-7 days')
        ''')
        await self.conn.commit()
        logger.info("Очищены устаревшие сессии парсинга")
    
    async def close(self):
        """Закрыть соединение"""
        if self.conn:
            await self.conn.close()
            logger.info("✅ Соединение с БД закрыто")

# Глобальный экземпляр
db = Database()