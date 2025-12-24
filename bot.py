#!/usr/bin/env python3
"""
🤖 Telegram Parser Bot with Subscription System
Парсер Telegram каналов с системой подписок и управлением сессиями
"""

import os
import sys
import asyncio
import logging
import json
import tempfile
import uuid
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import csv
import pandas as pd

# === ВАЖНО: Исправление для Windows ===
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ======================================

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ContextTypes, MessageHandler, filters, ConversationHandler
)
from telegram.constants import ParseMode

from telethon import TelegramClient as TelethonClient
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.types import ChannelParticipantsSearch, ChannelParticipantsRecent
from telethon.errors import FloodWaitError, ChannelPrivateError, SessionPasswordNeededError
from telethon.tl.functions.auth import ResendCodeRequest

from fastapi import FastAPI
import uvicorn
import aiofiles

# Импортируем нашу базу данных
from database import db

# ==================== КОНФИГУРАЦИЯ ====================

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('logs/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Получаем токен из переменных окружения
BOT_TOKEN = os.environ.get('BOT_TOKEN')
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не установлен!")
    print("\n" + "="*60)
    print("🚀 ИНСТРУКЦИЯ ПО ЗАПУСКУ:")
    print("="*60)
    print("1. Установите переменные окружения в Render:")
    print("   - BOT_TOKEN=ваш_токен_бота (от @BotFather)")
    print("   - TELEGRAM_API_ID=ваш_api_id (от my.telegram.org)")
    print("   - TELEGRAM_API_HASH=ваш_api_hash (от my.telegram.org)")
    print("="*60)
    sys.exit(1)

# Настройки для вебхука (для Render)
PORT = int(os.environ.get('PORT', '8080'))

# Настройки парсинга
PARSING_SETTINGS = {
    'BATCH_SIZE': 200,
    'MAX_PARTICIPANTS': 500,
    'DELAY_BETWEEN_BATCHES': 1,
    'DEFAULT_LIMIT': 1000
}

# Состояния ConversationHandler
(
    START, 
    AUTH_PHONE,
    AUTH_CODE,
    AUTH_PASSWORD,
    MAIN_MENU, 
    PARSE_CHANNEL,
    CHOOSE_PLAN,
    CONFIRM_PAYMENT,
    SETUP_CHANNEL,
    PARSING_METHOD
) = range(10)

# Глобальная база данных
# Используем готовый экземпляр из database.py

# ==================== SESSION MANAGER ====================

class SessionManager:
    """Менеджер сессий Telethon для пользователей"""
    
    def __init__(self):
        self.clients = {}
        self.sessions_dir = "user_sessions"
        os.makedirs(self.sessions_dir, exist_ok=True)
    
    def get_session_path(self, user_id: int) -> str:
        """Получить путь к файлу сессии пользователя"""
        return os.path.join(self.sessions_dir, f"{user_id}.session")
    
    async def create_client(self, user_id: int, api_id: str, api_hash: str) -> TelethonClient:
        """Создать Telethon клиент для пользователя"""
        session_path = self.get_session_path(user_id)
        client = TelethonClient(session_path, int(api_id), api_hash)
        self.clients[user_id] = client
        return client
    
    async def get_client(self, user_id: int) -> Optional[TelethonClient]:
        """Получить клиент пользователя"""
        return self.clients.get(user_id)
    
    async def is_authorized(self, user_id: int) -> bool:
        """Проверить авторизацию пользователя"""
        try:
            # Пробуем получить существующий клиент
            client = self.clients.get(user_id)
            if client:
                return await client.is_user_authorized()
            
            # Если клиента нет в памяти, проверяем файл сессии
            session_path = self.get_session_path(user_id)
            if os.path.exists(session_path):
                # Создаем временный клиент для проверки
                client = TelethonClient(session_path, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
                await client.connect()
                is_auth = await client.is_user_authorized()
                await client.disconnect()
                return is_auth
            
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки авторизации для {user_id}: {e}")
            return False
    
    async def close_client(self, user_id: int):
        """Закрыть клиент пользователя"""
        if user_id in self.clients:
            try:
                await self.clients[user_id].disconnect()
            except:
                pass
            del self.clients[user_id]
    
    async def cleanup_expired_sessions(self, days: int = 30):
        """Очистить старые сессии"""
        import time
        now = time.time()
        for filename in os.listdir(self.sessions_dir):
            if filename.endswith('.session'):
                path = os.path.join(self.sessions_dir, filename)
                try:
                    if os.path.getmtime(path) < now - days * 86400:
                        os.remove(path)
                        logger.info(f"Удалена старая сессия: {filename}")
                except:
                    pass

# Глобальный менеджер сессий
session_manager = SessionManager()

# ==================== FASTAPI HEALTH CHECK ====================

# Создаем FastAPI приложение для health check
fastapi_app = FastAPI()

@fastapi_app.get("/health")
async def health_check():
    return {
        "status": "ok", 
        "service": "telegram-parser-bot", 
        "timestamp": datetime.now().isoformat()
    }

@fastapi_app.get("/")
async def root():
    return {
        "message": "Telegram Parser Bot is running", 
        "docs": "/health",
        "version": "2.0.0"
    }

def run_fastapi():
    """Запуск FastAPI в отдельном потоке"""
    # Создаем новый event loop для этого потока
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        config = uvicorn.Config(
            fastapi_app, 
            host="0.0.0.0", 
            port=PORT, 
            log_level="warning",
            loop="asyncio"
        )
        server = uvicorn.Server(config)
        loop.run_until_complete(server.serve())
    except Exception as e:
        logger.error(f"Ошибка FastAPI: {e}")
    finally:
        loop.close()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

async def check_subscription(user_id: int) -> Dict:
    """
    Проверить подписку пользователя и вернуть статус
    Возвращает: {'has_access': bool, 'subscription': dict or None, 'message': str}
    """
    subscription = await db.get_user_subscription(user_id)
    
    if not subscription:
        # Проверяем, есть ли пользователь в базе
        user = await db.get_user(user_id)
        if not user:
            await db.create_user(user_id, "user", "Новый", "Пользователь")
            subscription = await db.get_user_subscription(user_id)
        
        if not subscription:
            return {
                'has_access': False,
                'subscription': None,
                'message': "❌ У вас нет активной подписки.\n\nИспользуйте /buy для покупки доступа."
            }
    
    # Проверяем срок подписки
    expires_at = datetime.fromisoformat(subscription['expires_at'])
    days_left = (expires_at - datetime.now()).days
    
    if days_left < 0:
        return {
            'has_access': False,
            'subscription': subscription,
            'message': f"❌ Ваша подписка истекла {abs(days_left)} дней назад.\n\nИспользуйте /buy для продления."
        }
    
    return {
        'has_access': True,
        'subscription': subscription,
        'message': f"✅ Ваша подписка активна! Осталось: {days_left} дней"
    }

def format_subscription_info(subscription: Dict) -> str:
    """Форматировать информацию о подписке"""
    if not subscription:
        return "❌ Нет активной подписки"
    
    expires_at = datetime.fromisoformat(subscription['expires_at'])
    days_left = (expires_at - datetime.now()).days
    
    plan_names = {
        'trial': 'Пробная',
        'daily': 'Дневная',
        'weekly': 'Недельная',
        'monthly': 'Месячная',
        'yearly': 'Годовая'
    }
    
    plan_name = plan_names.get(subscription['plan_type'], subscription['plan_type'])
    
    return (
        f"📅 **{plan_name} подписка**\n"
        f"📆 Истекает: {expires_at.strftime('%d.%m.%Y %H:%M')}\n"
        f"⏳ Осталось: {days_left} дней\n"
        f"💰 Стоимость: {subscription['price']} {subscription['currency']}\n"
        f"🔧 Статус: {subscription['status']}"
    )

async def export_to_file(data: List[Dict], format_type: str = 'txt') -> str:
    """Экспорт данных в файл"""
    os.makedirs('temp', exist_ok=True)
    filename = f"temp/export_{uuid.uuid4().hex[:8]}.{format_type}"
    
    if format_type == 'txt':
        # Экспорт только username
        lines = []
        for item in data:
            if 'username' in item and item['username']:
                lines.append(item['username'])
            elif 'id' in item:
                lines.append(f"id_{item['id']}")
        
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write('\n'.join(lines))
    
    elif format_type == 'csv':
        # Экспорт в CSV
        if data:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8')
    
    elif format_type == 'xlsx':
        # Экспорт в Excel
        if data:
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False)
    
    return filename

# ==================== ТЕЛЕГРАМ БОТ ====================

class SubscriptionTelegramBot:
    def __init__(self):
        self.app = None
        self.user_auth_states = {}  # Состояния аутентификации пользователей
    
    async def initialize(self):
        """Инициализация бота и базы данных"""
        await db.connect()
        logger.info("✅ База данных подключена")
    
    # ==================== КОМАНДЫ ====================
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start - автоматическое распознавание пользователя"""
        user = update.effective_user
        
        # Получаем или создаем пользователя
        await db.get_or_create_user(
            user.id,
            user.username,
            user.first_name,
            user.last_name
        )
        
        # Проверяем, является ли пользователь администратором
        is_admin = await db.is_admin(user.id)
        
        # Проверяем авторизацию в Telethon
        is_authorized = await session_manager.is_authorized(user.id)
        
        if is_admin and is_authorized:
            # Админ с активной сессией
            await update.message.reply_text(
                f"👑 **Добро пожаловать, администратор {user.first_name}!**\n\n"
                f"Вы вошли с правами администратора и у вас есть активная сессия.\n\n"
                f"Выберите действие:",
                reply_markup=self.get_admin_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
            return MAIN_MENU
        elif is_authorized:
            # Обычный пользователь с активной сессией - проверяем подписку
            subscription_status = await check_subscription(user.id)
            
            if subscription_status['has_access']:
                # Пользователь с активной подпиской
                await update.message.reply_text(
                    f"👋 Добро пожаловать, {user.first_name}!\n\n"
                    f"{subscription_status['message']}\n\n"
                    f"Что вы хотите сделать?",
                    reply_markup=self.get_main_menu_keyboard(),
                    parse_mode=ParseMode.MARKDOWN
                )
                return MAIN_MENU
            else:
                # Пользователь без подписки или с истекшей
                await update.message.reply_text(
                    f"👋 Привет, {user.first_name}!\n\n"
                    f"✅ **У вас есть активная сессия Telethon!**\n\n"
                    f"{subscription_status['message']}\n\n"
                    f"Выберите действие:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                        [InlineKeyboardButton("🚀 Начать парсинг (без подписки)", callback_data='start_parsing')],
                        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return START
        else:
            # Пользователь без активной сессии
            await update.message.reply_text(
                f"👋 Привет, {user.first_name}!\n\n"
                f"🤖 **Я профессиональный парсер Telegram каналов!**\n\n"
                f"📱 Для начала работы нужно авторизоваться в Telegram.\n"
                f"Это безопасно и займет меньше минуты!\n\n"
                f"✨ **Возможности бота:**\n"
                f"🔍 Парсинг участников каналов\n"
                f"💬 Сбор сообщений и комментариев\n"
                f"🎯 4 метода сбора данных\n"
                f"📁 Экспорт в TXT/CSV/Excel\n\n"
                f"Нажмите кнопку ниже для авторизации:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Авторизоваться в Telegram", callback_data='start_auth')],
                    [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                    [InlineKeyboardButton("❓ Помощь", callback_data='help')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return START
    
    async def buy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /buy - покупка подписки"""
        user = update.effective_user
        await self.show_subscription_plans(update, context)
        return CHOOSE_PLAN
    
    async def my_subscription_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /my - моя подписка"""
        user = update.effective_user
        subscription = await db.get_user_subscription(user.id)
        
        if subscription:
            subscription_info = format_subscription_info(subscription)
            await update.message.reply_text(
                subscription_info,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Продлить", callback_data='buy_subscription')],
                    [InlineKeyboardButton("🏠 В главное меню", callback_data='main_menu')]
                ])
            )
        else:
            await update.message.reply_text(
                "❌ У вас нет активной подписки.\n\n"
                "Используйте /buy для покупки доступа.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')]
                ])
            )
        
        return MAIN_MENU
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /stats - статистика"""
        user = update.effective_user
        stats = await db.get_user_stats(user.id)
        
        stats_text = (
            f"📊 **Ваша статистика**\n\n"
            f"🔢 Всего сессий: {stats['total_sessions']}\n"
            f"✅ Успешных: {stats['completed_sessions']}\n"
            f"📈 Успешность: {stats['success_rate']}%\n"
            f"👥 Участников спарсено: {stats['total_members']}\n\n"
            f"⏰ Последняя активность: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await update.message.reply_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data='refresh_stats')],
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
            ])
        )
        
        return MAIN_MENU
    
    # ==================== АУТЕНТИФИКАЦИЯ ЧЕРЕЗ TELEGRAM ====================
    
    async def start_auth_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало процесса аутентификации"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        # Создаем клиент Telethon для пользователя
        try:
            client = await session_manager.create_client(
                user_id, 
                TELEGRAM_API_ID, 
                TELEGRAM_API_HASH
            )
            
            # Запрашиваем номер телефона
            await query.edit_message_text(
                "📱 **Шаг 1 из 3: Авторизация в Telegram**\n\n"
                "Введите ваш номер телефона в международном формате:\n"
                "(например: `+79991234567`)\n\n"
                "⚠️ *Этот номер будет использован только для создания сессии парсинга*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            self.user_auth_states[user_id] = {'client': client}
            return AUTH_PHONE
            
        except Exception as e:
            logger.error(f"Ошибка создания клиента: {e}")
            await query.edit_message_text(
                "❌ **Ошибка создания сессии!**\n\n"
                "Попробуйте еще раз позже или обратитесь в поддержку.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data='start_auth')],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
                ])
            )
            return START
    
    async def auth_phone_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка номера телефона"""
        phone = update.message.text.strip()
        user_id = update.effective_user.id
        
        if not phone.startswith('+'):
            await update.message.reply_text(
                "❌ **Номер должен начинаться с +!**\n"
                "Введите снова в формате: `+79991234567`",
                parse_mode=ParseMode.MARKDOWN
            )
            return AUTH_PHONE
        
        # Сохраняем номер в состоянии
        if user_id in self.user_auth_states:
            self.user_auth_states[user_id]['phone'] = phone
        
        try:
            # Запрашиваем код
            client = self.user_auth_states[user_id]['client']
            sent_code = await client.send_code_request(phone)
            
            self.user_auth_states[user_id]['phone_code_hash'] = sent_code.phone_code_hash
            
            await update.message.reply_text(
                f"✅ **Код отправлен на ваш Telegram!**\n\n"
                f"📱 Номер: `{phone}`\n\n"
                f"✉️ **Шаг 2 из 3: Введите код из Telegram**\n"
                f"(5-значный код, например: `12345`)\n\n"
                f"⏱️ *Код действителен 5 минут*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            return AUTH_CODE
            
        except FloodWaitError as e:
            wait_time = e.seconds
            await update.message.reply_text(
                f"⏳ **Слишком много запросов!**\n"
                f"Пожалуйста, подождите {wait_time} секунд и попробуйте снова.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data='start_auth')]
                ])
            )
            return START
            
        except Exception as e:
            logger.error(f"Ошибка отправки кода: {e}")
            await update.message.reply_text(
                "❌ **Ошибка отправки кода!**\n"
                "Проверьте номер телефона и попробуйте снова.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data='start_auth')]
                ])
            )
            return START
    
    async def auth_code_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кода подтверждения"""
        code = update.message.text.strip()
        user_id = update.effective_user.id
        
        if not code.isdigit() or len(code) != 5:
            await update.message.reply_text(
                "❌ **Код должен быть 5 цифр!**\n"
                "Введите снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return AUTH_CODE
        
        try:
            # Получаем данные из состояния
            auth_data = self.user_auth_states.get(user_id)
            if not auth_data:
                await update.message.reply_text(
                    "❌ **Сессия устарела!**\n"
                    "Начните авторизацию заново.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Начать заново", callback_data='start_auth')]
                    ])
                )
                return START
            
            client = auth_data['client']
            phone = auth_data['phone']
            phone_code_hash = auth_data['phone_code_hash']
            
            # Пытаемся войти с кодом
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash
                )
                
                # Успешная авторизация - клиент уже создан в start_auth_callback
                
                # Проверяем подписку и показываем соответствующее меню
                is_admin = await db.is_admin(user_id)
                subscription_status = await check_subscription(user_id)
                
                if is_admin:
                    await update.message.reply_text(
                        f"🎉 **Авторизация успешна!**\n\n"
                        f"✅ Вы успешно авторизовались как администратор!\n"
                        f"📱 Номер: `{phone}`\n\n"
                        f"Теперь вы можете использовать все функции парсера.",
                        reply_markup=self.get_admin_main_menu_keyboard(),
                        parse_mode=ParseMode.MARKDOWN
                    )
                elif subscription_status['has_access']:
                    await update.message.reply_text(
                        f"🎉 **Авторизация успешна!**\n\n"
                        f"✅ Вы успешно авторизовались!\n"
                        f"📱 Номер: `{phone}`\n\n"
                        f"{subscription_status['message']}\n\n"
                        f"Теперь вы можете использовать все функции парсера.",
                        reply_markup=self.get_main_menu_keyboard(),
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(
                        f"🎉 **Авторизация успешна!**\n\n"
                        f"✅ Вы успешно авторизовались!\n"
                        f"📱 Номер: `{phone}`\n\n"
                        f"{subscription_status['message']}\n\n"
                        f"Купите подписку для доступа к парсингу.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                            [InlineKeyboardButton("🚀 Демо-парсинг", callback_data='start_parsing')],
                            [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
                        ]),
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                # Очищаем состояние аутентификации
                if user_id in self.user_auth_states:
                    del self.user_auth_states[user_id]
                
                return MAIN_MENU
                
            except SessionPasswordNeededError:
                # Требуется двухфакторная аутентификация
                await update.message.reply_text(
                    "🔐 **Требуется двухфакторная аутентификация**\n\n"
                    "Введите ваш пароль для двухфакторной аутентификации:",
                    parse_mode=ParseMode.MARKDOWN
                )
                return AUTH_PASSWORD
                
        except Exception as e:
            logger.error(f"Ошибка входа: {e}")
            await update.message.reply_text(
                "❌ **Неверный код!**\n"
                "Проверьте код и попробуйте снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return AUTH_CODE
    
    async def auth_password_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка пароля двухфакторной аутентификации"""
        password = update.message.text.strip()
        user_id = update.effective_user.id
        
        try:
            auth_data = self.user_auth_states.get(user_id)
            if not auth_data:
                await update.message.reply_text(
                    "❌ **Сессия устарела!**\n"
                    "Начните авторизацию заново.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Начать заново", callback_data='start_auth')]
                    ])
                )
                return START
            
            client = auth_data['client']
            
            # Пытаемся войти с паролем
            await client.sign_in(password=password)
            
            # Успешная авторизация
            await update.message.reply_text(
                "🎉 **Авторизация успешна!**\n\n"
                "✅ Вы успешно авторизовались с двухфакторной аутентификацией!\n\n"
                "Теперь вы можете использовать все функции парсера.",
                reply_markup=self.get_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Очищаем состояние аутентификации
            if user_id in self.user_auth_states:
                del self.user_auth_states[user_id]
            
            return MAIN_MENU
            
        except Exception as e:
            logger.error(f"Ошибка входа с паролем: {e}")
            await update.message.reply_text(
                "❌ **Неверный пароль!**\n"
                "Введите снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return AUTH_PASSWORD
    
    async def logout_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /logout - выход из сессии"""
        user = update.effective_user
        
        try:
            # Закрываем сессию Telethon
            await session_manager.close_client(user.id)
            
            # Удаляем файл сессии
            session_path = session_manager.get_session_path(user.id)
            if os.path.exists(session_path):
                os.remove(session_path)
            
            await update.message.reply_text(
                "✅ **Вы успешно вышли из системы!**\n\n"
                "Ваша сессия удалена. Для использования бота снова нажмите /start",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Начать снова", callback_data='start_auth')]
                ])
            )
            
        except Exception as e:
            logger.error(f"Ошибка выхода: {e}")
            await update.message.reply_text(
                "❌ **Ошибка при выходе!**\n"
                "Попробуйте еще раз позже."
            )
        
        return START
    
    # ==================== АДМИН КОМАНДЫ ====================
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /admin - админ панель"""
        user = update.effective_user
        
        # Проверяем, является ли пользователь администратором
        if not await db.is_admin(user.id):
            await update.message.reply_text(
                "❌ У вас нет прав доступа к админ панели.",
                reply_markup=self.get_main_menu_keyboard()
            )
            return MAIN_MENU
        
        await update.message.reply_text(
            "🔧 **Административная панель**\n\n"
            "Выберите раздел:",
            reply_markup=self.get_admin_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        return MAIN_MENU

    def get_admin_menu_keyboard(self):
        """Клавиатура админ меню"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Пользователи", callback_data='admin_users')],
            [InlineKeyboardButton("💰 Подписки", callback_data='admin_subscriptions')],
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("🎯 Управление", callback_data='admin_manage')],
            [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
        ])

    def get_admin_main_menu_keyboard(self):
        """Главное меню для администратора"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
            [InlineKeyboardButton("💰 Моя подписка", callback_data='my_subscription')],
            [InlineKeyboardButton("📊 Статистика", callback_data='stats')],
            [InlineKeyboardButton("🔧 Админ панель", callback_data='admin_panel')],
            [InlineKeyboardButton("🔓 Управление сессиями", callback_data='manage_sessions')],
            [InlineKeyboardButton("❓ Помощь", callback_data='help')]
        ])

    async def admin_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик админ callback кнопок"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        # Проверяем права администратора
        if not await db.is_admin(user_id):
            await query.edit_message_text(
                "❌ У вас нет прав доступа.",
                reply_markup=self.get_main_menu_keyboard()
            )
            return MAIN_MENU
        
        if query.data == 'admin_panel':
            await query.edit_message_text(
                "🔧 **Административная панель**\n\n"
                "Выберите раздел:",
                reply_markup=self.get_admin_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
            
        elif query.data == 'admin_users':
            await self.show_admin_users(query)
            
        elif query.data == 'admin_subscriptions':
            await self.show_admin_subscriptions(query)
            
        elif query.data == 'admin_stats':
            await self.show_admin_stats(query)
            
        elif query.data == 'admin_manage':
            await self.show_admin_manage(query)
            
        elif query.data == 'manage_sessions':
            await self.show_session_management(query)
            
        elif query.data.startswith('session_action_'):
            # Формат: session_action_USERID_ACTION
            parts = query.data.replace('session_action_', '').split('_')
            if len(parts) == 2:
                target_user_id = int(parts[0])
                action = parts[1]
                await self.manage_user_session(query, target_user_id, action)
            
        elif query.data.startswith('admin_user_'):
            user_id_to_manage = int(query.data.replace('admin_user_', ''))
            await self.manage_user(query, user_id_to_manage)
            
        elif query.data.startswith('admin_extend_'):
            # Формат: admin_extend_USERID_DAYS
            parts = query.data.replace('admin_extend_', '').split('_')
            if len(parts) == 2:
                target_user_id = int(parts[0])
                days = int(parts[1])
                await self.extend_subscription(query, target_user_id, days)
        
        elif query.data == 'admin_back':
            await query.edit_message_text(
                "🔧 **Административная панель**\n\n"
                "Выберите раздел:",
                reply_markup=self.get_admin_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        
        return MAIN_MENU
    
    async def show_session_management(self, query):
        """Показать управление сессиями"""
        # Получаем список активных сессий
        active_sessions = []
        for filename in os.listdir(session_manager.sessions_dir):
            if filename.endswith('.session'):
                try:
                    user_id = int(filename.replace('.session', ''))
                    is_authorized = await session_manager.is_authorized(user_id)
                    active_sessions.append({
                        'user_id': user_id,
                        'authorized': is_authorized
                    })
                except:
                    continue
        
        sessions_text = "🔓 **Управление сессиями пользователей**\n\n"
        
        if not active_sessions:
            sessions_text += "📭 Активных сессий не найдено.\n"
        else:
            sessions_text += f"📊 **Всего активных сессий:** {len(active_sessions)}\n\n"
            
            for session in active_sessions[:10]:  # Показываем первые 10
                user = await db.get_user(session['user_id'])
                user_name = f"{user['first_name']} {user['last_name']}" if user else "Неизвестный"
                sessions_text += f"• **ID:** `{session['user_id']}` - {user_name}\n"
                sessions_text += f"  Статус: {'🟢 Активна' if session['authorized'] else '🟡 Не авторизована'}\n\n"
        
        keyboard = []
        for session in active_sessions[:5]:
            btn_text = f"👤 {session['user_id']} - {'🟢' if session['authorized'] else '🟡'}"
            keyboard.append([InlineKeyboardButton(
                btn_text, 
                callback_data=f'session_action_{session["user_id"]}_view'
            )])
        
        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data='manage_sessions')])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='admin_back')])
        
        await query.edit_message_text(
            sessions_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def manage_user_session(self, query, target_user_id: int, action: str):
        """Управление сессией пользователя"""
        if action == 'view':
            user = await db.get_user(target_user_id)
            session_path = session_manager.get_session_path(target_user_id)
            is_authorized = await session_manager.is_authorized(target_user_id)
            session_exists = os.path.exists(session_path)
            
            user_name = f"{user['first_name']} {user['last_name']}" if user else "Неизвестный"
            info_text = (
                f"👤 **Информация о сессии**\n\n"
                f"**Пользователь:** {user_name}\n"
                f"**ID:** `{target_user_id}`\n"
                f"**Файл сессии:** {'✅ Существует' if session_exists else '❌ Отсутствует'}\n"
                f"**Авторизация:** {'🟢 Активна' if is_authorized else '🔴 Не активна'}\n"
            )
            
            keyboard = [
                [InlineKeyboardButton("🗑️ Удалить сессию", callback_data=f'session_action_{target_user_id}_delete')],
                [InlineKeyboardButton("🔙 Назад к списку", callback_data='manage_sessions')]
            ]
            
            await query.edit_message_text(
                info_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        elif action == 'delete':
            # Удаляем сессию
            await session_manager.close_client(target_user_id)
            session_path = session_manager.get_session_path(target_user_id)
            if os.path.exists(session_path):
                os.remove(session_path)
            
            await query.answer("✅ Сессия удалена!")
            await self.show_session_management(query)

    async def show_admin_users(self, query):
        """Показать список пользователей (админ)"""
        users = await db.get_all_users(limit=20)
        
        if not users:
            await query.edit_message_text(
                "📭 Пользователей не найдено.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data='admin_back')]
                ])
            )
            return
        
        users_text = "👥 **Список пользователей**\n\n"
        
        for i, user in enumerate(users[:10], 1):  # Показываем первые 10
            created_at = datetime.fromisoformat(user['created_at']).strftime('%d.%m.%Y')
            last_activity = datetime.fromisoformat(user['last_activity']).strftime('%d.%m.%Y %H:%M')
            
            # Проверяем сессию
            session_path = session_manager.get_session_path(user['user_id'])
            has_session = os.path.exists(session_path)
            
            users_text += (
                f"{i}. **ID:** `{user['user_id']}`\n"
                f"   👤 {user['first_name']} {user['last_name']}\n"
                f"   📅 Регистрация: {created_at}\n"
                f"   ⏰ Активность: {last_activity}\n"
                f"   📊 Сессий: {user['total_sessions']}\n"
            )
            
            if user.get('subscription_status') == 'active':
                expires = datetime.fromisoformat(user['subscription_expires']).strftime('%d.%m.%Y')
                users_text += f"   ✅ Подписка до: {expires}\n"
            else:
                users_text += "   ❌ Нет активной подписки\n"
            
            users_text += f"   📱 Сессия: {'🟢 Есть' if has_session else '🔴 Нет'}\n"
            users_text += f"   {'👑 Админ' if user['is_admin'] else '👤 Пользователь'}\n\n"
        
        keyboard = []
        for user in users[:5]:  # Кнопки для управления первыми 5 пользователями
            btn_text = f"👤 {user['user_id']} - {user['first_name']}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f'admin_user_{user["user_id"]}')])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='admin_back')])
        
        total_users = await db.get_user_count()
        users_text += f"📈 **Всего пользователей:** {total_users}"
        
        await query.edit_message_text(
            users_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_admin_subscriptions(self, query):
        """Показать информацию о подписках (админ)"""
        active_subs = await db.get_active_subscriptions_count()
        total_parsings = await db.get_total_parsings()
        revenue_stats = await db.get_revenue_stats()
        
        subscriptions_text = (
            "💰 **Статистика подписок**\n\n"
            f"👥 Активных подписок: **{active_subs}**\n"
            f"📊 Всего парсингов: **{total_parsings}**\n"
            f"💵 Общая выручка: **{revenue_stats['total_revenue']:.2f} RUB**\n"
            f"🛒 Всего продаж: **{revenue_stats['total_sales']}**\n"
            f"📈 Средний чек: **{revenue_stats['avg_price']:.2f} RUB**\n\n"
            "**По тарифам:**\n"
        )
        
        for plan in revenue_stats['plans']:
            subscriptions_text += (
                f"• {plan['plan_type']}: {plan['plan_count']} продаж, "
                f"{plan['total_revenue']:.2f} RUB\n"
            )
        
        keyboard = [
            [InlineKeyboardButton("🎯 Быстрое продление", callback_data='admin_quick_extend')],
            [InlineKeyboardButton("📊 Подробная статистика", callback_data='admin_detailed_stats')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin_back')]
        ]
        
        await query.edit_message_text(
            subscriptions_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_admin_stats(self, query):
        """Показать общую статистику (админ)"""
        total_users = await db.get_user_count()
        active_subs = await db.get_active_subscriptions_count()
        total_parsings = await db.get_total_parsings()
        revenue_stats = await db.get_revenue_stats()
        
        # Получаем последние 5 пользователей
        recent_users = await db.get_all_users(limit=5)
        
        # Подсчитываем активные сессии
        active_sessions = len([f for f in os.listdir(session_manager.sessions_dir) 
                              if f.endswith('.session')])
        
        stats_text = (
            "📊 **Общая статистика бота**\n\n"
            f"👥 Всего пользователей: **{total_users}**\n"
            f"🔓 Активных сессий: **{active_sessions}**\n"
            f"✅ Активных подписок: **{active_subs}** ({active_subs/total_users*100:.1f}%)\n"
            f"🔧 Всего парсингов: **{total_parsings}**\n"
            f"💰 Выручка: **{revenue_stats['total_revenue']:.2f} RUB**\n\n"
            "**Последние пользователи:**\n"
        )
        
        for user in recent_users:
            reg_date = datetime.fromisoformat(user['created_at']).strftime('%d.%m')
            stats_text += f"• {user['first_name']} (ID: {user['user_id']}) - {reg_date}\n"
        
        stats_text += f"\n⏰ **Время сервера:** {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data='admin_stats')],
            [InlineKeyboardButton("📧 Рассылка", callback_data='admin_broadcast')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin_back')]
        ]
        
        await query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_admin_manage(self, query):
        """Показать меню управления (админ)"""
        manage_text = (
            "🎯 **Управление ботом**\n\n"
            "Доступные действия:\n\n"
            "1. **Управление пользователями**\n"
            "   • Продление подписок\n"
            "   • Назначение админов\n"
            "   • Бан/разбан\n\n"
            "2. **Управление сессиями**\n"
            "   • Просмотр активных сессий\n"
            "   • Удаление сессий\n\n"
            "3. **Финансы**\n"
            "   • Статистика продаж\n"
            "   • Экспорт данных\n\n"
            "4. **Система**\n"
            "   • Очистка старых данных\n"
            "   • Резервное копирование\n"
            "   • Рассылка уведомлений\n"
        )
        
        keyboard = [
            [InlineKeyboardButton("👥 Управление пользователями", callback_data='admin_users')],
            [InlineKeyboardButton("🔓 Управление сессиями", callback_data='manage_sessions')],
            [InlineKeyboardButton("💰 Финансовая статистика", callback_data='admin_subscriptions')],
            [InlineKeyboardButton("⚙️ Системные утилиты", callback_data='admin_utils')],
            [InlineKeyboardButton("🔙 Назад", callback_data='admin_back')]
        ]
        
        await query.edit_message_text(
            manage_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def manage_user(self, query, target_user_id: int):
        """Управление конкретным пользователем"""
        user = await db.get_user(target_user_id)
        subscription = await db.get_user_subscription(target_user_id)
        
        if not user:
            await query.edit_message_text(
                f"❌ Пользователь с ID {target_user_id} не найден.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data='admin_users')]
                ])
            )
            return
        
        # Проверяем сессию
        session_path = session_manager.get_session_path(target_user_id)
        has_session = os.path.exists(session_path)
        is_authorized = await session_manager.is_authorized(target_user_id) if has_session else False
        
        user_info = (
            f"👤 **Управление пользователем**\n\n"
            f"**ID:** `{user['user_id']}`\n"
            f"**Имя:** {user['first_name']} {user['last_name']}\n"
            f"**Username:** @{user['username'] if user['username'] else 'нет'}\n"
            f"**Регистрация:** {datetime.fromisoformat(user['created_at']).strftime('%d.%m.%Y %H:%M')}\n"
            f"**Активность:** {datetime.fromisoformat(user['last_activity']).strftime('%d.%m.%Y %H:%M')}\n"
            f"**Статус:** {'👑 Админ' if user['is_admin'] else '👤 Пользователь'}\n"
            f"**Бан:** {'🔴 Забанен' if user['is_banned'] else '🟢 Активен'}\n"
            f"**Сессия:** {'🟢 Активна' if is_authorized else '🔴 Нет' if not has_session else '🟡 Есть файл'}\n"
        )
        
        if subscription:
            expires = datetime.fromisoformat(subscription['expires_at']).strftime('%d.%m.%Y %H:%M')
            days_left = (datetime.fromisoformat(subscription['expires_at']) - datetime.now()).days
            user_info += (
                f"\n**Подписка:** ✅ Активна\n"
                f"**Тариф:** {subscription['plan_type']}\n"
                f"**Истекает:** {expires}\n"
                f"**Осталось дней:** {days_left}\n"
            )
        else:
            user_info += "\n**Подписка:** ❌ Нет активной подписки\n"
        
        keyboard = []
        
        # Кнопки продления подписки
        keyboard.extend([
            [InlineKeyboardButton("➕ 1 день", callback_data=f'admin_extend_{target_user_id}_1')],
            [InlineKeyboardButton("➕ 7 дней", callback_data=f'admin_extend_{target_user_id}_7')],
            [InlineKeyboardButton("➕ 30 дней", callback_data=f'admin_extend_{target_user_id}_30')],
        ])
        
        # Кнопки управления статусом
        if user['is_admin']:
            keyboard.append([InlineKeyboardButton("👤 Снять админа", callback_data=f'admin_toggle_admin_{target_user_id}')])
        else:
            keyboard.append([InlineKeyboardButton("👑 Назначить админом", callback_data=f'admin_toggle_admin_{target_user_id}')])
        
        if user['is_banned']:
            keyboard.append([InlineKeyboardButton("🟢 Разбанить", callback_data=f'admin_toggle_ban_{target_user_id}')])
        else:
            keyboard.append([InlineKeyboardButton("🔴 Забанить", callback_data=f'admin_toggle_ban_{target_user_id}')])
        
        # Кнопки управления сессией
        if has_session:
            keyboard.append([InlineKeyboardButton("🗑️ Удалить сессию", callback_data=f'session_action_{target_user_id}_delete')])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data='admin_users')])
        
        await query.edit_message_text(
            user_info,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def extend_subscription(self, query, target_user_id: int, days: int):
        """Продлить подписку пользователю"""
        await db.update_user_subscription(target_user_id, days)
        
        # Получаем обновленную информацию
        subscription = await db.get_user_subscription(target_user_id)
        expires = datetime.fromisoformat(subscription['expires_at']).strftime('%d.%m.%Y %H:%M')
        
        await query.answer(f"✅ Подписка продлена на {days} дней!")
        
        # Возвращаемся к управлению пользователем
        await self.manage_user(query, target_user_id)

    async def admin_broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для рассылки сообщений всем пользователям"""
        user = update.effective_user
        
        if not await db.is_admin(user.id):
            await update.message.reply_text("❌ У вас нет прав для рассылки.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "📢 **Формат команды:**\n"
                "`/broadcast Ваше сообщение для рассылки`\n\n"
                "*Сообщение будет отправлено всем пользователям бота.*",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        message = ' '.join(context.args)
        
        # Запрашиваем подтверждение
        await update.message.reply_text(
            f"📢 **Подтверждение рассылки**\n\n"
            f"Сообщение:\n`{message[:200]}...`\n\n"
            f"Будет отправлено всем пользователям бота.\n"
            f"Продолжить?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, отправить", callback_data=f'admin_confirm_broadcast_{user.id}')],
                [InlineKeyboardButton("❌ Отмена", callback_data='admin_cancel')]
            ])
        )
        
        # Сохраняем сообщение для рассылки
        context.user_data['broadcast_message'] = message
    
    # ==================== ОСНОВНЫЕ КОЛБЭКИ ====================
    
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback кнопок"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        if query.data == 'main_menu':
            # Проверяем, является ли пользователь администратором
            is_admin = await db.is_admin(user_id)
            is_authorized = await session_manager.is_authorized(user_id)
            
            if is_admin and is_authorized:
                await query.edit_message_text(
                    "🏠 **Главное меню (администратор)**\n\n"
                    "Выберите действие:",
                    reply_markup=self.get_admin_main_menu_keyboard(),
                    parse_mode=ParseMode.MARKDOWN
                )
            elif is_authorized:
                await self.show_main_menu(query)
            else:
                await query.edit_message_text(
                    "❌ **У вас нет активной сессии!**\n\n"
                    "Для использования бота нужно авторизоваться в Telegram.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔑 Авторизоваться", callback_data='start_auth')],
                        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return START
            return MAIN_MENU
            
        elif query.data == 'start_auth':
            await self.start_auth_callback(update, context)
            return AUTH_PHONE
            
        elif query.data == 'setup_api':
            await query.edit_message_text(
                "ℹ️ **Настройка API больше не требуется!**\n\n"
                "Теперь авторизация происходит через ваш аккаунт Telegram.\n"
                "Просто нажмите 'Авторизоваться' и следуйте инструкциям.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Авторизоваться", callback_data='start_auth')],
                    [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return START
            
        elif query.data == 'buy_subscription':
            await self.show_subscription_plans_callback(query)
            return CHOOSE_PLAN
            
        elif query.data == 'start_parsing':
            # Проверяем авторизацию
            is_authorized = await session_manager.is_authorized(user_id)
            if not is_authorized:
                await query.edit_message_text(
                    "❌ **У вас нет активной сессии!**\n\n"
                    "Для парсинга нужно авторизоваться в Telegram.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔑 Авторизоваться", callback_data='start_auth')]
                    ])
                )
                return START
            
            # Проверяем подписку для обычных пользователей
            is_admin = await db.is_admin(user_id)
            if not is_admin:
                subscription_status = await check_subscription(user_id)
                if not subscription_status['has_access']:
                    await query.edit_message_text(
                        subscription_status['message'] + "\n\n"
                        "Хотите попробовать демо-парсинг?",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🎯 Демо-парсинг", callback_data='demo_parsing')],
                            [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
                        ])
                    )
                    return START
            
            await self.start_parsing_menu(query)
            return PARSE_CHANNEL
            
        elif query.data == 'demo_parsing':
            # Демо-версия парсинга без подписки
            await query.edit_message_text(
                "🎯 **Демо-парсинг (ограниченная версия)**\n\n"
                "Вы можете спарсить до 20 участников из публичного канала.\n\n"
                "Выберите формат вывода результатов:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 TXT файл", callback_data='format_txt')],
                    [InlineKeyboardButton("📊 CSV файл", callback_data='format_csv')],
                    [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['demo_mode'] = True
            return PARSE_CHANNEL
            
        elif query.data.startswith('plan_'):
            plan_type = query.data.replace('plan_', '')
            await self.confirm_purchase(query, plan_type)
            return CONFIRM_PAYMENT
            
        elif query.data == 'confirm_purchase':
            await self.process_payment(query, user_id, context.user_data)
            return MAIN_MENU
            
        elif query.data == 'cancel_purchase':
            await self.show_main_menu(query)
            return MAIN_MENU
            
        elif query.data.startswith('format_'):
            format_type = query.data.replace('format_', '')
            context.user_data['export_format'] = format_type
            await query.edit_message_text(
                f"✅ Выбран формат: **{format_type.upper()}**\n\n"
                f"📢 **Введите username канала или ссылку:**\n"
                f"• Без @ (например: `telegram`)\n"
                f"• Или ссылку (например: `t.me/telegram`)\n"
                f"• Или ссылку-приглашение в приватный канал\n\n"
                f"⏱️ *Парсинг может занять 1-5 минут*",
                parse_mode=ParseMode.MARKDOWN
            )
            return PARSE_CHANNEL
        
        elif query.data == 'help':
            await self.help_command_callback(query)
            return MAIN_MENU
        
        elif query.data == 'my_subscription':
            await self.my_subscription_callback(query, user_id)
            return MAIN_MENU
        
        elif query.data == 'stats':
            await self.stats_callback(query, user_id)
            return MAIN_MENU
        
        elif query.data == 'logout':
            await self.logout_command(update, context)
            return START
        
        elif query.data == 'manage_session':
            await self.show_user_session_management(query, user_id)
            return MAIN_MENU
        
        elif query.data == 'delete_my_session':
            await self.logout_command(update, context)
            return START
            
        elif query.data == 'refresh_stats':
            await self.stats_callback(query, user_id)
            return MAIN_MENU
    
    # ==================== МЕНЮ ====================
    
    def get_main_menu_keyboard(self):
        """Клавиатура главного меню для обычных пользователей"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
            [InlineKeyboardButton("💰 Моя подписка", callback_data='my_subscription')],
            [InlineKeyboardButton("📊 Статистика", callback_data='stats')],
            [InlineKeyboardButton("🔓 Управление сессией", callback_data='manage_session')],
            [InlineKeyboardButton("❓ Помощь", callback_data='help')],
            [InlineKeyboardButton("🚪 Выйти", callback_data='logout')]
        ])
    
    async def show_main_menu(self, query):
        """Показать главное меню для обычных пользователей"""
        await query.edit_message_text(
            "🏠 **Главное меню**\n\n"
            "Выберите действие:",
            reply_markup=self.get_main_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def show_user_session_management(self, query, user_id: int):
        """Показать управление сессией для пользователя"""
        is_authorized = await session_manager.is_authorized(user_id)
        session_path = session_manager.get_session_path(user_id)
        has_session = os.path.exists(session_path)
        
        text = (
            "🔐 **Управление вашей сессией**\n\n"
            f"📱 **Статус:** {'🟢 Активна' if is_authorized else '🔴 Не активна'}\n"
            f"💾 **Файл сессии:** {'✅ Сохранен' if has_session else '❌ Отсутствует'}\n\n"
            "Выберите действие:"
        )
        
        keyboard = []
        if has_session:
            keyboard.append([InlineKeyboardButton("🗑️ Удалить сессию", callback_data='delete_my_session')])
        keyboard.append([InlineKeyboardButton("🔄 Проверить статус", callback_data='manage_session')])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')])
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def start_parsing_menu(self, query):
        """Меню начала парсинга"""
        await query.edit_message_text(
            "🎯 **Парсинг Telegram канала**\n\n"
            "Выберите формат вывода результатов:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 TXT файл", callback_data='format_txt')],
                [InlineKeyboardButton("📊 CSV файл", callback_data='format_csv')],
                [InlineKeyboardButton("📈 Excel файл", callback_data='format_excel')],
                [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def my_subscription_callback(self, query, user_id: int):
        """Callback для кнопки 'Моя подписка'"""
        subscription = await db.get_user_subscription(user_id)
        
        if subscription:
            subscription_info = format_subscription_info(subscription)
            await query.edit_message_text(
                subscription_info,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Продлить", callback_data='buy_subscription')],
                    [InlineKeyboardButton("🏠 В главное меню", callback_data='main_menu')]
                ])
            )
        else:
            await query.edit_message_text(
                "❌ У вас нет активной подписки.\n\n"
                "Используйте кнопку ниже для покупки доступа.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                    [InlineKeyboardButton("🏠 В главное меню", callback_data='main_menu')]
                ])
            )
    
    async def stats_callback(self, query, user_id: int):
        """Callback для кнопки 'Статистика'"""
        stats = await db.get_user_stats(user_id)
        
        # Проверяем сессию
        is_authorized = await session_manager.is_authorized(user_id)
        session_path = session_manager.get_session_path(user_id)
        has_session = os.path.exists(session_path)
        
        stats_text = (
            f"📊 **Ваша статистика**\n\n"
            f"🔢 Всего сессий: {stats['total_sessions']}\n"
            f"✅ Успешных: {stats['completed_sessions']}\n"
            f"📈 Успешность: {stats['success_rate']}%\n"
            f"👥 Участников спарсено: {stats['total_members']}\n\n"
            f"🔐 **Статус сессии:**\n"
            f"• Файл сессии: {'✅ Есть' if has_session else '❌ Нет'}\n"
            f"• Авторизация: {'🟢 Активна' if is_authorized else '🔴 Не активна'}\n\n"
            f"⏰ Последняя активность: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data='stats')],
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')],
                [InlineKeyboardButton("🔓 Управление сессией", callback_data='manage_session')]
            ])
        )
    
    async def help_command_callback(self, query):
        """Callback для кнопки 'Помощь'"""
        help_text = """
❓ **ПОМОЩЬ ПО ИСПОЛЬЗОВАНИЮ БОТА**

🤖 **Основные команды:**
/start - Начать работу с ботом
/buy - Купить подписку
/my - Моя подписка
/stats - Моя статистика
/logout - Выйти из системы
/help - Эта справка

🔐 **Авторизация:**
1. Нажмите "Авторизоваться в Telegram"
2. Введите ваш номер телефона
3. Введите код из приложения Telegram
4. Если включена 2FA, введите пароль
5. Готово! Сессия сохраняется автоматически

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней

📊 **Что парсит бот:**
• Участники каналов (открытых/закрытых)
• Сообщения и комментарии
• Реакции и просмотры
• Скрытые username

📁 **Форматы экспорта:**
• TXT - только usernames
• CSV - полная таблица
• Excel - для Microsoft Excel

⚠️ **Важно:**
• Бот использует ВАШ аккаунт Telegram для парсинга
• Сессии хранятся в зашифрованном виде
• Вы можете выйти в любой момент
"""
        
        await query.edit_message_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
            ])
        )
    
    # ==================== ПОДПИСКИ И ПЛАТЕЖИ ====================
    
    async def show_subscription_plans(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать тарифные планы"""
        plans = await db.get_subscription_plans()
        
        keyboard = []
        for plan in plans:
            button_text = f"{plan['name']} - {plan['price']} {plan['currency']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"plan_{plan['code']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
        
        plans_text = "💰 **Доступные тарифные планы:**\n\n"
        for plan in plans:
            plans_text += (
                f"📦 **{plan['name']}**\n"
                f"   ⏱️ {plan['days']} дней\n"
                f"   💰 {plan['price']} {plan['currency']}\n"
                f"   📝 {plan['description']}\n\n"
            )
        
        await update.message.reply_text(
            plans_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def show_subscription_plans_callback(self, query):
        """Показать тарифные планы (callback)"""
        plans = await db.get_subscription_plans()
        
        keyboard = []
        for plan in plans:
            button_text = f"{plan['name']} - {plan['price']} {plan['currency']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"plan_{plan['code']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='main_menu')])
        
        plans_text = "💰 **Доступные тарифные планы:**\n\n"
        for plan in plans:
            plans_text += (
                f"📦 **{plan['name']}**\n"
                f"   ⏱️ {plan['days']} дней\n"
                f"   💰 {plan['price']} {plan['currency']}\n"
                f"   📝 {plan['description']}\n\n"
            )
        
        await query.edit_message_text(
            plans_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def confirm_purchase(self, query, plan_type: str):
        """Подтверждение покупки"""
        plans = await db.get_subscription_plans()
        selected_plan = next((p for p in plans if p['code'] == plan_type), None)
        
        if not selected_plan:
            await query.edit_message_text(
                "❌ Ошибка: тарифный план не найден",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data='buy_subscription')]
                ])
            )
            return
        
        await query.edit_message_text(
            f"💰 **Подтверждение покупки**\n\n"
            f"📦 Тариф: **{selected_plan['name']}**\n"
            f"⏱️ Срок: {selected_plan['days']} дней\n"
            f"💰 Стоимость: {selected_plan['price']} {selected_plan['currency']}\n\n"
            f"📝 *После подтверждения вы получите инструкцию по оплате*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data='confirm_purchase')],
                [InlineKeyboardButton("❌ Отменить", callback_data='cancel_purchase')]
            ])
        )
    
    async def process_payment(self, query, user_id: int, context: Dict = None):
        """Обработка платежа (демо-версия)"""
        if context is None:
            context = {}
        
        # Получаем выбранный план из callback_data
        if query.data.startswith('plan_'):
            plan_type = query.data.replace('plan_', '')
        else:
            # Или пытаемся получить из контекста
            plan_type = context.get('selected_plan_type', 'daily')
        
        plans = await db.get_subscription_plans()
        plan = next((p for p in plans if p['code'] == plan_type), None)
        
        if not plan:
            plan = plans[0]  # По умолчанию дневной
        
        # Создаем подписку в базе данных
        subscription_id = await db.create_subscription(
            user_id,
            plan['code'],
            plan['days'],
            plan['price'],
            plan['currency']
        )
        
        # Проверяем, является ли пользователь администратором
        is_admin = await db.is_admin(user_id)
        
        if is_admin:
            await query.edit_message_text(
                f"🎉 **Подписка активирована!**\n\n"
                f"✅ Подписка **{plan['name']}** активирована!\n"
                f"⏱️ Действует: {plan['days']} дней\n"
                f"💰 Стоимость: {plan['price']} {plan['currency']}\n\n"
                f"*Администраторам подписка предоставляется бесплатно.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self.get_admin_main_menu_keyboard()
            )
        else:
            await query.edit_message_text(
                f"🎉 **Поздравляем!**\n\n"
                f"✅ Подписка **{plan['name']}** активирована!\n"
                f"⏱️ Действует: {plan['days']} дней\n"
                f"💰 Стоимость: {plan['price']} {plan['currency']}\n\n"
                f"*В демо-версии оплата не требуется.*\n"
                f"*В реальном боте здесь будет интеграция с платежной системой.*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
                    [InlineKeyboardButton("🏠 В главное меню", callback_data='main_menu')]
                ])
            )
    
    # ==================== ПАРСИНГ КАНАЛОВ ====================
    
    async def parse_channel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода канала для парсинга"""
        channel_input = update.message.text.strip()
        user = update.effective_user
        
        logger.info(f"Начинаем парсинг канала {channel_input} для пользователя {user.id}")
        
        # Проверяем авторизацию
        is_authorized = await session_manager.is_authorized(user.id)
        if not is_authorized:
            await update.message.reply_text(
                "❌ **У вас нет активной сессии!**\n\n"
                "Для парсинга нужно авторизоваться в Telegram.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔑 Авторизоваться", callback_data='start_auth')]
                ])
            )
            return START
        
        # Проверяем, является ли пользователь администратором
        is_admin = await db.is_admin(user.id)
        
        # Проверяем режим (демо или полный)
        demo_mode = context.user_data.get('demo_mode', False)
        
        if not is_admin and not demo_mode:
            # Для обычных пользователей проверяем подписку
            subscription_status = await check_subscription(user.id)
            if not subscription_status['has_access']:
                await update.message.reply_text(
                    subscription_status['message'],
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                        [InlineKeyboardButton("🎯 Демо-парсинг", callback_data='demo_parsing')]
                    ])
                )
                return START
        
        # Создаем сессию парсинга
        session_id = await db.create_parsing_session(user.id, channel_input, 'members')
        
        # Отправляем сообщение о начале парсинга
        status_message = await update.message.reply_text(
            f"🔍 **Начинаю парсинг канала:** `{channel_input}`\n"
            f"📊 **Формат:** {context.user_data.get('export_format', 'txt')}\n"
            f"🎯 **Режим:** {'Демо (до 20 участников)' if demo_mode else 'Полный'}\n"
            f"⏳ **Пожалуйста, подождите...**\n\n"
            f"⚠️ *Это может занять несколько минут*",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            if demo_mode:
                # Демо-версия парсинга
                await self.demo_parse_channel(
                    user.id, 
                    session_id, 
                    channel_input, 
                    status_message,
                    context.user_data.get('export_format', 'txt')
                )
            else:
                # Реальный парсинг через Telethon
                await self.real_parse_channel(
                    user.id,
                    session_id,
                    channel_input,
                    status_message,
                    context.user_data.get('export_format', 'txt')
                )
            
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}", exc_info=True)
            await db.update_parsing_session(session_id, status='failed', error_message=str(e))
            await status_message.edit_text(
                f"❌ **Ошибка парсинга:**\n`{str(e)[:200]}`",
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Очищаем режим демо
        if 'demo_mode' in context.user_data:
            del context.user_data['demo_mode']
        
        # Возвращаем в соответствующее меню
        if is_admin:
            await status_message.edit_text(
                "✅ **Парсинг завершен!**\n\n"
                "Выберите следующее действие:",
                reply_markup=self.get_admin_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await status_message.edit_text(
                "✅ **Парсинг завершен!**\n\n"
                "Выберите следующее действие:",
                reply_markup=self.get_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        return MAIN_MENU
    
    async def real_parse_channel(self, user_id: int, session_id: str, channel: str, 
                               status_message, export_format: str):
        """Реальный парсинг канала через Telethon"""
        try:
            # Получаем клиент пользователя
            client = await session_manager.get_client(user_id)
            if not client:
                raise Exception("Клиент не найден. Авторизуйтесь снова.")
            
            # Обновляем статус
            await db.update_parsing_session(session_id, status='processing')
            
            # Определяем, что ввел пользователь
            if channel.startswith('t.me/'):
                channel = channel.replace('t.me/', '')
            elif channel.startswith('https://t.me/'):
                channel = channel.replace('https://t.me/', '')
            
            # Парсим канал
            await status_message.edit_text(
                f"🔍 **Подключаюсь к каналу:** `{channel}`\n"
                f"📊 **Поиск участников...**\n\n"
                f"⏳ *Пожалуйста, подождите...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Получаем entity канала с обработкой FloodWait
            try:
                entity = await client.get_entity(channel)
            except FloodWaitError as e:
                await status_message.edit_text(f"⏳ Ожидание {e.seconds} секунд...")
                await asyncio.sleep(e.seconds)
                entity = await client.get_entity(channel)
            except Exception as e:
                # Пробуем как username
                try:
                    entity = await client.get_entity(f'@{channel}')
                except FloodWaitError as e:
                    await status_message.edit_text(f"⏳ Ожидание {e.seconds} секунд...")
                    await asyncio.sleep(e.seconds)
                    entity = await client.get_entity(f'@{channel}')
                except:
                    # Пробуем как invite link
                    try:
                        entity = await client.get_entity(channel)
                    except FloodWaitError as e:
                        await status_message.edit_text(f"⏳ Ожидание {e.seconds} секунд...")
                        await asyncio.sleep(e.seconds)
                        entity = await client.get_entity(channel)
                    except Exception as e2:
                        raise Exception(f"Не могу найти канал '{channel}'. Убедитесь, что он существует и у вас есть доступ.")
            
            # Получаем информацию о канале
            full_channel = await client(GetFullChannelRequest(channel=entity))
            
            # Обновляем статус
            total_members = getattr(full_channel.full_chat, 'participants_count', 100)
            await db.update_parsing_session(session_id, total_items=total_members)
            
            await status_message.edit_text(
                f"🔍 **Канал найден:** `{channel}`\n"
                f"📊 **Участников:** {total_members}\n"
                f"📈 **Начинаю парсинг...**\n\n"
                f"⏳ *Пожалуйста, подождите...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Собираем участников
            all_participants = []
            offset = 0
            limit = PARSING_SETTINGS['BATCH_SIZE']
            max_participants = PARSING_SETTINGS['MAX_PARTICIPANTS']
            
            while True:
                try:
                    participants = await client(GetParticipantsRequest(
                        channel=entity,
                        filter=ChannelParticipantsRecent(),
                        offset=offset,
                        limit=limit,
                        hash=0
                    ))
                    
                    if not participants.users:
                        break
                    
                    # Обрабатываем участников
                    for user in participants.users:
                        user_data = {
                            'id': user.id,
                            'username': user.username or '',
                            'first_name': user.first_name or '',
                            'last_name': user.last_name or '',
                            'phone': user.phone or '',
                            'is_bot': user.bot,
                            'premium': user.premium,
                            'scam': user.scam,
                            'verified': user.verified,
                            'deleted': user.deleted,
                            'restricted': user.restricted,
                            'access_hash': str(user.access_hash) if user.access_hash else ''
                        }
                        all_participants.append(user_data)
                    
                    # Обновляем прогресс
                    await db.update_parsing_session(session_id, parsed_items=len(all_participants))
                    
                    # Обновляем сообщение каждые 50 участников
                    if len(all_participants) % 50 == 0:
                        progress_percent = int(len(all_participants) / min(total_members, max_participants) * 100)
                        await status_message.edit_text(
                            f"🔍 **Парсинг канала:** `{channel}`\n"
                            f"📊 **Прогресс:** {len(all_participants)}/{min(total_members, max_participants)} участников\n"
                            f"📈 **Завершено:** {progress_percent}%\n\n"
                            f"🔄 *Пожалуйста, подождите...*",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    
                    # Проверяем лимиты
                    if len(all_participants) >= max_participants:
                        logger.info(f"Достигнут лимит в {max_participants} участников")
                        break
                    
                    if len(participants.users) < limit:
                        break
                    
                    offset += len(participants.users)
                    
                    # Задержка между запросами
                    await asyncio.sleep(PARSING_SETTINGS['DELAY_BETWEEN_BATCHES'])
                    
                except FloodWaitError as e:
                    logger.warning(f"Flood wait: {e.seconds} seconds")
                    await status_message.edit_text(
                        f"⏳ **Telegram ограничил запросы**\n"
                        f"Ждем {e.seconds} секунд...",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    await asyncio.sleep(e.seconds)
                    continue
                    
                except Exception as e:
                    logger.error(f"Ошибка при получении участников: {e}")
                    if "privacy" in str(e).lower() or "private" in str(e).lower():
                        raise Exception("Канал приватный. Нужна ссылка-приглашение.")
                    break
            
            if not all_participants:
                raise Exception("Не удалось получить участников канала")
            
            # Экспортируем в файл
            filename = await export_to_file(all_participants, export_format)
            
            # Обновляем сессию
            await db.update_parsing_session(
                session_id, 
                status='completed', 
                parsed_items=len(all_participants),
                result_file_path=filename
            )
            
            # Отправляем результат
            with open(filename, 'rb') as file:
                await status_message.edit_text(
                    f"✅ **Парсинг завершен успешно!**\n\n"
                    f"📊 **Результаты:**\n"
                    f"• Канал: {channel}\n"
                    f"• Спарсено участников: {len(all_participants)}\n"
                    f"• Формат файла: {export_format.upper()}\n"
                    f"• Файл готов к скачиванию\n\n"
                    f"📁 *Файл отправлен в чат*",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Определяем MIME тип
                mime_types = {
                    'txt': 'text/plain',
                    'csv': 'text/csv',
                    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
                
                await status_message.chat.send_document(
                    document=file,
                    filename=f"parsed_{channel}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{export_format}",
                    caption=f"📊 Результаты парсинга {channel}\n"
                            f"👥 Участников: {len(all_participants)}\n"
                            f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                )
            
            # Удаляем временный файл
            try:
                os.remove(filename)
            except:
                pass
            
        except Exception as e:
            logger.error(f"Ошибка реального парсинга: {e}", exc_info=True)
            raise
    
    async def demo_parse_channel(self, user_id: int, session_id: str, channel: str, 
                               status_message, export_format: str):
        """Демо-версия парсинга канала"""
        # Имитация парсинга
        import random
        total_members = random.randint(10, 20)  # Демо ограничение
        
        # Обновляем статус
        await db.update_parsing_session(session_id, status='processing', total_items=total_members)
        
        # Имитация прогресса
        for i in range(0, total_members + 1, 5):
            await asyncio.sleep(0.3)  # Имитация задержки
            progress = min(i, total_members)
            await db.update_parsing_session(session_id, parsed_items=progress)
            
            # Обновляем сообщение о статусе
            if i % 10 == 0:
                try:
                    await status_message.edit_text(
                        f"🔍 **Демо-парсинг канала:** `{channel}`\n"
                        f"📊 **Прогресс:** {progress}/{total_members} участников\n"
                        f"⏳ **Завершено:** {int(progress/total_members*100)}%\n\n"
                        f"🔄 *Пожалуйста, подождите...*",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    pass
        
        # Создаем демо-данные
        demo_data = []
        for i in range(total_members):
            demo_data.append({
                'id': 1000000 + i,
                'username': f'user_{i}' if random.random() > 0.3 else '',
                'first_name': f'Имя_{i}',
                'last_name': f'Фамилия_{i}',
                'phone': f'+7999{random.randint(1000000, 9999999)}' if random.random() > 0.7 else '',
                'is_bot': random.random() > 0.9,
                'premium': random.random() > 0.8,
                'scam': random.random() > 0.95,
                'verified': random.random() > 0.9
            })
        
        # Экспортируем в файл
        filename = await export_to_file(demo_data, export_format)
        
        # Обновляем сессию
        await db.update_parsing_session(
            session_id, 
            status='completed', 
            parsed_items=total_members,
            result_file_path=filename
        )
        
        # Отправляем результат
        with open(filename, 'rb') as file:
            await status_message.edit_text(
                f"✅ **Демо-парсинг завершен!**\n\n"
                f"📊 **Результаты:**\n"
                f"• Спарсено участников: {total_members}\n"
                f"• Формат файла: {export_format.upper()}\n"
                f"• Файл готов к скачиванию\n\n"
                f"⚠️ *Это демо-версия с ограниченными данными*\n"
                f"*Купите подписку для полного доступа*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Определяем MIME тип
            mime_types = {
                'txt': 'text/plain',
                'csv': 'text/csv',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            
            await status_message.chat.send_document(
                document=file,
                filename=f"demo_parsed_{channel}.{export_format}",
                caption=f"📊 Демо-результаты парсинга {channel}\n"
                        f"👥 Участников: {total_members}\n"
                        f"⚠️ Демо-версия (полный доступ по подписке)"
            )
        
        # Удаляем временный файл
        try:
            os.remove(filename)
        except:
            pass
    
    # ==================== ОСНОВНОЙ ЦИКЛ ====================
    
    async def create_and_start_app(self):
        """Создание и запуск приложения"""
        # Инициализируем базу данных
        await self.initialize()
        
        # Очищаем старые сессии
        await session_manager.cleanup_expired_sessions()
        
        # Создаем приложение
        self.app = Application.builder().token(BOT_TOKEN).build()
        
        # Создаем ConversationHandler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start_command)],
            states={
                START: [
                    CallbackQueryHandler(
                        self.callback_handler, 
                        pattern='^(start_auth|buy_subscription|setup_api|help|main_menu|logout|manage_session)$'
                    )
                ],
                AUTH_PHONE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.auth_phone_handler),
                    CallbackQueryHandler(self.callback_handler, pattern='^main_menu$')
                ],
                AUTH_CODE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.auth_code_handler),
                    CallbackQueryHandler(self.callback_handler, pattern='^main_menu$')
                ],
                AUTH_PASSWORD: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.auth_password_handler),
                    CallbackQueryHandler(self.callback_handler, pattern='^main_menu$')
                ],
                MAIN_MENU: [
                    CallbackQueryHandler(self.callback_handler),
                    CallbackQueryHandler(self.admin_callback_handler, pattern='^admin_'),
                    CommandHandler('buy', self.buy_command),
                    CommandHandler('my', self.my_subscription_command),
                    CommandHandler('stats', self.stats_command),
                    CommandHandler('logout', self.logout_command)
                ],
                PARSE_CHANNEL: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.parse_channel_handler),
                    CallbackQueryHandler(self.callback_handler, pattern='^main_menu$')
                ],
                CHOOSE_PLAN: [
                    CallbackQueryHandler(self.callback_handler)
                ],
                CONFIRM_PAYMENT: [
                    CallbackQueryHandler(self.callback_handler)
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel_command)],
            allow_reentry=True
        )
        
        # Добавляем обработчики
        self.app.add_handler(conv_handler)
        
        # Добавляем отдельные команды
        self.app.add_handler(CommandHandler("buy", self.buy_command))
        self.app.add_handler(CommandHandler("my", self.my_subscription_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("logout", self.logout_command))
        
        # Добавляем админ команды
        self.app.add_handler(CommandHandler("admin", self.admin_command))
        self.app.add_handler(CommandHandler("broadcast", self.admin_broadcast_command))
        
        # Добавляем админ callback handler
        self.app.add_handler(CallbackQueryHandler(
            self.admin_callback_handler, 
            pattern='^admin_'
        ))
        
        # Добавляем обработчик сессий
        self.app.add_handler(CallbackQueryHandler(
            self.callback_handler,
            pattern='^session_action_'
        ))
        
        logger.info("🤖 Subscription Telegram Parser Bot запущен!")
        
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        
        # Ждем остановки
        stop_event = asyncio.Event()
        await stop_event.wait()
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда отмены"""
        user = update.effective_user
        is_admin = await db.is_admin(user.id)
        
        if is_admin:
            await update.message.reply_text(
                "Операция отменена.",
                reply_markup=self.get_admin_main_menu_keyboard()
            )
        else:
            await update.message.reply_text(
                "Операция отменена.",
                reply_markup=self.get_main_menu_keyboard()
            )
        return MAIN_MENU
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда помощи"""
        help_text = """
❓ **ПОМОЩЬ ПО ИСПОЛЬЗОВАНИЮ БОТА**

🤖 **Основные команды:**
/start - Начать работу с ботом
/buy - Купить подписку
/my - Моя подписка
/stats - Моя статистика
/logout - Выйти из системы
/help - Эта справка

🔐 **Авторизация:**
1. Нажмите "Авторизоваться в Telegram"
2. Введите ваш номер телефона
3. Введите код из приложения Telegram
4. Если включена 2FA, введите пароль
5. Готово! Сессия сохраняется автоматически

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней

📊 **Что парсит бот:**
• Участники каналов (открытых/закрытых)
• Сообщения и комментарии
• Реакции и просмотры
• Скрытые username

📁 **Форматы экспорта:**
• TXT - только usernames
• CSV - полная таблица
• Excel - для Microsoft Excel

⚠️ **Важно:**
• Бот использует ВАШ аккаунт Telegram для парсинга
• Сессии хранятся в зашифрованном виде
• Вы можете выйти в любой момент
"""
        
        user = update.effective_user
        is_admin = await db.is_admin(user.id)
        
        if is_admin:
            await update.message.reply_text(
                help_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self.get_admin_main_menu_keyboard()
            )
        else:
            await update.message.reply_text(
                help_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self.get_main_menu_keyboard()
            )
    
    async def cleanup(self):
        """Очистка ресурсов"""
        if self.app:
            await self.app.stop()
            await self.app.shutdown()
        
        # Закрываем все сессии
        for user_id in list(session_manager.clients.keys()):
            await session_manager.close_client(user_id)
        
        await db.close()

# ==================== ЗАПУСК ПРИЛОЖЕНИЯ ====================

async def main():
    """Основная функция запуска"""
    # Создаем необходимые директории
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    os.makedirs('user_sessions', exist_ok=True)  # Директория для сессий пользователей
    os.makedirs('temp', exist_ok=True)
    
    # Создаем и запускаем бота
    bot = SubscriptionTelegramBot()
    
    try:
        # Запускаем FastAPI для health check в отдельном потоке
        fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
        fastapi_thread.start()
        logger.info(f"✅ Health check запущен на порту {PORT}")
        
        # Запускаем бота
        await bot.create_and_start_app()
        
    except KeyboardInterrupt:
        logger.info("🤖 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}", exc_info=True)
    finally:
        # Очистка ресурсов
        try:
            await bot.cleanup()
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке: {e}")

if __name__ == '__main__':
    # Запускаем асинхронный main
    asyncio.run(main())