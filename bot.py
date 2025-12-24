#!/usr/bin/env python3
"""
🤖 Telegram Parser Bot (Улучшенная версия)
Парсинг с разными методами + исправленная админ-панель
"""

import os
import sys
import asyncio
import logging
import uuid
import random
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

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

from fastapi import FastAPI, Request
import uvicorn

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

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не установлен!")
    sys.exit(1)

# Настройки для вебхука (для Render)
PORT = int(os.environ.get('PORT', '8080'))

# Состояния ConversationHandler
(START, MAIN_MENU, PARSE_CHANNEL, CHOOSE_PLAN, 
 CONFIRM_PAYMENT, ADMIN_PANEL) = range(6)

# Глобальные переменные
app_instance = None
fastapi_app = FastAPI()

# ==================== FASTAPI HEALTH CHECK ====================

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
        "version": "2.2.0"
    }

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

async def check_subscription(user_id: int) -> Dict:
    """Проверить подписку пользователя"""
    subscription = await db.get_user_subscription(user_id)
    
    if not subscription:
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

async def export_to_txt_enhanced(data: List[Dict], method: str = 'basic') -> str:
    """Улучшенный экспорт с разными форматами для разных методов"""
    import aiofiles
    os.makedirs('temp', exist_ok=True)
    filename = f"temp/export_{uuid.uuid4().hex[:8]}.txt"
    
    lines = []
    
    if method == 'comments':
        lines.append("=== ПАРСИНГ ПО КОММЕНТАРИЯМ ===")
        lines.append(f"Всего комментаторов: {len(data)}\n")
        for item in data:
            if item.get('username'):
                lines.append(f"@{item['username']} - {item.get('comments_count', 0)} коммент.")
            else:
                lines.append(f"id_{item.get('id', 'unknown')} - {item.get('comments_count', 0)} коммент.")
    
    elif method == 'reactions':
        lines.append("=== ПАРСИНГ ПО РЕАКЦИЯМ ===")
        lines.append(f"Всего реакций: {len(data)}\n")
        for item in data:
            if item.get('username'):
                lines.append(f"@{item['username']} - {item.get('reaction', '👍')} x{item.get('reactions_count', 1)}")
            else:
                lines.append(f"id_{item.get('id', 'unknown')} - {item.get('reaction', '👍')} x{item.get('reactions_count', 1)}")
    
    elif method == 'hidden':
        lines.append("=== ПАРСИНГ СКРЫТЫХ USERNAME ===")
        lines.append("(только пользователи без username)\n")
        for item in data:
            if not item.get('username'):  # Только те, у кого нет username
                lines.append(f"id_{item.get('id', 'unknown')}")
    
    else:  # базовый метод
        lines.append("=== БАЗОВЫЙ ПАРСИНГ ===")
        lines.append(f"Всего участников: {len(data)}\n")
        for item in data:
            if item.get('username'):
                lines.append(f"@{item['username']}")
            else:
                lines.append(f"id_{item.get('id', 'unknown')}")
    
    async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
        await f.write('\n'.join(lines))
    
    return filename

# ==================== ТЕЛЕГРАМ БОТ ====================

class SubscriptionTelegramBot:
    def __init__(self):
        self.app = None
        global app_instance
        app_instance = self
    
    async def initialize(self):
        """Инициализация бота и базы данных"""
        await db.connect()
        logger.info("✅ База данных подключена")
    
    # ==================== КОМАНДЫ ====================
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user = update.effective_user
        
        await db.get_or_create_user(
            user.id,
            user.username,
            user.first_name,
            user.last_name
        )
        
        is_admin = await db.is_admin(user.id)
        
        if is_admin:
            await update.message.reply_text(
                f"👑 **Добро пожаловать, администратор {user.first_name}!**\n\n"
                f"Выберите действие:",
                reply_markup=self.get_admin_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            subscription_status = await check_subscription(user.id)
            
            if subscription_status['has_access']:
                await update.message.reply_text(
                    f"👋 Добро пожаловать, {user.first_name}!\n\n"
                    f"{subscription_status['message']}\n\n"
                    f"Что вы хотите сделать?",
                    reply_markup=self.get_main_menu_keyboard(),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"👋 Привет, {user.first_name}!\n\n"
                    f"🤖 **Я парсер Telegram каналов!**\n\n"
                    f"{subscription_status['message']}\n\n"
                    f"Выберите действие:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                        [InlineKeyboardButton("🚀 Демо-парсинг", callback_data='demo_parsing')],
                        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
        
        return MAIN_MENU
    
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
            expires_at = datetime.fromisoformat(subscription['expires_at'])
            days_left = (expires_at - datetime.now()).days
            
            plan_names = {
                'trial': 'Пробная', 'daily': 'Дневная', 
                'weekly': 'Недельная', 'monthly': 'Месячная', 
                'yearly': 'Годовая', 'lifetime': 'Пожизненная'
            }
            
            plan_name = plan_names.get(subscription['plan_type'], subscription['plan_type'])
            subscription_info = (
                f"📅 **{plan_name} подписка**\n"
                f"📆 Истекает: {expires_at.strftime('%d.%m.%Y %H:%M')}\n"
                f"⏳ Осталось: {days_left} дней\n"
                f"💰 Стоимость: {subscription['price']} {subscription['currency']}"
            )
            
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
            f"📈 Успешность: {stats['success_rate']:.1f}%\n"
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
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /admin - админ панель"""
        user = update.effective_user
        
        if not await db.is_admin(user.id):
            await update.message.reply_text(
                "❌ У вас нет прав доступа к админ панели.",
                reply_markup=self.get_main_menu_keyboard()
            )
            return
        
        total_users = await db.get_user_count()
        active_subs = await db.get_active_subscriptions_count()
        
        admin_text = (
            "🔧 **Административная панель**\n\n"
            f"👥 Всего пользователей: **{total_users}**\n"
            f"✅ Активных подписок: **{active_subs}**\n\n"
            "Выберите раздел:"
        )
        
        keyboard = [
            [InlineKeyboardButton("👥 Пользователи", callback_data='admin_users')],
            [InlineKeyboardButton("💰 Подписки", callback_data='admin_subscriptions')],
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
        ]
        
        await update.message.reply_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ADMIN_PANEL
    
    # ==================== МЕНЮ ====================
    
    def get_main_menu_keyboard(self):
        """Клавиатура главного меню"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
            [InlineKeyboardButton("💰 Моя подписка", callback_data='my_subscription')],
            [InlineKeyboardButton("📊 Статистика", callback_data='stats')],
            [InlineKeyboardButton("❓ Помощь", callback_data='help')]
        ])

    def get_admin_main_menu_keyboard(self):
        """Главное меню для администратора"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
            [InlineKeyboardButton("💰 Моя подписка", callback_data='my_subscription')],
            [InlineKeyboardButton("📊 Статистика", callback_data='stats')],
            [InlineKeyboardButton("🔧 Админ панель", callback_data='admin_panel')],
            [InlineKeyboardButton("❓ Помощь", callback_data='help')]
        ])

    async def show_main_menu(self, query):
        """Показать главное меню"""
        is_admin = await db.is_admin(query.from_user.id)
        
        if is_admin:
            await query.edit_message_text(
                "🏠 **Главное меню (администратор)**\n\nВыберите действие:",
                reply_markup=self.get_admin_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(
                "🏠 **Главное меню**\n\nВыберите действие:",
                reply_markup=self.get_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
    
    # ==================== ОСНОВНЫЕ КОЛБЭКИ ====================
    
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback кнопок"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        # Обработка админских кнопок
        if query.data.startswith('admin_'):
            if not await db.is_admin(user_id):
                await query.edit_message_text(
                    "❌ У вас нет прав доступа.",
                    reply_markup=self.get_main_menu_keyboard()
                )
                return MAIN_MENU
        
        if query.data == 'main_menu':
            await self.show_main_menu(query)
            return MAIN_MENU
            
        elif query.data == 'buy_subscription':
            await self.show_subscription_plans_callback(query)
            return CHOOSE_PLAN
            
        elif query.data == 'start_parsing':
            is_admin = await db.is_admin(user_id)
            if not is_admin:
                subscription_status = await check_subscription(user_id)
                if not subscription_status['has_access']:
                    await query.edit_message_text(
                        subscription_status['message'] + "\n\nХотите попробовать демо-парсинг?",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🎯 Демо-парсинг", callback_data='demo_parsing')],
                            [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
                        ])
                    )
                    return MAIN_MENU
            
            await self.parsing_methods_menu(query)
            return PARSE_CHANNEL
            
        elif query.data == 'demo_parsing':
            await query.edit_message_text(
                "🎯 **Демо-парсинг (ограниченная версия)**\n\n"
                "Вы можете спарсить демо-данные (20 участников).\n\n"
                "Введите username канала (для демо можно любое название):",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['demo_mode'] = True
            context.user_data['parse_method'] = 'demo'
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
        
        elif query.data == 'refresh_stats':
            await self.stats_callback(query, user_id)
            return MAIN_MENU
        
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
            
        elif query.data == 'admin_panel':
            await self.admin_command_callback(query, user_id)
            return ADMIN_PANEL
        
        elif query.data == 'admin_users':
            await self.admin_users_callback(query)
            return ADMIN_PANEL
        
        elif query.data == 'admin_subscriptions':
            await self.admin_subscriptions_callback(query)
            return ADMIN_PANEL
        
        elif query.data == 'admin_stats':
            await self.admin_stats_callback(query)
            return ADMIN_PANEL
        
        elif query.data == 'admin_back':
            await self.admin_command_callback(query, user_id)
            return ADMIN_PANEL
        
        elif query.data.startswith('parse_'):
            method = query.data.replace('parse_', '')
            context.user_data['parse_method'] = method
            
            method_names = {
                'basic': '🔍 базовый парсинг',
                'hidden': '👻 скрытые username',
                'private': '🔒 приватные каналы',
                'comments': '💬 по комментариям',
                'reactions': '👍 по реакциям',
                'demo': '🎯 демо-парсинг'
            }
            
            method_name = method_names.get(method, 'Парсинг')
            
            if method == 'private':
                await query.edit_message_text(
                    f"🎯 **{method_name}**\n\n"
                    "⚠️ **Для парсинга приватных каналов:**\n"
                    "1. Добавьте бота в канал как администратора\n"
                    "2. Убедитесь, что бот имеет права на просмотр участников\n"
                    "3. Введите @username канала:\n\n"
                    "Пример: `privatechannel` или `t.me/privatechannel`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await query.edit_message_text(
                    f"🎯 **{method_name}**\n\n"
                    "Введите @username канала:\n"
                    "• Без @ (например: `telegram`)\n"
                    "• Или ссылку (например: `t.me/telegram`)",
                    parse_mode=ParseMode.MARKDOWN
                )
            return PARSE_CHANNEL
    
    async def parsing_methods_menu(self, query):
        """Меню выбора метода парсинга"""
        await query.edit_message_text(
            "🎯 **Выберите метод парсинга:**\n\n"
            "1. 🔍 **Базовый парсинг** - обычные участники\n"
            "2. 👻 **Скрытые username** - пользователи без @username\n"
            "3. 🔒 **Приватные каналы** (требуются права админа в канале)\n"
            "4. 💬 **По комментариям** - парсинг комментаторов\n"
            "5. 👍 **По реакциям** - пользователи, ставившие реакции",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Базовый парсинг", callback_data='parse_basic')],
                [InlineKeyboardButton("👻 Скрытые username", callback_data='parse_hidden')],
                [InlineKeyboardButton("🔒 Приватные каналы", callback_data='parse_private')],
                [InlineKeyboardButton("💬 По комментариям", callback_data='parse_comments')],
                [InlineKeyboardButton("👍 По реакциям", callback_data='parse_reactions')],
                [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
            ])
        )
    
    async def my_subscription_callback(self, query, user_id: int):
        """Callback для кнопки 'Моя подписка'"""
        subscription = await db.get_user_subscription(user_id)
        
        if subscription:
            expires_at = datetime.fromisoformat(subscription['expires_at'])
            days_left = (expires_at - datetime.now()).days
            
            plan_names = {
                'trial': 'Пробная', 'daily': 'Дневная', 
                'weekly': 'Недельная', 'monthly': 'Месячная', 
                'yearly': 'Годовая', 'lifetime': 'Пожизненная'
            }
            
            plan_name = plan_names.get(subscription['plan_type'], subscription['plan_type'])
            subscription_info = (
                f"📅 **{plan_name} подписка**\n"
                f"📆 Истекает: {expires_at.strftime('%d.%m.%Y %H:%M')}\n"
                f"⏳ Осталось: {days_left} дней\n"
                f"💰 Стоимость: {subscription['price']} {subscription['currency']}"
            )
            
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
        
        stats_text = (
            f"📊 **Ваша статистика**\n\n"
            f"🔢 Всего сессий: {stats['total_sessions']}\n"
            f"✅ Успешных: {stats['completed_sessions']}\n"
            f"📈 Успешность: {stats['success_rate']:.1f}%\n"
            f"👥 Участников спарсено: {stats['total_members']}\n\n"
            f"⏰ Последняя активность: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data='stats')],
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
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
/help - Эта справка

🎯 **Методы парсинга:**
• 🔍 Базовый - обычные участники каналов
• 👻 Скрытые username - пользователи без @username
• 🔒 Приватные каналы - нужны права администратора
• 💬 По комментариям - парсинг комментаторов
• 👍 По реакциям - пользователи, ставившие реакции

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней

📊 **Что парсит бот:**
• Данные участников каналов
• Экспорт в TXT файл
• Разные форматы для разных методов
"""
        
        await query.edit_message_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
            ])
        )
    
    # ==================== АДМИН МЕТОДЫ ====================
    
    async def admin_command_callback(self, query, user_id: int):
        """Callback для админ панели"""
        total_users = await db.get_user_count()
        active_subs = await db.get_active_subscriptions_count()
        
        admin_text = (
            "🔧 **Административная панель**\n\n"
            f"👥 Всего пользователей: **{total_users}**\n"
            f"✅ Активных подписок: **{active_subs}**\n\n"
            "Выберите раздел:"
        )
        
        keyboard = [
            [InlineKeyboardButton("👥 Пользователи", callback_data='admin_users')],
            [InlineKeyboardButton("💰 Подписки", callback_data='admin_subscriptions')],
            [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
            [InlineKeyboardButton("🔙 Назад", callback_data='main_menu')]
        ]
        
        await query.edit_message_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
   async def admin_users_callback(self, query):
    """Список пользователей для админа"""
    users = await db.get_all_users_with_stats(limit=10)  # Используем новый метод
        
        if not users:
            await query.edit_message_text(
                "👥 **Нет пользователей в базе данных**",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад в админку", callback_data='admin_back')]
                ])
            )
            return
        
        users_text = "👥 **Последние 10 пользователей:**\n\n"
        for user in users:
            users_text += f"• ID: `{user['user_id']}`\n"
            users_text += f"  👤: {user['first_name']} {user['last_name']}\n"
            if user['username']:
                users_text += f"  @{user['username']}\n"
            users_text += f"  📅: {user['created_at'][:10]}\n"
            if user['is_admin']:
                users_text += f"  👑 Администратор\n"
            users_text += f"  📊 Сессий: {user.get('total_sessions', 0)}\n\n"
        
        await query.edit_message_text(
            users_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад в админку", callback_data='admin_back')],
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
            ])
        )
    
    async def admin_subscriptions_callback(self, query):
        """Список активных подписок"""
        # Здесь нужен метод get_active_subscriptions в database.py
        # Покажем общую статистику
        total_users = await db.get_user_count()
        active_subs = await db.get_active_subscriptions_count()
        
        subs_text = (
            "💰 **Статистика подписок:**\n\n"
            f"👥 Всего пользователей: **{total_users}**\n"
            f"✅ Активных подписок: **{active_subs}**\n"
            f"📊 Процент подписок: **{(active_subs/total_users*100 if total_users > 0 else 0):.1f}%**\n\n"
            f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await query.edit_message_text(
            subs_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад в админку", callback_data='admin_back')],
                [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
            ])
        )
    
    async def admin_stats_callback(self, query):
        """Общая статистика для админа"""
        total_users = await db.get_user_count()
        active_subs = await db.get_active_subscriptions_count()
        total_sessions = await db.get_total_parsings()
        
        stats_text = (
            "📊 **Общая статистика:**\n\n"
            f"👥 Всего пользователей: **{total_users}**\n"
            f"✅ Активных подписок: **{active_subs}**\n"
            f"🔢 Всего сессий парсинга: **{total_sessions}**\n\n"
            f"⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад в админку", callback_data='admin_back')],
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
        plans_data = await db.get_subscription_plans()
        plans = {p['code']: p for p in plans_data}
        
        selected_plan = plans.get(plan_type)
        
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
            f"📝 *В демо-версии оплата не требуется.*",
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
        
        # Получаем выбранный план из user_data или callback_data
        plan_type = context.get('selected_plan', 'trial')
        
        plans_data = await db.get_subscription_plans()
        plans = {p['code']: p for p in plans_data}
        plan = plans.get(plan_type, plans.get('trial'))
        
        # Создаем подписку в базе данных
        await db.create_subscription(
            user_id,
            plan_type,
            plan['days'],
            plan['price'],
            plan['currency']
        )
        
        is_admin = await db.is_admin(user_id)
        
        success_text = (
            f"🎉 **Подписка активирована!**\n\n"
            f"✅ Подписка **{plan['name']}** активирована!\n"
            f"⏱️ Действует: {plan['days']} дней\n"
            f"💰 Стоимость: {plan['price']} {plan['currency']}\n\n"
        )
        
        if is_admin:
            success_text += "*Администраторам подписка предоставляется бесплатно.*"
            keyboard = self.get_admin_main_menu_keyboard()
        else:
            success_text += "*В демо-версии оплата не требуется.*"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
                [InlineKeyboardButton("🏠 В главное меню", callback_data='main_menu')]
            ])
        
        await query.edit_message_text(
            success_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard
        )
    
    # ==================== ПАРСИНГ КАНАЛОВ ====================
    
    async def parse_channel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода канала для парсинга"""
        channel_input = update.message.text.strip()
        user = update.effective_user
        
        parse_method = context.user_data.get('parse_method', 'basic')
        demo_mode = context.user_data.get('demo_mode', False)
        
        method_names = {
            'basic': '🔍 базовый парсинг',
            'hidden': '👻 скрытые username',
            'private': '🔒 приватные каналы',
            'comments': '💬 по комментариям',
            'reactions': '👍 по реакциям',
            'demo': '🎯 демо-парсинг'
        }
        
        logger.info(f"Начинаем {parse_method} парсинг канала {channel_input} для пользователя {user.id}")
        
        is_admin = await db.is_admin(user.id)
        
        if not is_admin and not demo_mode:
            subscription_status = await check_subscription(user.id)
            if not subscription_status['has_access']:
                await update.message.reply_text(
                    subscription_status['message'],
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💰 Купить подписку", callback_data='buy_subscription')],
                        [InlineKeyboardButton("🎯 Демо-парсинг", callback_data='demo_parsing')]
                    ])
                )
                return MAIN_MENU
        
        # Создаем сессию парсинга
        session_id = await db.create_parsing_session(user.id, channel_input, parse_method)
        
        status_message = await update.message.reply_text(
            f"🔍 **Начинаю парсинг канала:** `{channel_input}`\n"
            f"🎯 **Метод:** {method_names.get(parse_method, 'Базовый')}\n"
            f"🎯 **Режим:** {'Демо' if demo_mode else 'Полный'}\n"
            f"⏳ **Пожалуйста, подождите...**",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Имитация разных методов парсинга
            if parse_method == 'hidden':
                # Для скрытых username больше ID, меньше usernames
                total_members = random.randint(30, 50) if demo_mode else random.randint(100, 300)
                username_ratio = 0.2  # только 20% имеют username
            elif parse_method == 'private':
                # Для приватных каналов меньше участников
                total_members = random.randint(5, 15) if demo_mode else random.randint(20, 50)
                username_ratio = 0.5
            elif parse_method in ['comments', 'reactions']:
                # Для комментариев и реакций еще меньше
                total_members = random.randint(10, 25) if demo_mode else random.randint(30, 80)
                username_ratio = 0.7
            else:
                # Базовый парсинг или демо
                total_members = random.randint(10, 20) if demo_mode else random.randint(50, 200)
                username_ratio = 0.7
            
            # Обновляем прогресс
            await status_message.edit_text(
                f"🔍 **Парсинг канала:** `{channel_input}`\n"
                f"🎯 **Метод:** {method_names.get(parse_method, 'Базовый')}\n"
                f"📊 **Прогресс:** 0/{total_members} участников\n"
                f"⏳ **Завершено:** 0%\n\n"
                f"🔄 *Пожалуйста, подождите...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Имитация прогресса
            for i in range(0, total_members + 1, 5):
                await asyncio.sleep(0.2)
                progress = min(i, total_members)
                await db.update_parsing_session(session_id, parsed_items=progress)
                
                if i % 15 == 0 or i == total_members:
                    try:
                        percent = int(progress/total_members*100)
                        await status_message.edit_text(
                            f"🔍 **Парсинг канала:** `{channel_input}`\n"
                            f"🎯 **Метод:** {method_names.get(parse_method, 'Базовый')}\n"
                            f"📊 **Прогресс:** {progress}/{total_members} участников\n"
                            f"⏳ **Завершено:** {percent}%\n\n"
                            f"🔄 *Пожалуйста, подождите...*",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except:
                        pass
            
            # Создаем демо-данные в зависимости от метода
            demo_data = []
            for i in range(total_members):
                has_username = random.random() < username_ratio
                
                user_data = {
                    'id': 1000000 + i,
                    'username': f'user_{i}' if has_username else '',
                    'first_name': f'Имя_{i}',
                    'last_name': f'Фамилия_{i}',
                }
                
                # Для метода "по комментариям" добавляем дополнительные данные
                if parse_method == 'comments':
                    user_data['comments_count'] = random.randint(1, 20)
                    user_data['last_comment'] = datetime.now().strftime("%d.%m.%Y %H:%M")
                
                # Для метода "по реакциям"
                elif parse_method == 'reactions':
                    reactions = ['👍', '❤️', '🔥', '🎉', '👀']
                    user_data['reaction'] = random.choice(reactions)
                    user_data['reactions_count'] = random.randint(1, 10)
                
                demo_data.append(user_data)
            
            # Экспортируем в файл
            filename = await export_to_txt_enhanced(demo_data, parse_method)
            
            await db.update_parsing_session(
                session_id, 
                status='completed', 
                parsed_items=total_members,
                result_file_path=filename
            )
            
            with open(filename, 'rb') as file:
                result_text = (
                    f"✅ **Парсинг завершен успешно!**\n\n"
                    f"📊 **Результаты:**\n"
                    f"• Канал: {channel_input}\n"
                    f"• Метод: {method_names.get(parse_method, 'Базовый')}\n"
                    f"• Спарсено участников: {total_members}"
                )
                
                if demo_mode:
                    result_text += f"\n\n⚠️ *Это демо-версия*\n*Купите подписку для полного доступа*"
                
                await status_message.edit_text(
                    result_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                
                caption = (
                    f"📊 Результаты парсинга {channel_input}\n"
                    f"🎯 Метод: {method_names.get(parse_method, 'Базовый')}\n"
                    f"👥 Участников: {total_members}"
                )
                
                await status_message.chat.send_document(
                    document=file,
                    filename=f"parsed_{channel_input}_{parse_method}.txt",
                    caption=caption
                )
            
            try:
                os.remove(filename)
            except:
                pass
            
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}", exc_info=True)
            await db.update_parsing_session(session_id, status='failed', error_message=str(e))
            await status_message.edit_text(
                f"❌ **Ошибка парсинга:**\n`{str(e)[:200]}`",
                parse_mode=ParseMode.MARKDOWN
            )
        
        # Очищаем данные контекста
        if 'demo_mode' in context.user_data:
            del context.user_data['demo_mode']
        if 'parse_method' in context.user_data:
            del context.user_data['parse_method']
        
        if is_admin:
            await status_message.edit_text(
                "✅ **Парсинг завершен!**\n\nВыберите следующее действие:",
                reply_markup=self.get_admin_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await status_message.edit_text(
                "✅ **Парсинг завершен!**\n\nВыберите следующее действие:",
                reply_markup=self.get_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
        return MAIN_MENU
    
    # ==================== ОСНОВНОЙ ЦИКЛ ====================
    
    async def create_and_start_app(self):
        """Создание и запуск приложения"""
        await self.initialize()
        
        self.app = Application.builder().token(BOT_TOKEN).build()
        
        # Настраиваем обработчики
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start_command)],
            states={
                START: [
                    CallbackQueryHandler(
                        self.callback_handler, 
                        pattern='^(buy_subscription|help|main_menu)$'
                    )
                ],
                MAIN_MENU: [
                    CallbackQueryHandler(self.callback_handler),
                    CommandHandler('buy', self.buy_command),
                    CommandHandler('my', self.my_subscription_command),
                    CommandHandler('stats', self.stats_command),
                    CommandHandler('admin', self.admin_command),
                    CommandHandler('help', self.help_command)
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
                ],
                ADMIN_PANEL: [
                    CallbackQueryHandler(self.callback_handler)
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel_command)],
            allow_reentry=True
        )
        
        self.app.add_handler(conv_handler)
        self.app.add_handler(CommandHandler("buy", self.buy_command))
        self.app.add_handler(CommandHandler("my", self.my_subscription_command))
        self.app.add_handler(CommandHandler("stats", self.stats_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("admin", self.admin_command))
        
        logger.info("🤖 Telegram Parser Bot инициализирован!")
        
        # Инициализируем приложение
        await self.app.initialize()
        
        # Удаляем вебхук если он был установлен
        try:
            await self.app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Вебхук удален, pending updates очищены")
        except Exception as e:
            logger.warning(f"Не удалось удалить вебхук: {e}")
        
        # Запускаем приложение
        await self.app.start()
        
        # Запускаем polling стандартным способом
        await self.app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        logger.info("✅ Бот запущен в режиме polling")
        
        # Ждем остановки
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("🤖 Бот остановлен по команде")
        except Exception as e:
            logger.error(f"Ошибка в главном цикле: {e}")
    
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
/help - Эта справка

🎯 **Методы парсинга:**
• 🔍 Базовый - обычные участники каналов
• 👻 Скрытые username - пользователи без @username
• 🔒 Приватные каналы - нужны права администратора
• 💬 По комментариям - парсинг комментаторов
• 👍 По реакциям - пользователи, ставившие реакции

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней
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
            try:
                if self.app.updater.running:
                    await self.app.updater.stop()
                
                await self.app.stop()
                await self.app.shutdown()
                logger.info("✅ Бот остановлен")
            except Exception as e:
                logger.error(f"Ошибка при остановке бота: {e}")
        
        await db.close()

# ==================== ЗАПУСК ПРИЛОЖЕНИЯ ====================

def run_fastapi_server():
    """Запуск FastAPI сервера"""
    try:
        config = uvicorn.Config(
            fastapi_app, 
            host="0.0.0.0", 
            port=PORT, 
            log_level="warning",
            access_log=False
        )
        server = uvicorn.Server(config)
        
        import asyncio
        asyncio.run(server.serve())
    except Exception as e:
        logger.error(f"Ошибка FastAPI: {e}")

async def main():
    """Основная функция запуска"""
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    os.makedirs('temp', exist_ok=True)
    
    bot = SubscriptionTelegramBot()
    
    try:
        # Запускаем FastAPI в отдельном потоке
        fastapi_thread = threading.Thread(target=run_fastapi_server, daemon=True)
        fastapi_thread.start()
        logger.info(f"✅ Health check запущен на порту {PORT}")
        
        # Даем время FastAPI запуститься
        await asyncio.sleep(2)
        
        # Запускаем бота
        await bot.create_and_start_app()
        
    except KeyboardInterrupt:
        logger.info("🤖 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}", exc_info=True)
    finally:
        try:
            await bot.cleanup()
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке: {e}")

if __name__ == '__main__':
    # Проверяем, не запущен ли уже бот
    import psutil
    current_pid = os.getpid()
    python_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if 'python' in proc.info['name'].lower() and proc.info['pid'] != current_pid:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'bot.py' in cmdline or 'python' in cmdline and 'bot.py' in cmdline:
                    python_processes.append(proc.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    if python_processes:
        logger.warning(f"Найден запущенный бот с PID: {python_processes}. Завершаем...")
        for pid in python_processes:
            try:
                p = psutil.Process(pid)
                p.terminate()
                p.wait(timeout=5)
            except:
                pass
    
    asyncio.run(main())