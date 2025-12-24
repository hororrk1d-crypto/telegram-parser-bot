#!/usr/bin/env python3
"""
🤖 Telegram Parser Bot (Упрощенная версия для Render) v2.1
Парсер Telegram каналов с системой подписок (без Telethon)
"""

import os
import sys
import asyncio
import logging
import uuid
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
(START, MAIN_MENU, PARSE_CHANNEL, CHOOSE_PLAN, CONFIRM_PAYMENT) = range(5)

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
        "version": "2.1.1"
    }

@fastapi_app.post("/webhook")
async def webhook(request: Request):
    """Обработчик webhook от Telegram"""
    try:
        data = await request.json()
        if app_instance and app_instance.app:
            from telegram import Update
            update = Update.de_json(data, app_instance.app.bot)
            await app_instance.app.process_update(update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Ошибка webhook: {e}")
        return {"status": "error", "message": str(e)}

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

async def export_to_txt(data: List[Dict]) -> str:
    """Экспорт данных в TXT файл"""
    import aiofiles
    os.makedirs('temp', exist_ok=True)
    filename = f"temp/export_{uuid.uuid4().hex[:8]}.txt"
    
    lines = []
    for item in data:
        if 'username' in item and item['username']:
            lines.append(f"@{item['username']}")
        elif 'id' in item:
            lines.append(f"id_{item['id']}")
    
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
        """Команда /start - упрощенная версия"""
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
                'yearly': 'Годовая'
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
        
        if query.data == 'main_menu':
            is_admin = await db.is_admin(user_id)
            
            if is_admin:
                await query.edit_message_text(
                    "🏠 **Главное меню (администратор)**\n\nВыберите действие:",
                    reply_markup=self.get_admin_main_menu_keyboard(),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
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
            
            await self.start_parsing_menu(query)
            return PARSE_CHANNEL
            
        elif query.data == 'demo_parsing':
            await query.edit_message_text(
                "🎯 **Демо-парсинг (ограниченная версия)**\n\n"
                "Вы можете спарсить демо-данные (20 участников).\n\n"
                "Введите username канала (для демо можно любое название):",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data['demo_mode'] = True
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
            return MAIN_MENU
    
    async def start_parsing_menu(self, query):
        """Меню начала парсинга"""
        await query.edit_message_text(
            "🎯 **Парсинг Telegram канала**\n\n"
            "Введите username канала:\n"
            "• Без @ (например: `telegram`)\n"
            "• Или ссылку (например: `t.me/telegram`)",
            parse_mode=ParseMode.MARKDOWN
        )
        return PARSE_CHANNEL
    
    async def my_subscription_callback(self, query, user_id: int):
        """Callback для кнопки 'Моя подписка'"""
        subscription = await db.get_user_subscription(user_id)
        
        if subscription:
            expires_at = datetime.fromisoformat(subscription['expires_at'])
            days_left = (expires_at - datetime.now()).days
            
            plan_names = {
                'trial': 'Пробная', 'daily': 'Дневная', 
                'weekly': 'Недельная', 'monthly': 'Месячная', 
                'yearly': 'Годовая'
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
            f"📈 Успешность: {stats['success_rate']}%\n"
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

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней

📊 **Что парсит бот:**
• Демо-данные участников каналов
• Экспорт в TXT файл

⚠️ **Важно:**
• Бот работает через официальный Telegram Bot API
• Для демо-версии используется тестовая база данных
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
        plans = [
            {'name': 'Пробная', 'code': 'trial', 'days': 3, 'price': 0, 'currency': 'RUB', 'description': 'Бесплатно 3 дня'},
            {'name': 'Дневная', 'code': 'daily', 'days': 1, 'price': 50, 'currency': 'RUB', 'description': 'Доступ на 1 день'},
            {'name': 'Недельная', 'code': 'weekly', 'days': 7, 'price': 250, 'currency': 'RUB', 'description': 'Доступ на 7 дней'},
            {'name': 'Месячная', 'code': 'monthly', 'days': 30, 'price': 800, 'currency': 'RUB', 'description': 'Доступ на 30 дней'},
            {'name': 'Годовая', 'code': 'yearly', 'days': 365, 'price': 5000, 'currency': 'RUB', 'description': 'Доступ на 365 дней'},
        ]
        
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
        plans = [
            {'name': 'Пробная', 'code': 'trial', 'days': 3, 'price': 0, 'currency': 'RUB', 'description': 'Бесплатно 3 дня'},
            {'name': 'Дневная', 'code': 'daily', 'days': 1, 'price': 50, 'currency': 'RUB', 'description': 'Доступ на 1 день'},
            {'name': 'Недельная', 'code': 'weekly', 'days': 7, 'price': 250, 'currency': 'RUB', 'description': 'Доступ на 7 дней'},
            {'name': 'Месячная', 'code': 'monthly', 'days': 30, 'price': 800, 'currency': 'RUB', 'description': 'Доступ на 30 дней'},
            {'name': 'Годовая', 'code': 'yearly', 'days': 365, 'price': 5000, 'currency': 'RUB', 'description': 'Доступ на 365 дней'},
        ]
        
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
        plans = {
            'trial': {'name': 'Пробная', 'days': 3, 'price': 0, 'currency': 'RUB'},
            'daily': {'name': 'Дневная', 'days': 1, 'price': 50, 'currency': 'RUB'},
            'weekly': {'name': 'Недельная', 'days': 7, 'price': 250, 'currency': 'RUB'},
            'monthly': {'name': 'Месячная', 'days': 30, 'price': 800, 'currency': 'RUB'},
            'yearly': {'name': 'Годовая', 'days': 365, 'price': 5000, 'currency': 'RUB'},
        }
        
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
        
        # Получаем выбранный план из callback_data
        if query.data.startswith('plan_'):
            plan_type = query.data.replace('plan_', '')
        else:
            plan_type = 'daily'
        
        plans = {
            'trial': {'name': 'Пробная', 'days': 3, 'price': 0, 'currency': 'RUB'},
            'daily': {'name': 'Дневная', 'days': 1, 'price': 50, 'currency': 'RUB'},
            'weekly': {'name': 'Недельная', 'days': 7, 'price': 250, 'currency': 'RUB'},
            'monthly': {'name': 'Месячная', 'days': 30, 'price': 800, 'currency': 'RUB'},
            'yearly': {'name': 'Годовая', 'days': 365, 'price': 5000, 'currency': 'RUB'},
        }
        
        plan = plans.get(plan_type, plans['daily'])
        
        # Создаем подписку в базе данных
        await db.create_subscription(
            user_id,
            plan_type,
            plan['days'],
            plan['price'],
            plan['currency']
        )
        
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
                f"*В демо-версии оплата не требуется.*",
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
        
        is_admin = await db.is_admin(user.id)
        demo_mode = context.user_data.get('demo_mode', False)
        
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
        session_id = await db.create_parsing_session(user.id, channel_input, 'members')
        
        status_message = await update.message.reply_text(
            f"🔍 **Начинаю парсинг канала:** `{channel_input}`\n"
            f"🎯 **Режим:** {'Демо' if demo_mode else 'Полный'}\n"
            f"⏳ **Пожалуйста, подождите...**",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            import random
            total_members = random.randint(10, 20) if demo_mode else random.randint(50, 200)
            
            await status_message.edit_text(
                f"🔍 **Парсинг канала:** `{channel_input}`\n"
                f"📊 **Прогресс:** 0/{total_members} участников\n"
                f"⏳ **Завершено:** 0%\n\n"
                f"🔄 *Пожалуйста, подождите...*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Имитация прогресса
            for i in range(0, total_members + 1, 5):
                await asyncio.sleep(0.3)
                progress = min(i, total_members)
                await db.update_parsing_session(session_id, parsed_items=progress)
                
                if i % 10 == 0:
                    try:
                        await status_message.edit_text(
                            f"🔍 **Парсинг канала:** `{channel_input}`\n"
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
                })
            
            # Экспортируем в файл
            filename = await export_to_txt(demo_data)
            
            await db.update_parsing_session(
                session_id, 
                status='completed', 
                parsed_items=total_members,
                result_file_path=filename
            )
            
            with open(filename, 'rb') as file:
                if demo_mode:
                    await status_message.edit_text(
                        f"✅ **Демо-парсинг завершен!**\n\n"
                        f"📊 **Результаты:**\n"
                        f"• Канал: {channel_input}\n"
                        f"• Спарсено участников: {total_members}\n\n"
                        f"⚠️ *Это демо-версия*\n"
                        f"*Купите подписку для полного доступа*",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await status_message.edit_text(
                        f"✅ **Парсинг завершен успешно!**\n\n"
                        f"📊 **Результаты:**\n"
                        f"• Канал: {channel_input}\n"
                        f"• Спарсено участников: {total_members}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                
                await status_message.chat.send_document(
                    document=file,
                    filename=f"parsed_{channel_input}.txt",
                    caption=f"📊 Результаты парсинга {channel_input}\n👥 Участников: {total_members}"
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
        
        if 'demo_mode' in context.user_data:
            del context.user_data['demo_mode']
        
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
    
    # ==================== АДМИН КОМАНДЫ ====================
    
    async def admin_command_callback(self, query, user_id: int):
        """Callback для админ панели"""
        if not await db.is_admin(user_id):
            await query.edit_message_text(
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
            [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
        ]
        
        await query.edit_message_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
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
            [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
        ]
        
        await update.message.reply_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return MAIN_MENU
    
    # ==================== ОСНОВНОЙ ЦИКЛ ====================
    
    async def create_and_start_app(self):
        """Создание и запуск приложения БЕЗ конфликтов"""
        await self.initialize()
        
        self.app = Application.builder().token(BOT_TOKEN).build()
        
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
        
        await self.app.initialize()
        
        # ОЧЕНЬ ВАЖНО: Очищаем старые обновления и запускаем polling правильно
        try:
            # Удаляем вебхук если он был установлен
            await self.app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Вебхук удален, pending updates очищены")
        except Exception as e:
            logger.warning(f"Не удалось удалить вебхук: {e}")
        
        # Запускаем polling с drop_pending_updates
        await self.app.start()
        
        # Используем низкоуровневый запуск polling
        self.app.updater._running = True
        
        logger.info("✅ Бот запущен в режиме polling")
        
        # Главный цикл
        while self.app.updater._running:
            try:
                # Получаем обновления с правильным offset
                updates = await self.app.bot.get_updates(
                    offset=self.app.updater._update_queue.maxsize,
                    timeout=30,
                    allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=False
                )
                
                if updates:
                    for update in updates:
                        await self.app.update_queue.put(update)
            except Exception as e:
                logger.error(f"Ошибка получения обновлений: {e}")
                await asyncio.sleep(5)
        
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
/help - Эта справка

💰 **Система подписок:**
• Пробная подписка: 3 дня бесплатно
• Дневная: 50 RUB / 1 день
• Недельная: 250 RUB / 7 дней
• Месячная: 800 RUB / 30 дней
• Годовая: 5000 RUB / 365 дней

📊 **Что парсит бот:**
• Демо-данные участников каналов
• Экспорт в TXT файл
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
                self.app.updater._running = False
                await self.app.stop()
                await self.app.shutdown()
            except:
                pass
        await db.close()

# ==================== ЗАПУСК ПРИЛОЖЕНИЯ ====================

def run_fastapi_server():
    """Запуск FastAPI сервера"""
    config = uvicorn.Config(
        fastapi_app, 
        host="0.0.0.0", 
        port=PORT, 
        log_level="warning"
    )
    server = uvicorn.Server(config)
    
    import asyncio
    asyncio.run(server.serve())

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
                if 'bot.py' in cmdline:
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