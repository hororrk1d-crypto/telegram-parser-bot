"""
Основной класс Telegram бота
"""

import asyncio
import logging
from typing import Dict, Any
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters, ConversationHandler
)
from telegram.constants import ParseMode

from config.settings import Config
from utils.database import db
from utils.cache import cache
from utils.helpers import (
    save_participants, cleanup_files, format_number,
    validate_channel_input, extract_channel_username
)

logger = logging.getLogger(__name__)

# Состояния ConversationHandler
SETUP_API, SETUP_HASH, SETUP_PHONE, MAIN_MENU, PARSE_CHANNEL, CHOOSE_METHOD = range(6)

class TelegramBot:
    def __init__(self, parser):
        self.parser = parser
        self.app = None
        self.user_methods = {}  # Выбранные методы по user_id
        self.user_sessions = {}  # Сессии пользователей в памяти
        
    def get_main_menu_keyboard(self):
        """Клавиатура главного меню"""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
            [InlineKeyboardButton("⚙️ Выбрать методы", callback_data='choose_methods')],
            [InlineKeyboardButton("⚙️ Мои настройки", callback_data='my_settings')],
            [InlineKeyboardButton("📊 Статистика", callback_data='stats')],
            [InlineKeyboardButton("❓ Помощь", callback_data='help_main')]
        ])
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработчик команды /start"""
        user = update.effective_user
        user_id = user.id
        
        logger.info(f"Пользователь {user_id} ({user.username}) запустил бота")
        
        # Сохраняем пользователя в БД
        if Config.ENABLE_DATABASE:
            db.save_user({
                'user_id': user_id,
                'username': user.username,
                'first_name': user.first_name,
                'last_name': user.last_name
            })
        
        # Проверяем сохраненные настройки
        saved_data = await self._load_user_settings(user_id)
        
        if saved_data and saved_data.get('api_id'):
            # Уже настроен
            await update.message.reply_text(
                f"👋 С возвращением, {user.first_name}!\n"
                f"✅ Ваши настройки загружены.\n\n"
                f"Что хотите сделать?",
                reply_markup=self.get_main_menu_keyboard(),
                parse_mode=ParseMode.MARKDOWN
            )
            return MAIN_MENU
        else:
            # Новый пользователь
            await update.message.reply_text(
                f"👋 Привет, {user.first_name}!\n\n"
                f"🤖 **Я улучшенный облачный парсер Telegram!**\n\n"
                f"✨ **Новые возможности:**\n"
                f"⚡ 4 метода сбора данных\n"
                f"🔓 Парсинг приватных каналов\n"
                f"📊 Детальная статистика\n"
                f"🎯 Выбор методов парсинга\n\n"
                f"Начнем настройку?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🚀 Начать настройку", callback_data='start_setup')],
                    [InlineKeyboardButton("❓ Как получить API ключи", callback_data='help_api')],
                    [InlineKeyboardButton("🎯 Начать парсинг", callback_data='start_parsing')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return SETUP_API
    
    async def _load_user_settings(self, user_id: int) -> Dict:
        """Загрузка настроек пользователя"""
        # Пробуем из кэша
        if cache.is_available():
            cached = cache.get_user_session(user_id)
            if cached:
                return cached
        
        # Пробуем из БД
        if Config.ENABLE_DATABASE:
            session = db.get_session()
            try:
                user = session.query(db.User).filter_by(user_id=user_id).first()
                if user and user.api_id:
                    settings = {
                        'api_id': user.api_id,
                        'api_hash': user.api_hash,
                        'phone': user.phone
                    }
                    # Сохраняем в кэш
                    cache.cache_user_session(user_id, settings)
                    return settings
            finally:
                session.close()
        
        return None
    
    async def start_setup(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Начало настройки API"""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "🔧 **Шаг 1 из 3: Настройка API**\n\n"
            "📝 **Введите ваш API ID:**\n"
            "(только цифры, например: `1234567`)\n\n"
            "Отправьте сообщением в этот чат.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return SETUP_API
    
    async def setup_api_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Получение API ID"""
        api_id = update.message.text.strip()
        
        if not api_id.isdigit():
            await update.message.reply_text(
                "❌ **API ID должен содержать только цифры!**\n"
                "Пожалуйста, введите снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return SETUP_API
        
        # Сохраняем в context
        if 'api_data' not in context.user_data:
            context.user_data['api_data'] = {}
        context.user_data['api_data']['api_id'] = api_id
        
        await update.message.reply_text(
            f"✅ **API ID сохранен:** `{api_id}`\n\n"
            f"📝 **Шаг 2 из 3: Введите API Hash**\n"
            f"(буквы и цифры, например: `a1b2c3d4e5f67890abc123def456`)",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return SETUP_HASH
    
    async def setup_api_hash(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Получение API Hash"""
        api_hash = update.message.text.strip()
        
        if len(api_hash) < 20:
            await update.message.reply_text(
                "❌ **API Hash слишком короткий!**\n"
                "Должен быть не менее 20 символов.\n"
                "Введите снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return SETUP_HASH
        
        # Сохраняем в context
        context.user_data['api_data']['api_hash'] = api_hash
        
        await update.message.reply_text(
            f"✅ **API Hash сохранен:** `{api_hash[:10]}...`\n\n"
            f"📱 **Шаг 3 из 3: Введите номер телефона**\n"
            f"(с кодом страны, например: `+79991234567`)",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return SETUP_PHONE
    
    async def setup_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Получение номера телефона"""
        phone = update.message.text.strip()
        
        if not phone.startswith('+'):
            await update.message.reply_text(
                "❌ **Номер должен начинаться с +!**\n"
                "Введите снова:",
                parse_mode=ParseMode.MARKDOWN
            )
            return SETUP_PHONE
        
        # Сохраняем в context
        context.user_data['api_data']['phone'] = phone
        user_id = update.effective_user.id
        
        # Сохраняем в БД
        if Config.ENABLE_DATABASE:
            db.save_user({
                'user_id': user_id,
                'api_id': context.user_data['api_data']['api_id'],
                'api_hash': context.user_data['api_data']['api_hash'],
                'phone': phone
            })
        
        # Сохраняем в кэш
        if cache.is_available():
            cache.cache_user_session(user_id, context.user_data['api_data'])
        
        # Сохраняем в сессию парсера
        self.parser.user_sessions[user_id] = context.user_data['api_data']
        
        # Показываем сводку
        api_id = context.user_data['api_data']['api_id']
        
        await update.message.reply_text(
            f"🎉 **Настройка завершена!**\n\n"
            f"📋 **Ваши данные:**\n"
            f"📱 Номер: `{phone}`\n"
            f"🆔 API ID: `{api_id}`\n"
            f"🔑 API Hash: `{context.user_data['api_data']['api_hash'][:10]}...`\n\n"
            f"✅ **Теперь можете начать парсинг!**",
            reply_markup=self.get_main_menu_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return MAIN_MENU
    
    async def choose_methods_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Меню выбора методов парсинга"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        selected_methods = self.user_methods.get(user_id, ['participants'])
        
        # Создаем кнопки с отметками выбранных методов
        buttons = []
        
        methods_info = {
            'participants': '👥 Основной метод (участники)',
            'messages': '📨 Из истории сообщений',
            'comments': '💬 Из комментариев',
            'reactions': '👍 Из реакций'
        }
        
        for method_id, method_name in methods_info.items():
            check = "✅" if method_id in selected_methods else "⬜"
            buttons.append([InlineKeyboardButton(
                f"{check} {method_name}", 
                callback_data=f'toggle_{method_id}'
            )])
        
        buttons.extend([
            [InlineKeyboardButton("💾 Сохранить выбор", callback_data='save_methods')],
            [InlineKeyboardButton("⚡ Быстрый набор", callback_data='preset_fast')],
            [InlineKeyboardButton("🔍 Полный набор", callback_data='preset_full')],
            [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
        ])
        
        await query.edit_message_text(
            "🎛️ **ВЫБОР МЕТОДОВ ПАРСИНГА**\n\n"
            "📊 **Доступные методы:**\n"
            "• 👥 Основной - участники канала\n"
            "• 📨 Сообщения - из истории чата\n"
            "• 💬 Комментарии - из обсуждений\n"
            "• 👍 Реакции - пользователи реакций\n\n"
            "⚠️ **Чем больше методов - тем больше данных, но дольше парсинг!**",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return CHOOSE_METHOD
    
    async def toggle_method(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Переключение метода"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        method_id = query.data.replace('toggle_', '')
        
        if user_id not in self.user_methods:
            self.user_methods[user_id] = ['participants']
        
        if method_id in self.user_methods[user_id]:
            self.user_methods[user_id].remove(method_id)
        else:
            self.user_methods[user_id].append(method_id)
        
        # Обновляем меню
        await self.choose_methods_menu(update, context)
        return CHOOSE_METHOD
    
    async def save_methods(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Сохранение выбранных методов"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        selected_methods = self.user_methods.get(user_id, ['participants'])
        
        methods_text = []
        for method in selected_methods:
            if method == 'participants':
                methods_text.append("👥 Основной метод")
            elif method == 'messages':
                methods_text.append("📨 Из сообщений")
            elif method == 'comments':
                methods_text.append("💬 Из комментариев")
            elif method == 'reactions':
                methods_text.append("👍 Из реакций")
        
        await query.edit_message_text(
            f"✅ **Методы сохранены!**\n\n"
            f"📋 **Выбранные методы:**\n" + "\n".join(methods_text) + "\n\n"
            f"Теперь можете начать парсинг!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
                [InlineKeyboardButton("🔙 В главное меню", callback_data='back_to_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return MAIN_MENU
    
    async def apply_preset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Применение пресета"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        preset = query.data.replace('preset_', '')
        
        if preset == 'fast':
            self.user_methods[user_id] = ['participants']
            await query.answer("⚡ Быстрый набор применен!")
        elif preset == 'full':
            self.user_methods[user_id] = ['participants', 'messages', 'comments', 'reactions']
            await query.answer("🔍 Полный набор применен!")
        
        await self.choose_methods_menu(update, context)
        return CHOOSE_METHOD
    
    async def start_parsing_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню начала парсинга"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        selected_methods = self.user_methods.get(user_id, ['participants'])
        methods_count = len(selected_methods)
        
        # Проверяем настройки пользователя
        api_data = await self._load_user_settings(user_id)
        if not api_data or not api_data.get('api_id'):
            await query.edit_message_text(
                "❌ **Сначала нужно настроить API ключи!**\n"
                "Используйте /start чтобы начать настройку.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ Настроить API", callback_data='start_setup')],
                    [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return MAIN_MENU
        
        await query.edit_message_text(
            f"🎯 **НАЧАЛО ПАРСИНГА**\n\n"
            f"📊 **Выбрано методов:** {methods_count}\n"
            f"⚡ **Режим:** {'Быстрый' if methods_count == 1 else 'Расширенный'}\n\n"
            f"Выберите тип канала для парсинга:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Публичный канал", callback_data='parse_public')],
                [InlineKeyboardButton("🔒 Приватный канал", callback_data='parse_private')],
                [InlineKeyboardButton("🎯 Оба типа", callback_data='parse_both')],
                [InlineKeyboardButton("⚙️ Изменить методы", callback_data='choose_methods')],
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return PARSE_CHANNEL
    
    async def choose_channel_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Выбор типа канала"""
        query = update.callback_query
        await query.answer()
        
        action = query.data
        
        if action == 'parse_public':
            context.user_data['channel_type'] = 'public'
            text = "📢 **Выбран публичный канал**\n\n"
        elif action == 'parse_private':
            context.user_data['channel_type'] = 'private'
            text = "🔒 **Выбран приватный канал**\n\n"
        elif action == 'parse_both':
            context.user_data['channel_type'] = 'both'
            text = "🎯 **Выбраны оба типа каналов**\n\n"
        else:
            return await self.start_parsing_menu(update, context)
        
        text += "📝 **Теперь выберите формат вывода результатов:**"
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 TXT файл", callback_data='format_txt'),
                 InlineKeyboardButton("📊 CSV файл", callback_data='format_csv')],
                [InlineKeyboardButton("📈 Excel файл", callback_data='format_excel'),
                 InlineKeyboardButton("🎯 Все форматы", callback_data='format_all')],
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_parsing_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return PARSE_CHANNEL
    
    async def choose_format(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Выбор формата"""
        query = update.callback_query
        await query.answer()
        
        if query.data.startswith('format_'):
            format_type = query.data.replace('format_', '')
            context.user_data['parsing_format'] = format_type
            
            # Получаем выбранные методы
            user_id = query.from_user.id
            selected_methods = self.user_methods.get(user_id, ['participants'])
            methods_text = ", ".join(selected_methods)
            
            await query.edit_message_text(
                f"✅ **Настройки парсинга:**\n\n"
                f"📁 Формат: **{format_type.upper()}**\n"
                f"🎯 Методы: **{methods_text}**\n"
                f"🔒 Тип: **{context.user_data.get('channel_type', 'public')}**\n\n"
                f"📢 **Теперь введите username канала:**\n"
                f"• Без @ (например: `telegram`)\n"
                f"• Или ссылку (например: `t.me/telegram`)\n"
                f"• Для приватного: `t.me/+invite_link`\n\n"
                f"⏱️ *Парсинг может занять 1-10 минут*",
                parse_mode=ParseMode.MARKDOWN
            )
            
            return PARSE_CHANNEL
        
        elif query.data == 'back_to_parsing_menu':
            return await self.start_parsing_menu(update, context)
    
    async def parse_channel_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка ввода канала и запуск парсинга"""
        channel_input = update.message.text.strip()
        user_id = update.effective_user.id
        
        # Валидация ввода
        if not validate_channel_input(channel_input):
            await update.message.reply_text(
                "❌ **Некорректный формат канала!**\n\n"
                "Допустимые форматы:\n"
                "• username (например: telegram)\n"
                "• @username (например: @telegram)\n"
                "• Ссылка (например: t.me/telegram)\n"
                "• Приватная ссылка (например: t.me/+invite_link)",
                parse_mode=ParseMode.MARKDOWN
            )
            return PARSE_CHANNEL
        
        # Извлекаем username
        channel = extract_channel_username(channel_input)
        
        # Проверяем настройки пользователя
        api_data = await self._load_user_settings(user_id)
        if not api_data or not api_data.get('api_id'):
            await update.message.reply_text(
                "❌ **Сначала нужно настроить API ключи!**\n"
                "Используйте /start чтобы начать настройку.",
                parse_mode=ParseMode.MARKDOWN
            )
            return MAIN_MENU
        
        # Сохраняем данные в сессию парсера
        self.parser.user_sessions[user_id] = api_data
        
        # Получаем настройки
        channel_type = context.user_data.get('channel_type', 'public')
        format_type = context.user_data.get('parsing_format', 'txt')
        selected_methods = self.user_methods.get(user_id, ['participants'])
        is_private = channel_type in ['private', 'both']
        
        # Проверяем дневной лимит
        if Config.ENABLE_DATABASE:
            stats = db.get_user_stats(user_id)
            if stats and stats.get('today_parses', 0) >= Config.DAILY_PARSE_LIMIT:
                await update.message.reply_text(
                    f"❌ **Достигнут дневной лимит!**\n\n"
                    f"Вы уже использовали {stats['today_parses']} из {Config.DAILY_PARSE_LIMIT} попыток сегодня.\n"
                    f"Попробуйте завтра или обратитесь к администратору.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return MAIN_MENU
        
        # Отправляем статус
        status_msg = await update.message.reply_text(
            f"🔍 **Начинаю парсинг...**\n\n"
            f"📢 Канал: `{channel}`\n"
            f"🔒 Тип: {'Приватный' if is_private else 'Публичный'}\n"
            f"🎯 Методы: {len(selected_methods)}\n"
            f"📁 Формат: {format_type}\n\n"
            f"⏳ **Пожалуйста, подождите...**",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Создаем запись в БД
            job_id = None
            if Config.ENABLE_DATABASE:
                job_id = db.create_parse_job({
                    'user_id': user_id,
                    'channel': channel,
                    'channel_type': channel_type,
                    'methods': selected_methods,
                    'format_type': format_type,
                    'status': 'processing'
                })
            
            # Определяем лимит
            limit = Config.PRIVATE_CHANNEL_LIMIT if is_private else Config.MAX_PARTICIPANTS
            
            # Запускаем парсинг
            start_time = datetime.now()
            result = await self.parser.parse_with_methods(
                user_id=user_id,
                channel=channel,
                methods=selected_methods,
                limit=limit,
                is_private=is_private
            )
            duration = (datetime.now() - start_time).total_seconds()
            
            participants = result['participants']
            stats = result['stats']
            channel_info = result['channel_info']
            
            if not participants:
                await status_msg.edit_text(
                    f"❌ **Не удалось собрать данные**\n\n"
                    f"Канал: `{channel}`\n"
                    f"Возможные причины:\n"
                    f"• Канал не существует\n"
                    f"• Нет доступа\n"
                    f"• У канала нет участников",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                if Config.ENABLE_DATABASE and job_id:
                    db.update_parse_job(job_id, status='failed', error_message='No participants found')
                
                return MAIN_MENU
            
            # Сохраняем в файл
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"parsed_{channel_info['username']}_{timestamp}"
            
            files = save_participants(participants, format_type, base_filename)
            
            # Обновляем статус
            stats_text = f"""
✅ **Парсинг завершен!**

📊 **Результаты:**
• Канал: {channel_info['title']}
• Участников собрано: {format_number(len(participants))}
• Уникальных: {format_number(stats['unique'])}
• Время парсинга: {duration:.1f} сек

🔍 **Методы сбора:**
• 👥 Участники: {format_number(stats['participants']['count'])}
• 📨 Сообщения: {format_number(stats['messages']['count'])}
• 💬 Комментарии: {format_number(stats['comments']['count'])}
• 👍 Реакции: {format_number(stats['reactions']['count'])}

📁 **Файлы готовы:**
"""
            
            await status_msg.edit_text(
                stats_text,
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Отправляем файлы
            for file_path in files:
                with open(file_path, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        caption=f"📊 {channel_info['title']}",
                        parse_mode=ParseMode.MARKDOWN
                    )
            
            # Обновляем БД
            if Config.ENABLE_DATABASE:
                if job_id:
                    db.update_parse_job(
                        job_id,
                        status='completed',
                        participants_count=len(participants),
                        completed_at=datetime.now()
                    )
                
                # Обновляем статистику пользователя
                db.update_user_stats(user_id, len(participants))
            
            # Кнопки для дальнейших действий
            await update.message.reply_text(
                "🎯 **Что дальше?**",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Парсить другой канал", callback_data='start_parsing')],
                    [InlineKeyboardButton("⚙️ Изменить методы", callback_data='choose_methods')],
                    [InlineKeyboardButton("📊 Посмотреть статистику", callback_data='stats')],
                    [InlineKeyboardButton("🏠 В главное меню", callback_data='back_to_menu')]
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Очищаем временные файлы
            cleanup_files(files)
            
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}", exc_info=True)
            
            error_text = f"""
❌ **Произошла ошибка!**

Канал: `{channel}`
Ошибка: {str(e)}

Попробуйте:
1. Проверить username канала
2. Использовать меньше методов
3. Подождать и повторить
"""
            
            await status_msg.edit_text(
                error_text,
                parse_mode=ParseMode.MARKDOWN
            )
            
            if Config.ENABLE_DATABASE and job_id:
                db.update_parse_job(job_id, status='failed', error_message=str(e))
        
        return MAIN_MENU
    
    async def show_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Показать статистику"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        stats_text = f"""
📊 **СТАТИСТИКА БОТА**

👤 Ваш ID: `{user_id}`
🤖 Статус: ✅ Работает в облаке
🕒 Время: {datetime.now().strftime('%H:%M:%S')}

"""
        
        if Config.ENABLE_DATABASE:
            stats = db.get_user_stats(user_id)
            if stats:
                stats_text += f"""
📈 **Ваша статистика:**
• Всего парсингов: {stats['total_parses']}
• Всего участников: {format_number(stats['total_participants'])}
• Сегодня парсингов: {stats['today_parses']}/{Config.DAILY_PARSE_LIMIT}

"""
        
        stats_text += """
✨ **Особенности облачной версии:**
• Работает 24/7 без перерывов
• Не требует установки
• Файлы отправляются в чат
• Данные хранятся безопасно
"""
        
        await query.edit_message_text(
            stats_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
        
        return MAIN_MENU
    
    async def show_settings(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Показать настройки"""
        user_id = query.from_user.id
        api_data = await self._load_user_settings(user_id)
        
        if api_data:
            text = f"""
⚙️ **Ваши настройки:**

📱 Номер: `{api_data.get('phone', 'Не указан')}`
🆔 API ID: `{api_data.get('api_id', 'Не указан')}`
🔑 API Hash: `{api_data.get('api_hash', 'Не указан')[:10]}...`

💾 **Данные хранятся безопасно в базе данных.**
"""
        else:
            text = "❌ **Настройки не найдены.**\nИспользуйте /start для настройки."
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Изменить настройки", callback_data='start_setup')],
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def help_api(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Помощь по API ключам"""
        query = update.callback_query
        await query.answer()
        
        await query.edit_message_text(
            "📝 **Как получить API ключи:**\n\n"
            "1. **Перейдите на:** https://my.telegram.org\n"
            "2. **Войдите** своим номером телефона\n"
            "3. **Перейдите** в 'API Development Tools'\n"
            "4. **Создайте** новое приложение:\n"
            "   • App title: Telegram Parser\n"
            "   • Short name: tgparser\n"
            "   • Platform: Desktop\n"
            "5. **Скопируйте:**\n"
            "   • `api_id` (только цифры)\n"
            "   • `api_hash` (буквы+цифры)\n\n"
            "⚠️ **Не делитесь ключами с другими!**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_start')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def show_help(self, query):
        """Показать помощь"""
        help_text = """
❓ **ПОМОЩЬ ПО ИСПОЛЬЗОВАНИЮ БОТА**

🎛️ **МЕТОДЫ ПАРСИНГА:**
1. 👥 **Основной** - участники канала (быстро)
2. 📨 **Сообщения** - из истории чата (+20-40% данных)
3. 💬 **Комментарии** - из обсуждений (+10-30% данных)
4. 👍 **Реакции** - пользователи реакций (+5-15% данных)

⚡ **ПРЕСЕТЫ:**
• Быстрый - только основной метод
• Полный - все 4 метода

🔒 **ПРИВАТНЫЕ КАНАЛЫ:**
• Должны быть подписанным участником
• Используйте ссылку-приглашение
• Ограничение: 500 участников

⚠️ **РЕКОМЕНДАЦИИ:**
• Начните с 1-2 методов
• Для больших каналов используйте "Быстрый"
• Для максимальных данных - "Полный"
• При Flood Wait - подождите 5-10 минут
"""
        
        await query.edit_message_text(
            help_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Выбрать методы", callback_data='choose_methods')],
                [InlineKeyboardButton("🚀 Начать парсинг", callback_data='start_parsing')],
                [InlineKeyboardButton("🔙 Назад", callback_data='back_to_menu')]
            ]),
            parse_mode=ParseMode.MARKDOWN
        )
    
    async def main_menu_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка главного меню"""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'start_parsing':
            await self.start_parsing_menu(update, context)
            return PARSE_CHANNEL
            
        elif query.data == 'choose_methods':
            await self.choose_methods_menu(update, context)
            return CHOOSE_METHOD
            
        elif query.data == 'my_settings':
            await self.show_settings(query, context)
            return MAIN_MENU
            
        elif query.data == 'help_main':
            await self.show_help(query)
            return MAIN_MENU
            
        elif query.data == 'stats':
            await self.show_stats(update, context)
            return MAIN_MENU
        
        elif query.data == 'back_to_menu':
            await query.edit_message_text(
                "Главное меню:",
                reply_markup=self.get_main_menu_keyboard()
            )
            return MAIN_MENU
        
        elif query.data == 'back_to_start':
            await self.start(update, context)
            return SETUP_API
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Отмена операции"""
        await update.message.reply_text(
            "Операция отменена.",
            reply_markup=self.get_main_menu_keyboard()
        )
        return MAIN_MENU
    
    async def health_check(self, request):
        """Health check endpoint для Render"""
        from aiohttp import web
        
        # Проверяем доступность сервисов
        status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {}
        }
        
        # Проверка базы данных
        if Config.ENABLE_DATABASE:
            try:
                with db.get_session() as session:
                    session.execute("SELECT 1")
                status["services"]["database"] = "healthy"
            except Exception as e:
                status["services"]["database"] = "unhealthy"
                status["status"] = "degraded"
        
        # Проверка кэша
        if cache.is_available():
            try:
                cache.client.ping()
                status["services"]["cache"] = "healthy"
            except:
                status["services"]["cache"] = "unhealthy"
                status["status"] = "degraded"
        
        return web.json_response(status)
    
    async def setup_webhook(self):
        """Настройка вебхука"""
        webhook_url = Config.get_webhook_url()
        
        if webhook_url:
            await self.app.bot.set_webhook(
                url=webhook_url,
                secret_token=Config.WEBHOOK_SECRET,
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True
            )
            logger.info(f"✅ Webhook настроен на {webhook_url}")
        else:
            logger.info("⚠️ WEBHOOK_URL не указан, используем polling")
    
    async def run_with_webhook(self):
        """Запуск бота с вебхуком"""
        import aiohttp
        from aiohttp import web
        
        # Создаем aiohttp приложение
        web_app = web.Application()
        
        # Добавляем health check endpoint
        web_app.router.add_get('/health', self.health_check)
        
        # Создаем и настраиваем бота
        self.app = Application.builder().token(Config.BOT_TOKEN).build()
        
        # Настраиваем обработчики
        await self._setup_handlers()
        
        # Инициализируем бота
        await self.app.initialize()
        
        # Настраиваем вебхук если указан URL
        if Config.WEBHOOK_URL:
            await self.setup_webhook()
            
            # Добавляем обработчик вебхука
            async def handle_webhook(request):
                # Проверяем секретный токен
                if Config.WEBHOOK_SECRET:
                    token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
                    if token != Config.WEBHOOK_SECRET:
                        return web.Response(status=403)
                
                # Получаем данные
                data = await request.json()
                update = Update.de_json(data, self.app.bot)
                
                # Обрабатываем обновление
                await self.app.process_update(update)
                return web.Response()
            
            web_app.router.add_post(Config.WEBHOOK_PATH, handle_webhook)
        
        # Запускаем HTTP сервер
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', Config.PORT)
        await site.start()
        
        logger.info(f"🤖 Бот запущен на порту {Config.PORT}")
        if Config.WEBHOOK_URL:
            logger.info(f"✅ Webhook настроен: {Config.get_webhook_url()}")
        else:
            logger.info("✅ Используется polling режим")
        
        # Бесконечный цикл
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
        finally:
            await self.app.stop()
            await runner.cleanup()
    
    async def _setup_handlers(self):
        """Настройка обработчиков"""
        # Создаем ConversationHandler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start)],
            states={
                SETUP_API: [
                    CallbackQueryHandler(self.start_setup, pattern='^start_setup$'),
                    CallbackQueryHandler(self.help_api, pattern='^help_api$'),
                    CallbackQueryHandler(self.main_menu_handler, pattern='^back_to_start$'),
                    CallbackQueryHandler(self.main_menu_handler, pattern='^start_parsing$'),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.setup_api_id)
                ],
                SETUP_HASH: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.setup_api_hash)
                ],
                SETUP_PHONE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.setup_phone)
                ],
                MAIN_MENU: [
                    CallbackQueryHandler(self.main_menu_handler, 
                                       pattern='^(start_parsing|my_settings|stats|help_main|back_to_menu|choose_methods)$')
                ],
                CHOOSE_METHOD: [
                    CallbackQueryHandler(self.toggle_method, pattern='^toggle_'),
                    CallbackQueryHandler(self.save_methods, pattern='^save_methods$'),
                    CallbackQueryHandler(self.apply_preset, pattern='^preset_'),
                    CallbackQueryHandler(self.main_menu_handler, pattern='^back_to_menu$')
                ],
                PARSE_CHANNEL: [
                    CallbackQueryHandler(self.choose_channel_type, pattern='^parse_'),
                    CallbackQueryHandler(self.choose_format, pattern='^format_|back_to_parsing_menu$'),
                    CallbackQueryHandler(self.start_parsing_menu, pattern='^back_to_parsing_menu$'),
                    CallbackQueryHandler(self.main_menu_handler, pattern='^back_to_menu$'),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.parse_channel_input)
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel)],
            allow_reentry=True
        )
        
        self.app.add_handler(conv_handler)
        
        # Обработчик команды /help
        self.app.add_handler(CommandHandler('help', self.show_help))
        
        # Обработчик команды /stats
        self.app.add_handler(CommandHandler('stats', self.show_stats))
        
        # Обработчик команды /settings
        self.app.add_handler(CommandHandler('settings', self.show_settings))
        
        # Обработчик неизвестных команд
        async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
            await update.message.reply_text(
                "❌ Неизвестная команда.\n"
                "Используйте /start для начала работы.",
                reply_markup=self.get_main_menu_keyboard()
            )
        
        self.app.add_handler(MessageHandler(filters.COMMAND, unknown))
    
    async def run_with_polling(self):
        """Запуск бота в режиме polling"""
        self.app = Application.builder().token(Config.BOT_TOKEN).build()
        
        # Настраиваем обработчики
        await self._setup_handlers()
        
        # Запускаем бота
        logger.info("🤖 Улучшенный Telegram Parser Bot запущен (polling режим)!")
        await self.app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
    
    async def run(self):
        """Основной метод запуска"""
        # Создаем необходимые директории
        os.makedirs(Config.SESSIONS_DIR, exist_ok=True)
        os.makedirs(Config.LOGS_DIR, exist_ok=True)
        
        # Выбираем режим запуска
        if Config.WEBHOOK_URL:
            # Запуск с вебхуком (для продакшена)
            await self.run_with_webhook()
        else:
            # Запуск с polling (для разработки)
            await self.run_with_polling()