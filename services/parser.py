"""
Класс для парсинга Telegram каналов с обходными методами
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, ChatAdminRequiredError,
    UsernameNotOccupiedError
)

from config.settings import Config
from utils.cache import cache

logger = logging.getLogger(__name__)

class EnhancedTelegramParser:
    def __init__(self):
        self.clients = {}  # Кэш клиентов по user_id
        self.user_sessions = {}  # Сессии пользователей в памяти
        
    async def get_client(self, user_id: int, api_data: dict) -> TelegramClient:
        """Получение или создание клиента Telethon"""
        if user_id in self.clients:
            return self.clients[user_id]
        
        try:
            # Создаем директорию для сессий
            os.makedirs(Config.SESSIONS_DIR, exist_ok=True)
            
            session_path = os.path.join(Config.SESSIONS_DIR, f"user_{user_id}.session")
            
            client = TelegramClient(
                session_path,
                int(api_data['api_id']),
                api_data['api_hash']
            )
            
            await client.start(phone=api_data['phone'])
            self.clients[user_id] = client
            
            logger.info(f"✅ Создан клиент для пользователя {user_id}")
            return client
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания клиента для user {user_id}: {e}")
            raise
    
    def extract_user_data(self, user):
        """Извлечение данных пользователя"""
        try:
            username = f"@{user.username}" if user.username else f"id_{user.id}"
            
            return {
                'username': username,
                'id': user.id,
                'first_name': user.first_name or '',
                'last_name': user.last_name or '',
                'phone': getattr(user, 'phone', '') or '',
                'is_bot': user.bot,
                'is_deleted': getattr(user, 'deleted', False),
                'has_username': bool(user.username),
                'premium': getattr(user, 'premium', False),
                'scam': getattr(user, 'scam', False),
                'verified': getattr(user, 'verified', False),
                'fake': getattr(user, 'fake', False),
                'support': getattr(user, 'support', False),
                'collect_method': 'unknown'
            }
        except Exception as e:
            logger.error(f"Ошибка извлечения данных пользователя: {e}")
            return None
    
    async def parse_with_methods(self, user_id: int, channel: str, methods: List[str], 
                                limit: int, is_private: bool = False) -> Dict[str, Any]:
        """Парсинг с использованием выбранных методов"""
        api_data = self.user_sessions.get(user_id, {})
        if not api_data:
            raise ValueError("API данные не настроены")
        
        client = await self.get_client(user_id, api_data)
        all_participants = []
        unique_user_ids = set()
        
        # Статистика методов
        collection_stats = {
            'participants': {'count': 0},
            'messages': {'count': 0},
            'comments': {'count': 0},
            'reactions': {'count': 0},
            'total': 0,
            'unique': 0
        }
        
        try:
            # Проверяем кэш
            cache_key = f"channel:{channel}:methods:{','.join(sorted(methods))}:limit:{limit}"
            if cache.is_available():
                cached = cache.get(cache_key)
                if cached:
                    logger.info(f"Используем кэшированные данные для {channel}")
                    return cached
            
            # Получаем сущность канала
            logger.info(f"Получение сущности канала: {channel}")
            
            if is_private:
                entity = await self._resolve_private_channel(client, channel)
            else:
                entity = await client.get_entity(channel)
            
            # Сохраняем информацию о канале
            channel_info = {
                'title': getattr(entity, 'title', 'Неизвестно'),
                'username': getattr(entity, 'username', 'Приватный'),
                'id': entity.id,
                'participants_count': getattr(entity, 'participants_count', 0)
            }
            
            logger.info(f"Канал найден: {channel_info['title']}")
            
            # МЕТОД 1: Основной - участники канала
            if 'participants' in methods:
                try:
                    logger.info("Метод 1: Получение участников канала")
                    participants = await self._get_channel_participants(client, entity, limit)
                    
                    for user_data in participants:
                        if user_data['id'] not in unique_user_ids:
                            unique_user_ids.add(user_data['id'])
                            user_data['collect_method'] = 'participants'
                            all_participants.append(user_data)
                            collection_stats['participants']['count'] += 1
                    
                    logger.info(f"Метод 1: найдено {len(participants)} участников")
                except Exception as e:
                    logger.error(f"Ошибка основного метода: {e}")
            
            # МЕТОД 2: Из истории сообщений
            if 'messages' in methods and len(all_participants) < limit:
                try:
                    logger.info("Метод 2: Анализ истории сообщений")
                    message_users = await self._get_users_from_messages(client, entity, 
                                                                      Config.LIMIT_MESSAGES)
                    
                    for user_data in message_users:
                        if user_data['id'] not in unique_user_ids:
                            unique_user_ids.add(user_data['id'])
                            user_data['collect_method'] = 'messages'
                            all_participants.append(user_data)
                            collection_stats['messages']['count'] += 1
                    
                    logger.info(f"Метод 2: найдено {len(message_users)} пользователей из сообщений")
                except Exception as e:
                    logger.error(f"Ошибка метода сообщений: {e}")
            
            # МЕТОД 3: Из комментариев
            if 'comments' in methods and len(all_participants) < limit:
                try:
                    logger.info("Метод 3: Анализ комментариев")
                    comment_users = await self._get_users_from_comments(client, entity, 
                                                                       Config.LIMIT_COMMENTS)
                    
                    for user_data in comment_users:
                        if user_data['id'] not in unique_user_ids:
                            unique_user_ids.add(user_data['id'])
                            user_data['collect_method'] = 'comments'
                            all_participants.append(user_data)
                            collection_stats['comments']['count'] += 1
                    
                    logger.info(f"Метод 3: найдено {len(comment_users)} пользователей из комментариев")
                except Exception as e:
                    logger.error(f"Ошибка метода комментариев: {e}")
            
            # МЕТОД 4: Из реакций
            if 'reactions' in methods and len(all_participants) < limit:
                try:
                    logger.info("Метод 4: Анализ реакций")
                    reaction_users = await self._get_users_from_reactions(client, entity, 50)
                    
                    for user_data in reaction_users:
                        if user_data['id'] not in unique_user_ids:
                            unique_user_ids.add(user_data['id'])
                            user_data['collect_method'] = 'reactions'
                            all_participants.append(user_data)
                            collection_stats['reactions']['count'] += 1
                    
                    logger.info(f"Метод 4: найдено {len(reaction_users)} пользователей из реакций")
                except Exception as e:
                    logger.error(f"Ошибка метода реакций: {e}")
            
            # Обрезаем до лимита
            all_participants = all_participants[:limit]
            collection_stats['total'] = len(all_participants)
            collection_stats['unique'] = len(unique_user_ids)
            
            result = {
                'participants': all_participants,
                'stats': collection_stats,
                'channel_info': channel_info
            }
            
            # Сохраняем в кэш
            if cache.is_available():
                cache.set(cache_key, result, ttl=3600)  # 1 час
            
            logger.info(f"Парсинг завершен: собрано {len(all_participants)} участников")
            return result
            
        except FloodWaitError as e:
            logger.warning(f"Flood wait: {e.seconds} секунд")
            raise
        except ChannelPrivateError:
            logger.error(f"Канал {channel} является приватным")
            raise
        except UsernameNotOccupiedError:
            logger.error(f"Канал {channel} не существует")
            raise
        except Exception as e:
            logger.error(f"Общая ошибка парсинга: {e}")
            raise
    
    async def _get_channel_participants(self, client, entity, limit):
        """Основной метод получения участников"""
        participants = []
        offset = 0
        request_count = 0
        
        while offset < limit and request_count < Config.MAX_REQUESTS_PER_CHANNEL:
            try:
                result = await client(GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsSearch(''),
                    offset=offset,
                    limit=Config.PARSING_BATCH_SIZE,
                    hash=0
                ))
                
                if not result.users:
                    break
                
                for user in result.users:
                    user_data = self.extract_user_data(user)
                    if user_data:
                        participants.append(user_data)
                
                offset += Config.PARSING_BATCH_SIZE
                request_count += 1
                
                if len(participants) % 500 == 0:
                    logger.info(f"Собрано {len(participants)} участников...")
                
                await asyncio.sleep(Config.DELAY_BETWEEN_REQUESTS)
                
            except FloodWaitError as e:
                logger.warning(f"Flood wait при получении участников: {e.seconds} секунд")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"Ошибка получения участников: {e}")
                break
        
        return participants
    
    async def _get_users_from_messages(self, client, entity, limit_messages):
        """Получение пользователей из сообщений"""
        users = []
        
        try:
            message_count = 0
            
            async for message in client.iter_messages(entity, limit=limit_messages):
                if message and message.sender:
                    try:
                        user = await client.get_entity(message.sender)
                        user_data = self.extract_user_data(user)
                        if user_data:
                            users.append(user_data)
                    except:
                        continue
                
                message_count += 1
                if message_count % 100 == 0:
                    logger.debug(f"Проанализировано {message_count} сообщений...")
                    
        except Exception as e:
            logger.error(f"Ошибка в методе сообщений: {e}")
        
        return users
    
    async def _get_users_from_comments(self, client, channel_entity, limit_comments):
        """Получение пользователей из комментариев"""
        users = []
        
        try:
            # Получаем полную информацию о канале
            full_channel = await client(GetFullChannelRequest(channel=channel_entity))
            
            if hasattr(full_channel, 'linked_chat') and full_channel.linked_chat:
                comments_chat = full_channel.linked_chat
                
                comment_count = 0
                async for message in client.iter_messages(comments_chat, limit=limit_comments):
                    if message and message.sender:
                        try:
                            user = await client.get_entity(message.sender)
                            user_data = self.extract_user_data(user)
                            if user_data:
                                users.append(user_data)
                        except:
                            continue
                    
                    comment_count += 1
                    if comment_count % 50 == 0:
                        logger.debug(f"Проанализировано {comment_count} комментариев...")
                        
        except Exception as e:
            logger.error(f"Ошибка в методе комментариев: {e}")
        
        return users
    
    async def _get_users_from_reactions(self, client, entity, limit_messages):
        """Получение пользователей из реакций"""
        users = []
        
        try:
            message_count = 0
            
            async for message in client.iter_messages(entity, limit=limit_messages):
                if hasattr(message, 'reactions') and message.reactions:
                    # Собираем отправителя сообщения
                    if message.sender:
                        try:
                            user = await client.get_entity(message.sender)
                            user_data = self.extract_user_data(user)
                            if user_data:
                                users.append(user_data)
                        except:
                            continue
                
                message_count += 1
                if message_count % 10 == 0:
                    logger.debug(f"Проанализировано {message_count} сообщений с реакциями...")
                    
        except Exception as e:
            logger.error(f"Ошибка в методе реакций: {e}")
        
        return users
    
    async def _resolve_private_channel(self, client, channel_input):
        """Разрешение приватного канала"""
        try:
            # Пробуем получить как обычный канал
            entity = await client.get_entity(channel_input)
            return entity
        except ChannelPrivateError:
            # Пробуем через ссылку-приглашение
            try:
                if 't.me/+' in channel_input:
                    invite_hash = channel_input.split('+')[-1]
                    logger.info(f"Попытка присоединиться к приватному каналу по инвайту: {invite_hash}")
                    entity = await client.join_channel(f'https://t.me/+{invite_hash}')
                    return entity
                else:
                    raise
            except Exception as e:
                logger.error(f"Ошибка доступа к приватному каналу: {e}")
                raise
    
    async def close_clients(self):
        """Закрытие всех клиентов"""
        for user_id, client in self.clients.items():
            try:
                await client.disconnect()
                logger.info(f"Закрыт клиент для пользователя {user_id}")
            except Exception as e:
                logger.error(f"Ошибка закрытия клиента для пользователя {user_id}: {e}")
        
        self.clients.clear()
        self.user_sessions.clear()