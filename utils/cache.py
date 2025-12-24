import redis
import json
import pickle
from datetime import timedelta
import logging
from typing import Optional, Any

from config.settings import Config

logger = logging.getLogger(__name__)

class Cache:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_cache()
        return cls._instance
    
    def _init_cache(self):
        """Инициализация кэша"""
        try:
            if Config.ENABLE_CACHE:
                self.client = redis.from_url(
                    Config.REDIS_URL,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                # Тестируем подключение
                self.client.ping()
                logger.info("✅ Redis кэш инициализирован")
            else:
                self.client = None
                logger.info("⚠️ Кэш отключен")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось подключиться к Redis: {e}")
            self.client = None
    
    def is_available(self) -> bool:
        """Проверка доступности кэша"""
        return self.client is not None
    
    def get(self, key: str) -> Optional[Any]:
        """Получить значение из кэша"""
        if not self.is_available():
            return None
        
        try:
            value = self.client.get(key)
            if value:
                try:
                    return json.loads(value)
                except:
                    return value
            return None
        except Exception as e:
            logger.error(f"Ошибка получения из кэша: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Установить значение в кэш"""
        if not self.is_available():
            return False
        
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            self.client.setex(key, timedelta(seconds=ttl), value)
            return True
        except Exception as e:
            logger.error(f"Ошибка установки в кэш: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Удалить значение из кэша"""
        if not self.is_available():
            return False
        
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            logger.error(f"Ошибка удаления из кэша: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Проверить существование ключа"""
        if not self.is_available():
            return False
        
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error(f"Ошибка проверки ключа: {e}")
            return False
    
    def incr(self, key: str, amount: int = 1) -> Optional[int]:
        """Увеличить значение"""
        if not self.is_available():
            return None
        
        try:
            return self.client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Ошибка увеличения значения: {e}")
            return None
    
    def cache_user_session(self, user_id: int, session_data: dict) -> bool:
        """Кэширование сессии пользователя"""
        key = f"user_session:{user_id}"
        return self.set(key, session_data, ttl=86400)  # 24 часа
    
    def get_user_session(self, user_id: int) -> Optional[dict]:
        """Получение сессии пользователя из кэша"""
        key = f"user_session:{user_id}"
        return self.get(key)
    
    def cache_channel_info(self, channel: str, info: dict) -> bool:
        """Кэширование информации о канале"""
        key = f"channel_info:{channel}"
        return self.set(key, info, ttl=3600)  # 1 час
    
    def get_channel_info(self, channel: str) -> Optional[dict]:
        """Получение информации о канале из кэша"""
        key = f"channel_info:{channel}"
        return self.get(key)

# Глобальный экземпляр кэша
cache = Cache()