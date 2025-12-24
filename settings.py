import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # ====================
    # TELEGRAM SETTINGS
    # ====================
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
    TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
    
    # ====================
    # WEBHOOK SETTINGS
    # ====================
    WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')
    WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/webhook')
    WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')
    PORT = int(os.getenv('PORT', '8080'))
    
    # ====================
    # DATABASE SETTINGS
    # ====================
    DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/bot.db')
    REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    
    # ====================
    # BOT SETTINGS
    # ====================
    ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]
    MAX_PARTICIPANTS = int(os.getenv('MAX_PARTICIPANTS', '1000'))
    DAILY_PARSE_LIMIT = int(os.getenv('DAILY_PARSE_LIMIT', '10'))
    MAX_CHANNELS_PER_USER = int(os.getenv('MAX_CHANNELS_PER_USER', '5'))
    
    # ====================
    # PARSING SETTINGS
    # ====================
    PARSING_BATCH_SIZE = int(os.getenv('PARSING_BATCH_SIZE', '200'))
    DELAY_BETWEEN_REQUESTS = float(os.getenv('DELAY_BETWEEN_REQUESTS', '1.0'))
    MAX_REQUESTS_PER_CHANNEL = int(os.getenv('MAX_REQUESTS_PER_CHANNEL', '50'))
    LIMIT_MESSAGES = 500
    LIMIT_COMMENTS = 200
    PRIVATE_CHANNEL_LIMIT = 500
    
    # ====================
    # PATHS
    # ====================
    SESSIONS_DIR = os.getenv('SESSIONS_DIR', 'data/sessions')
    LOGS_DIR = os.getenv('LOGS_DIR', 'logs')
    
    # ====================
    # LOGGING
    # ====================
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # ====================
    # FEATURES
    # ====================
    ENABLE_CACHE = os.getenv('ENABLE_CACHE', 'true').lower() == 'true'
    ENABLE_DATABASE = os.getenv('ENABLE_DATABASE', 'true').lower() == 'true'
    DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'
    
    @classmethod
    def validate(cls):
        """Проверка обязательных переменных"""
        errors = []
        
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN не установлен")
        
        if not cls.TELEGRAM_API_ID:
            errors.append("TELEGRAM_API_ID не установлен")
        
        if not cls.TELEGRAM_API_HASH:
            errors.append("TELEGRAM_API_HASH не установлен")
        
        if errors:
            raise ValueError(f"Ошибки конфигурации: {', '.join(errors)}")
        
        return True
    
    @classmethod
    def get_webhook_url(cls):
        """Получить полный URL вебхука"""
        if cls.WEBHOOK_URL:
            return f"{cls.WEBHOOK_URL}{cls.WEBHOOK_PATH}"
        return None