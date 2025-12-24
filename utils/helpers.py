import os
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Any
import csv
import json

logger = logging.getLogger(__name__)

def create_temp_file(prefix: str = "temp", suffix: str = ".txt") -> str:
    """Создание временного файла"""
    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        prefix=prefix,
        suffix=suffix
    )
    temp_file.close()
    return temp_file.name

def save_to_txt(participants: List[Dict], filename: str) -> str:
    """Сохранение в TXT формат"""
    with open(filename, 'w', encoding='utf-8') as f:
        for user in participants:
            f.write(f"{user.get('username', 'N/A')}\n")
    return filename

def save_to_csv(participants: List[Dict], filename: str) -> str:
    """Сохранение в CSV формат"""
    if not participants:
        return filename
    
    fieldnames = participants[0].keys()
    
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(participants)
    
    return filename

def save_to_excel(participants: List[Dict], filename: str) -> str:
    """Сохранение в Excel формат"""
    try:
        import pandas as pd
        
        df = pd.DataFrame(participants)
        df.to_excel(filename, index=False)
        return filename
    except ImportError:
        logger.warning("Pandas не установлен, используем CSV")
        return save_to_csv(participants, filename.replace('.xlsx', '.csv'))

def save_participants(participants: List[Dict], format_type: str, base_filename: str) -> List[str]:
    """Сохранение участников в указанном формате"""
    files = []
    
    if format_type == 'all':
        formats = ['txt', 'csv', 'excel']
    else:
        formats = [format_type]
    
    for fmt in formats:
        filename = f"{base_filename}.{fmt}"
        
        if fmt == 'txt':
            save_to_txt(participants, filename)
        elif fmt == 'csv':
            save_to_csv(participants, filename)
        elif fmt == 'excel':
            save_to_excel(participants, filename)
        
        files.append(filename)
        logger.info(f"Сохранен файл: {filename}")
    
    return files

def cleanup_files(files: List[str]):
    """Очистка временных файлов"""
    for file_path in files:
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.debug(f"Удален файл: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка удаления файла {file_path}: {e}")

def format_number(number: int) -> str:
    """Форматирование числа с разделителями"""
    return f"{number:,}"

def format_duration(seconds: float) -> str:
    """Форматирование длительности"""
    if seconds < 60:
        return f"{seconds:.1f} сек"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} мин"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} час"

def validate_channel_input(channel: str) -> bool:
    """Валидация ввода канала"""
    if not channel or len(channel) > 255:
        return False
    
    # Проверяем допустимые символы
    import re
    pattern = r'^[a-zA-Z0-9_+@\./\-]+$'
    return bool(re.match(pattern, channel))

def extract_channel_username(channel_input: str) -> str:
    """Извлечение username из ввода"""
    # Убираем протокол и домен
    if 't.me/' in channel_input:
        channel_input = channel_input.split('t.me/')[-1]
    
    # Убираем @ если есть
    if channel_input.startswith('@'):
        channel_input = channel_input[1:]
    
    # Убираем параметры
    if '?' in channel_input:
        channel_input = channel_input.split('?')[0]
    
    return channel_input.strip()

def get_file_size(filepath: str) -> str:
    """Получение размера файла в читаемом формате"""
    try:
        size = os.path.getsize(filepath)
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        
        return f"{size:.1f} TB"
    except:
        return "Неизвестно"

def safe_get(dictionary: Dict, key: str, default: Any = None) -> Any:
    """Безопасное получение значения из словаря"""
    keys = key.split('.')
    value = dictionary
    
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default
    
    return value

def generate_stats_text(stats: Dict, duration: float) -> str:
    """Генерация текста статистики"""
    return f"""
📊 **СТАТИСТИКА ПАРСИНГА**

✅ **Результаты:**
• Участников собрано: {format_number(stats.get('total', 0))}
• Уникальных: {format_number(stats.get('unique', 0))}
• Время парсинга: {format_duration(duration)}

🔍 **Методы сбора:**
• 👥 Участники: {format_number(stats.get('participants', {}).get('count', 0))}
• 📨 Сообщения: {format_number(stats.get('messages', {}).get('count', 0))}
• 💬 Комментарии: {format_number(stats.get('comments', {}).get('count', 0))}
• 👍 Реакции: {format_number(stats.get('reactions', {}).get('count', 0))}

📈 **Эффективность методов:**
• Основной метод: {(stats.get('participants', {}).get('count', 0) / max(stats.get('total', 1), 1) * 100):.1f}%
• Дополнительные методы: {((stats.get('messages', {}).get('count', 0) + stats.get('comments', {}).get('count', 0) + stats.get('reactions', {}).get('count', 0)) / max(stats.get('total', 1), 1) * 100):.1f}%
"""