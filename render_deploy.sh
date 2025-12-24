#!/bin/bash
# Скрипт для деплоя на Render

echo "🚀 Подготовка к деплою на Render..."

# Проверка переменных окружения
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ Ошибка: BOT_TOKEN не установлен"
    exit 1
fi

if [ -z "$TELEGRAM_API_ID" ]; then
    echo "❌ Ошибка: TELEGRAM_API_ID не установлен"
    exit 1
fi

if [ -z "$TELEGRAM_API_HASH" ]; then
    echo "❌ Ошибка: TELEGRAM_API_HASH не установлен"
    exit 1
fi

echo "✅ Все необходимые переменные установлены"

# Создаем .env файл для локального тестирования
cat > .env << EOF
BOT_TOKEN=$BOT_TOKEN
TELEGRAM_API_ID=$TELEGRAM_API_ID
TELEGRAM_API_HASH=$TELEGRAM_API_HASH
ADMIN_IDS=$ADMIN_IDS
PORT=8080
PYTHONUNBUFFERED=1
EOF

echo "📁 Создан .env файл"

# Проверяем структуру проекта
echo "🔍 Проверка структуры проекта..."
ls -la

echo "✅ Готово к деплою!"