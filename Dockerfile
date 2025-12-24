FROM python:3.11-slim

# Минимальные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости отдельно для кэширования
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Создаем директории
RUN mkdir -p data logs user_sessions temp

# Оптимизируем для Render (меньше памяти)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONHASHSEED=random

# Запускаем
CMD ["python", "bot.py"]