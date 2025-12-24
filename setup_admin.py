import asyncio
from database import db

async def setup_admin():
    await db.connect()
    
    # ID администратора (ваш Telegram ID)
    admin_id = 588378991  # Замените на ваш ID
    
    # Проверяем, есть ли пользователь
    user = await db.get_user(admin_id)
    if not user:
        # Создаем пользователя
        await db.create_user(admin_id, "admin", "Администратор", "Бота")
        print(f"✅ Создан пользователь {admin_id}")
    
    # Назначаем администратором
    await db.set_admin(admin_id, True)
    
    # Создаем подписку на год
    await db.create_subscription(admin_id, 'yearly', 365, 0.0, 'RUB')
    
    print(f"✅ Пользователь {admin_id} назначен администратором")
    print(f"✅ Создана годовая подписка")
    
    await db.close()

if __name__ == "__main__":
    asyncio.run(setup_admin())