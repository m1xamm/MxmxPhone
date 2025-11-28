# bot.py
import os
import json
import asyncio
import random
from pathlib import Path
from io import BytesIO
from background import keep_alive
# keep_alive() будет вызван в main()
import aiohttp
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance
import base64

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    FSInputFile
)
from aiogram.filters import Command, StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage


# === Настройки ===
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем токен бота из переменных окружения
TOKEN = os.getenv('BOT_TOKEN', '8057917930:AAH67CjfNADz83ddUnj9bqNtF6WjQXV8Fx4')

# Импортируем PostgreSQL базу данных
from database import db, get_player, create_player, update_player

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

async def main():
    print("🔄 Инициализация базы данных...")
    try:
        # Проверяем подключение к базе данных
        test_player = await db.get_player(1)  # Тестовый запрос
        print("✅ База данных инициализирована успешно")
        
        # Запускаем бота
        print("✅ Бот запущен")
        await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        # Выходим, так как без БД бот работать не сможет
        return

# Функции для обратной совместимости
async def get_user(user_id: int, username: str = None, first_name: str = None) -> dict:
    """Получить данные пользователя, создать если нет"""
    user = await db.get_player(user_id)
    
    if not user:
        # Создаем нового пользователя
        user_data = {
            "user_id": user_id,
            "username": username or f"user_{user_id}",
            "name": first_name or username or f"User {user_id}",
            "balance": 0,
            "inventory": [],
            "last_claim": "0",
            "daily_streak": 0,
            "last_daily_claim": None
        }
        user = await db.create_player(user_id, user_data["username"], user_data["name"])
    else:
        # Обновляем имя пользователя, если оно изменилось
        updates = {}
        if username and user.get("username") != username:
            updates["username"] = username
            updates["name"] = username  # Обновляем также отображаемое имя
        elif first_name and not user.get("username"):
            updates["name"] = first_name
            
        if updates:
            await db.update_player(user_id, updates)
            user.update(updates)
    
    # Обновляем глобальный кеш для обратной совместимости
    data["players"][str(user_id)] = user
    
    return user

async def update_user(user_id: int, user_data: dict):
    """Обновить данные пользователя"""
    # Удаляем user_id из данных, если он там есть, чтобы не обновлять первичный ключ
    user_data_copy = user_data.copy()
    user_data_copy.pop("user_id", None)
    
    # Обновляем пользователя в базе данных
    await db.update_player(user_id, user_data_copy)
    
    # Обновляем кеш
    data["players"][str(user_id)] = user_data
    
    return True

# === Вызов загрузки при старте ===
# Загрузка будет выполнена асинхронно в main()







# Явный маппинг для эксклюзивов
EXCLUSIVE_IMAGE_MAP = {
    "Хлібікфон Про Макс Ульро": "Hlibikphone.png",
    "NIGHT N200C": "Night_N200c.png",
}

def _to_path_safe(p):
    if p is None:
        return None
    return Path(p) if not isinstance(p, Path) else p

# Для отображения
rarity_emojis = {
    "Обычный": "📱",
    "Необычный": "📲",
    "Редкий": "⭐️",
    "Эпический": "👾",
    "Мистический": "🚨",
    "Легендарный": "🏆",
    "Платина": "💠",
    "Эксклюзив": "🍞",
    "Экcклюзив": "🌓"
}

# Для логики
rarity_names = {
    "Обычный": "Обычный",
    "Необычный": "Необычный",
    "Редкий": "Редкий",
    "Эпический": "Эпический",
    "Мистический": "Мистический",
    "Легендарный": "Легендарный",
    "Платина": "Платина",
    "Эксклюзив": "Эксклюзив",
    "Экcклюзив": "Экcклюзив"
}


donate_ranks = {
    "VIP": {
        "emote": "⚡",
        "limit": 1,
        "cd": 7200,
        "theme": "⚡"
    },
    "Premium": {
        "emote": "🏅",
        "limit": 2,
        "cd": 6000,
        "theme": "🏅"
    },
    "Deluxe": {
        "emote": "💠",
        "limit": 3,
        "cd": 5400,
        "theme": "💠"
    },
    "Legend": {
        "emote": "👑",
        "limit": 5,
        "cd": 4800,
        "theme": "👑"
    },
    "ULTRA": {
        "emote": "🔮",
        "limit": 10,
        "cd": 4200,
        "theme": "🔮"
    }
}



# === Случайный телефон ===
def get_random_phone():
    rarities = list(rarity_chances.keys())
    weights = list(rarity_chances.values())
    rarity = random.choices(rarities, weights=weights, k=1)[0]
    phone = random.choice(list(phones[rarity].keys()))
    price = phones[rarity][phone]
    
    # Проверяем шанс сломанного телефона (1%, кроме Платины)
    is_broken = False
    if rarity != "Платина" and random.random() < BROKEN_CHANCE:
        is_broken = True
    
    return rarity, phone, price, is_broken

def mention_user(obj):
    if hasattr(obj, 'username') and obj.username:
        return f'@{obj.username}'
    return f'<a href="tg://user?id={obj.id}">{obj.full_name}</a>'

# Лок для пользователя
_user_locks = {}
def _get_user_lock(user_id: int):
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]

DATA_FILE = "data.json"
COOLDOWN_HOURS = 2  # кулдаун 2 часа

# === Работа с базой данных ===
import os, json

# Глобальный кеш для обратной совместимости
data: dict = {"players": {}, "market": []}

def save_data_sync(data_to_save: dict):
    """Синхронная версия сохранения (для обратной совместимости)"""
    try:
        # Обновляем данные игроков в базе данных
        if 'players' in data_to_save:
            for user_id_str, user_data in data_to_save['players'].items():
                try:
                    user_id = int(user_id_str)
                    # Обновляем данные игрока в базе
                    db.update_player(user_id, user_data)
                except Exception as e:
                    print(f"Ошибка при обновлении пользователя {user_id_str}: {e}")
                    continue
                
        # Обновляем данные маркета в базе данных
        if 'market' in data_to_save:
            # Пока сохраняем маркет в файл, но можно перенести в БД
            try:
                with open('market.json', 'w', encoding='utf-8') as f:
                    json.dump(data_to_save.get('market', []), f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Ошибка при сохранении маркета: {e}")
                
    except Exception as e:
        print(f"Критическая ошибка при сохранении данных: {e}")
        # В крайнем случае сохраняем полный дамп в файл
        try:
            with open('data_backup.json', 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        except Exception as backup_err:
            print(f"Не удалось создать резервную копию: {backup_err}")

def _migrate_legacy_users_to_players():
    """Перенос старых ключей (если были) в players"""
    if not isinstance(data, dict):
        return
    players = data.setdefault("players", {})
    legacy_keys = [k for k in list(data.keys()) if k.isdigit()]
    for k in legacy_keys:
        if k not in players:
            players[k] = data.pop(k)
    data.setdefault("market", [])
    save_data_sync(data)

def _get_players_map() -> dict:
    """Получить словарь игроков"""
    if not isinstance(data, dict):
        return {}
    if "players" not in data:
        _migrate_legacy_users_to_players()
    return data.setdefault("players", {})

def get_user(user_id: int, username: str = None, first_name: str = None) -> dict:
    """Получить данные пользователя, создать если нет"""
    players = _get_players_map()
    uid = str(user_id)
    if uid not in players:
        players[uid] = {
            "id": user_id,
            "username": username,
            "name": username or first_name or f"ID{user_id}",
            "balance": 0,
            "inventory": [],
            "last_claim": "0"
        }
    else:
        # обновляем только мета-данные, баланс и инвентарь не трогаем
        if username:
            players[uid]["username"] = username
            players[uid]["name"] = username
        elif first_name and not players[uid].get("username"):
            players[uid]["name"] = first_name

    save_data_sync(data)
    return players[uid]

def update_user(user_id: int, user: dict):
    """Обновить данные пользователя"""
    data["players"][str(user_id)] = user
    save_data_sync(data)

# === Вызов загрузки при старте ===
# Загрузка будет выполнена асинхронно в main()



# === Телефоны и цены ===
phones = {
    "Обычный": {
        "Samsung A01 Core": 450,
        "iPhone 3G": 500,
        "Redmi A3X": 550,
        "Samsung Galaxy Note 3": 700,
        "Poco M3": 650,
        "Realme C11": 480,
        "Honor 7A": 520,
        "Tecno Spark Go": 490,
        "Oppo A3S": 560,
        "Nokia 3.1": 530
    },
    "Необычный": {
        "Redmi 10": 1200,
        "Realme C30": 1100,
        "Samsung A12": 1300,
        "Honor X6": 1250,
        "Tecno Spark 20": 1400,
        "Oppo A16": 1350,
        "Redmi Note 9": 1500,
        "Nokia G10": 1150,
        "Realme Narzo 30": 1450,
        "Vivo Y20": 1300
    },
    "Редкий": {
        "Redmi Note 12": 4000,
        "Samsung M14": 3800,
        "Realme 9 Pro": 4100,
        "Poco X4 Pro": 4200,
        "Oppo Reno 8": 3900,
        "Honor 90 Lite": 4300,
        "Vivo V25": 4150,
        "Tecno Camon 30": 4400,
        "OnePlus Nord 2": 4500,
        "Google Pixel 6A": 4600
    },
    "Эпический": {
        "Redmi Note 13 Pro": 7000,
        "Samsung S20 FE": 7200,
        "Realme GT Neo 3": 7100,
        "OnePlus 10R": 7400,
        "Vivo V29": 7300,
        "Honor 100": 7250,
        "Google Pixel 7": 7500,
        "Oppo Reno 9 Pro": 7700,
        "Asus Zenfone 9": 7800,
        "Nothing Phone 2": 7600
    },
    "Мистический": {
        "iPhone 14 Pro": 20000,
        "Samsung S23 Ultra": 19500,
        "Xiaomi 14 Pro": 19800,
        "OnePlus 12": 20500,
        "Vivo X100 Pro": 21000,
        "Google Pixel 8 Pro": 22000,
        "Asus ROG Phone 7": 23000,
        "Oppo Find X6 Pro": 21500,
        "Huawei Mate 60 Pro": 22500,
        "Sony Xperia 1 V": 20000
    },
    "Легендарный": {
        "iPhone 16 Pro Max": 60000,
        "Samsung Galaxy S24 Ultra": 58000,
        "Xiaomi 15 Ultra": 61000,
        "Oppo Find X7 Ultra": 59000,
        "Vivo X200 Pro": 60500,
        "OnePlus 13": 61500,
        "Google Pixel 9 Pro": 62000,
        "Huawei Mate 70 Pro": 61000,
        "Sony Xperia 1 VI": 63000,
        "Asus ROG Phone 8": 60000
    },
    "Платина": {
        "iPhone 17 Pro Max": 300000,
        "Samsung Galaxy Z Fold 7": 300000,
        "Xiaomi 17 Pro Max": 300000,
        "Oppo Find X8 Ultra": 300000,
        "Vivo X300 Ultra": 300000,
        "OnePlus 15": 300000,
        "Google Pixel 10 Pro Fold": 300000,
        "Huawei Mate XT": 300000,
        "Sony Xperia 1 VII": 300000,
        "Asus ROG Phone 9 Pro": 300000
         },
    "Эксклюзив": {
        "Хлібікфон Про Макс Ульро": 0,
    },
    "Экcклюзив": {
        "NIGHT N200C": 0,
    }
}

phone_pool = {
    rarity: list(phone_dict.keys())
    for rarity, phone_dict in phones.items()
}
# === Шансы по редкости ===
rarity_chances = {
    "Обычный": 33,
    "Необычный": 26,
    "Редкий": 17,
    "Эпический": 12,
    "Мистический": 8,
    "Легендарный": 3,
    "Платина": 1, 
    "Эксклюзив": 0
}
# Порядок редкостей
rarity_order = ["Обычный", "Необычный", "Редкий", "Эпический", "Мистический", "Легендарный", "Платина", "Эксклюзив", "Экcклюзив"]

# Для отображения
rarity_emojis = {
    "Обычный": "📱",
    "Необычный": "📲",
    "Редкий": "⭐️",
    "Эпический": "👾",
    "Мистический": "🚨",
    "Легендарный": "🏆",
    "Платина": "💠",
    "Эксклюзив": "🍞",
    "Экcклюзив": "🌓"
}

# Для логики
rarity_names = {
    "Обычный": "Обычный",
    "Необычный": "Необычный",
    "Редкий": "Редкий",
    "Эпический": "Эпический",
    "Мистический": "Мистический",
    "Легендарный": "Легендарный",
    "Платина": "Платина",
    "Эксклюзив": "Эксклюзив",
    "Экcклюзив": "Экcклюзив"
}


donate_ranks = {
    "VIP": {
        "emote": "⚡",
        "limit": 1,
        "cd": 7200,
        "theme": "⚡"
    },
    "Premium": {
        "emote": "🏅",
        "limit": 2,
        "cd": 6000,
        "theme": "🏅"
    },
    "Deluxe": {
        "emote": "💠",
        "limit": 3,
        "cd": 5400,
        "theme": "💠"
    },
    "Legend": {
        "emote": "👑",
        "limit": 5,
        "cd": 4800,
        "theme": "👑"
    },
    "ULTRA": {
        "emote": "🔮",
        "limit": 10,
        "cd": 4200,
        "theme": "🔮"
    }
}



# === Случайный телефон ===
def get_random_phone():
    rarities = list(rarity_chances.keys())
    weights = list(rarity_chances.values())
    rarity = random.choices(rarities, weights=weights, k=1)[0]
    phone = random.choice(list(phones[rarity].keys()))
    price = phones[rarity][phone]
    
    # Проверяем шанс сломанного телефона (1%, кроме Платины)
    is_broken = False
    if rarity != "Платина" and random.random() < BROKEN_CHANCE:
        is_broken = True
    
    return rarity, phone, price, is_broken

# === Регистрация обработчиков ===
# bot and dispatcher already created abovejson
import psutil
import platform
import time
from time import perf_counter
from datetime import time as dt_time
from aiogram import types
from aiogram.filters import Command

def sizeof_json_kb(path: str = "data.json") -> str:
    try:
        return f"{os.path.getsize(path) // 1024} KB"
    except Exception:
        return "?"

def human_uptime_hm(seconds: int) -> str:
    minutes = seconds // 60
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours} ч {minutes} м"

@dp.message(Command("techinfo"))
async def techinfo(message: types.Message):
    start = perf_counter()

    # Общие данные
    players = data.get("players", {})
    total_users = len(players)
    total_items = sum(len(u.get("inventory", [])) for u in players.values())
    json_size = sizeof_json_kb("data.json")

    # Система и процесс
    process = psutil.Process()
    with process.oneshot():
        rss_mb = process.memory_info().rss // (1024 * 1024)
        cpu_proc = process.cpu_percent(interval=0.1)
    cpu_cores = psutil.cpu_count(logical=True)

    # RAM всей системы
    mem = psutil.virtual_memory()
    free_ram_mb = mem.available // (1024 * 1024)

    # Аптайм в ч/м

    # Среда
    py_ver = platform.python_version()
    platform_info = f"{platform.system()} {platform.release()}"
    try:
        import aiogram
        aio_ver = getattr(aiogram, "__version__", "unknown")
    except Exception:
        aio_ver = "unknown"

    # Пинг
    ping_ms = round((perf_counter() - start) * 1000)

    text = (
        "🛠 Техническая информация\n"
        "📊 Данные:\n"
        f"• Пользователи: {total_users}\n"
        f"• Всего телефонов: {total_items}\n"
        f"• data.json: {json_size}\n"
        "\n"
        "🧠 Система:\n"
        f"• CPU: {cpu_proc}%\n"
        f"• Ядер CPU: {cpu_cores}\n"
        f"• RAM: {rss_mb}/{free_ram_mb} MB\n"
        "\n"
        "⚙️ Среда:\n"
        f"• Python: {py_ver}\n"
        f"• Платформа: {platform_info}\n"
        f"• aiogram: {aio_ver}\n"
        "\n"
        f"⏱ Пинг: {ping_ms} мс"
    )

    await message.answer(text)



def display_username(user):
    return f"@{user.get('username')}" if user.get("username") else user.get("name", "без имени")

from aiogram import Bot, Dispatcher, types
from aiogram.types import FSInputFile
from aiogram.filters import Command
import json
from pathlib import Path

# список админов
ADMIN_IDS = [6861499989]  # сюда свои Telegram ID
DATA_FILE = Path("/var/data/data.json")  # путь к твоему файлу

# --- команды для админов ---

@dp.message(Command("dumpdata"))
async def cmd_dumpdata(message: types.Message):
    """Отправить весь data.json как файл (только админам)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Эта команда доступна только админам.")
        return
    try:
        # Сохраняем данные во временный файл
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            temp_path = f.name
        
        file = FSInputFile(temp_path)
        await message.answer_document(file, caption="📂 Текущий data.json")
        
        # Удаляем временный файл
        try:
            os.unlink(temp_path)
        except:
            pass
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке файла: {e}")

@dp.message(Command("showdata"))
async def cmd_showdata(message: types.Message):
    """Показать первые строки data.json текстом (только админам)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Эта команда доступна только админам.")
        return
    try:
        # Преобразуем данные в JSON строку
        data_json = json.dumps(data, ensure_ascii=False, indent=2)
        lines = data_json.split('\n')
        preview = "\n".join(lines[:30])  # первые 30 строк
        
        # Если данных много, показываем статистику
        players_count = len(data.get("players", {}))
        market_count = len(data.get("market", []))
        info = f"📊 Статистика:\n👥 Игроков: {players_count}\n🏪 Лотов на рынке: {market_count}\n\n"
        
        await message.answer(f"{info}📄 Первые строки data.json:\n\n<pre>{preview}</pre>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка при чтении данных: {e}")

@dp.message(Command("removeitem"))
async def cmd_removeitem(message: types.Message):
    """Удалить предмет из инвентаря по ID (только для админов)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Эта команда доступна только администраторам.")
        return
    
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("Использование: /removeitem [user_id] [item_id]")
        return
    
    try:
        target_user_id = int(parts[1])
        item_id = parts[2]
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте: /removeitem [user_id] [item_id]")
        return
    
    # Проверяем существование пользователя
    target_user = get_user(target_user_id)
    if not target_user:
        await message.answer(f"❌ Пользователь с ID {target_user_id} не найден.")
        return
    
    # Ищем предмет в инвентаре
    inventory = target_user.get("inventory", [])
    removed_item = None
    
    for i, item in enumerate(inventory):
        if item.get("id") == item_id:
            removed_item = inventory.pop(i)
            break
    
    if not removed_item:
        await message.answer(f"❌ Предмет с ID {item_id} не найден в инвентаре пользователя.")
        return
    
    # Сохраняем изменения
    save_user(target_user)
    
    await message.answer(
        f"✅ Предмет удален из инвентаря пользователя "
        f"@{target_user.get('username', target_user.get('name', target_user_id))} (ID: {target_user_id})\n"
        f"📱 Удаленный телефон: {removed_item.get('phone', 'Неизвестно')}"
    )
    
    # Отправляем уведомление пользователю
    try:
        await bot.send_message(
            target_user_id,
            f"🗑️ Администратор удалил предмет из вашего инвентаря:\n"
            f"📱 Телефон: {removed_item.get('phone', 'Неизвестно')}"
        )
    except Exception as e:
        await message.answer(f"⚠️ Предмет удален, но не удалось отправить уведомление пользователю: {e}")


@dp.message(Command("givephone"))
async def cmd_givephone(message: types.Message):
    """Выдать любой телефон пользователю (только для админов)"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Эта команда доступна только администраторам.")
        return
    
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("Использование: /givephone [user_id] [phone_name]")
        return
    
    try:
        target_user_id = int(parts[1])
        phone_name = parts[2]
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте: /givephone [user_id] [phone_name]")
        return
    
    # Проверяем существование пользователя
    target_user = get_user(target_user_id)
    if not target_user:
        await message.answer(f"❌ Пользователь с ID {target_user_id} не найден.")
        return
    
    # Определяем редкость телефона по его номеру
    phone_rarity = None
    for rarity, phones in phone_pool.items():
        if phone_name in phones:
            phone_rarity = rarity
            break
    
    if not phone_rarity:
        await message.answer(f"❌ Телефон {phone_name} не найден в базе.")
        return
    
    # Добавляем телефон в инвентарь
    phone_item = {
        "phone": phone_name,
        "rarity": phone_rarity,
        "price": 0  # Бесплатный телефон от админа
    }
    # Генерируем имя изображения (как в команде /claim)
    image_name = EXCLUSIVE_IMAGE_MAP.get(phone_name, phone_name.replace(" ", "_") + ".png")
    phone_item["image"] = image_name
    _ensure_item_id(phone_item)  # Генерируем уникальный ID для предметa
    
    target_user.setdefault("inventory", []).append(phone_item)
    save_user(target_user)
    
    emoji = rarity_emojis.get(phone_rarity, "")
    await message.answer(
        f"✅ Телефон {phone_name} ({emoji}{phone_rarity}) выдан пользователю "
        f"@{target_user.get('username', target_user.get('name', target_user_id))} (ID: {target_user_id})"
    )
    
    # Отправляем уведомление пользователю
    try:
        await bot.send_message(
            target_user_id,
            f"🎁 Вы получили подарок от администратора!\n"
            f"📱 Телефон: {phone_name} ({emoji}{phone_rarity})"
        )
    except Exception as e:
        await message.answer(f"⚠️ Телефон выдан, но не удалось отправить уведомление пользователю: {e}")


from aiogram.filters import Command
from aiogram import types
import asyncio

ADMINS = {6861499989}  # твой ID

@dp.message(Command("msg"))
async def cmd_msg(message: types.Message):
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У тебя нет прав на рассылку.")
        return

    # текст рассылки
    parts = (message.text or "").split(maxsplit=1)
    text = parts[1] if len(parts) > 1 else None

    # проверяем, есть ли фото
    photo = None
    if message.photo:
        photo = message.photo[-1].file_id  # берём самое большое фото

    players = data.get("players", {})
    total, success, failed = 0, 0, 0
    failed_ids = []

    for uid in players.keys():
        total += 1
        try:
            if photo:
                await bot.send_photo(int(uid), photo, caption=text or "")
            else:
                await bot.send_message(int(uid), text or " ")
            success += 1
        except Exception as e:
            failed += 1
            failed_ids.append(uid)
            print(f"Ошибка отправки {uid}: {e}")
        await asyncio.sleep(0.1)  # антиспам

    report = (
        f"📢 Рассылка завершена\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибки: {failed}\n"
        f"👥 Всего пользователей: {total}"
    )
    if failed_ids:
        report += f"\n🚫 Недоступные ID: {', '.join(failed_ids)}"

    await message.answer(report)



# === Команда /start ===
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    user = get_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    username = display_username(user)

    # deep-link обработка
    args = None
    try:
        parts = (message.text or "").split(maxsplit=1)
        args = parts[1] if len(parts) > 1 else None
    except Exception:
        args = None

    if args:
        a = args.strip()
        if a.startswith("market"):
            await cmd_market(message)
            return
        if a.startswith("sell"):
            if message.chat.type != "private":
                await message.answer("Пожалуйста, откройте бота в личных сообщениях для подачи объявления.")
                return
            await sell_command(message, state)
            return

    # обычный старт
    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Информация", callback_data="start_info")
    kb.button(text="➕ Добавить бота в чат", url="https://t.me/mxphone_bot?startgroup=true")
    kb.adjust(1, 1)

    await message.answer(
        f"👋🏻 Добро пожаловать, {username}!\n\n"
        "📱 В MxPhoneBot вы можете:\n"
        "• 📦 Собирать коллекции телефонов\n"
        "• 🤝 Торговаться с другими игроками\n"
        "• 📈 Развивать экономику\n"
        "• 🛠 И многое другое!\n\n"
        "🎁 Чтобы получить свою первую карточку — просто напишите: <b>Слаим</b>\n\n"
        "👇 Выберите действие:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )


@dp.callback_query(lambda c: c.data == "start_info")
async def cb_start_info(callback: CallbackQuery):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="start_back")
    kb.adjust(1)

    await callback.message.edit_text(
        "📖 Команды: /commands или \"команды\"\n\n"
        "👨‍💻 <b>Разработчики бота</b>\n"
        "• 👑 Создатель и главный кодер: @mixam_max\n"
        "• 🎨 Главный дизайнер: @hleb1kk\n\n"
        "📢 <b>Телеграм-канал бота</b>\n"
        "https://t.me/mixam_channel\n\n"
        "🐞 Нашли баг или хотите предложить идею?\n"
        "Напишите в бот поддержки: @mxphone_support_bot",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "start_back")
async def cb_start_back(callback: CallbackQuery):
    user = get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    username = display_username(user)

    kb = InlineKeyboardBuilder()
    kb.button(text="ℹ️ Информация", callback_data="start_info")
    kb.button(text="➕ Добавить бота в чат", url="https://t.me/mxphone_bot?startgroup=true")
    kb.adjust(1, 1)

    await callback.message.edit_text(
        f"👋🏻 Добро пожаловать, {username}!\n\n"
        "📱 В MxPhoneBot вы можете:\n"
        "• 📦 Собирать коллекции телефонов\n"
        "• 🤝 Торговаться с другими игроками\n"
        "• 📈 Развивать экономику\n"
        "• 🛠 И многое другое!\n\n"
        "🎁 Чтобы получить свою первую карточку — просто напишите: <b>Слаим</b>\n\n"
        "👇 Выберите действие:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await callback.answer()

    




# === Команда /commands ===
@dp.message(Command("commands"))
async def commands(message: types.Message):
    await message.answer(
        "📜 Команды MxPhoneBot:\n\n"
        "🔹 /claim, слаим — получить случайный телефон (раз в 2 часа)\n"
        "🔹 /daily, ежедневная награда — получить ежедневную награду\n"
        "🔹 /inv, инв — показать твой инвентарь\n"
        "🔹 /account, аккаунт — профиль игрока: имя, баланс, статистика\n"
        "🔹 /sell, продажа — продать телефон из инвентаря\n"
        "🔹 /repair, починить — починить сломанный телефон\n"
        "🔹 /shop, магазин — магазин с телефонами\n"
        "🔹 /market, рынок — рынок игроков, покупка/продажа\n"
        "🔹 /combine, слияние — объединить одинаковые телефоны\n"
        "🔹 /leaderboard, лидерборд — топ игроков по балансу\n"
        "🔹 /pay — перевести монеты другому игроку: /pay @username сумма\n"
        "🔹 /transfers — история переводов\n"
        "🔹 /give — передать предмет другому игроку: /give @username\n"
        "🔹 /techinfo, техинфо — техническая информация о боте\n"
        "🔹 /ping, пинг — проверить отклик бота\n"
        "🔹 /commands, команды — показать этот список команд\n\n"
    )


# ---------- add_exclusive_via_data.py (вставь в bot.py) ----------
import json
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from aiogram import types

DATA_PATH = Path("data.json")
TEMPLATES_PATH = Path("phones_templates.json")
ADMINS = {6861499989}  # <- замени на свой numeric id

def load_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json_atomic(path: Path, data: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def find_template(key: str) -> Optional[Dict[str, Any]]:
    tpl_list = load_json(TEMPLATES_PATH) or []
    for t in tpl_list:
        if t.get("template_id") == key or t.get("name") == key:
            return t
    return None

def get_user_from_data(user_id: int):
    data = load_json(DATA_PATH) or {}
    return data.get(str(user_id)), data

def instantiate_from_template(template: Dict[str, Any], granted_by: Dict[str, Any]) -> Dict[str, Any]:
    inst = dict(template)  # shallow copy of template fields (emoji, image, name, rarity, etc.)
    inst["id"] = str(uuid.uuid4())
    inst["template_id"] = template.get("template_id")
    # normalize fields used by your inventory (phone vs name)
    if "phone" not in inst:
        inst["phone"] = template.get("name") or inst.get("phone")
    inst.setdefault("price", 0)
    inst.setdefault("chance", 0)
    inst.setdefault("meta", {})
    inst["meta"]["granted_by"] = granted_by
    return inst

async def add_exclusive_to_user_via_data(admin_user: types.User, target_user_id: int, template_key: str) -> Dict[str, Any]:
    tpl = find_template(template_key)
    if not tpl:
        raise RuntimeError("Template not found")

    user_obj, data_all = get_user_from_data(target_user_id)
    if user_obj is None:
        raise RuntimeError("User not found in data.json")

    item = instantiate_from_template(tpl, {"id": admin_user.id, "username": admin_user.username})
    user_obj.setdefault("inventory", []).append(item)

    # Save back
    data_all[str(target_user_id)] = user_obj
    save_json_atomic(DATA_PATH, data_all)

    # Try to notify user (best-effort)
    try:
        await dp.bot.send_message(
            target_user_id,
            f"🎁 Тебе выдан предмет: <b>{item.get('name') or item.get('phone')}</b>\nРедкость: {item.get('rarity')} {item.get('emoji','')}",
            parse_mode="HTML"
        )
    except Exception:
        pass

    return item


# === Команда /claim ===
from datetime import datetime, timedelta, timezone
import uuid, os
from aiogram import types
from aiogram.filters import Command
from aiogram.types import CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

SELL_COEF = 0.75
BROKEN_SELL_COEF = 0.05  # Сломанные телефоны продаются за 5% от цены
REPAIR_COEF = 0.40  # Починка стоит 40% от цены целого телефона
BROKEN_CHANCE = 0.01  # Шанс выпадения сломанного телефона (1%)
COOLDOWN_HOURS = 2  # кулдаун 2 часа

# === Команда /claim ===
@dp.message(Command("claim"))
async def claim(message: types.Message):
    user = get_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    now = datetime.now(timezone.utc)

    raw_last = user.get("last_claim", "0")
    if raw_last != "0":
        last = datetime.fromisoformat(raw_last)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    else:
        last = datetime(2000, 1, 1, tzinfo=timezone.utc)

    if now - last < timedelta(hours=COOLDOWN_HOURS):
        remaining = timedelta(hours=COOLDOWN_HOURS) - (now - last)
        total_seconds = int(remaining.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        time_str = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes} мин"
        await message.answer(f"@{user.get('username') or user['name']}, подожди ещё {time_str} перед следующим получением телефона.")
        return

    lock = _get_user_lock(message.from_user.id)
    async with lock:
        user = get_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
        now = datetime.now(timezone.utc)

        raw_last = user.get("last_claim", "0")
        if raw_last != "0":
            last = datetime.fromisoformat(raw_last)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
        else:
            last = datetime(2000, 1, 1, tzinfo=timezone.utc)

        if now - last < timedelta(hours=COOLDOWN_HOURS):
            remaining = timedelta(hours=COOLDOWN_HOURS) - (now - last)
            total_seconds = int(remaining.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            time_str = f"{hours}ч {minutes}мин" if hours > 0 else f"{minutes} мин"
            await message.answer(f"@{user.get('username') or user['name']}, подожди {time_str} перед следующим получением телефона.")
            return

        # Генерируем телефон
        rarity, phone_name, price, is_broken = get_random_phone()
        image_name = EXCLUSIVE_IMAGE_MAP.get(phone_name, phone_name.replace(" ", "_") + ".png")
        image_path = os.path.join("phones", image_name)

        item = {
            "id": uuid.uuid4().hex[:8],
            "phone": phone_name,
            "rarity": rarity,
            "price": price,
            "image": image_name,
            "broken": is_broken
        }

        user.setdefault("inventory", []).append(item)
        user["last_claim"] = now.isoformat()
        update_user(message.from_user.id, user)  # сохраняем изменения

        # Цена продажи зависит от того, сломан ли телефон
        sell_coef = BROKEN_SELL_COEF if is_broken else SELL_COEF
        sell_price = int(price * sell_coef)
        kb = InlineKeyboardBuilder()
        kb.button(
            text="Продать",
            callback_data=f"sell_confirm|{message.from_user.id}|{item['id']}|{sell_price}"
        )
        kb.adjust(1)

        broken_text = " ⚠️ СЛОМАН" if is_broken else ""
        caption = (
            f"@{user.get('username') or user['name']} Ты получил: {phone_name}{broken_text}\n"
            f"Редкость: {rarity_emojis.get(rarity, '')}{rarity}\n"
            f"💰 Цена: {price} монет"
        )

        if os.path.exists(image_path):
            try:
                # Если телефон сломан, делаем изображение сероватым
                if is_broken:
                    photo_obj = _create_broken_image(image_path)
                else:
                    photo_obj = FSInputFile(image_path)
                await message.answer_photo(photo=photo_obj, caption=caption, reply_markup=kb.as_markup())
            except Exception as e:
                await message.answer(caption + f"\n⚠️ Ошибка при загрузке изображения: {e}", reply_markup=kb.as_markup())
        else:
            await message.answer(caption + "\n⚠️ Картинка не найдена.", reply_markup=kb.as_markup())


# === SELL CONFIRM (кнопка) ===
@dp.callback_query(lambda c: c.data and c.data.startswith("sell_confirm|"))
async def sell_confirm(callback: CallbackQuery):
    try:
        _, owner_id, item_id, sell_price = callback.data.split("|")
        owner_id, sell_price = int(owner_id), int(sell_price)
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != owner_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(owner_id, callback.from_user.username)
    inv = user.get("inventory", [])
    item = next((i for i in inv if i["id"] == item_id), None)
    if not item:
        await callback.answer("❌ Этот предмет уже недоступен.", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Продать", callback_data=f"sell_final|{owner_id}|{item_id}|{sell_price}")
    kb.button(text="❌ Отмена", callback_data=f"sell_cancel|{owner_id}|{item_id}|{sell_price}")
    kb.adjust(2)

    text_to_put = f"@{user.get('username') or user['name']}, ты точно хочешь продать {item['phone']} за {sell_price}💰?"
    await safe_edit(callback.message, text=text_to_put, caption=text_to_put, reply_markup=kb.as_markup())
    await callback.answer()


# === SELL CANCEL (кнопка) ===
@dp.callback_query(lambda c: c.data and c.data.startswith("sell_cancel|"))
async def sell_cancel(callback: CallbackQuery):
    try:
        _, owner_id, item_id, sell_price = callback.data.split("|")
        owner_id, sell_price = int(owner_id), int(sell_price)
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != owner_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(owner_id, callback.from_user.username)
    inv = user.get("inventory", [])
    item = next((i for i in inv if i["id"] == item_id), None)
    if not item:
        await callback.answer("❌ Этот предмет уже недоступен.", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="Продать", callback_data=f"sell_confirm|{owner_id}|{item_id}|{sell_price}")
    kb.adjust(1)

    rarity = item.get("rarity", "")
    rarity_emoji = rarity_emojis.get(rarity, "")
    display_text = (
        f"@{user.get('username') or user['name']}, тебе выпал {item['phone']}\n"
        f"Редкость: {rarity_emoji}{rarity}{rarity_emoji}\n"
        f"💰 Цена: {item['price']} монет"
    )

    await safe_edit(callback.message, text=display_text, caption=display_text, reply_markup=kb.as_markup())
    await callback.answer()


# === SELL FINAL (кнопка) ===
@dp.callback_query(lambda c: c.data and c.data.startswith("sell_final|"))
async def sell_final(callback: CallbackQuery):
    try:
        _, owner_id, item_id, sell_price = callback.data.split("|")
        owner_id, sell_price = int(owner_id), int(sell_price)
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != owner_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(owner_id, callback.from_user.username)
    inv = user.get("inventory", [])
    item = next((i for i in inv if i["id"] == item_id), None)
    if not item:
        await callback.answer("❌ Этот предмет уже продан или недоступен.", show_alert=True)
        return

    inv.remove(item)
    user["balance"] = user.get("balance", 0) + sell_price
    user["inventory"] = inv
    update_user(owner_id, user)  # сохраняем изменения

    new_caption = (
        f"@{user.get('username') or user['name']}, ты продал {item['phone']} за {sell_price} монет!\n"
        f"💰 Баланс: {user['balance']}"
    )
    await safe_edit(callback.message, text=new_caption, caption=new_caption, reply_markup=None)
    await callback.answer()



# === Команда /inv ===
import uuid
from aiogram.types import FSInputFile, InputMediaPhoto
from typing import Dict, List, Optional, Tuple
from aiogram import types
from aiogram.types import CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ---------------- Config / Constants ----------------
ITEMS_PER_PAGE = 9999  # не используется, оставлено для совместимости
_inventory_sessions: Dict[int, Dict] = {}  # user_id -> {step, rarity, msg_id, chat_id, current_item_id}

# ---------------- Helpers ----------------
def _ensure_item_id(item: dict):
    if not item.get("id"):
        item["id"] = uuid.uuid4().hex[:8]  # вместо длинного UUID


def _get_rarity_counts(user: dict) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for it in user.get("inventory", []) or []:
        r = it.get("rarity", "Обычный")
        counts[r] = counts.get(r, 0) + 1
    return counts

def _normalize_name(item: dict) -> str:
    # гарантируем, что возьмём удобное поле для отображения
    return item.get("name") or item.get("phone") or item.get("title") or "Unknown"

def _collect_names_and_sample_ids(inv_list: List[dict]) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    """
    Возвращает:
      - список (display_name, sample_item_id) в порядке первого появления
      - словарь counts {name: count}
    """
    counts: Dict[str, int] = {}
    first_ids: Dict[str, str] = {}
    order: List[str] = []
    for it in inv_list:
        name = _normalize_name(it)
        counts[name] = counts.get(name, 0) + 1
        if name not in first_ids:
            _ensure_item_id(it)
            first_ids[name] = it["id"]
            order.append(name)
    names_and_ids = [(n, first_ids[n]) for n in order]
    return names_and_ids, counts

def _find_item_by_id(inventory: List[dict], item_id: str) -> Optional[dict]:
    for it in inventory:
        if it.get("id") == item_id:
            return it
    return None

def _count_global_occurrences(phone_name: str) -> int:
    total = 0
    for uid, user in _get_players_map().items():
        inv = user.get("inventory", []) or []
        total += sum(1 for it in inv if _normalize_name(it) == phone_name)
    return total

from aiogram.types import Message

async def _safe_edit_or_send(msg: Message, *, text: Optional[str] = None, media: Optional[InputMediaPhoto] = None, reply_markup=None):
    try:
        if media:
            await msg.edit_media(media=media, reply_markup=reply_markup)
        elif text:
            await msg.edit_text(text, reply_markup=reply_markup)
    except Exception:
        if media:
            await msg.answer_photo(photo=media.media, caption=media.caption, reply_markup=reply_markup)
        elif text:
            await msg.answer(text, reply_markup=reply_markup)

async def safe_edit(msg: Message, *, text: Optional[str] = None, caption: Optional[str] = None, reply_markup=None):
    """Безопасное редактирование сообщения (текст или подпись к фото)"""
    # Определяем, какой текст использовать (приоритет caption, если оба переданы)
    content = caption if caption is not None else text
    
    try:
        # Если сообщение содержит фото, редактируем caption
        if msg.photo:
            if caption is not None:
                await msg.edit_caption(caption=caption, reply_markup=reply_markup)
            elif text is not None:
                # Пробуем отредактировать caption текстом, если нет caption
                try:
                    await msg.edit_caption(caption=text, reply_markup=reply_markup)
                except Exception:
                    # Если не получилось, отправляем новое текстовое сообщение
                    await msg.answer(text=text, reply_markup=reply_markup)
        # Если это текстовое сообщение, редактируем text
        elif text is not None:
            await msg.edit_text(text=text, reply_markup=reply_markup)
        elif caption is not None:
            # Пробуем отредактировать как caption, если не получилось - как text
            try:
                await msg.edit_caption(caption=caption, reply_markup=reply_markup)
            except Exception:
                await msg.edit_text(text=caption, reply_markup=reply_markup)
    except Exception:
        # Если редактирование не удалось, отправляем новое сообщение
        if msg.photo and content is not None:
            try:
                await msg.answer_photo(photo=msg.photo[-1].file_id, caption=content, reply_markup=reply_markup)
            except Exception:
                await msg.answer(text=content, reply_markup=reply_markup)
        elif content is not None:
            await msg.answer(text=content, reply_markup=reply_markup)

def format_phone_name(phone_name: str) -> str:
    """Форматирует название телефона с большой буквы"""
    if not phone_name:
        return phone_name
    # Разделяем по словам и делаем каждое слово с большой буквы
    words = phone_name.split()
    formatted = ' '.join(word.capitalize() for word in words)
    return formatted

def _create_broken_image(image_path: str):
    """Создает сероватое/тусклое изображение для сломанного телефона"""
    try:
        from io import BytesIO
        from aiogram.types import BufferedInputFile
        
        img = Image.open(image_path)
        # Конвертируем в RGB если нужно
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Делаем изображение сероватым и тусклым
        # Уменьшаем яркость и насыщенность
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(0.5)  # Уменьшаем яркость до 50%
        
        enhancer = ImageEnhance.Color(img)
        img = enhancer.enhance(0.3)  # Уменьшаем насыщенность до 30%
        
        # Добавляем серый оттенок
        img = img.convert('L').convert('RGB')  # Сначала в grayscale, потом обратно в RGB
        
        # Сохраняем во временный буфер
        output = BytesIO()
        img.save(output, format='PNG')
        output.seek(0)
        
        return BufferedInputFile(output.read(), filename="broken_phone.png")
    except Exception as e:
        print(f"⚠️ Ошибка при создании сломанного изображения: {e}")
        # Если не получилось, возвращаем обычное изображение
        return FSInputFile(image_path)

def phone_to_path(phone_name: str) -> str:
    # Приводим название к формату как в именах файлов
    # Каждое слово с большой буквы, пробелы заменяем на подчеркивания
    formatted_name = ' '.join(word.capitalize() for word in phone_name.split())
    filename = formatted_name.replace(" ", "_") + ".png"
    return os.path.join("phones", filename)

# ---------------- Keyboards ----------------
def kb_rarity_list(user: dict):
    counts = _get_rarity_counts(user)
    available = [r for r in (rarity_order if "rarity_order" in globals() else list(counts.keys())) if r in counts]
    kb = InlineKeyboardBuilder()
    for r in available:
        emoji = rarity_emojis.get(r, "")
        kb.button(text=f"{emoji} {r} ({counts[r]})", callback_data=f"inv:r:{r}")
    
    # Добавляем кнопку для сломанных телефонов
    broken_count = sum(1 for item in (user.get("inventory", []) or []) if item.get("broken", False))
    if broken_count > 0:
        kb.button(text=f"⚠️ Сломаные ({broken_count})", callback_data=f"inv:r:Сломаные")
    
    kb.adjust(1)  # по одному в ряд
    return kb.as_markup()

def kb_list_all(owner_id: int, names_and_ids: List[Tuple[str, str]], counts: Dict[str, int]):
    kb = InlineKeyboardBuilder()
    for name, sample_id in names_and_ids:
        cnt = counts.get(name, 0)
        label = f"{name} х{cnt}" if cnt > 1 else name
        kb.button(text=label, callback_data=f"inv:item:{owner_id}:{sample_id}")
    kb.adjust(1)
    kb.row()
    kb.button(text="⬅ Назад к редкости", callback_data=f"inv:back:{owner_id}")
    kb.adjust(1)
    return kb.as_markup()

def kb_card(owner_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅ Назад к списку", callback_data=f"inv:back_to_list:{owner_id}")
    kb.adjust(1)
    return kb.as_markup()

# ---------------- Handlers ----------------
@dp.message(Command("inv", "инв"))
async def cmd_inventory(message: types.Message):
    user = get_user(message.from_user.id, message.from_user.username)
    inventory = user.get("inventory", []) or []
    if not inventory:
        await message.answer(f"@{user.get('username') or user['name']}, Твой инвентарь пуст.")
        return

    markup = kb_rarity_list(user)
    text = f"📦 Инвентарь пользователя @{user.get('name','user')}\nВыбери редкость:"
    sent = await message.answer(text, reply_markup=markup)
    _inventory_sessions[message.from_user.id] = {
        "step": "rarity",
        "rarity": None,
        "msg_id": sent.message_id,
        "chat_id": sent.chat.id,
        "current_item_id": None
    }

@dp.callback_query(lambda c: c.data and c.data.startswith("inv:r:"))
async def on_rarity_selected(callback: CallbackQuery):
    # Формат: inv:r:<rarity>
    user_id = callback.from_user.id
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer("Неверные данные.", show_alert=True)
        return
    rarity = parts[2]

    user = get_user(user_id, callback.from_user.username)
    
    # Обработка вкладки "Сломаные"
    if rarity == "Сломаные":
        inv = [it for it in (user.get("inventory", []) or []) if it.get("broken", False)]
        if not inv:
            await callback.answer("У тебя нет сломанных телефонов.", show_alert=True)
            return
        
        # гарантируем id и собираем пары (name, id)
        names_and_ids, counts = _collect_names_and_sample_ids(inv)
        
        text = (
            f"⚠️ Сломаные телефоны\n"
            f"🔢 Количество: {len(inv)}"
        )
        markup = kb_list_all(user_id, names_and_ids, counts)
    else:
        inv = [it for it in (user.get("inventory", []) or []) if it.get("rarity") == rarity and not it.get("broken", False)]
        if not inv:
            await callback.answer("У тебя нет предметов этой редкости.", show_alert=True)
            return

        # гарантируем id и собираем пары (name, id)
        names_and_ids, counts = _collect_names_and_sample_ids(inv)

        text = (
            f"📱 Телефоны редкости {rarity_emojis.get(rarity,'')} {rarity}\n"
            f"🔢 Количество: {len(inv)}"
        )
        markup = kb_list_all(user_id, names_and_ids, counts)

    sess = _inventory_sessions.get(user_id, {})
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        sent = await _replace_or_send(callback.message, text, reply_markup=markup)
        if sent:
            sess["msg_id"] = sent.message_id
            sess["chat_id"] = sent.chat.id

    _inventory_sessions[user_id] = {
        "step": "list",
        "rarity": rarity,
        "msg_id": sess.get("msg_id", callback.message.message_id),
        "chat_id": sess.get("chat_id", callback.message.chat.id),
        "current_item_id": None
    }
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("inv:item:"))
async def on_item_pressed(callback: CallbackQuery):
    # Формат: inv:item:<owner_id>:<item_id>
    parts = callback.data.split(":", 3)
    if len(parts) < 4:
        await callback.answer("Неверные данные.", show_alert=True)
        return
    _, _, owner_str, item_id = parts
    try:
        owner_id = int(owner_str)
    except Exception:
        await callback.answer("Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != owner_id:
        await callback.answer("Это не твоё меню.", show_alert=True)
        return

    user = get_user(owner_id, callback.from_user.username)
    inv = user.get("inventory", []) or []
    item = _find_item_by_id(inv, item_id)
    if not item:
        await callback.answer("Предмет не найден.", show_alert=True)
        return

    _ensure_item_id(item)
    name = _normalize_name(item)
    rarity = item.get("rarity", "Обычный")
    price = item.get("price", 0)
    is_broken = item.get("broken", False)
    # Цена продажи зависит от того, сломан ли телефон
    sell_coef = BROKEN_SELL_COEF if is_broken else SELL_COEF
    sell_price = int(price * sell_coef) if "SELL_COEF" in globals() else price
    count = sum(1 for it in inv if _normalize_name(it) == name)
    global_count = _count_global_occurrences(name)

    # Шанс выпадения
    chance_value = item.get("chance") or rarity_chances.get(rarity)
    if isinstance(chance_value, float) and not chance_value.is_integer():
        chance = f"{round(chance_value, 1)}%"
    elif isinstance(chance_value, (int, float)):
        chance = f"{int(chance_value)}%"
    else:
        chance = str(chance_value) if chance_value is not None else "—"

    broken_text = " ⚠️ СЛОМАН" if is_broken else ""
    caption = (
        f"📱 {name}{broken_text}\n"
        f"{rarity_emojis.get(rarity,'')} Редкость: {rarity}\n"
        f"💵 Цена покупки: {price}\n"
        f"💰 Цена продажи: {sell_price}\n"
        f"🎯 Шанс выпадения: {chance}\n"
        f"🔢 У тебя: {count}\n"
        f"🌍 У всех игроков: {global_count}"
    )
    kb = kb_card(owner_id)

    # Путь к картинке
    try:
        img_path = phone_to_path(name)
    except Exception:
        img_path = None

    # Обновляем сессию
    sess = _inventory_sessions.get(owner_id, {})
    sess["current_item_id"] = item.get("id")
    _inventory_sessions[owner_id] = sess

    # Отправка фото телефона
    try:
        if img_path and os.path.exists(img_path):
            # Создаем сломанное изображение если телефон сломан
            if is_broken:
                photo_file = _create_broken_image(img_path)
            else:
                photo_file = FSInputFile(img_path)
            # Удаляем старое сообщение и отправляем новое с фото
            await callback.message.delete()
            await callback.message.answer_photo(photo=photo_file, caption=caption, reply_markup=kb)
        else:
            await callback.message.edit_text(caption + "\n🖼️ Изображение отсутствует.", reply_markup=kb)  # type: ignore
    except Exception as e:
        await callback.message.edit_text(caption + f"\n⚠️ Ошибка при показе фото: {e}", reply_markup=kb)  # type: ignore

    await callback.answer()

# ... (rest of the code remains the same)


@dp.callback_query(lambda c: c.data and c.data.startswith("inv:back_to_list:"))
async def on_back_to_list(callback: CallbackQuery):
    # Формат: inv:back_to_list:<owner_id>
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer()
        return
    _, _, owner_str = parts
    try:
        owner_id = int(owner_str)
    except Exception:
        await callback.answer()
        return


    if callback.from_user.id != owner_id:
        await callback.answer("Это не твоё меню.", show_alert=True)
        return

    sess = _inventory_sessions.get(owner_id)
    if not sess or not sess.get("rarity"):
        await callback.answer("Сессия устарела. Вызови /inventory.", show_alert=True)
        return

    rarity = sess["rarity"]
    user = get_user(owner_id, callback.from_user.username)
    inv = [it for it in (user.get("inventory", []) or []) if it.get("rarity") == rarity]
    if not inv:
        await callback.answer("Нет элементов для показа.", show_alert=True)
        return
    
    try:
        await callback.message.delete()
    except Exception:
        pass


    names_and_ids, counts = _collect_names_and_sample_ids(inv)
    text = (
        f"📱 Телефоны редкости {rarity_emojis.get(rarity,'')} {rarity}\n"
        f"🔢 Количество: {len(inv)}"
    )
    markup = kb_list_all(owner_id, names_and_ids, counts)

    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        sent = await _replace_or_send(callback.message, text, reply_markup=markup)
        if sent:
            sess["msg_id"] = sent.message_id
            sess["chat_id"] = sent.chat.id
        _inventory_sessions[owner_id] = sess

    sess["current_item_id"] = None
    _inventory_sessions[owner_id] = sess
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("inv:back:"))
async def on_back_to_rarity(callback: CallbackQuery):
    # Формат: inv:back:<owner_id>
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer()
        return
    _, _, owner_str = parts
    try:
        owner_id = int(owner_str)
    except Exception:
        await callback.answer("❌ Ошибка обработки запроса", show_alert=True)
        return

    if callback.from_user.id != owner_id:
        await callback.answer("Это не твоё меню.", show_alert=True)
        return

    user = get_user(owner_id, callback.from_user.username)
    counts = _get_rarity_counts(user)
    if not counts:
        _inventory_sessions.pop(owner_id, None)
        try:
            await callback.message.edit_text("📦 Твой инвентарь пуст.")
        except Exception:
            await _replace_or_send(callback.message, "📦 Твой инвентарь пуст.")
        await callback.answer()
        return

    markup = kb_rarity_list(user)
    text = f"📦 Инвентарь пользователя @{user.get('name','user')}\nВыбери редкость:"
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        sent = await _replace_or_send(callback.message, text, reply_markup=markup)
        if sent:
            _inventory_sessions[owner_id] = {
                "step": "rarity",
                "rarity": None,
                "msg_id": sent.message_id,
                "chat_id": sent.chat.id,
                "current_item_id": None
            }
    await callback.answe


# ---------------- Debug command (temporary) ----------------
@dp.message(Command("inv_debug"))
async def inv_debug(message: types.Message):
    user = get_user(message.from_user.id, message.from_user.username)
    inv = user.get("inventory", []) or []
    lines = [f"DEBUG inventory ({len(inv)} items):"]
    for i, it in enumerate(inv, 1):
        lines.append(f"{i}. id={it.get('id')} name={_normalize_name(it)} rarity={it.get('rarity')} price={it.get('price')} chance={it.get('chance')}")
    await message.answer("\n".join(lines))
# ---------------- End of inventory block ----------------




# === Команда /account ===

from io import BytesIO
import os
from PIL import Image, ImageDraw, ImageFilter, ImageFont
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile

@dp.message(Command("account"))
async def account_command(message: Message):
    try:
        user_id = str(message.from_user.id)
        user = get_user(message.from_user.id, message.from_user.username)

        # --- Получаем фото профиля или используем локальный avatar.png ---
        photos = await bot.get_user_profile_photos(message.from_user.id, limit=1)
        if photos.photos:
            file = await bot.get_file(photos.photos[0][-1].file_id)
            photo_bytes = await bot.download_file(file.file_path)
            avatar = Image.open(BytesIO(photo_bytes.read())).convert("RGBA")
        else:
            default_path = os.path.join(os.path.dirname(__file__), "avatar.png")
            if not os.path.exists(default_path):
                await message.answer("⚠️ Ошибка при генерации карточки!")
                return
            avatar = Image.open(default_path).convert("RGBA")

        # --- Настройки карточки ---
        card_width, card_height = 600, 300
        avatar_size = 125
        avatar_pos = (40, 40)  # позиция левого верхнего угла аватарки на карточке

        # Параметры обводки
        border_width = 5
        border_color = (255, 255, 255, 120)  # RGBA (здесь — золотистая)

        # --- Создаём фон карточки (размытие + затемнение) ---
        bg_avatar = avatar.resize((card_width, card_height))
        bg_avatar = bg_avatar.filter(ImageFilter.GaussianBlur(15))
        overlay = Image.new("RGBA", (card_width, card_height), (0, 0, 0, 120))
        card = Image.alpha_composite(bg_avatar, overlay)

        # --- Белая скруглённая ячейка на фоне ---
        box_padding = 25
        box_height = 250
        radius = 35
        box_layer = Image.new("RGBA", card.size, (0, 0, 0, 0))
        draw_box = ImageDraw.Draw(box_layer)
        left = box_padding
        top = card.height - box_height - box_padding
        right = card.width - box_padding
        bottom = card.height - box_padding
        draw_box.rounded_rectangle(
            (left, top, right, bottom),
            radius=radius,
            fill=(255, 255, 255, 30),
            outline=(255, 255, 255, 80),
            width=3
        )
        card = Image.alpha_composite(card, box_layer)

        # --- Круглая аватарка с маской ---
        circular_avatar = avatar.copy().resize((avatar_size, avatar_size), Image.LANCZOS)
        mask = Image.new("L", (avatar_size, avatar_size), 0)
        draw_mask = ImageDraw.Draw(mask)
        draw_mask.ellipse((0, 0, avatar_size - 1, avatar_size - 1), fill=255)
        circular_avatar.putalpha(mask)

        # --- Слой для обводки (чтобы обводка была за аватаркой или поверх — на выбор) ---
        border_layer = Image.new("RGBA", card.size, (0, 0, 0, 0))
        draw_border = ImageDraw.Draw(border_layer)

        # Координаты внешнего прямоугольника для эллипса обводки
        x, y = avatar_pos
        # Немного расширяем bbox на половину толщины обводки, чтобы линия была симметрична
        half = border_width / 2.0
        bbox = (x - half, y - half, x + avatar_size + half, y + avatar_size + half)

        # Рисуем несколько эллипсов для более ровного края (антиалиасинг)
        # Рисуем outline, используя width=border_width (Pillow >=5)
        try:
            # основной контур
            draw_border.ellipse(bbox, outline=border_color, width=border_width)
        except TypeError:
            # На старых версиях Pillow нет параметра width — рисуем несколько концентрических эллипсов
            for i in range(border_width):
                bb = (bbox[0] - i, bbox[1] - i, bbox[2] + i, bbox[3] + i)
                draw_border.ellipse(bb, outline=border_color)

        # Сначала помещаем обводку, затем аватарку поверх — если хочется чтобы обводка была за аватаркой.
        card = Image.alpha_composite(card, border_layer)
        card.paste(circular_avatar, avatar_pos, circular_avatar)

        # --- Вычисляем место пользователя по топу стоимости телефонов ---
        players_map = _get_players_map()
        if not isinstance(players_map, dict) or not players_map:
            place_value = 0
        else:
            ranking_value = sorted(
                [{"uid": uid, "value": sum(item.get("price", 0) for item in u.get("inventory", []))} for uid, u in players_map.items()],
                key=lambda x: x["value"],
                reverse=True
            )
            try:
                place_value = next((i + 1 for i, u in enumerate(ranking_value) if u["uid"] == user_id), len(ranking_value) + 1)
            except Exception:
                place_value = len(ranking_value) + 1

        # --- Шрифты ---
        font_path = os.path.join(os.path.dirname(__file__), "Blazma-Regular.ttf")
        try:
            font_info = ImageFont.truetype(font_path, 28)
            font_top = ImageFont.truetype(font_path, 60)
        except OSError:
            font_info = ImageFont.load_default()
            font_top = ImageFont.load_default()

        # --- Текст на карточке ---
        draw_text = ImageDraw.Draw(card)
        draw_text.text((180, 60), f"Имя: {user.get('name', 'Игрок')}", font=font_info, fill="white")
        draw_text.text((180, 95), f"ТОП {place_value}", font=font_top, fill="white")
        draw_text.text((40, card.height - 125), f"Баланс: {user.get('balance', 0)}", font=font_info, fill="white")
        inventory = user.get("inventory", [])
        draw_text.text((40, card.height - 65), f"Телефонов: {len(inventory)}", font=font_info, fill="white")
        total_value = sum(item.get("price", 0) for item in inventory)
        draw_text.text((40, card.height - 95), f"Общая стоимость: {total_value}", font=font_info, fill="white")

        # --- Создаем caption для отправки вместе с фото ---
        caption = (
            f"👤 Профиль игрока @{user.get('name', 'Игрок')}\n"
            f"📊 Место в топе: {place_value}\n"
            f"💰 Баланс: {user.get('balance', 0)} монет\n"
            f"📱 Телефонов в коллекции: {len(inventory)}\n"
            f"💎 Общая стоимость телефонов: {total_value}"
        )

        # --- Сохраняем и отправляем ---
        output = BytesIO()
        card.save(output, format="PNG")
        output.seek(0)
        await message.answer_photo(
            photo=BufferedInputFile(output.getvalue(), filename="account.png"),
            caption=caption
        )

    except Exception as e:
        print(f"Ошибка при создании карточки: {e}")
        await message.answer("⚠️ Не удалось создать карточку.")



def display_username(user: dict) -> str:
    return f"@{user.get('username')}" if user.get("username") else user.get("name", "без имени")

def format_top(ranking, key_name, label, suffix="", limit=10):
    text = f"{label}\n"
    for i, u in enumerate(ranking[:limit], start=1):
        value = u[key_name]
        text += f"{i}. {display_username(u)} — {value}{suffix}\n"
    return text

@dp.message(Command("leaderboard"))
async def leaderboard_command(message: Message):
    players_map = _get_players_map()
    if not isinstance(players_map, dict) or not players_map:
        await message.answer("📦 Пока нет игроков.")
        return

    # Топ по балансу
    ranking_balance = sorted(
        [{"uid": uid, "username": u.get("username"), "balance": u.get("balance", 0)} for uid, u in players_map.items()],
        key=lambda x: x["balance"], reverse=True
    )
    text_balance = format_top(ranking_balance, "balance", "💰 Топ по балансу:", " монет")

    # Топ по стоимости телефонов
    ranking_value = sorted(
        [{"uid": uid, "username": u.get("username"),
          "value": sum(item.get("price", 0) for item in u.get("inventory", []))}
         for uid, u in players_map.items()],
        key=lambda x: x["value"], reverse=True
    )
    text_value = format_top(ranking_value, "value", "📦 Топ по стоимости телефонов:", " монет")

    # Топ по количеству телефонов
    ranking_count = sorted(
        [{"uid": uid, "username": u.get("username"), "count": len(u.get("inventory", []))} for uid, u in players_map.items()],
        key=lambda x: x["count"], reverse=True
    )
    text_count = format_top(ranking_count, "count", "📱 Топ по количеству телефонов:", " шт.")

    full_text = f"{text_balance}\n{text_value}\n{text_count}"
    await message.answer(full_text)










# === Команда /sell ===


SELL_COEF = 0.75  # пользователь получает 75% от цены телефона при продаже

# === FSM для продажи ===




class SellPhone(StatesGroup):
    choosing_rarity = State()
    choosing_phone = State()

@dp.message(Command("sell"))
async def sell_command(message: types.Message, state: FSMContext):
    user = get_user(message.from_user.id, message.from_user.username)

    inventory = user.get("inventory", [])
    if not inventory:
        await message.answer(f"@{user.get('username') or user['name']}, у тебя нет телефонов для продажи.")
        return

    # Выбор редкости — безопасно
    rarities_in_inventory = sorted(
        {i.get("rarity") for i in inventory if i.get("rarity")},
        key=lambda r: rarity_order.index(r)
    )

    if not rarities_in_inventory:
        await message.answer(f"@{user.get('username') or user['name']}, ни один телефон не имеет редкости.")
        return

    kb = InlineKeyboardBuilder()
    for rarity in rarities_in_inventory:
        kb.button(
            text=f"{rarity_emojis.get(rarity, '')} {rarity}",
            callback_data=f"sell_rarity|{message.from_user.id}|{rarity}"
        )
    kb.adjust(1)

    await message.answer(
        f"📦 @{user.get('username') or user['name']}, выберите редкость телефона для продажи:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(SellPhone.choosing_rarity)


# --- Выбор редкости ---
@dp.callback_query(lambda c: c.data.startswith("sell_rarity|"))
async def sell_rarity(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("|")
    user_id = int(parts[1])
    rarity = parts[2]

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inventory = user.get("inventory", [])
    phones_in_rarity = [i for i in inventory if i.get("rarity") == rarity and i.get("phone")]

    if not phones_in_rarity:
        await callback.answer("❌ У тебя нет телефонов этой редкости.", show_alert=True)
        return

    # Группируем одинаковые телефоны
    phone_counts = {}
    for item in phones_in_rarity:
        phone = item.get("phone")
        if phone:
            phone_counts[phone] = phone_counts.get(phone, 0) + 1

    kb = InlineKeyboardBuilder()
    for phone, count in phone_counts.items():
        kb.button(text=f"{phone} ×{count}", callback_data=f"sell_phone|{user_id}|{phone}")
    kb.button(text="⬅️ Назад", callback_data=f"sell_back|{user_id}")
    kb.adjust(1)

    await state.update_data(chosen_rarity=rarity)
    await callback.message.edit_text(
        f"📱 @{user.get('username') or user['name']}, выберите телефон для продажи:",
        reply_markup=kb.as_markup()
    )
    await state.set_state(SellPhone.choosing_phone)

# --- Назад к выбору редкости ---
@dp.callback_query(lambda c: c.data.startswith("sell_back|"))
async def sell_back(callback: CallbackQuery):
    user_id = int(callback.data.split("|")[1])
    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inventory = user.get("inventory", [])
    rarities_in_inventory = sorted(
        {i.get("rarity") for i in inventory if i.get("rarity")},
        key=lambda r: rarity_order.index(r)
    )

    if not rarities_in_inventory:
        await callback.answer("❌ У тебя нет телефонов с редкостью.", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    for rarity in rarities_in_inventory:
        kb.button(
            text=f"{rarity_emojis.get(rarity, '')} {rarity}",
            callback_data=f"sell_rarity|{user_id}|{rarity}"
        )
    kb.adjust(1)

    await callback.message.edit_text(
        f"📦 @{user.get('username') or user['name']}, выберите редкость телефона для продажи:",
        reply_markup=kb.as_markup()
    )
    await callback.answer()

# --- Выбор телефона ---
@dp.callback_query(StateFilter(SellPhone.choosing_phone), lambda c: c.data.startswith("sell_phone|"))
async def sell_choose_phone(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("|")
    user_id = int(parts[1])
    phone_name = parts[2]

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inventory = user.get("inventory", [])
    items = [i for i in inventory if i.get("phone") == phone_name]

    if not items:
        await callback.answer("❌ Телефон не найден.", show_alert=True)
        return

    quantity = len(items)
    total_price = sum(int(i.get("price", 0) * (BROKEN_SELL_COEF if i.get("broken", False) else SELL_COEF)) for i in items)
    rarity = items[0].get("rarity", "❓")

    kb = InlineKeyboardBuilder()
    kb.button(text=f"✅ Продать 1", callback_data=f"sell_one|{user_id}|{phone_name}")
    kb.button(text=f"💰 Продать все ({quantity}×)", callback_data=f"sell_all|{user_id}|{phone_name}")
    kb.button(text="⬅️ Назад", callback_data=f"sell_back|{user_id}")
    kb.adjust(2)

    await callback.message.edit_text(
        f"📱 @{user.get('username') or user['name']}\n"
        f"Телефон: {phone_name}\n"
        f"⭐ Редкость: {rarity}\n"
        f"📦 Количество: {quantity}\n"
        f"💰 Суммарная цена: {total_price} монет\n"
        f"💳 Баланс: {user['balance']} монет\n\n"
        f"Выберите действие:",
        reply_markup=kb.as_markup()
    )
    await callback.answer()

# --- Продажа одного телефона ---
@dp.callback_query(lambda c: c.data.startswith("sell_one|"))
async def sell_one(callback: CallbackQuery):
    parts = callback.data.split("|")
    user_id = int(parts[1])
    phone_name = parts[2]

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inventory = user.get("inventory", [])
    item = next((i for i in inventory if i.get("phone") == phone_name), None)

    if not item:
        await callback.answer("❌ Телефон не найден.", show_alert=True)
        return

    is_broken = item.get("broken", False)
    sell_coef = BROKEN_SELL_COEF if is_broken else SELL_COEF
    sell_price = int(item.get("price", 0) * sell_coef)
    inventory.remove(item)
    user["balance"] = user.get("balance", 0) + sell_price
    user["inventory"] = inventory
    update_user(user_id, user)

    await callback.message.edit_text(
        f"✅ @{user.get('username') or user['name']}, продано 1× {phone_name} за {sell_price} монет.\n"
        f"💳 Баланс: {user['balance']} монет"
    )
    await callback.answer()

# --- Продажа всех телефонов ---
@dp.callback_query(lambda c: c.data.startswith("sell_all|"))
async def sell_all(callback: CallbackQuery):
    parts = callback.data.split("|")
    user_id = int(parts[1])
    phone_name = parts[2]

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inventory = user.get("inventory", [])
    items_to_sell = [i for i in inventory if i.get("phone") == phone_name]

    if not items_to_sell:
        await callback.answer("❌ Телефон не найден.", show_alert=True)
        return

    total_price = sum(int(i.get("price", 0) * (BROKEN_SELL_COEF if i.get("broken", False) else SELL_COEF)) for i in items_to_sell)
    quantity = len(items_to_sell)

    # Удаляем все подходящие телефоны
    user["inventory"] = [i for i in inventory if i.get("phone") != phone_name]
    user["balance"] = user.get("balance", 0) + total_price
    update_user(user_id, user)

    await callback.message.edit_text(
        f"✅ @{user.get('username') or user['name']}, продано {quantity}× {phone_name} за {total_price} монет.\n"
        f"💳 Баланс: {user['balance']} монет"
    )
    await callback.answer()




# ============ Команда /shop ============== 
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Хранилище последних сообщений магазина
shop_sessions = {}  # user_id -> msg_id

@dp.message(Command("shop"))
async def shop_cmd(message: types.Message, user_id: int = None):
    if user_id is None:
        user_id = message.from_user.id

    # Удаляем старое сообщение, если есть
    old_msg_id = shop_sessions.get(user_id)
    if old_msg_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=old_msg_id)
        except Exception:
            pass

    # Создаём клавиатуру
    kb = InlineKeyboardBuilder()
    for rarity in rarity_order:
        if rarity not in ("Эксклюзив", "Платина", "Экcклюзив"):
            kb.button(
                text=f"{rarity_emojis.get(rarity, '')} {rarity}",
                callback_data=f"shop_rarity|{user_id}|{rarity}"
            )
    kb.adjust(2)

    # Получаем @username или 👤
    mention = f"@{message.from_user.username}" if message.from_user.username else "👤"

    # Отправляем сообщение
    sent = await message.answer(
        f"{mention}, 🛒 выберите редкость:",
        reply_markup=kb.as_markup()
    )
    shop_sessions[user_id] = sent.message_id

    return sent



@dp.callback_query(lambda c: c.data and c.data.startswith("shop_rarity|"))
async def shop_rarity(callback: types.CallbackQuery):
    parts = callback.data.split("|")
    if len(parts) != 3:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    user_id_str, rarity = parts[1], parts[2]
    try:
        user_id = int(user_id_str)
    except ValueError:
        await callback.answer("❌ Неверный ID.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    items = list(phones.get(rarity, {}).items())
    if not items:
        await callback.answer("❌ Нет телефонов этой редкости.", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    for idx, (name, price) in enumerate(items):
        kb.button(
            text=f"{name} ({price}💰)",
            callback_data=f"shop_phone|{user_id}|{rarity}|{idx}"
        )
    kb.button(text="🔙 Назад", callback_data=f"shop_back|{user_id}")
    kb.adjust(1)

    mention = f"@{callback.from_user.username}" if callback.from_user.username else "👤"
    text = f"{mention}, {rarity_emojis.get(rarity,'')} выбери телефон редкости {rarity}:"

    try:
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        sent = await callback.message.answer(text, reply_markup=kb.as_markup())
        shop_sessions[user_id] = sent.message_id

    await callback.answer()



@dp.callback_query(lambda c: c.data and c.data.startswith("shop_phone|"))
async def shop_phone(callback: types.CallbackQuery):
    parts = callback.data.split("|")
    if len(parts) != 4:
        await callback.answer("❌ Неверный формат данных.")
        return

    user_id_str, rarity, idx_str = parts[1], parts[2], parts[3]
    try:
        user_id = int(user_id_str)
        idx = int(idx_str)
    except ValueError:
        await callback.answer("❌ Неверные данные.")
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    phone_list = list(phones.get(rarity, {}).items())
    if idx >= len(phone_list):
        await callback.answer("❌ Телефон не найден.")
        return

    phone_name, price = phone_list[idx]
    mention = f"@{callback.from_user.username}" if callback.from_user.username else "👤"
    caption = (
        f"{mention}, выбери телефон для покупки:\n\n"
        f"{phone_name}\n"
        f"💰 Цена: {price} монет\n\n"
        f"Нажми кнопку, чтобы купить."
    )

    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"✅ Купить за {price}",
        callback_data=f"shop_buy|{user_id}|{phone_name}|{price}|{rarity}"
    )
    kb.button(
        text="🔙 Назад",
        callback_data=f"shop_rarity|{user_id}|{rarity}"
    )
    kb.adjust(1)

    try:
        if (img_path := phone_to_path(phone_name)):
            file = FSInputFile(img_path)
            sent = await bot.send_photo(
                chat_id=callback.message.chat.id,
                photo=file,
                caption=caption,
                reply_markup=kb.as_markup()
            )
        else:
            sent = await callback.message.answer(caption, reply_markup=kb.as_markup())

        try:
            await callback.message.delete()
        except Exception:
            pass

        shop_sessions[user_id] = sent.message_id

    except Exception:
        await callback.message.answer(caption, reply_markup=kb.as_markup())

    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("shop_back|"))
async def shop_back(callback: types.CallbackQuery):
    parts = callback.data.split("|")
    if len(parts) != 2:
        await callback.answer("❌ Неверные данные.")
        return

    user_id_str = parts[1]
    try:
        user_id = int(user_id_str)
    except ValueError:
        await callback.answer("❌ Неверный ID.")
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    try:
        await callback.message.delete()
    except Exception:
        pass

    sent = await shop_cmd(callback.message, user_id=callback.from_user.id)
    shop_sessions[user_id] = sent.message_id

    await callback.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("shop_buy|"))
async def shop_buy(callback: types.CallbackQuery):
    parts = callback.data.split("|")
    if len(parts) not in (5, 6):
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    user_id = int(parts[1])
    phone_name = parts[2]
    price = int(parts[3])
    rarity = parts[4]
    qty = int(parts[5]) if len(parts) == 6 else 1

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)

    # --- Лимит 15 покупок в день ---
    today = datetime.utcnow().date()
    user_date = user.get("last_shop_date")
    user_count = user.get("shop_daily_count", 0)
    if user_date != str(today):
        user["last_shop_date"] = str(today)
        user["shop_daily_count"] = 0
        user_count = 0
    if user_count + qty > 15:
        await callback.answer("❌ Лимит 15 покупок в день!", show_alert=True)
        return
    user["shop_daily_count"] = user_count + qty

    total_price = price * qty
    if user.get("balance", 0) < total_price:
        await callback.answer("❌ Недостаточно монет.", show_alert=True)
        return

    user["balance"] -= total_price
    inv = user.setdefault("inventory", [])
    for _ in range(qty):
        inv.append({
            "phone": phone_name,
            "price": price,
            "rarity": rarity,
            "id": uuid.uuid4().hex[:8]

        })
    update_user(user_id, user)

    try:
        await callback.message.delete()
    except Exception:
        pass

    sent = await callback.message.answer(
        f"✅ Куплено {qty}× {phone_name} за {total_price} монет!\n💰 Баланс: {user['balance']}\n📊 Покупок сегодня: {user['shop_daily_count']}/15"
    )
    shop_sessions[user_id] = sent.message_id
    await callback.answer()







import os
import random
from time import perf_counter
from aiogram import types
from aiogram.filters import Command

@dp.message(Command("ping", "пинг"))
async def ping(message: types.Message):
    start = perf_counter()
    msg = await message.answer("Пингую...")
    duration = round((perf_counter() - start) * 1000)
    await msg.edit_text(f"Пинг: {duration} мс")



from aiogram.types import InputMediaPhoto


from datetime import datetime, timedelta, time
from aiogram import types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

DAILY_COOLDOWN_HOURS = 24
DAILY_REWARDS = {
    1: "💰 500 монет",
    2: "💰 1000 монет",
    3: "💰 1500 монет",
    4: "📱 Случайный телефон",
    5: "💰 2000 монет",
    6: "💰 2500 монет",
    7: "🎁 Мистический телефон"
}
DAILY_IMAGES_DIR = os.path.join(os.path.dirname(__file__), "daily_images")

@dp.message(Command("daily"))
async def cmd_daily(message: types.Message):
    user = get_user(message.from_user.id, message.from_user.username)
    now = datetime.now(timezone.utc) + timedelta(hours=2)  # твой локальный часовой пояс
    last_daily = user.get("last_daily", "0")
    last_dt = datetime.fromisoformat(last_daily) if last_daily != "0" else datetime(2000, 1, 1)
    streak = user.get("daily_streak", 0)

    # Сброс streak при пропуске
    if now.date() > last_dt.date() + timedelta(days=1):
        streak = 0

    can_claim = now.date() > last_dt.date()
    next_day = (streak % 7) + 1 if can_claim else (streak % 7)


    lines = [f"@{user.get('username') or user['name']}", f"🔥 Текущий стрик: {streak} дней", "💎 Награды за 7 дней:"]
    for i in range(1, 8):
        prefix = "➡️" if i == next_day else "▫️"
        lines.append(f"{prefix} День {i}: {DAILY_REWARDS[i]}")
    if not can_claim:
        midnight = datetime.combine(now.date() + timedelta(days=1), time(0, 0))
        remaining = midnight - now
        hours = remaining.seconds // 3600
        mins = (remaining.seconds % 3600) // 60
        lines.append(f"\n⏳ Награда уже получена. Подожди {hours}ч {mins}мин")
    caption = "\n".join(lines)

    kb = InlineKeyboardBuilder()
    if can_claim:
        kb.button(
            text=f"Собрать награду (день {next_day})",
            callback_data=f"daily_claim:{next_day}:{message.from_user.id}"
        )
    kb.adjust(1)

    img_path = os.path.join(DAILY_IMAGES_DIR, f"daily_{next_day}.png")
    if os.path.exists(img_path):
        try:
            photo_obj = FSInputFile(img_path)
            await message.answer_photo(photo=photo_obj, caption=caption, reply_markup=kb.as_markup())
        except Exception as e:
            print(f"⚠️ Ошибка при отправке фото daily: {e}")
            await message.answer(caption, reply_markup=kb.as_markup())
    else:
        # Если фото нет, отправляем только текст
        print(f"⚠️ Фото не найдено: {img_path}")
        await message.answer(caption, reply_markup=kb.as_markup())
from aiogram.types import InputMediaPhoto

@dp.callback_query(lambda c: c.data and c.data.startswith("daily_claim:"))
async def cb_daily_claim(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
        if len(parts) != 3:
            await callback.answer("⚠️ Неверный формат кнопки", show_alert=True)
            return

        day = int(parts[1])
        owner_id = int(parts[2])
        if callback.from_user.id != owner_id:
            await callback.answer("⛔ Это не твоя награда", show_alert=True)
            return
    except Exception:
        await callback.answer("⚠️ Ошибка при обработке кнопки", show_alert=True)
        return

    user = get_user(callback.from_user.id, callback.from_user.username)
    now = datetime.utcnow() + timedelta(hours=2)
    last_dt = datetime.fromisoformat(user.get("last_daily", "2000-01-01T00:00:00"))

    if now.date() <= last_dt.date():
        await callback.answer("⏳ Награда уже получена. Подожди.", show_alert=True)
        return

    user["last_daily"] = now.isoformat()
    user["daily_streak"] = user.get("daily_streak", 0) + 1
    reward_msg = ""

    if day == 1:
        user["balance"] += 500
        reward_msg = "💰 Ты получил 500 монет!"
    elif day == 2:
        user["balance"] += 1000
        reward_msg = "💰 Ты получил 1000 монет!"
    elif day == 3:
        user["balance"] += 1500
        reward_msg = "💰 Ты получил 1500 монет!"
    elif day == 4:
        rarity, phone, price, is_broken = get_random_phone()
        item = {"rarity": rarity, "phone": phone, "price": price, "broken": is_broken}
        _ensure_item_id(item)
        user.setdefault("inventory", []).append(item)
        broken_text = " ⚠️ СЛОМАН" if is_broken else ""
        reward_msg = f"📱 Тебе выпал {phone}{broken_text} ({rarity_emojis.get(rarity,'')}{rarity})!"
    elif day == 5:
        user["balance"] += 2000
        reward_msg = "💰 Ты получил 2000 монет!"
    elif day == 6:
        user["balance"] += 2500
        reward_msg = "💰 Ты получил 2500 монет!"
    elif day == 7:
        mythics = list(phones.get("Мистический", {}).items())
        phone, price = random.choice(mythics)
        item = {"rarity": "Мистический", "phone": phone, "price": price}
        _ensure_item_id(item)
        user.setdefault("inventory", []).append(item)
        reward_msg = f"🎁 Ты получил {phone} (🚨Мистический)!"

    update_user(callback.from_user.id, user)

    caption = f"🎉 @{user.get('username') or user['name']}, награда за день {day}:\n{reward_msg}"
    img_path = os.path.join(DAILY_IMAGES_DIR, f"daily_{day}.png")
    
    # Пробуем обновить медиа с фото
    if os.path.exists(img_path):
        try:
            photo_obj = FSInputFile(img_path)
            await callback.message.edit_media(
                media=InputMediaPhoto(media=photo_obj, caption=caption),
                reply_markup=None
            )
        except Exception as e:
            print(f"⚠️ Ошибка при обновлении фото daily: {e}")
            # Если не получилось обновить медиа, пробуем обновить только подпись
            try:
                await callback.message.edit_caption(caption, reply_markup=None)
            except Exception:
                # Если и это не получилось, отправляем новое сообщение
                await callback.message.answer_photo(photo=photo_obj, caption=caption)
    else:
        # Если фото нет, обновляем только текст
        try:
            await callback.message.edit_caption(caption, reply_markup=None)
        except Exception:
            await callback.message.edit_text(caption, reply_markup=None)

    await callback.answer()



# -------------------- Market (stored in SQLite database) --------------------
async def _get_market_list() -> list:
    # Получаем список товаров с рынка из базы данных
    market_items = await db.get_market_list(page_size=1000)  # Большой лимит, чтобы получить все
    # Обновляем кеш для обратной совместимости
    data["market"] = market_items
    return market_items


def _save_market_list(lots: list):
    # В новой версии не сохраняем в память, так как работаем напрямую с БД
    # Оставляем для обратной совместимости
    data["market"] = lots


async def _add_market_lot(lot: dict):
    # Добавляем в базу данных
    seller_id = lot.get("seller_id")
    item_data = lot.get("item", {})
    price = lot.get("price", 0)
    description = lot.get("description", "")
    
    # Если это новый лот, добавляем его в базу
    if seller_id and item_data and price > 0:
        # Добавляем дополнительную информацию в item_data
        item_data_with_meta = item_data.copy()
        item_data_with_meta["description"] = description
        item_data_with_meta["seller_name"] = lot.get("seller_name", str(seller_id))
        
        lot_id = await db.add_market_item(seller_id, item_data_with_meta, price)
        lot["id"] = lot_id  # Обновляем ID лота
    
    # Добавляем в кеш для обратной совместимости
    lots = data.get("market", [])
    lots.append(lot)
    data["market"] = lots


async def _remove_market_lot(lot_id: str):
    # Удаляем из базы данных
    success = await db.remove_market_item(lot_id)
    
    # Обновляем кеш
    if success:
        lots = data.get("market", [])
        data["market"] = [l for l in lots if str(l.get("id")) != str(lot_id)]
    
    return success


async def _get_market_lot(lot_id: str):
    # Пытаемся найти в кеше
    for lot in data.get("market", []):
        if str(lot.get("id")) == str(lot_id):
            return lot
    
    # Если не нашли в кеше, ищем в базе
    lot = await db.get_market_item(lot_id)
    if lot:
        # Добавляем в кеш
        if "market" not in data:
            data["market"] = []
        data["market"].append(lot)
        return lot
    
    return None


# Pagination for market views
MARKET_PAGE_SIZE = 3  # 3 объявления на странице
# Максимальное количество активных объявлений у одного пользователя
MAX_USER_MARKET_LOTS = 5


async def _render_market_page(message: types.Message, lots: list, page: int, prefix: str = "market"):
    """Отображение страницы с объявлениями"""
    items_per_page = MARKET_PAGE_SIZE  # Используем глобальную константу
    total_pages = (len(lots) + items_per_page - 1) // items_per_page if lots else 1
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    page_lots = lots[start_idx:end_idx]
    
    if not page_lots and page > 1:
        return await _render_market_page(message, lots, page - 1, prefix)
    
    kb = InlineKeyboardBuilder()
    
    # Добавляем кнопки для каждого лота
    pages = (total + MARKET_PAGE_SIZE - 1) // MARKET_PAGE_SIZE if total else 1
    page = max(1, min(page, pages))
    start = (page - 1) * MARKET_PAGE_SIZE
    page_lots = all_lots[start:start + MARKET_PAGE_SIZE]

    lines = ["📈 Добро пожаловать на рынок!\n"]

    # Build top row with BUY buttons — always placed in the top row.
    top_buttons = []
    for lot in page_lots:
        item = lot.get("item", {})
        seller = lot.get("seller_name") or str(lot.get("seller_id"))
        # global display index
        try:
            display_idx = all_lots.index(lot) + 1
        except Exception:
            display_idx = "?"
        lines.append(f"🆔 Объявление №{display_idx}\n📱 {item.get('phone')} ({rarity_emojis.get(item.get('rarity',''))} {item.get('rarity')})\n💰 Цена: {lot.get('price')} монет\n📝 Описание: {lot.get('description')}\n🧑 Продавец: @{seller}\n")
        top_buttons.append(InlineKeyboardButton(text=f"Купить {display_idx}", callback_data=f"market_buy:{lot.get('id')}"))

    rows = []
    if top_buttons:
        # place all buy buttons in a single top row
        rows.append(top_buttons)

    # action buttons row (static at the bottom)
    action_row = [
        InlineKeyboardButton(text="📤 Подать объявление", callback_data="market_sell"),
        InlineKeyboardButton(text="📦 Мои объявления", callback_data="market_my"),
    ]
    rows.append(action_row)

    # pagination row: [◀][X/Y][▶]
    pagers = []
    if page > 1:
        prev_cb = f"{prefix}_page:{page-1}"
        if rarity:
            prev_cb = f"{prefix}:{rarity}:{page-1}"
        pagers.append(InlineKeyboardButton(text="◀", callback_data=prev_cb))
    else:
        pagers.append(InlineKeyboardButton(text=" ", callback_data=f"{prefix}_noop"))

    pagers.append(InlineKeyboardButton(text=f"{page}/{pages}", callback_data=f"{prefix}_noop"))

    if page < pages:
        next_cb = f"{prefix}_page:{page+1}"
        if rarity:
            next_cb = f"{prefix}:{rarity}:{page+1}"
        pagers.append(InlineKeyboardButton(text="▶", callback_data=next_cb))
    else:
        pagers.append(InlineKeyboardButton(text=" ", callback_data=f"{prefix}_noop"))

    rows.append(pagers)

    text = "\n".join(lines)
    markup = InlineKeyboardMarkup(inline_keyboard=rows)

    # try to edit existing message when possible (target is CallbackQuery.message), otherwise send new
    try:
        if hasattr(target, 'edit_text'):
            await target.edit_text(text, reply_markup=markup)
        else:
            await target.answer(text, reply_markup=markup)
    except Exception:
        try:
            await target.answer(text, reply_markup=markup)
        except Exception:
            pass


async def _replace_or_send(msg_obj, text, reply_markup=None):
    """Try to edit the message `msg_obj` (CallbackQuery.message). If editing isn't possible,
    send a new message and try to delete the old one to avoid clutter.
    """
    # Try to edit in-place first
    try:
        if hasattr(msg_obj, 'edit_text'):
            await msg_obj.edit_text(text, reply_markup=reply_markup)
            return
    except Exception:
        pass

    # Fallback: send a new message and attempt to delete the old one
    try:
        sent = await msg_obj.answer(text, reply_markup=reply_markup)
        try:
            await msg_obj.delete()
        except Exception:
            pass
        return sent
    except Exception:
        # Last resort: use bot.send_message if possible
        try:
            chat_id = msg_obj.chat.id if hasattr(msg_obj, 'chat') else None
            if chat_id:
                await bot.send_message(chat_id, text, reply_markup=reply_markup)
        except Exception:
            pass



from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery


class MarketPostFSM(StatesGroup):
    choosing_rarity = State()
    choosing_item = State()
    entering_price = State()
    entering_description = State()


# Fallback maps (user_id -> temp data) to support flows when FSM context is lost (e.g., user clicks in group then replies in PM)
_market_pending_price = {}       # user_id -> item dict
_market_pending_description = {} # user_id -> {item, price}


@dp.message(Command("market"))
async def cmd_market(message: types.Message):
    # Force /market to work only in private chat — if called from group, prompt user to open PM
    if message.chat.type != "private":
        try:
            me = await bot.get_me()
            username = getattr(me, "username", None) or ""
            url = f"https://t.me/{username}?start=market"
        except Exception:
            url = None

        kb = InlineKeyboardBuilder()
        if url:
            kb.button(text="Открыть в личных сообщениях", url=url)
        kb.adjust(1)
        await message.answer("📭 Для работы с рынком перейдите в личные сообщения бота.", reply_markup=kb.as_markup())
        return


@dp.callback_query(lambda c: c.data and c.data.startswith("market_buy:"))
async def cb_market_buy(callback: CallbackQuery):
    lot_id = callback.data.split(":", 1)[1]
    lot = await _get_market_lot(lot_id)
    
    if not lot:
        await callback.answer("❌ Объявление не найдено или уже снято с продажи", show_alert=True)
        return
    
    buyer = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    seller_id = lot.get("seller_id")
    seller = await get_user(seller_id)
    
    item = lot.get("item_data", {}) or lot.get("item", {})  # Поддержка старого формата
    price = lot.get("price", 0)
    
    # Проверяем, не пытается ли пользователь купить у самого себя
    if buyer["user_id"] == seller_id:
        await callback.answer("❌ Нельзя купить у самого себя", show_alert=True)
        return
    
    # Проверяем, хватает ли у покупателя денег
    if buyer["balance"] < price:
        await callback.answer("❌ Недостаточно средств для покупки", show_alert=True)
        return
    
    # Проверяем, не продан ли уже товар
    if lot.get("sold", False):
        await callback.answer("❌ Товар уже продан", show_alert=True)
        return
    
    # Подтверждение покупки
    confirm_markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить покупку", callback_data=f"market_confirm:{lot_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="market_cancel")
        ]
    ])
    
    await callback.message.edit_caption(
        caption=f"Вы уверены, что хотите купить {item.get('phone', 'предмет')} за {price} монет?",
        reply_markup=confirm_markup
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("market_confirm:"))
async def cb_market_confirm(callback: CallbackQuery):
    """Подтверждение покупки товара"""
    _, lot_id = callback.data.split(":", 1)
    lot = await _get_market_lot(lot_id)
    
    if not lot:
        await callback.answer("❌ Объявление не найдено или уже снято с продажи", show_alert=True)
        return
    
    buyer = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    seller_id = lot.get("seller_id")
    seller = await get_user(seller_id)
    
    item = lot.get("item_data", {}) or lot.get("item", {})  # Поддержка старого формата
    price = lot.get("price", 0)
    
    # Проверяем, не пытается ли пользователь купить у самого себя
    if buyer["user_id"] == seller_id:
        await callback.answer("❌ Нельзя купить у самого себя", show_alert=True)
        return
    
    # Проверяем, хватает ли у покупателя денег
    if buyer["balance"] < price:
        await callback.answer("❌ Недостаточно средств для покупки", show_alert=True)
        return
    
    # Проверяем, не продан ли уже товар
    if lot.get("sold", False):
        await callback.answer("❌ Товар уже продан", show_alert=True)
        return
    
    # Выполняем транзакцию
    try:
        # Обновляем баланс покупателя
        buyer_updates = {
            "balance": buyer["balance"] - price,
            "inventory": buyer.get("inventory", []) + [item]
        }
        await update_user(buyer["user_id"], buyer_updates)
        
        # Обновляем баланс продавца
        seller_updates = {
            "balance": seller["balance"] + price
        }
        await update_user(seller_id, seller_updates)
        
        # Удаляем лот с рынка
        await _remove_market_lot(lot_id)
        
        # Записываем транзакцию
        await db.record_transaction(
            from_user_id=buyer["user_id"],
            to_user_id=seller_id,
            amount=price,
            item_data=item,
            transaction_type="market_purchase"
        )
        
        # Уведомляем покупателя
        await callback.message.edit_caption(
            caption=f"✅ Вы успешно приобрели {item.get('phone', 'предмет')} за {price} монет!"
        )
        
        # Уведомляем продавца
        try:
            await bot.send_message(
                chat_id=seller_id,
                text=f"🎉 Ваш товар {item.get('phone', 'предмет')} продан за {price} монет!"
            )
        except Exception as e:
            print(f"Не удалось отправить уведомление продавцу {seller_id}: {e}")
        
    except Exception as e:
        print(f"Ошибка при обработке покупки: {e}")
        import traceback
        traceback.print_exc()
        await callback.answer("❌ Произошла ошибка при обработке покупки. Пожалуйста, попробуйте позже.", show_alert=True)


@dp.callback_query(lambda c: c.data == "market_sell")
async def cb_market_sell(callback: CallbackQuery, state: FSMContext):
    # Проверяем, есть ли у пользователя предметы для продажи
    user = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    inventory = user.get("inventory", [])
    
    if not inventory:
        await callback.answer("❌ У вас нет предметов для продажи", show_alert=True)
        return
    
    # Сохраняем информацию о продавце и его инвентаре
    await state.update_data(seller_id=user["user_id"])
    
    # Показываем выбор редкости предмета для продажи
    kb = InlineKeyboardBuilder()
    
    # Группируем предметы по редкости
    rarity_items = {}
    for item in inventory:
        if not isinstance(item, dict):
            continue
            
        rarity = item.get("rarity", "обычный")
        if rarity not in rarity_items:
            rarity_items[rarity] = []
        rarity_items[rarity].append(item)
    
    # Сортируем редкости в порядке убывания редкости
    rarity_order = ["легендарный", "эпический", "редкий", "необычный", "обычный"]
    sorted_rarities = sorted(rarity_items.keys(), 
                           key=lambda x: rarity_order.index(x) if x in rarity_order else len(rarity_order))
    
    # Добавляем кнопки для выбора редкости
    for rarity in sorted_rarities:
        count = len(rarity_items[rarity])
        emoji = rarity_emojis.get(rarity, "")
        kb.button(text=f"{emoji} {rarity.capitalize()} ({count})", callback_data=f"market_rarity:{rarity}")
    
    kb.adjust(2)
    
    # Добавляем кнопку отмены
    kb.row(InlineKeyboardButton(text="❌ Отмена", callback_data="market_cancel"))
    
    await callback.message.edit_caption(
        caption="📦 Выберите редкость предмета, который хотите выставить на продажу:",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("market_rarity:"))
async def cb_market_rarity(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора редкости при продаже предмета"""
    rarity = callback.data.split(":", 1)[1]
    
    # Получаем данные пользователя
    user = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    inventory = user.get("inventory", [])
    
    # Фильтруем предметы по выбранной редкости
    items = [item for item in inventory if isinstance(item, dict) and item.get("rarity") == rarity]
    
    if not items:
        await callback.answer("❌ У вас нет предметов этой редкости", show_alert=True)
        return
    
    # Сохраняем выбранную редкость в состоянии
    await state.update_data(selected_rarity=rarity)
    
    # Создаем клавиатуру с предметами
    kb = InlineKeyboardBuilder()
    
    # Группируем предметы по названию
    item_groups = {}
    for item in items:
        name = item.get("phone", "Неизвестный предмет")
        if name not in item_groups:
            item_groups[name] = []
        item_groups[name].append(item)
    
    # Добавляем кнопки для каждого типа предмета
    for name, group in item_groups.items():
        # Берем первый предмет из группы для отображения иконки
        sample_item = group[0]
        emoji = rarity_emojis.get(sample_item.get("rarity", ""), "")
        kb.button(text=f"{emoji} {name} (x{len(group)})", callback_data=f"market_item:{name}")
    
    kb.adjust(1)
    
    # Добавляем кнопку назад
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="market_sell"))
    
    await callback.message.edit_caption(
        caption=f"📱 Выберите предмет редкости '{rarity}' для продажи:",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("market_item:"))
async def cb_market_item(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора предмета для продажи"""
    item_name = callback.data.split(":", 1)[1]
    
    # Получаем данные пользователя
    user = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    inventory = user.get("inventory", [])
    
    # Находим все предметы с таким именем
    matching_items = [item for item in inventory if isinstance(item, dict) and item.get("phone") == item_name]
    
    if not matching_items:
        await callback.answer("❌ Предмет не найден в вашем инвентаре", show_alert=True)
        return
    
    # Берем первый подходящий предмет
    item = matching_items[0]
    
    # Сохраняем ID предмета в состоянии (индекс в инвентаре)
    item_index = next((i for i, x in enumerate(inventory) 
                      if isinstance(x, dict) and x.get("phone") == item_name), -1)
    
    if item_index == -1:
        await callback.answer("❌ Ошибка: предмет не найден", show_alert=True)
        return
    
    await state.update_data(item_index=item_index, item=item)
    
    # Запрашиваем цену
    await state.set_state(MarketPostFSM.entering_price)
    
    await callback.message.edit_caption(
        caption=f"💰 Введите цену продажи для {item.get('phone', 'предмета')} (от 1 до 1,000,000 монет):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data=f"market_rarity:{item.get('rarity', '')}")]
        ])
    )


@dp.message(MarketPostFSM.entering_price)
async def msg_market_price(message: types.Message, state: FSMContext):
    """Обработка ввода цены для продажи предмета"""
    try:
        price = int(message.text.strip())
        if price < 1 or price > 1_000_000:
            raise ValueError("Цена должна быть от 1 до 1,000,000 монет")
    except ValueError as e:
        await message.answer("❌ Пожалуйста, введите корректную цену (целое число от 1 до 1,000,000):")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    item = data.get("item", {})
    
    # Обновляем состояние
    await state.update_data(price=price)
    await state.set_state(MarketPostFSM.entering_description)
    
    # Запрашиваем описание
    await message.answer(
        f"📝 Введите описание для {item.get('phone', 'вашего предмета')} (не более 200 символов):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="market_cancel")]
        ])
    )


@dp.message(MarketPostFSM.entering_description)
async def msg_market_description(message: types.Message, state: FSMContext):
    """Обработка ввода описания для продажи предмета"""
    description = message.text.strip()
    if len(description) > 200:
        await message.answer("❌ Описание не должно превышать 200 символов. Пожалуйста, введите более короткое описание:")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    item = data.get("item", {})
    price = data.get("price", 0)
    item_index = data.get("item_index")
    
    # Проверяем, что предмет все еще в инвентаре пользователя
    user = await get_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    inventory = user.get("inventory", [])
    
    if item_index >= len(inventory) or not isinstance(inventory[item_index], dict) or \
       inventory[item_index].get("phone") != item.get("phone"):
        await message.answer("❌ Ошибка: предмет больше не найден в вашем инвентаре")
        await state.clear()
        return
    
    # Создаем лот для продажи
    lot = {
        "id": str(uuid.uuid4()),
        "seller_id": user["user_id"],
        "seller_name": user.get("username", str(user["user_id"])),
        "item": item,
        "price": price,
        "description": description,
        "created_at": datetime.now().isoformat(),
        "sold": False
    }
    
    # Добавляем лот на рынок
    await _add_market_lot(lot)
    
    # Удаляем предмет из инвентаря пользователя
    inventory.pop(item_index)
    await update_user(user["user_id"], {"inventory": inventory})
    
    # Очищаем состояние
    await state.clear()
    
    # Отправляем подтверждение
    await message.answer(
        f"✅ Вы успешно выставили на продажу {item.get('phone', 'предмет')} за {price} монет!\n"
        f"📝 Описание: {description}\n\n"
        f"Вы можете управлять своими объявлениями через /market"
    )


@dp.callback_query(lambda c: c.data == "market_cancel")
async def cb_market_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена создания объявления"""
    await state.clear()
    await callback.message.edit_caption(
        "❌ Создание объявления отменено.",
        reply_markup=None
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("give_item:"))
async def cb_give_item(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия на кнопку передачи предмета"""
    try:
        _, item_id = callback.data.split(":", 1)
        item_id = int(item_id)
        
        # Получаем данные пользователя
        user = await get_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
        inventory = user.get("inventory", [])
        
        # Проверяем, что предмет есть в инвентаре
        if item_id < 0 or item_id >= len(inventory):
            await callback.answer("❌ Предмет не найден в вашем инвентаре", show_alert=True)
            return
            
        item = inventory[item_id]
        
        # Сохраняем информацию о передаче в состояние
        await state.update_data(item_id=item_id, item=item)
        await state.set_state("waiting_receiver")
        
        # Запрашиваем ID получателя
        await callback.message.answer(
            f"🆔 Введите ID пользователя, которому хотите передать {item.get('phone', 'предмет')}:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="inventory_cancel")]
            ])
        )
        
    except Exception as e:
        print(f"Ошибка при обработке передачи предмета: {e}")
        await callback.answer("❌ Произошла ошибка при обработке запроса", show_alert=True)


@dp.callback_query(lambda c: c.data and c.data.startswith("give_confirm:"))
async def cb_give_confirm(callback: CallbackQuery):
    # ... (rest of the code remains the same)
    try:
        _, sender_id, receiver_id, item_id = callback.data.split(":")
        sender_id = int(sender_id)
        receiver_id = int(receiver_id)
    except:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return

    if callback.from_user.id != sender_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    sender = get_user(sender_id)
    item = next((i for i in sender.get("inventory", []) if i.get("id") == item_id), None)
    if not item:
        await callback.answer("❌ Предмет не найден", show_alert=True)
        return
    
    # Проверяем, не сломан ли телефон
    if item.get("broken", False):
        await callback.answer("❌ Нельзя передать сломанный телефон!", show_alert=True)
        return

    receiver = get_user(receiver_id)
    sender["inventory"] = [i for i in sender["inventory"] if i.get("id") != item_id]
    receiver.setdefault("inventory", []).append(item)
    update_user(sender_id, sender)
    update_user(receiver_id, receiver)

    await callback.message.edit_text(f"✅ @{sender['name']}, вы передали {item.get('phone')} пользователю @{receiver.get('name')}")
    await callback.answer()

    try:
        await callback.bot.send_message(receiver_id, f"📦 Вам передан предмет: {item.get('phone')} ({item.get('rarity')}) от @{sender.get('name')}")
    except:
        pass


@dp.callback_query(lambda c: c.data == "give_cancel")
async def cb_give_cancel(callback: CallbackQuery):
    await callback.message.edit_text("❌ Передача отменена.")
    await callback.answer()

# === Команда /repair (починить) ===
@dp.message(Command("repair", "починить"))
async def cmd_repair(message: types.Message):
    """Команда для починки сломанных телефонов"""
    user = get_user(message.from_user.id, message.from_user.username)
    inv = user.get("inventory", []) or []
    
    # Находим все сломанные телефоны
    broken_items = [item for item in inv if item.get("broken", False)]
    
    if not broken_items:
        await message.answer(f"⚠️ @{user.get('username') or user['name']}, у тебя нет сломанных телефонов для ремонта.")
        return
    
    # Группируем по названию и редкости
    grouped = {}
    for item in broken_items:
        key = (item.get("phone"), item.get("rarity"))
        if key not in grouped:
            grouped[key] = {
                "item": item,
                "count": 0,
                "price": item.get("price", 0)
            }
        grouped[key]["count"] += 1
    
    kb = InlineKeyboardBuilder()
    for (phone, rarity), data in grouped.items():
        item = data["item"]
        count = data["count"]
        price = data["price"]
        repair_cost = int(price * REPAIR_COEF)
        label = f"{phone} ({rarity}) - {repair_cost}💰"
        if count > 1:
            label += f" x{count}"
        kb.button(
            text=label,
            callback_data=f"repair_item:{message.from_user.id}:{item.get('id')}:{repair_cost}"
        )
    
    kb.adjust(1)
    await message.answer(
        f"🔧 @{user.get('username') or user['name']}, выбери сломанный телефон для починки:\n\n"
        f"💡 Стоимость починки: 40% от цены целого телефона",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(lambda c: c.data and c.data.startswith("repair_item:"))
async def cb_repair_item(callback: CallbackQuery):
    """Обработка выбора телефона для починки"""
    try:
        parts = callback.data.split(":")
        user_id = int(parts[1])
        item_id = parts[2]
        repair_cost = int(parts[3])
    except Exception:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return
    
    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []
    
    # Находим сломанный телефон
    item = next((i for i in inv if i.get("id") == item_id and i.get("broken", False)), None)
    if not item:
        await callback.answer("❌ Сломанный телефон не найден", show_alert=True)
        return
    
    # Проверяем баланс
    if user.get("balance", 0) < repair_cost:
        await callback.answer(f"❌ Недостаточно монет! Нужно {repair_cost}💰", show_alert=True)
        return
    
    # Подтверждение
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"✅ Починить за {repair_cost}💰",
        callback_data=f"repair_confirm:{user_id}:{item_id}:{repair_cost}"
    )
    kb.button(
        text="❌ Отмена",
        callback_data=f"repair_cancel:{user_id}"
    )
    kb.adjust(2)
    
    await callback.message.edit_text(
        f"⚠️ @{user.get('username') or user['name']}, ты хочешь починить {item.get('phone')} ({item.get('rarity')})?\n\n"
        f"💰 Стоимость починки: {repair_cost} монет\n"
        f"💳 Твой баланс: {user.get('balance', 0)} монет",
        reply_markup=kb.as_markup()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("repair_confirm:"))
async def cb_repair_confirm(callback: CallbackQuery):
    """Подтверждение починки"""
    try:
        parts = callback.data.split(":")
        user_id = int(parts[1])
        item_id = parts[2]
        repair_cost = int(parts[3])
    except Exception:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return
    
    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []
    
    # Находим сломанный телефон
    item = next((i for i in inv if i.get("id") == item_id and i.get("broken", False)), None)
    if not item:
        await callback.answer("❌ Сломанный телефон не найден", show_alert=True)
        return
    
    # Проверяем баланс еще раз
    if user.get("balance", 0) < repair_cost:
        await callback.answer(f"❌ Недостаточно монет! Нужно {repair_cost}💰", show_alert=True)
        return
    
    # Починить телефон
    item["broken"] = False
    user["balance"] = user.get("balance", 0) - repair_cost
    user["inventory"] = inv
    update_user(user_id, user)
    
    await callback.message.edit_text(
        f"✅ @{user.get('username') or user['name']}, ты починил {item.get('phone')} ({item.get('rarity')})!\n\n"
        f"💰 Потрачено: {repair_cost} монет\n"
        f"💳 Твой баланс: {user.get('balance', 0)} монет"
    )
    await callback.answer("✅ Телефон починен!")

@dp.callback_query(lambda c: c.data and c.data.startswith("repair_cancel:"))
async def cb_repair_cancel(callback: CallbackQuery):
    """Отмена починки"""
    try:
        parts = callback.data.split(":")
        user_id = int(parts[1])
    except Exception:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        return
    
    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return
    
    await callback.message.edit_text("❌ Починка отменена.")
    await callback.answer()






@dp.message(Command("combine"))
async def cmd_combine(message: types.Message):
    user = get_user(message.from_user.id, message.from_user.username)
    inv = user.get("inventory", []) or []




    # Считаем количество каждого телефона по названию и редкости (исключаем сломанные)
    counter = {}
    for item in inv:
        if not item.get("broken", False):  # Исключаем сломанные
            key = (item.get("rarity"), item.get("phone"))
            counter[key] = counter.get(key, 0) + 1

    # Группируем по редкости, исключая Платину
    rarity_to_phones = {}
    for (rarity, phone), count in counter.items():
        if count >= 2 and rarity != "Платина":
            rarity_to_phones.setdefault(rarity, []).append((phone, count))

    if not rarity_to_phones:
        await message.answer("❌ У тебя нет телефонов, доступных для слияния.")
        return


    # Сортируем редкости по порядку
    available_rarities = sorted(
    [r for r in rarity_to_phones.keys() if r in rarity_order],
    key=lambda r: rarity_order.index(r)
)


    kb = InlineKeyboardBuilder()
    for r in available_rarities:
        emoji = rarity_emojis.get(r, "")
        kb.button(text=f"{emoji} {r}", callback_data=f"combine_rarity|{message.from_user.id}|{r}")
    kb.adjust(1)

    await message.answer(f"@{user.get('username') or user['name']}, выбери редкость для слияния:", reply_markup=kb.as_markup())

import json

def save_user(user):
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    data[str(user["id"])] = user

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@dp.callback_query(lambda c: c.data and c.data.startswith("combine_choose|"))
async def cb_combine_choose(callback: CallbackQuery):
    try:
        parts = callback.data.split("|")
        user_id = int(parts[1])
        rarity = parts[2]
        phone = parts[3]
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []
    user["id"] = user_id



    # Считаем количество копий выбранного телефона (исключаем сломанные)
    count = sum(1 for item in inv if item.get("rarity") == rarity and item.get("phone") == phone and not item.get("broken", False))
    if count < 2:
        await callback.answer("❌ Недостаточно копий для слияния. Нужно минимум 2.", show_alert=True)
        return

    max_count = min(count, 5)
    kb = InlineKeyboardBuilder()
    for i in range(2, max_count + 1):
        chance = {2: 40, 3: 60, 4: 80, 5: 100}[i]
        kb.button(text=f"{i} тел. — {chance}%", callback_data=f"combine_count|{user_id}|{rarity}|{phone}|{i}")
    kb.adjust(1)

    await callback.message.edit_text(
        f"@{user.get('username') or user['name']}, сколько телефонов ты хочешь объединить?\n(чем больше — тем выше шанс)",
        reply_markup=kb.as_markup()
    )
    await callback.answer()
@dp.callback_query(lambda c: c.data and c.data.startswith("combine_count|"))
async def cb_combine_count(callback: CallbackQuery):
    try:
        parts = callback.data.split("|")
        user_id = int(parts[1])
        rarity = parts[2]
        phone = parts[3]
        count = int(parts[4])
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    if count < 2 or count > 5:
        await callback.answer("❌ Можно объединить от 2 до 5 телефонов.", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []



    # Проверяем, что у игрока достаточно копий
    actual_count = sum(1 for item in inv if item.get("rarity") == rarity and item.get("phone") == phone)
    if actual_count < count:
        await callback.answer("❌ У тебя недостаточно копий этого телефона.", show_alert=True)
        return

    # Определяем следующую редкость
    try:
        next_rarity = rarity_order[rarity_order.index(rarity) + 1]
    except IndexError:
        await callback.answer("❌ Этот телефон нельзя улучшить.", show_alert=True)
        return

    emoji = rarity_emojis.get(next_rarity, "")
    chance = {2: 40, 3: 60, 4: 80, 5: 100}[count]

    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Объединить", callback_data=f"combine_confirm|{user_id}|{rarity}|{phone}|{count}")
    kb.button(text="❌ Отмена", callback_data=f"combine_cancel|{user_id}")
    kb.adjust(2)

    await callback.message.edit_text(
        f"@{user.get('username') or user['name']}, ты выбрал {count} телефона {phone}\n"
        f"При успехе будет получен рандомный {emoji} {next_rarity} телефон\n"
        f"🎯 Шанс на апгрейд: {chance}%",
        reply_markup=kb.as_markup()
    )
    await callback.answer()
@dp.callback_query(lambda c: c.data and c.data.startswith("combine_confirm|"))
async def cb_combine_confirm(callback: CallbackQuery):
    try:
        parts = callback.data.split("|")
        user_id = int(parts[1])
        rarity = parts[2]
        phone = parts[3]
        count = int(parts[4])
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []

    # Проверка наличия нужного количества копий (исключаем сломанные)
    matching = [item for item in inv if item.get("rarity") == rarity and item.get("phone") == phone and not item.get("broken", False)]
    if len(matching) < count:
        await callback.answer("❌ У тебя недостаточно копий этого телефона.", show_alert=True)
        return

    # Удаляем выбранные копии
    for _ in range(count):
        inv.remove(matching.pop())

    # Определяем шанс
    chance = {2: 40, 3: 60, 4: 80, 5: 100}[count]
    success = random.randint(1, 100) <= chance

    if success:
        # Получаем следующую редкость
        try:
            next_rarity = rarity_order[rarity_order.index(rarity) + 1]
        except IndexError:
            await callback.answer("❌ Этот телефон нельзя улучшить.", show_alert=True)
            return

        # Выбираем рандомный телефон из пула следующей редкости
        pool = phone_pool.get(next_rarity, [])
        if not pool:
            await callback.answer("❌ Нет доступных телефонов этой редкости.", show_alert=True)
            return

        new_phone = random.choice(pool)
        inv.append({"phone": new_phone, "rarity": next_rarity})
        emoji = rarity_emojis.get(next_rarity, "")

        await callback.message.edit_text(
            f"✅ Успех! Ты получил новый телефон: {new_phone} ({next_rarity}{emoji})"
        )
    else:
        await callback.message.edit_text(
            f"❌ Неудача! Все {count} копии {phone} сгорели."
        )

    save_user(user)
    await callback.answer()
    
@dp.callback_query(lambda c: c.data and c.data.startswith("combine_cancel|"))
async def cb_combine_cancel(callback: CallbackQuery):
    try:
        parts = callback.data.split("|")
        user_id = int(parts[1])
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    await callback.message.edit_text("❌ Слияние отменено.")
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("combine_rarity|"))
async def cb_combine_rarity(callback: CallbackQuery):
    try:
        parts = callback.data.split("|")
        user_id = int(parts[1])
        rarity = parts[2]
    except Exception:
        await callback.answer("❌ Неверные данные.", show_alert=True)
        return

    if callback.from_user.id != user_id:
        await callback.answer("❌ Это не твоя кнопка!", show_alert=True)
        return

    user = get_user(user_id, callback.from_user.username)
    inv = user.get("inventory", []) or []



    # Считаем количество каждого телефона этой редкости (исключаем сломанные)
    phone_counts = {}
    for item in inv:
        if item.get("rarity") == rarity and not item.get("broken", False):
            phone = item.get("phone")
            phone_counts[phone] = phone_counts.get(phone, 0) + 1

    # Оставляем только те, у которых ≥2 копии
    filtered = [(phone, count) for phone, count in phone_counts.items() if count >= 2]
    if not filtered:
        await callback.answer("❌ У тебя нет подходящих телефонов этой редкости.", show_alert=True)
        return

    # Сортировка по количеству (по убыванию)
    filtered.sort(key=lambda x: -x[1])

    kb = InlineKeyboardBuilder()
    for phone, count in filtered:
        kb.button(text=f"{phone} — {count} шт.", callback_data=f"combine_choose|{user_id}|{rarity}|{phone}")
    kb.adjust(1)

    await callback.message.edit_text(
        f"@{user.get('username') or user['name']}, выберите количество телефонов для слияния:\n(минимум 2, максимум 5)",
        reply_markup=kb.as_markup()
    )
    await callback.answer()




from aiogram.fsm.context import FSMContext

@dp.message()
async def handle_plain_russian_commands(message: types.Message, state: FSMContext):
    if not message.text:
        return  # игнорируем апдейты без текста (стикеры, фото и т.п.)
    text = message.text.lower().strip()


    if text == "слаим":
        await claim(message)
        return

    if text == "инв":
        await cmd_inventory(message)
        return

    if text == "аккаунт":
        await account_command(message)
        return

    if text == "лидерборд":
        await leaderboard_command(message)
        return

    if text == "продажа":
        await sell_command(message, state)
        return

    if text == "магазин":
        await shop_cmd(message)
        return

    if text == "техинфо":
        await techinfo(message)
        return

    if text == "пинг":
        await ping(message)
        return

    if text == "ежедневная награда":
        await cmd_daily(message)
        return

    if text == "рынок":
        await cmd_market(message)
        return

    if text == "команды":
        await commands(message)
        return

    if text == "слияние":
        await cmd_combine(message)
        return

    if text == "починить":
        await cmd_repair(message)
        return






if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

































