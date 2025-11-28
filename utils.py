import json
from pathlib import Path

USERS_FILE = "users.json"

def get_user(user_id, username=None):
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            users = json.load(f)
    except:
        users = {}

    user = users.get(str(user_id))
    if not user:
        user = {"id": user_id, "username": username or "", "balance": 0, "inventory": []}
        users[str(user_id)] = user
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
    return user

def update_user(user_id, user_data):
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            users = json.load(f)
    except:
        users = {}
    users[str(user_id)] = user_data
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

def phone_to_path(phone_name):
    path = Path(f"images/{phone_name}.jpg")
    return str(path) if path.exists() else None

rarity_emojis = {
    "Обычный": "📱",
    "Необычный": "📲",
    "Редкий": "⭐️",
    "Эпический": "👾",
    "Мистический": "🚨",
    "Легендарный": "🏆",
    "Платина": "💠"
}
