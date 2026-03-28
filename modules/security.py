# Логика безопасности и проверки доступа
import json
from typing import List

TRUSTED_USERS_FILE = 'trusted_users.json'

# Загрузка доверенных пользователей
def load_trusted_users() -> List[int]:
    try:
        with open(TRUSTED_USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# Проверка доступа по user_id
def is_trusted(user_id: int) -> bool:
    return user_id in load_trusted_users()

# Добавление пользователя в доверенные
def add_trusted_user(user_id: int):
    users = load_trusted_users()
    if user_id not in users:
        users.append(user_id)
        with open(TRUSTED_USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
