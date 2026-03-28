# Логика админки: команды, токен, логирование
import datetime

LOG_FILE = 'admin_actions.log'
ADMIN_TOKEN = '12345'  # Можно вынести в .env

# Проверка токена
def check_token(token: str) -> bool:
    # Здесь происходит проверка токена
    return token == ADMIN_TOKEN

# Логирование действий
def log_action(user_id: int, action: str):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.datetime.now()} | {user_id} | {action}\n")
