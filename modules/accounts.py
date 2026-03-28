# Логика отображения и управления списком Telegram-аккаунтов
import json
from typing import List, Dict

ACCOUNTS_FILE = 'accounts.json'

# Загрузка списка аккаунтов

def load_accounts() -> List[Dict]:
    try:
        with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# Сохранение списка аккаунтов

def save_accounts(accounts: List[Dict]):
    with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)

# Добавление аккаунта
def add_account(username: str, description: str, icon: str = '🤖'):
    accounts = load_accounts()
    accounts.append({
        'username': username,
        'description': description,
        'icon': icon
    })
    save_accounts(accounts)

# Удаление аккаунта
def remove_account(username: str):
    accounts = load_accounts()
    accounts = [a for a in accounts if a['username'] != username]
    save_accounts(accounts)

# Редактирование описания
def edit_account(username: str, new_description: str):
    accounts = load_accounts()
    for a in accounts:
        if a['username'] == username:
            a['description'] = new_description
    save_accounts(accounts)

# Сортировка аккаунтов
def sort_accounts(by: str = 'username'):
    accounts = load_accounts()
    accounts.sort(key=lambda x: x.get(by, ''))
    save_accounts(accounts)

# Получить страницу аккаунтов
def get_accounts_page(page: int, page_size: int = 10) -> List[Dict]:
    accounts = load_accounts()
    start = (page - 1) * page_size
    end = start + page_size
    return accounts[start:end], len(accounts)
