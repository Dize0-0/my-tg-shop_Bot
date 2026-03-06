import asyncio
import datetime
import html
import logging
import os
import re
import threading
from urllib.parse import urlencode
from typing import Any, Dict, Optional, Set

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from aiogram.utils.exceptions import MessageNotModified
from aiogram.utils import executor

from db import (
    activate_promo,
    add_product,
    apply_auto_restock,
    change_balance,
    claim_daily_bonus,
    confirm_topup,
    create_order,
    create_promo,
    create_topup,
    deactivate_product,
    get_balance,
    get_daily_bonus_remaining_seconds,
    get_order,
    get_product,
    init_db,
    list_products,
    list_pending_topups_for_auto,
    list_all_products_admin,
    list_user_orders,
    list_user_topups,
    seed_products,
    set_order_code,
    set_order_phone,
    set_order_status,
    set_stock,
    set_topup_payment_data,
    set_topup_external_status,
    try_spend_balance,
    update_stock,
)

try:
    from FunPayAPI import Account as FunPayAccount, Runner as FunPayRunner, enums as funpay_enums
    FUNPAY_API_AVAILABLE = True
except Exception:
    FunPayAccount = None
    FunPayRunner = None
    funpay_enums = None
    FUNPAY_API_AVAILABLE = False

logging.basicConfig(level=logging.INFO)


def load_local_env_file(file_path: str = '.env') -> None:
    if not os.path.exists(file_path):
        return
    try:
        with open(file_path, 'r', encoding='utf-8') as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key == 'TG_BOT_TOKEN' and value.upper() in {'YOUR_BOT_TOKEN', 'CHANGE_ME', ''}:
                    continue
                if key:
                    os.environ.setdefault(key, value)
    except Exception as error:
        logging.warning('Failed to read .env file: %s', error)


load_local_env_file('.env')
load_local_env_file('.env.example')


def env_or_default(key: str, default: str) -> str:
    value = os.getenv(key)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def env_int_or_default(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def env_float_or_default(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(raw.strip().replace(',', '.'))
    except Exception:
        return default


def env_bool_or_default(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'on'}

API_TOKEN = '8436518410:AAFF9AG58xsr1iWsidkD9yoDEqAKfgaAHkY'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ADMIN_IDS: Set[int] = set(
    int(value.strip())
    for value in os.getenv('ADMIN_IDS', '8594771951,8466199706').split(',')
    if value.strip()
)

FUNPAY_PAYMENT_URL = env_or_default('FUNPAY_PAYMENT_URL', 'https://funpay.com/')
FUNPAY_TOPUP_LOT_URL = env_or_default('FUNPAY_TOPUP_LOT_URL', '')
FUNPAY_LOT_MAP = os.getenv('FUNPAY_LOT_MAP', '').strip()
FUNPAY_GOLDEN_KEY = os.getenv('FUNPAY_GOLDEN_KEY', '').strip()
FUNPAY_LISTEN_DELAY = env_int_or_default('FUNPAY_LISTEN_DELAY', 4)
FUNPAY_MATCH_BY_AMOUNT = env_or_default('FUNPAY_MATCH_BY_AMOUNT', '1').lower() in {'1', 'true', 'yes', 'on'}
PURCHASE_CASHBACK_PERCENT = env_float_or_default('PURCHASE_CASHBACK_PERCENT', 2.0)
DAILY_BONUS_AMOUNT = env_float_or_default('DAILY_BONUS_AMOUNT', 5.0)
DAILY_BONUS_COOLDOWN_HOURS = env_int_or_default('DAILY_BONUS_COOLDOWN_HOURS', 24)
GITHUB_BACKUP_ENABLED = env_bool_or_default('GITHUB_BACKUP_ENABLED', False)
GITHUB_BACKUP_INTERVAL_SECONDS = max(60, env_int_or_default('GITHUB_BACKUP_INTERVAL_SECONDS', 300))
GITHUB_BACKUP_FILES = [
    item.strip()
    for item in env_or_default('GITHUB_BACKUP_FILES', 'products.db').split(',')
    if item.strip()
]
MAIN_MENU_PHOTO_URL = os.getenv('MAIN_MENU_PHOTO_URL', 'https://i.postimg.cc/4y127DrY/daa7dd69bf0ef2cf8efba080e603f2be.jpg')
CHANNEL_URL = os.getenv('CHANNEL_URL', 'https://t.me/H0MER0K')
REVIEWS_URL = os.getenv('REVIEWS_URL', 'https://t.me/Lune_shop_bot_0')
TG_CATEGORY_PHOTO_URL = os.getenv('TG_CATEGORY_PHOTO_URL', 'https://i.postimg.cc/QM32CZ4z/8BEC6871-6346-4FBE-AB56-1BE98473650D.png')
PROXY_CATEGORY_PHOTO_URL = os.getenv('PROXY_CATEGORY_PHOTO_URL', MAIN_MENU_PHOTO_URL)
EMAIL_CATEGORY_PHOTO_URL = os.getenv('EMAIL_CATEGORY_PHOTO_URL', MAIN_MENU_PHOTO_URL)
REVIEWS_PHOTO_URL_1 = os.getenv('REVIEWS_PHOTO_URL_1', 'https://i.postimg.cc/DfMRsXp7/a33681df3b4a34e1706da52d4484ab7e.jpg')
REVIEWS_PHOTO_URL_2 = os.getenv('REVIEWS_PHOTO_URL_2', 'https://i.postimg.cc/wMyfFw4J/EB328F27-0B7A-4338-A923-2BF9D774A300.png')
AGREEMENT_TEXT = (
    '╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М╤Б╨║╨╛╨╡ ╤Б╨╛╨│╨╗╨░╤И╨╡╨╜╨╕╨╡\n\n'
    '1) ╨Ю╨▒╤Й╨╕╨╡ ╨┐╨╛╨╗╨╛╨╢╨╡╨╜╨╕╤П\n'
    '1.1. ╨Ш╤Б╨┐╨╛╨╗╤М╨╖╤Г╤П ╨▒╨╛╤В╨░, ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨░╨╡╤В, ╤З╤В╨╛ ╨╛╨╖╨╜╨░╨║╨╛╨╝╨╕╨╗╤Б╤П ╤Б ╤Г╤Б╨╗╨╛╨▓╨╕╤П╨╝╨╕ ╨╕ ╨┐╨╛╨╗╨╜╨╛╤Б╤В╤М╤О ╨╕╤Е ╨┐╤А╨╕╨╜╨╕╨╝╨░╨╡╤В.\n'
    '1.2. ╨Т╤Б╨╡ ╤В╨╛╨▓╨░╤А╤Л ╨╕ ╤Г╤Б╨╗╤Г╨│╨╕ ╨▓ ╨▒╨╛╤В╨╡ ╤П╨▓╨╗╤П╤О╤В╤Б╤П ╤Ж╨╕╤Д╤А╨╛╨▓╤Л╨╝╨╕ ╨╕ ╨┐╤А╨╡╨┤╨╛╤Б╤В╨░╨▓╨╗╤П╤О╤В╤Б╤П ╨▓ ╤Н╨╗╨╡╨║╤В╤А╨╛╨╜╨╜╨╛╨╝ ╨▓╨╕╨┤╨╡.\n'
    '1.3. ╨д╨░╨║╤В ╨╛╨┐╨╗╨░╤В╤Л (╤В╨╛╨▓╨░╤А╨░ ╨╕╨╗╨╕ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П) ╨░╨▓╤В╨╛╨╝╨░╤В╨╕╤З╨╡╤Б╨║╨╕ ╨╛╨╖╨╜╨░╤З╨░╨╡╤В ╨░╨║╤Ж╨╡╨┐╤В ╨╜╨░╤Б╤В╨╛╤П╤Й╨╡╨│╨╛ ╤Б╨╛╨│╨╗╨░╤И╨╡╨╜╨╕╤П ╨▒╨╡╨╖ ╨┤╨╛╨┐╨╛╨╗╨╜╨╕╤В╨╡╨╗╤М╨╜╤Л╤Е ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╕╨╣.\n\n'
    '2) ╨Ы╨╕╤З╨╜╨░╤П ╨╛╤В╨▓╨╡╤В╤Б╤В╨▓╨╡╨╜╨╜╨╛╤Б╤В╤М ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П\n'
    '2.1. ╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╨╜╨╡╤Б╨╡╤В ╨┐╨╛╨╗╨╜╤Г╤О ╨╕ ╨┐╨╡╤А╤Б╨╛╨╜╨░╨╗╤М╨╜╤Г╤О ╨╛╤В╨▓╨╡╤В╤Б╤В╨▓╨╡╨╜╨╜╨╛╤Б╤В╤М ╨╖╨░ ╨▓╤Л╨▒╨╛╤А ╤В╨╛╨▓╨░╤А╨░, ╨▓╨▓╨╛╨┤ ╤А╨╡╨║╨▓╨╕╨╖╨╕╤В╨╛╨▓, ╨║╨╛╨╝╨╝╨╡╨╜╤В╨░╤А╨╕╨╡╨▓ ╨╕ ╨║╨╛╨┤╨░ ╨╛╨┐╨╗╨░╤В╤Л.\n'
    '2.2. ╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╤Б╨░╨╝╨╛╤Б╤В╨╛╤П╤В╨╡╨╗╤М╨╜╨╛ ╨╜╨╡╤Б╨╡╤В ╨╛╤В╨▓╨╡╤В╤Б╤В╨▓╨╡╨╜╨╜╨╛╤Б╤В╤М ╨╖╨░ ╨┤╨░╨╗╤М╨╜╨╡╨╣╤И╨╡╨╡ ╨╕╤Б╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╨╜╨╕╨╡ ╨┐╨╛╨╗╤Г╤З╨╡╨╜╨╜╤Л╤Е ╨┤╨░╨╜╨╜╤Л╤Е (╨░╨║╨║╨░╤Г╨╜╤В╤Л, ╨┐╤А╨╛╨║╤Б╨╕, ╨┐╨╛╤З╤В╤Л, ╨║╨╛╨┤╤Л ╨╕ ╨╕╨╜╤Л╨╡ ╤Ж╨╕╤Д╤А╨╛╨▓╤Л╨╡ ╨┤╨░╨╜╨╜╤Л╨╡).\n'
    '2.3. ╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╤Б╨░╨╝╨╛╤Б╤В╨╛╤П╤В╨╡╨╗╤М╨╜╨╛ ╨╛╤В╨▓╨╡╤З╨░╨╡╤В ╨╖╨░ ╤Б╨╛╨▒╨╗╤О╨┤╨╡╨╜╨╕╨╡ ╨┐╤А╨░╨▓╨╕╨╗ ╤Б╤В╨╛╤А╨╛╨╜╨╜╨╕╤Е ╤Б╨╡╤А╨▓╨╕╤Б╨╛╨▓, ╨┐╨╗╨░╤В╤Д╨╛╤А╨╝, ╨╕╨│╤А, ╨╝╨╡╤Б╤Б╨╡╨╜╨┤╨╢╨╡╤А╨╛╨▓ ╨╕ ╨┐╤А╨╕╨╝╨╡╨╜╨╕╨╝╨╛╨│╨╛ ╨╖╨░╨║╨╛╨╜╨╛╨┤╨░╤В╨╡╨╗╤М╤Б╤В╨▓╨░.\n'
    '2.4. ╨Т╤Б╨╡ ╤А╨╕╤Б╨║╨╕, ╤Б╨▓╤П╨╖╨░╨╜╨╜╤Л╨╡ ╤Б ╨▒╨╗╨╛╨║╨╕╤А╨╛╨▓╨║╨░╨╝╨╕, ╨╛╨│╤А╨░╨╜╨╕╤З╨╡╨╜╨╕╤П╨╝╨╕, ╤Б╨░╨╜╨║╤Ж╨╕╤П╨╝╨╕, ╨╕╨╖╨╝╨╡╨╜╨╡╨╜╨╕╨╡╨╝ ╨┐╤А╨░╨▓╨╕╨╗ ╤Б╤В╨╛╤А╨╛╨╜╨╜╨╕╤Е ╤Б╨╡╤А╨▓╨╕╤Б╨╛╨▓, ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╨┐╤А╨╕╨╜╨╕╨╝╨░╨╡╤В ╨╜╨░ ╤Б╨╡╨▒╤П.\n'
    '2.5. ╨Я╨╡╤А╨╡╨┤╨░╤З╨░ ╨┐╨╛╨╗╤Г╤З╨╡╨╜╨╜╤Л╤Е ╨┤╨░╨╜╨╜╤Л╤Е ╤В╤А╨╡╤В╤М╨╕╨╝ ╨╗╨╕╤Ж╨░╨╝, ╤А╨░╨╖╨│╨╗╨░╤И╨╡╨╜╨╕╨╡, ╤Г╤В╤А╨░╤В╨░ ╨┤╨╛╤Б╤В╤Г╨┐╨░, ╨║╨╛╨╝╨┐╤А╨╛╨╝╨╡╤В╨░╤Ж╨╕╤П ╨┐╨╛ ╨▓╨╕╨╜╨╡ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П ╨╜╨╡ ╨╛╤В╨╜╨╛╤Б╨╕╤В╤Б╤П ╨║ ╨╖╨╛╨╜╨╡ ╨╛╤В╨▓╨╡╤В╤Б╤В╨▓╨╡╨╜╨╜╨╛╤Б╤В╨╕ ╨╝╨░╨│╨░╨╖╨╕╨╜╨░.\n\n'
    '3) ╨Ю╨┐╨╗╨░╤В╨░, ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╨╕ ╨▒╨░╨╗╨░╨╜╤Б\n'
    '3.1. ╨С╨░╨╗╨░╨╜╤Б ╨▒╨╛╤В╨░ ╤П╨▓╨╗╤П╨╡╤В╤Б╤П ╨▓╨╜╤Г╤В╤А╨╡╨╜╨╜╨╡╨╣ ╤А╨░╤Б╤З╨╡╤В╨╜╨╛╨╣ ╨╡╨┤╨╕╨╜╨╕╤Ж╨╡╨╣ ╤Б╨╡╤А╨▓╨╕╤Б╨░ ╨╕ ╨╕╤Б╨┐╨╛╨╗╤М╨╖╤Г╨╡╤В╤Б╤П ╤В╨╛╨╗╤М╨║╨╛ ╨┤╨╗╤П ╨╛╨┐╨╗╨░╤В╤Л ╨▓╨╜╤Г╤В╤А╨╕ ╨▒╨╛╤В╨░.\n'
    '3.2. ╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╨╖╨░╤Б╤З╨╕╤В╤Л╨▓╨░╨╡╤В╤Б╤П ╤В╨╛╨╗╤М╨║╨╛ ╨┐╨╛╤Б╨╗╨╡ ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╕╤П ╨┐╨╗╨░╤В╨╡╨╢╨░ ╤Б╨╕╤Б╤В╨╡╨╝╨╛╨╣/╨┐╤А╨╛╨▓╨░╨╣╨┤╨╡╤А╨╛╨╝ ╨╕╨╗╨╕ ╨░╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤В╨╛╤А╨╛╨╝.\n'
    '3.3. ╨Я╤А╨╕ ╨╛╨┐╨╗╨░╤В╨╡ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╨╛╨▒╤П╨╖╨░╨╜ ╤Г╨║╨░╨╖╤Л╨▓╨░╤В╤М ╨║╨╛╤А╤А╨╡╨║╤В╨╜╤Л╨╣ ╨║╨╛╨┤ ╨┐╨╗╨░╤В╨╡╨╢╨░ (╨╡╤Б╨╗╨╕ ╨╛╨╜ ╤В╤А╨╡╨▒╤Г╨╡╤В╤Б╤П ╨▓ ╨╕╨╜╤Б╤В╤А╤Г╨║╤Ж╨╕╨╕).\n'
    '3.4. ╨Ю╤И╨╕╨▒╨║╨╕ ╨▓ ╤Б╤Г╨╝╨╝╨╡, ╨║╨╛╨╝╨╝╨╡╨╜╤В╨░╤А╨╕╨╕ ╨╕╨╗╨╕ ╤А╨╡╨║╨▓╨╕╨╖╨╕╤В╨░╤Е, ╨┤╨╛╨┐╤Г╤Й╨╡╨╜╨╜╤Л╨╡ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╨╡╨╝, ╨╝╨╛╨│╤Г╤В ╨┐╤А╨╕╨▓╨╡╤Б╤В╨╕ ╨║ ╨╖╨░╨┤╨╡╤А╨╢╨║╨╡/╨╛╤В╨║╨░╨╖╤Г ╨▓ ╨╖╨░╤З╨╕╤Б╨╗╨╡╨╜╨╕╨╕ ╨╕ ╤А╨░╤Б╤Б╨╝╨░╤В╤А╨╕╨▓╨░╤О╤В╤Б╤П ╨╕╨╜╨┤╨╕╨▓╨╕╨┤╤Г╨░╨╗╤М╨╜╨╛.\n'
    '3.5. ╨Р╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤Ж╨╕╤П ╨▓╨┐╤А╨░╨▓╨╡ ╨╖╨░╨┐╤А╨╛╤Б╨╕╤В╤М ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╕╨╡ ╨╛╨┐╨╗╨░╤В╤Л ╨┐╤А╨╕ ╤Б╨┐╨╛╤А╨╜╤Л╤Е ╤Б╨╕╤В╤Г╨░╤Ж╨╕╤П╤Е (╤З╨╡╨║, ╨╜╨╛╨╝╨╡╤А ╨╖╨░╨║╨░╨╖╨░, ╤Б╨║╤А╨╕╨╜╤И╨╛╤В ╨╕ ╤В.╨┤.).\n\n'
    '4) ╨Т╤Л╨┤╨░╤З╨░ ╤Ж╨╕╤Д╤А╨╛╨▓╨╛╨│╨╛ ╤В╨╛╨▓╨░╤А╨░\n'
    '4.1. ╨в╨╛╨▓╨░╤А ╤Б╤З╨╕╤В╨░╨╡╤В╤Б╤П ╨▓╤Л╨┤╨░╨╜╨╜╤Л╨╝ ╨╜╨░╨┤╨╗╨╡╨╢╨░╤Й╨╕╨╝ ╨╛╨▒╤А╨░╨╖╨╛╨╝ ╤Б ╨╝╨╛╨╝╨╡╨╜╤В╨░ ╨╛╤В╨╛╨▒╤А╨░╨╢╨╡╨╜╨╕╤П ╨┤╨░╨╜╨╜╤Л╤Е ╨▓ ╤З╨░╤В╨╡ ╨▒╨╛╤В╨░ ╨╕╨╗╨╕ ╨▓ ╨╕╤Б╤В╨╛╤А╨╕╨╕ ╨╖╨░╨║╨░╨╖╨░.\n'
    '4.2. ╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М ╨╛╨▒╤П╨╖╨░╨╜ ╤Б╤А╨░╨╖╤Г ╨┐╤А╨╛╨▓╨╡╤А╨╕╤В╤М ╨┐╨╛╨╗╤Г╤З╨╡╨╜╨╜╤Л╨╡ ╨┤╨░╨╜╨╜╤Л╨╡ ╨┐╨╛╤Б╨╗╨╡ ╨▓╤Л╨┤╨░╤З╨╕.\n'
    '4.3. ╨Я╤А╨╡╤В╨╡╨╜╨╖╨╕╨╕ ╨┐╨╛ ╤В╨╛╨▓╨░╤А╤Г ╨┐╤А╨╕╨╜╨╕╨╝╨░╤О╤В╤Б╤П ╨┐╤А╨╕ ╨╜╨░╨╗╨╕╤З╨╕╨╕ ╨╛╨▒╤К╨╡╨║╤В╨╕╨▓╨╜╤Л╤Е ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╕╨╣ ╨╕ ╨▓ ╤А╨░╨╖╤Г╨╝╨╜╤Л╨╣ ╤Б╤А╨╛╨║ ╨┐╨╛╤Б╨╗╨╡ ╨┐╨╛╨║╤Г╨┐╨║╨╕.\n\n'
    '5) ╨Т╨╛╨╖╨▓╤А╨░╤В╤Л ╨╕ ╨╛╤В╨╝╨╡╨╜╤Л\n'
    '5.1. ╨Т╨╛╨╖╨▓╤А╨░╤В ╨▓╨╛╨╖╨╝╨╛╨╢╨╡╨╜ ╤В╨╛╨╗╤М╨║╨╛ ╨╡╤Б╨╗╨╕ ╤В╨╛╨▓╨░╤А ╨╜╨╡ ╨▒╤Л╨╗ ╨▓╤Л╨┤╨░╨╜ ╨┐╨╛ ╨▓╨╕╨╜╨╡ ╨╝╨░╨│╨░╨╖╨╕╨╜╨░ ╨╕╨╗╨╕ ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨░ ╤В╨╡╤Е╨╜╨╕╤З╨╡╤Б╨║╨░╤П ╨╛╤И╨╕╨▒╨║╨░ ╨╜╨░ ╤Б╤В╨╛╤А╨╛╨╜╨╡ ╤Б╨╡╤А╨▓╨╕╤Б╨░.\n'
    '5.2. ╨Х╤Б╨╗╨╕ ╤В╨╛╨▓╨░╤А ╨▓╤Л╨┤╨░╨╜, ╨╜╨╛ ╨╜╨╡ ╨┐╨╛╨┤╨╛╤И╨╡╨╗ ╨┐╨╛ ╨╗╨╕╤З╨╜╤Л╨╝ ╨┐╤А╨╕╤З╨╕╨╜╨░╨╝ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П, ╨▓╨╛╨╖╨▓╤А╨░╤В ╨╜╨╡ ╨┐╤А╨╛╨╕╨╖╨▓╨╛╨┤╨╕╤В╤Б╤П.\n'
    '5.3. ╨Т╨╛╨╖╨▓╤А╨░╤В ╨╜╨╡ ╨┐╤А╨╛╨╕╨╖╨▓╨╛╨┤╨╕╤В╤Б╤П ╨┐╤А╨╕ ╨╜╨░╤А╤Г╤И╨╡╨╜╨╕╨╕ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╨╡╨╝ ╨╕╨╜╤Б╤В╤А╤Г╨║╤Ж╨╕╨╕, ╨┐╤А╨░╨▓╨╕╨╗ ╨╕╤Б╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╨╜╨╕╤П ╤В╨╛╨▓╨░╤А╨░ ╨╕╨╗╨╕ ╤Г╤Б╨╗╨╛╨▓╨╕╨╣ ╤Б╤В╨╛╤А╨╛╨╜╨╜╨╕╤Е ╨┐╨╗╨░╤В╤Д╨╛╤А╨╝.\n\n'
    '6) ╨Ю╨│╤А╨░╨╜╨╕╤З╨╡╨╜╨╕╤П ╨╕ ╨┐╤А╨░╨▓╨╛ ╨╛╤В╨║╨░╨╖╨░\n'
    '6.1. ╨Р╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤Ж╨╕╤П ╨▓╨┐╤А╨░╨▓╨╡ ╨╛╤В╨║╨░╨╖╨░╤В╤М ╨▓ ╨╛╨▒╤Б╨╗╤Г╨╢╨╕╨▓╨░╨╜╨╕╨╕ ╨┐╤А╨╕ ╨┐╨╛╨┤╨╛╨╖╤А╨╡╨╜╨╕╨╕ ╨╜╨░ ╨╝╨╛╤И╨╡╨╜╨╜╨╕╤З╨╡╤Б╤В╨▓╨╛, ╨╖╨╗╨╛╤Г╨┐╨╛╤В╤А╨╡╨▒╨╗╨╡╨╜╨╕╨╡, ╨┐╨╛╨┐╤Л╤В╨║╨╕ ╨▓╨╖╨╗╨╛╨╝╨░, ╤Б╨┐╨░╨╝ ╨╕╨╗╨╕ ╨╕╨╜╤Л╨╡ ╨╜╨╡╨┤╨╛╨▒╤А╨╛╤Б╨╛╨▓╨╡╤Б╤В╨╜╤Л╨╡ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╤П.\n'
    '6.2. ╨Р╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤Ж╨╕╤П ╨▓╨┐╤А╨░╨▓╨╡ ╨▓╤А╨╡╨╝╨╡╨╜╨╜╨╛ ╨╛╨│╤А╨░╨╜╨╕╤З╨╕╤В╤М ╤Д╤Г╨╜╨║╤Ж╨╕╨╛╨╜╨░╨╗╤М╨╜╨╛╤Б╤В╤М ╨▒╨╛╤В╨░ ╨┐╤А╨╕ ╤В╨╡╤Е╨╜╨╕╤З╨╡╤Б╨║╨╕╤Е ╤А╨░╨▒╨╛╤В╨░╤Е, ╤Д╨╛╤А╤Б-╨╝╨░╨╢╨╛╤А╨░╤Е ╨╕ ╨╕╨╖╨╝╨╡╨╜╨╡╨╜╨╕╤П╤Е ╤Г ╨┐╨╗╨░╤В╨╡╨╢╨╜╤Л╤Е/╤Б╤В╨╛╤А╨╛╨╜╨╜╨╕╤Е ╤Б╨╡╤А╨▓╨╕╤Б╨╛╨▓.\n\n'
    '7) ╨Ч╨░╨║╨╗╤О╤З╨╕╤В╨╡╨╗╤М╨╜╤Л╨╡ ╨┐╨╛╨╗╨╛╨╢╨╡╨╜╨╕╤П\n'
    '7.1. ╨г╤Б╨╗╨╛╨▓╨╕╤П ╤Б╨╛╨│╨╗╨░╤И╨╡╨╜╨╕╤П ╨╝╨╛╨│╤Г╤В ╨▒╤Л╤В╤М ╨╕╨╖╨╝╨╡╨╜╨╡╨╜╤Л ╨▒╨╡╨╖ ╨┐╤А╨╡╨┤╨▓╨░╤А╨╕╤В╨╡╨╗╤М╨╜╨╛╨│╨╛ ╨┐╨╡╤А╤Б╨╛╨╜╨░╨╗╤М╨╜╨╛╨│╨╛ ╤Г╨▓╨╡╨┤╨╛╨╝╨╗╨╡╨╜╨╕╤П.\n'
    '7.2. ╨Р╨║╤В╤Г╨░╨╗╤М╨╜╨░╤П ╤А╨╡╨┤╨░╨║╤Ж╨╕╤П ╤Б╨╛╨│╨╗╨░╤И╨╡╨╜╨╕╤П ╨┐╤Г╨▒╨╗╨╕╨║╤Г╨╡╤В╤Б╤П ╨▓ ╨▒╨╛╤В╨╡ ╨╕ ╨▓╤Б╤В╤Г╨┐╨░╨╡╤В ╨▓ ╤Б╨╕╨╗╤Г ╤Б ╨╝╨╛╨╝╨╡╨╜╤В╨░ ╨┐╤Г╨▒╨╗╨╕╨║╨░╤Ж╨╕╨╕.\n'
    '7.3. ╨Я╤А╨╛╨┤╨╛╨╗╨╢╨╡╨╜╨╕╨╡ ╨╕╤Б╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╨╜╨╕╤П ╨▒╨╛╤В╨░ ╨┐╨╛╤Б╨╗╨╡ ╨┐╤Г╨▒╨╗╨╕╨║╨░╤Ж╨╕╨╕ ╨╕╨╖╨╝╨╡╨╜╨╡╨╜╨╕╨╣ ╨╛╨╖╨╜╨░╤З╨░╨╡╤В ╨┐╨╛╨╗╨╜╨╛╨╡ ╤Б╨╛╨│╨╗╨░╤Б╨╕╨╡ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П ╤Б ╨╛╨▒╨╜╨╛╨▓╨╗╨╡╨╜╨╜╤Л╨╝╨╕ ╤Г╤Б╨╗╨╛╨▓╨╕╤П╨╝╨╕.'
)

CATEGORY_NAMES = {
    'proxy': 'ЁЯМР ╨Я╤А╨╛╨║╤Б╨╕',
    'tg': 'ЁЯдЦ TG ╨░╨║╨║╨░╤Г╨╜╤В╤Л',
    'email': 'тЬЙя╕П ╨Я╨╛╤З╤В╤Л',
}
PRODUCTS_PAGE_SIZE = 5

bot = Bot(token=API_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

init_db()
seed_products()

pending_promo_input: Set[int] = set()
pending_custom_topup: Set[int] = set()
pending_tg_phone_order: Dict[int, int] = {}
pending_custom_qty_input: Dict[int, Dict[str, int]] = {}
admin_add_product_state: Dict[int, Dict[str, str]] = {}
admin_action_state: Dict[int, Dict[str, str]] = {}
active_buy_users: Set[int] = set()
funpay_events_queue: Optional[asyncio.Queue] = None
funpay_listener_started = False
topup_marker_re = re.compile(r'topup_(\d+)_(\d+)', re.IGNORECASE)


def payment_provider_label() -> str:
    return 'FunPay'


def calculate_cashback(total_amount: float) -> float:
    if PURCHASE_CASHBACK_PERCENT <= 0:
        return 0.0
    cashback = round(float(total_amount) * PURCHASE_CASHBACK_PERCENT / 100.0, 2)
    return cashback if cashback > 0 else 0.0


def parse_topup_marker(raw_text: str) -> tuple[Optional[int], Optional[int]]:
    text = str(raw_text or '')
    found = topup_marker_re.search(text)
    if not found:
        return None, None
    try:
        return int(found.group(1)), int(found.group(2))
    except Exception:
        return None, None


def _event_is_new_order(event: Any) -> bool:
    event_type = getattr(event, 'type', None)
    if event_type is None:
        return False

    if funpay_enums is not None and hasattr(funpay_enums, 'EventTypes'):
        try:
            target = funpay_enums.EventTypes.NEW_ORDER
            if event_type is target or event_type == target:
                return True
        except Exception:
            pass

    return 'NEW_ORDER' in str(event_type).upper()


def _extract_amount_from_order(order: Any) -> Optional[float]:
    for key in ('amount', 'price', 'sum', 'value', 'total'):
        value = getattr(order, key, None)
        if value is None:
            continue
        try:
            return float(str(value).replace(',', '.'))
        except Exception:
            continue
    return None


def _extract_payment_payload_from_order(order: Any) -> Optional[dict]:
    description_parts = [
        getattr(order, 'description', ''),
        getattr(order, 'comment', ''),
        getattr(order, 'title', ''),
        getattr(order, 'name', ''),
        getattr(order, 'note', ''),
    ]
    marker_text = ' '.join(str(part) for part in description_parts if part)
    topup_id, user_id = parse_topup_marker(marker_text)
    if not topup_id or not user_id:
        return None

    order_ref = getattr(order, 'id', None) or getattr(order, 'order_id', None) or ''
    return {
        'topup_id': int(topup_id),
        'user_id': int(user_id),
        'amount': _extract_amount_from_order(order),
        'status': 'paid',
        'order_ref': str(order_ref).strip(),
        'description': marker_text,
    }


def _funpay_listener_thread(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
    try:
        if not FUNPAY_API_AVAILABLE or not FUNPAY_GOLDEN_KEY:
            return

        account = FunPayAccount(FUNPAY_GOLDEN_KEY).get()
        runner = FunPayRunner(account)

        for event in runner.listen(requests_delay=max(2, FUNPAY_LISTEN_DELAY)):
            if not _event_is_new_order(event):
                continue
            order = getattr(event, 'order', None)
            if order is None:
                continue
            payload = _extract_payment_payload_from_order(order)
            if not payload:
                continue
            loop.call_soon_threadsafe(queue.put_nowait, payload)
    except Exception as error:
        logging.exception('FunPay listener thread failed: %s', error)


def create_funpay_payment(amount: float, user_id: int, topup_id: int) -> tuple[str, str]:
    lot_url = resolve_funpay_lot_url(amount)
    return lot_url, f'fp_{topup_id}'


def resolve_funpay_lot_url(amount: float) -> str:
    amount_key = f'{float(amount):.2f}'

    if FUNPAY_LOT_MAP:
        for part in FUNPAY_LOT_MAP.split(';'):
            item = part.strip()
            if not item or '=' not in item:
                continue
            left, right = item.split('=', 1)
            raw_amount = left.strip().replace(',', '.')
            url = right.strip()
            if not url:
                continue
            try:
                if f'{float(raw_amount):.2f}' == amount_key and url.startswith('http'):
                    return url
            except Exception:
                continue

    lot_url = FUNPAY_TOPUP_LOT_URL if FUNPAY_TOPUP_LOT_URL.startswith('http') else ''
    if lot_url:
        return lot_url

    if FUNPAY_PAYMENT_URL.startswith('http'):
        return FUNPAY_PAYMENT_URL
    return 'https://funpay.com/'


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('ЁЯУж ╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╨╕', callback_data='menu:catalog'),
        InlineKeyboardButton('ЁЯСд ╨Я╤А╨╛╤Д╨╕╨╗╤М', callback_data='menu:profile'),
    )
    kb.add(
        InlineKeyboardButton('ЁЯУД ╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М╤Б╨║╨╛╨╡ ╤Б╨╛╨│╨╗╨░╤И╨╡╨╜╨╕╨╡', callback_data='menu:agreement'),
        InlineKeyboardButton('ЁЯТ│ ╨Я╨╛╨┐╨╛╨╗╨╜╨╕╤В╤М ╨▒╨░╨╗╨░╨╜╤Б', callback_data='menu:topup'),
    )
    kb.add(
        InlineKeyboardButton('тнР ╨Ю╤В╨╖╤Л╨▓╤Л', callback_data='menu:reviews'),
        InlineKeyboardButton('ЁЯУг ╨Ъ╨░╨╜╨░╨╗', callback_data='menu:channel'),
    )
    if is_admin(user_id):
        kb.add(InlineKeyboardButton('ЁЯЫа ╨Р╨┤╨╝╨╕╨╜ ╨┐╨░╨╜╨╡╨╗╤М', callback_data='menu:adminpanel'))
    return kb


def back_to_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup().add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))


def user_get_code_kb(order_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('ЁЯФР ╨Я╨╛╨╗╤Г╤З╨╕╤В╤М ╨║╨╛╨┤ ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░', callback_data=f'userreqcode:{order_id}'))
    kb.add(InlineKeyboardButton('тнР ╨Ю╤Б╤В╨░╨▓╨╕╤В╤М ╨╛╤В╨╖╤Л╨▓', callback_data='menu:reviews'))
    kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
    return kb


def admin_get_code_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton('ЁЯФР ╨Я╨╛╨╗╤Г╤З╨╕╤В╤М ╨║╨╛╨┤', callback_data=f'admsendcode:{order_id}')
    )


def profile_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('ЁЯУК ╨ж╨╡╨╜╤В╤А ╤Г╨▓╨╡╨┤╨╛╨╝╨╗╨╡╨╜╨╕╨╣', callback_data='profile:hub'))
    kb.add(InlineKeyboardButton('ЁЯз╛ ╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨║╤Г╨┐╨╛╨║', callback_data='profile:orders'))
    kb.add(InlineKeyboardButton('ЁЯТ╕ ╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╣', callback_data='profile:topups'))
    kb.add(InlineKeyboardButton('ЁЯОп ╨Х╨╢╨╡╨┤╨╜╨╡╨▓╨╜╤Л╨╣ ╨▒╨╛╨╜╤Г╤Б', callback_data='profile:dailybonus'))
    kb.add(InlineKeyboardButton('ЁЯОБ ╨Р╨║╤В╨╕╨▓╨╕╤А╨╛╨▓╨░╤В╤М ╨┐╤А╨╛╨╝╨╛', callback_data='profile:promo'))
    kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
    return kb


def build_profile_hub_text(user_id: int) -> str:
    balance = get_balance(user_id)
    bonus_left = get_daily_bonus_remaining_seconds(user_id, cooldown_hours=DAILY_BONUS_COOLDOWN_HOURS)
    if bonus_left <= 0:
        bonus_text = '╨┤╨╛╤Б╤В╤Г╨┐╨╡╨╜ ╤Б╨╡╨╣╤З╨░╤Б тЬЕ'
    else:
        hours_left = bonus_left // 3600
        minutes_left = (bonus_left % 3600) // 60
        bonus_text = f'╤З╨╡╤А╨╡╨╖ {hours_left} ╤З {minutes_left} ╨╝╨╕╨╜'

    rows = list_user_orders(user_id, limit=3)
    if rows:
        last_orders = []
        for order_id, title, qty, total, status, _, _ in rows:
            safe_title = html.escape((title or '╨в╨╛╨▓╨░╤А').strip())
            safe_status = html.escape(str(status or 'unknown'))
            last_orders.append(f'#{order_id} {safe_title} x{qty} | {total:.2f} тВ╜ | {safe_status}')
        orders_text = '\n'.join(last_orders)
    else:
        orders_text = '╨Я╨╛╨║╤Г╨┐╨╛╨║ ╨┐╨╛╨║╨░ ╨╜╨╡╤В.'

    return (
        'ЁЯУК ╨ж╨╡╨╜╤В╤А ╤Г╨▓╨╡╨┤╨╛╨╝╨╗╨╡╨╜╨╕╨╣\n\n'
        f'ID: <code>{user_id}</code>\n'
        f'╨С╨░╨╗╨░╨╜╤Б: <b>{balance:.2f} тВ╜</b>\n'
        f'╨Ъ╨╡╤И╨▒╤Н╨║: <b>{PURCHASE_CASHBACK_PERCENT:.2f}%</b>\n'
        f'╨Х╨╢╨╡╨┤╨╜╨╡╨▓╨╜╤Л╨╣ ╨▒╨╛╨╜╤Г╤Б: <b>{bonus_text}</b>\n\n'
        '╨Я╨╛╤Б╨╗╨╡╨┤╨╜╨╕╨╡ ╨┐╨╛╨║╤Г╨┐╨║╨╕:\n'
        f'{orders_text}'
    )


def catalog_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['proxy'], callback_data='cat:proxy'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['tg'], callback_data='cat:tg'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['email'], callback_data='cat:email'))
    kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
    return kb


def category_products_kb(category: str, page: int = 0) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    products = list_products(category)
    total = len(products)
    start = page * PRODUCTS_PAGE_SIZE
    end = start + PRODUCTS_PAGE_SIZE
    page_items = products[start:end]

    for product_id, title, _, price, stock, _ in page_items:
        kb.add(
            InlineKeyboardButton(
                f'{title} тАФ {price:.0f} тВ╜ тАФ {stock} ╤И╤В.',
                callback_data=f'buy:{product_id}:{page}',
            )
        )

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('<< ╨Я╤А╨╡╨┤╤Л╨┤╤Г╤Й╨░╤П', callback_data=f'catpage:{category}:{page - 1}'))
    if end < total:
        nav_buttons.append(InlineKeyboardButton('╨б╨╗╨╡╨┤╤Г╤О╤Й╨░╤П >>', callback_data=f'catpage:{category}:{page + 1}'))
    if nav_buttons:
        kb.row(*nav_buttons)

    kb.add(InlineKeyboardButton('тЖйя╕П ╨Э╨░╨╖╨░╨┤', callback_data='menu:catalog'))
    return kb


def category_products_text(category: str, page: int = 0) -> str:
    products = list_products(category)
    total = len(products)
    start = page * PRODUCTS_PAGE_SIZE
    end = min(start + PRODUCTS_PAGE_SIZE, total)
    category_title = CATEGORY_NAMES.get(category, category)
    page_label = f'{start + 1}-{end}' if total > 0 else '0-0'
    return (
        'ЁЯЫТ ╨Ъ╤Г╨┐╨╕╤В╤М ╤В╨╛╨▓╨░╤А\n'
        f'тФЬ ╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╤П: {category_title}\n'
        'тФФ ╨Я╨╛╨╖╨╕╤Ж╨╕╤П: ╨Э╨╡ ╨▓╤Л╨▒╤А╨░╨╜╨░\n\n'
        f'ЁЯУМ ╨Т╤Л╨▒╨╡╤А╨╕╤В╨╡ ╨╛╨┤╨╕╨╜ ╨╕╨╖ ╨┐╤А╨╡╨┤╨╗╨╛╨╢╨╡╨╜╨╜╤Л╤Е ╨▓╨░╤А╨╕╨░╨╜╤В╨╛╨▓ ({page_label} ╨╕╨╖ {total}):'
    )


def quantity_kb(product_id: int, category: str, max_qty: int, page: int = 0) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=4)
    safe_max = min(max_qty, 10) if max_qty > 0 else 1
    for qty in range(1, safe_max + 1):
        kb.insert(InlineKeyboardButton(str(qty), callback_data=f'qty:{product_id}:{qty}'))
    kb.add(InlineKeyboardButton('тЬНя╕П ╨б╨▓╨╛╨╡ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛', callback_data=f'qtycustom:{product_id}:{page}'))
    kb.add(InlineKeyboardButton('тЖйя╕П ╨Э╨░╨╖╨░╨┤', callback_data=f'buy:{product_id}:{page}'))
    return kb


def product_card_text(title: str, category: str, price: float, stock: int, description: str) -> str:
    category_title = CATEGORY_NAMES.get(category, category)
    format_line = description if description else '╨Ы╨╛╨│╨╕╨╜:╨Я╨░╤А╨╛╨╗╤М'
    return (
        'ЁЯЫТ ╨Ъ╤Г╨┐╨╕╤В╤М ╤В╨╛╨▓╨░╤А\n'
        f'тФЬ ╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╤П: {category_title}\n'
        f'тФФ ╨Я╨╛╨╖╨╕╤Ж╨╕╤П: {title}\n\n'
        f'тФЬ ╨б╤В╨╛╨╕╨╝╨╛╤Б╤В╤М: {price:.2f} тВ╜\n'
        f'тФФ ╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛: {stock} ╤И╤В.\n'
        'тФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФАтФА\n'
        f'╨д╨╛╤А╨╝╨░╤В ╨▓╤Л╨┤╨░╤З╨╕: {format_line}'
    )


def format_delivery_credentials(credentials: str, qty: int) -> str:
    if qty <= 0:
        return ''

    items: list[str] = []
    for raw_line in (credentials or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if '|' in line:
            items.extend(chunk.strip() for chunk in line.split('|') if chunk.strip())
        else:
            items.append(line)

    if not items:
        return '╨Ф╨░╨╜╨╜╤Л╨╡ ╨╛╤В╤Б╤Г╤В╤Б╤В╨▓╤Г╤О╤В. ╨Ю╨▒╤А╨░╤В╨╕╤В╨╡╤Б╤М ╨║ ╨░╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤В╨╛╤А╤Г.'

    if len(items) >= qty:
        selected = items[:qty]
    else:
        selected = items + [items[-1]] * (qty - len(items))

    return '\n'.join(f'{index}. {value}' for index, value in enumerate(selected, start=1))


def topup_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('50тВ╜', callback_data='topup:50'),
        InlineKeyboardButton('100тВ╜', callback_data='topup:100'),
    )
    kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
    return kb


def admin_panel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('тЮХ ╨Ч╨░╨│╤А╤Г╨╖╨╕╤В╤М ╤В╨╛╨▓╨░╤А (╨╝╨░╤Б╤В╨╡╤А)', callback_data='admpanel:add_product'))
    kb.add(InlineKeyboardButton('ЁЯТ│ ╨Я╨╛╨┐╨╛╨╗╨╜╨╕╤В╤М ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П', callback_data='admpanel:add_balance'))
    kb.add(InlineKeyboardButton('тЬЕ ╨Я╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕╤В╤М ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡', callback_data='admpanel:confirm_topup'))
    kb.add(InlineKeyboardButton('ЁЯОБ ╨б╨╛╨╖╨┤╨░╤В╤М ╨┐╤А╨╛╨╝╨╛╨║╨╛╨┤', callback_data='admpanel:create_promo'))
    kb.add(InlineKeyboardButton('ЁЯУи ╨Ю╤В╨┐╤А╨░╨▓╨╕╤В╤М ╨║╨╛╨┤ ╨┐╨╛ ╨╖╨░╨║╨░╨╖╤Г', callback_data='admpanel:send_code'))
    kb.add(InlineKeyboardButton('ЁЯУе ╨Я╨╛╨┐╨╛╨╗╨╜╨╕╤В╤М ╨╛╤Б╤В╨░╤В╨╛╨║', callback_data='admpanel:refill'))
    kb.add(InlineKeyboardButton('ЁЯзо ╨г╤Б╤В╨░╨╜╨╛╨▓╨╕╤В╤М ╨╛╤Б╤В╨░╤В╨╛╨║', callback_data='admpanel:set_stock'))
    kb.add(InlineKeyboardButton('ЁЯЧС ╨г╨┤╨░╨╗╨╕╤В╤М ╤В╨╛╨▓╨░╤А', callback_data='admpanel:delete_product'))
    kb.add(InlineKeyboardButton('ЁЯУЛ ╨б╨┐╨╕╤Б╨╛╨║ ╤В╨╛╨▓╨░╤А╨╛╨▓', callback_data='admpanel:list_products'))
    kb.add(InlineKeyboardButton('тЭМ ╨Ю╤В╨╝╨╡╨╜╨░ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╤П', callback_data='admpanel:cancel_any'))
    kb.add(InlineKeyboardButton('ЁЯФЩ ╨У╨╗╨░╨▓╨╜╨╛╨╡ ╨╝╨╡╨╜╤О', callback_data='menu:main'))
    return kb


def admin_category_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['proxy'], callback_data='admaddcat:proxy'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['tg'], callback_data='admaddcat:tg'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['email'], callback_data='admaddcat:email'))
    kb.add(InlineKeyboardButton('тЭМ ╨Ю╤В╨╝╨╡╨╜╨░', callback_data='admpanel:cancel_add'))
    return kb


def admin_auto_restock_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('тЬЕ ╨Ф╨░, ╨░╨▓╤В╨╛╨╛╨▒╨╜╨╛╨▓╨╗╨╡╨╜╨╕╨╡', callback_data='admaddauto:yes'),
        InlineKeyboardButton('тЭМ ╨Э╨╡╤В', callback_data='admaddauto:no'),
    )
    kb.add(InlineKeyboardButton('тЭМ ╨Ю╤В╨╝╨╡╨╜╨░', callback_data='admpanel:cancel_add'))
    return kb


def admin_step_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('ЁЯФЩ ╨Т ╨░╨┤╨╝╨╕╨╜ ╨┐╨░╨╜╨╡╨╗╤М', callback_data='admpanel:home'),
        InlineKeyboardButton('тЭМ ╨Ю╤В╨╝╨╡╨╜╨░', callback_data='admpanel:cancel_any'),
    )
    return kb


def admin_confirm_product_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('тЬЕ ╨б╨╛╤Е╤А╨░╨╜╨╕╤В╤М ╤В╨╛╨▓╨░╤А', callback_data='admaddsave:yes'),
        InlineKeyboardButton('тЭМ ╨Ю╤В╨╝╨╡╨╜╨░', callback_data='admpanel:cancel_add'),
    )
    return kb


async def notify_admins_about_tg_order(
    order_id: int,
    buyer_user_id: int,
    product_title: str,
    quantity: int,
    total: float,
    phone: Optional[str] = None,
) -> None:
    safe_title = html.escape(str(product_title or 'TG ╨░╨║╨║╨░╤Г╨╜╤В'))
    phone_value = html.escape(str(phone).strip()) if phone else '╨╡╤Й╨╡ ╨╜╨╡ ╤Г╨║╨░╨╖╨░╨╜'
    text = (
        'ЁЯТ╕ ╨Ю╨┐╨╗╨░╤З╨╡╨╜ TG ╨╖╨░╨║╨░╨╖\n'
        f'╨Э╨╛╨╝╨╡╤А ╨╖╨░╨║╨░╨╖╨░: <b>#{order_id}</b>\n'
        f'╨Э╨╛╨╝╨╡╤А ╨░╨║╨║╨░╤Г╨╜╤В╨░ (ID): <code>{buyer_user_id}</code>\n'
        f'╨в╨╛╨▓╨░╤А: <b>{safe_title}</b>\n'
        f'╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛: <b>{int(quantity)}</b>\n'
        f'╨б╤Г╨╝╨╝╨░: <b>{float(total):.2f} тВ╜</b>\n'
        f'╨Э╨╛╨╝╨╡╤А ╨┤╨╗╤П ╨║╨╛╨┤╨░: <code>{phone_value}</code>'
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


async def show_main_menu(user_id: int, text: str = '╨У╨╗╨░╨▓╨╜╨╛╨╡ ╨╝╨╡╨╜╤О:') -> None:
    try:
        await bot.send_photo(user_id, photo=MAIN_MENU_PHOTO_URL)
    except Exception:
        pass
    await bot.send_message(user_id, text, reply_markup=main_menu_kb(user_id))


async def safe_edit_text(message: types.Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except MessageNotModified:
        return


async def _run_git_command(args: list[str]) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        'git',
        '-C',
        BASE_DIR,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    output = (stdout or b'').decode('utf-8', errors='ignore').strip()
    err_output = (stderr or b'').decode('utf-8', errors='ignore').strip()
    if err_output:
        output = f'{output}\n{err_output}'.strip()
    return process.returncode, output


async def github_backup_worker() -> None:
    if not GITHUB_BACKUP_ENABLED:
        logging.info('GitHub backup worker is disabled (set GITHUB_BACKUP_ENABLED=1 to enable).')
        return

    if not os.path.exists(os.path.join(BASE_DIR, '.git')):
        logging.warning('GitHub backup worker: .git directory not found in %s', BASE_DIR)
        return

    tracked_files = [path for path in GITHUB_BACKUP_FILES if os.path.exists(os.path.join(BASE_DIR, path))]
    if not tracked_files:
        logging.warning('GitHub backup worker: no existing files from GITHUB_BACKUP_FILES=%s', GITHUB_BACKUP_FILES)
        return

    logging.info('GitHub backup worker enabled: interval=%ss, files=%s', GITHUB_BACKUP_INTERVAL_SECONDS, tracked_files)

    while True:
        try:
            add_code, add_output = await _run_git_command(['add', *tracked_files])
            if add_code != 0:
                logging.warning('GitHub backup worker: git add failed: %s', add_output)
                await asyncio.sleep(GITHUB_BACKUP_INTERVAL_SECONDS)
                continue

            diff_code, _ = await _run_git_command(['diff', '--cached', '--quiet'])
            if diff_code == 0:
                await asyncio.sleep(GITHUB_BACKUP_INTERVAL_SECONDS)
                continue

            timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            commit_code, commit_output = await _run_git_command(['commit', '-m', f'Auto backup {timestamp}'])
            if commit_code != 0:
                logging.warning('GitHub backup worker: git commit failed: %s', commit_output)
                await asyncio.sleep(GITHUB_BACKUP_INTERVAL_SECONDS)
                continue

            push_code, push_output = await _run_git_command(['push'])
            if push_code != 0:
                logging.warning('GitHub backup worker: git push failed: %s', push_output)
            else:
                logging.info('GitHub backup worker: backup pushed successfully.')
        except FileNotFoundError:
            logging.warning('GitHub backup worker: git binary not found.')
            return
        except Exception as error:
            logging.exception('GitHub backup worker failed: %s', error)

        await asyncio.sleep(GITHUB_BACKUP_INTERVAL_SECONDS)


@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    await show_main_menu(message.from_user.id, '╨Ф╨╛╨▒╤А╨╛ ╨┐╨╛╨╢╨░╨╗╨╛╨▓╨░╤В╤М ╨▓ ╨╝╨░╨│╨░╨╖╨╕╨╜ ╤Ж╨╕╤Д╤А╨╛╨▓╤Л╤Е ╤В╨╛╨▓╨░╤А╨╛╨▓!')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('menu:'))
async def menu_router(cb: types.CallbackQuery):
    pending_custom_qty_input.pop(cb.from_user.id, None)
    action = cb.data.split(':', 1)[1]

    if action == 'main':
        try:
            await cb.message.delete()
        except Exception:
            pass
        await show_main_menu(cb.from_user.id, '╨У╨╗╨░╨▓╨╜╨╛╨╡ ╨╝╨╡╨╜╤О:')
    elif action == 'adminpanel':
        if not is_admin(cb.from_user.id):
            await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
            return
        admin_add_product_state.pop(cb.from_user.id, None)
        await cb.message.edit_text('╨Я╨░╨╜╨╡╨╗╤М ╨░╨┤╨╝╨╕╨╜╨░: ╨▓╤Л╨▒╨╡╤А╨╕ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╨╡', reply_markup=admin_panel_kb())
    elif action == 'catalog':
        await cb.message.edit_text('╨Т╤Л╨▒╨╡╤А╨╕╤В╨╡ ╨║╨░╤В╨╡╨│╨╛╤А╨╕╤О:', reply_markup=catalog_kb())
    elif action == 'profile':
        await safe_edit_text(cb.message, build_profile_hub_text(cb.from_user.id), reply_markup=profile_kb())
    elif action == 'agreement':
        await cb.message.edit_text(AGREEMENT_TEXT, reply_markup=back_to_main_kb())
    elif action == 'topup':
        await cb.message.edit_text(
            'тЪая╕П ╨Я╨╛ ╨▓╤Л╨╜╤Г╨╢╨┤╨╡╨╜╨╜╤Л╨╝ ╤Б╨╕╤В╤Г╨░╤Ж╨╕╤П╨╝ ╨┐╨╛╨║╨░ ╨┤╨╛╤Б╤В╤Г╨┐╨╡╨╜ ╤В╨╛╨╗╤М╨║╨╛ ╤В╨░╨║╨╛╨╣ ╤Б╨┐╨╛╤Б╨╛╨▒ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П.\n'
            '╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╤З╨╡╤А╨╡╨╖ FunPay\n'
            '╨Ф╨╛╤Б╤В╤Г╨┐╨╜╤Л╨╡ ╤Б╤Г╨╝╨╝╤Л: 50тВ╜ ╨╕ 100тВ╜.',
            reply_markup=topup_kb(),
        )
    elif action == 'reviews':
        try:
            await cb.message.delete()
        except Exception:
            pass
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
        await bot.send_message(cb.from_user.id, '╨Ю╤В╨╖╤Л╨▓╤Л ╨┐╨╛╨║╤Г╨┐╨░╤В╨╡╨╗╨╡╨╣:', reply_markup=kb)
    elif action == 'channel':
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton('╨Ю╤В╨║╤А╤Л╤В╤М ╨║╨░╨╜╨░╨╗', url=CHANNEL_URL))
        kb.add(InlineKeyboardButton('ЁЯФЩ ╨Э╨░╨╖╨░╨┤', callback_data='menu:main'))
        await cb.message.edit_text('╨Э╨░╤И ╨║╨░╨╜╨░╨╗:', reply_markup=kb)

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('profile:'))
async def profile_router(cb: types.CallbackQuery):
    action = cb.data.split(':', 1)[1]

    if action == 'hub':
        await safe_edit_text(cb.message, build_profile_hub_text(cb.from_user.id), reply_markup=profile_kb())
        await cb.answer()
        return

    if action == 'orders':
        rows = list_user_orders(cb.from_user.id, limit=15)
        if not rows:
            text = '╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨║╤Г╨┐╨╛╨║ ╨┐╤Г╤Б╤В╨░.'
        else:
            lines = ['ЁЯз╛ ╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨║╤Г╨┐╨╛╨║:']
            for order_id, title, qty, total, status, code_value, created in rows:
                safe_title = html.escape(str(title or '╨в╨╛╨▓╨░╤А'))
                safe_status = html.escape(str(status or 'unknown'))
                lines.append(f'#{order_id} | {safe_title} | x{qty} | {total:.2f}тВ╜ | {safe_status} | {created}')
                if code_value and str(status).lower() == 'delivered':
                    safe_value = html.escape(str(code_value).strip())
                    if len(safe_value) > 800:
                        safe_value = safe_value[:800] + ' ...'
                    lines.append(f'╨Ф╨░╨╜╨╜╤Л╨╡: <code>{safe_value}</code>')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'topups':
        rows = list_user_topups(cb.from_user.id, limit=15)
        if not rows:
            text = '╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╣ ╨┐╤Г╤Б╤В╨░.'
        else:
            lines = ['ЁЯТ╕ ╨Ш╤Б╤В╨╛╤А╨╕╤П ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╣:']
            for topup_id, amount, status, created in rows:
                lines.append(f'#{topup_id} | {amount:.2f}тВ╜ | {status} | {created}')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'promo':
        pending_promo_input.add(cb.from_user.id)
        await safe_edit_text(cb.message, '╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╨┐╤А╨╛╨╝╨╛╨║╨╛╨┤ ╨╛╨┤╨╜╨╕╨╝ ╤Б╨╛╨╛╨▒╤Й╨╡╨╜╨╕╨╡╨╝:', reply_markup=back_to_main_kb())

    elif action == 'dailybonus':
        ok, credited, seconds_left = claim_daily_bonus(
            cb.from_user.id,
            DAILY_BONUS_AMOUNT,
            cooldown_hours=DAILY_BONUS_COOLDOWN_HOURS,
        )
        if ok:
            balance = get_balance(cb.from_user.id)
            text = (
                f'ЁЯОЙ ╨Х╨╢╨╡╨┤╨╜╨╡╨▓╨╜╤Л╨╣ ╨▒╨╛╨╜╤Г╤Б ╨┐╨╛╨╗╤Г╤З╨╡╨╜: +{credited:.2f} тВ╜\n'
                f'╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜\n\n'
                f'╨б╨╗╨╡╨┤╤Г╤О╤Й╨╕╨╣ ╨▒╨╛╨╜╤Г╤Б ╨▒╤Г╨┤╨╡╤В ╨┤╨╛╤Б╤В╤Г╨┐╨╡╨╜ ╤З╨╡╤А╨╡╨╖ {DAILY_BONUS_COOLDOWN_HOURS} ╤З.'
            )
        else:
            hours_left = seconds_left // 3600
            minutes_left = (seconds_left % 3600) // 60
            text = (
                'тП│ ╨С╨╛╨╜╤Г╤Б ╤Г╨╢╨╡ ╨┐╨╛╨╗╤Г╤З╨╡╨╜.\n'
                f'╨б╨╗╨╡╨┤╤Г╤О╤Й╨░╤П ╨┐╨╛╨┐╤Л╤В╨║╨░ ╤З╨╡╤А╨╡╨╖: {hours_left} ╤З {minutes_left} ╨╝╨╕╨╜.'
            )
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cat:'))
async def category_router(cb: types.CallbackQuery):
    category = cb.data.split(':', 1)[1]
    if category not in CATEGORY_NAMES:
        await cb.answer('╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╤П ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜╨░', show_alert=True)
        return

    products = list_products(category)
    if not products:
        await cb.message.edit_text(
            f'╨Т ╨║╨░╤В╨╡╨│╨╛╤А╨╕╨╕ {CATEGORY_NAMES[category]} ╨┐╨╛╨║╨░ ╨╜╨╡╤В ╤В╨╛╨▓╨░╤А╨╛╨▓ ╨▓ ╨╜╨░╨╗╨╕╤З╨╕╨╕.',
            reply_markup=back_to_main_kb(),
        )
        await cb.answer()
        return

    category_photo = {
        'proxy': PROXY_CATEGORY_PHOTO_URL,
        'tg': TG_CATEGORY_PHOTO_URL,
        'email': EMAIL_CATEGORY_PHOTO_URL,
    }.get(category)

    try:
        await cb.message.delete()
    except Exception:
        pass

    if category_photo:
        try:
            await bot.send_photo(cb.from_user.id, photo=category_photo)
        except Exception:
            pass

    kb = category_products_kb(category, page=0)
    await bot.send_message(cb.from_user.id, category_products_text(category, page=0), reply_markup=kb)
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('catpage:'))
async def category_page_router(cb: types.CallbackQuery):
    _, category, page_str = cb.data.split(':')
    page = int(page_str)
    products = list_products(category)
    if not products:
        await cb.message.edit_text(
            f'╨Т ╨║╨░╤В╨╡╨│╨╛╤А╨╕╨╕ {CATEGORY_NAMES.get(category, category)} ╨┐╨╛╨║╨░ ╨╜╨╡╤В ╤В╨╛╨▓╨░╤А╨╛╨▓ ╨▓ ╨╜╨░╨╗╨╕╤З╨╕╨╕.',
            reply_markup=back_to_main_kb(),
        )
        await cb.answer()
        return

    max_page = max((len(products) - 1) // PRODUCTS_PAGE_SIZE, 0)
    safe_page = min(max(page, 0), max_page)
    await cb.message.edit_text(
        category_products_text(category, safe_page),
        reply_markup=category_products_kb(category, safe_page),
    )
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('buy:'))
async def buy_router(cb: types.CallbackQuery):
    pending_custom_qty_input.pop(cb.from_user.id, None)
    parts = cb.data.split(':')
    product_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    product = get_product(product_id)
    if not product:
        await cb.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
        return

    _, title, description, price, _, category, stock, *_ = product
    if stock <= 0:
        await cb.answer('╨Э╨╡╤В ╨▓ ╨╜╨░╨╗╨╕╤З╨╕╨╕', show_alert=True)
        return

    text = product_card_text(title, category, float(price), int(stock), description)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('ЁЯТ░ ╨Ъ╤Г╨┐╨╕╤В╤М ╤В╨╛╨▓╨░╤А', callback_data=f'qtymenu:{product_id}:{page}'))
    kb.add(InlineKeyboardButton('тЖйя╕П ╨Э╨░╨╖╨░╨┤', callback_data=f'catpage:{category}:{page}'))
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qtymenu:'))
async def qty_menu_router(cb: types.CallbackQuery):
    pending_custom_qty_input.pop(cb.from_user.id, None)
    _, product_id_str, page_str = cb.data.split(':')
    product_id = int(product_id_str)
    page = int(page_str)
    product = get_product(product_id)
    if not product:
        await cb.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
        return

    _, title, _, price, _, category, stock, *_ = product
    if stock <= 0:
        await cb.answer('╨Э╨╡╤В ╨▓ ╨╜╨░╨╗╨╕╤З╨╕╨╕', show_alert=True)
        return

    text = (
        f'ЁЯз╛ ╨Я╨╛╨║╤Г╨┐╨║╨░: <b>{title}</b>\n'
        f'╨ж╨╡╨╜╨░: <b>{float(price):.2f} тВ╜</b>\n'
        f'╨Ф╨╛╤Б╤В╤Г╨┐╨╜╨╛: <b>{int(stock)} ╤И╤В.</b>\n\n'
        '╨Т╤Л╨▒╨╡╤А╨╕╤В╨╡ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛:'
    )
    await cb.message.edit_text(text, reply_markup=quantity_kb(product_id, category, int(stock), page=page))
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qty:'))
async def qty_router(cb: types.CallbackQuery):
    _, product_id_str, qty_str = cb.data.split(':')
    product_id = int(product_id_str)
    qty = int(qty_str)
    user_id = cb.from_user.id

    if user_id in active_buy_users:
        await cb.answer('╨Я╨╛╨║╤Г╨┐╨║╨░ ╤Г╨╢╨╡ ╨╛╨▒╤А╨░╨▒╨░╤В╤Л╨▓╨░╨╡╤В╤Б╤П, ╨┐╨╛╨┤╨╛╨╢╨┤╨╕╤В╨╡...', show_alert=True)
        return
    active_buy_users.add(user_id)

    try:
        product = get_product(product_id)
        if not product:
            await cb.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
            return

        _, title, _, price, credentials, category, stock, *_ = product

        if qty <= 0 or qty > 10:
            await cb.answer('╨Ь╨╛╨╢╨╜╨╛ ╨║╤Г╨┐╨╕╤В╤М ╨╛╤В 1 ╨┤╨╛ 10 ╤И╤В ╨╖╨░ ╤А╨░╨╖', show_alert=True)
            return

        if qty > stock:
            await cb.answer('╨Э╨╡╨┤╨╛╤Б╤В╨░╤В╨╛╤З╨╜╨╛ ╤В╨╛╨▓╨░╤А╨░ ╨╜╨░ ╤Б╨║╨╗╨░╨┤╨╡', show_alert=True)
            return

        total = float(price) * qty
        if not try_spend_balance(user_id, total):
            await cb.answer('╨Э╨╡╨┤╨╛╤Б╤В╨░╤В╨╛╤З╨╜╨╛ ╨▒╨░╨╗╨░╨╜╤Б╨░', show_alert=True)
            await cb.message.edit_text(
                f'╨Э╨╡╨┤╨╛╤Б╤В╨░╤В╨╛╤З╨╜╨╛ ╤Б╤А╨╡╨┤╤Б╤В╨▓ ╨┤╨╗╤П ╨┐╨╛╨║╤Г╨┐╨║╨╕ <b>{title}</b>.\n╨Э╤Г╨╢╨╜╨╛: {total:.2f} тВ╜',
                reply_markup=topup_kb(),
            )
            return

        update_stock(product_id, -qty)

        if category == 'tg':
            order_id = create_order(user_id, product_id, qty, total, 'waiting_phone')
            pending_tg_phone_order[user_id] = order_id
            cashback = calculate_cashback(total)
            if cashback > 0:
                change_balance(user_id, cashback)
            balance = get_balance(user_id)
            cashback_text = f'╨Ъ╨╡╤И╨▒╤Н╨║: +{cashback:.2f} тВ╜\n' if cashback > 0 else ''
            await cb.message.edit_text(
                f'тЬЕ ╨Ю╨┐╨╗╨░╤В╨░ ╨┐╤А╨╕╨╜╤П╤В╨░. ╨Ч╨░╨║╨░╨╖ #{order_id} ╤Б╨╛╨╖╨┤╨░╨╜.\n'
                '╨Ю╤В╨┐╤А╨░╨▓╤М╤В╨╡ ╨╜╨╛╨╝╨╡╤А ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░ ╨▓ ╨░╨║╨║╨░╤Г╨╜╤В.\n'
                '╨Я╨╛╤Б╨╗╨╡ ╤Н╤В╨╛╨│╨╛ ╨╜╨░╨╢╨╝╨╕╤В╨╡ ╨║╨╜╨╛╨┐╨║╤Г ┬л╨Я╨╛╨╗╤Г╤З╨╕╤В╤М ╨║╨╛╨┤ ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░┬╗.\n'
                f'{cashback_text}'
                f'╨Ю╤Б╤В╨░╤В╨╛╨║ ╨▒╨░╨╗╨░╨╜╤Б╨░: {balance:.2f} тВ╜',
                reply_markup=user_get_code_kb(order_id),
            )
            await cb.answer('╨Ю╨╢╨╕╨┤╨░╤О ╨╜╨╛╨╝╨╡╤А')
            return

        order_id = create_order(user_id, product_id, qty, total, 'delivered')
        delivery_data = format_delivery_credentials(credentials, qty)
        set_order_code(order_id, delivery_data)
        cashback = calculate_cashback(total)
        if cashback > 0:
            change_balance(user_id, cashback)
        balance = get_balance(user_id)
        cashback_text = f'╨Ъ╨╡╤И╨▒╤Н╨║: +{cashback:.2f} тВ╜\n' if cashback > 0 else ''
        deliver_text = (
            f'тЬЕ ╨Ч╨░╨║╨░╨╖ #{order_id} ╤Г╤Б╨┐╨╡╤И╨╜╨╛ ╨╛╨┐╨╗╨░╤З╨╡╨╜ ╤Б ╨▒╨░╨╗╨░╨╜╤Б╨░.\n'
            f'╨в╨╛╨▓╨░╤А: <b>{title}</b>\n'
            f'╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛: {qty}\n'
            f'╨б╤Г╨╝╨╝╨░: {total:.2f} тВ╜\n'
            f'{cashback_text}'
            f'╨Ю╤Б╤В╨░╤В╨╛╨║ ╨▒╨░╨╗╨░╨╜╤Б╨░: {balance:.2f} тВ╜\n\n'
            f'╨Ф╨░╨╜╨╜╤Л╨╡:\n<code>{delivery_data}</code>'
        )
        await cb.message.edit_text(deliver_text, reply_markup=back_to_main_kb())
        await cb.answer('╨Я╨╛╨║╤Г╨┐╨║╨░ ╤Г╤Б╨┐╨╡╤И╨╜╨░')
    finally:
        active_buy_users.discard(user_id)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qtycustom:'))
async def qty_custom_router(cb: types.CallbackQuery):
    _, product_id_str, page_str = cb.data.split(':')
    product_id = int(product_id_str)
    page = int(page_str)
    product = get_product(product_id)
    if not product:
        await cb.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
        return

    _, title, _, _, _, _, stock, *_ = product
    if stock <= 0:
        await cb.answer('╨Э╨╡╤В ╨▓ ╨╜╨░╨╗╨╕╤З╨╕╨╕', show_alert=True)
        return

    pending_custom_qty_input[cb.from_user.id] = {'product_id': product_id, 'page': page}
    await cb.message.edit_text(
        f'╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨┤╨╗╤П <b>{title}</b> (╨╛╤В 1 ╨┤╨╛ 10).\n'
        f'╨б╨╡╨╣╤З╨░╤Б ╨┤╨╛╤Б╤В╤Г╨┐╨╜╨╛: <b>{int(stock)} ╤И╤В.</b>',
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton('тЖйя╕П ╨Э╨░╨╖╨░╨┤', callback_data=f'qtymenu:{product_id}:{page}')
        ),
    )
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('topup:'))
async def topup_router(cb: types.CallbackQuery):
    action = cb.data.split(':', 1)[1]

    if action == 'custom':
        pending_custom_topup.add(cb.from_user.id)
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╤Б╤Г╨╝╨╝╤Г ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П ╤З╨╕╤Б╨╗╨╛╨╝ (╨╜╨░╨┐╤А╨╕╨╝╨╡╤А 750):', reply_markup=back_to_main_kb())
        await cb.answer()
        return

    amount = float(action)
    topup_id = create_topup(cb.from_user.id, amount, '')
    payment_link, payment_id = create_funpay_payment(amount, cb.from_user.id, topup_id)
    set_topup_payment_data(topup_id, payment_link, payment_id)
    if payment_id:
        set_topup_external_status(topup_id, 'created')

    await cb.message.edit_text(
        f'╨Ч╨░╤П╨▓╨║╨░ ╨╜╨░ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id} ╤Б╨╛╨╖╨┤╨░╨╜╨░ ╨╜╨░ {amount:.2f} тВ╜\n'
        f'1) ╨Ю╨┐╨╗╨░╤В╨╕╤В╨╡ ╤В╨╛╨▓╨░╤А ╨┐╨╛ ╤Б╤Б╤Л╨╗╨║╨╡: {payment_link}\n'
        f'2) ╨Т ╨║╨╛╨╝╨╝╨╡╨╜╤В╨░╤А╨╕╨╕ ╨║ ╨╖╨░╨║╨░╨╖╤Г ╤Г╨║╨░╨╢╨╕╤В╨╡ ╨║╨╛╨┤: <code>topup_{topup_id}_{cb.from_user.id}</code>\n'
        f'3) ╨б╤Г╨╝╨╝╨░ ╨╛╨┐╨╗╨░╤В╤Л ╨┤╨╛╨╗╨╢╨╜╨░ ╨▒╤Л╤В╤М: {amount:.2f} тВ╜\n\n'
        '╨Я╨╛╤Б╨╗╨╡ ╨╛╨┐╨╗╨░╤В╤Л ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕╤В╤Б╤П ╨░╨▓╤В╨╛╨╝╨░╤В╨╕╤З╨╡╤Б╨║╨╕ (╨╕╨╗╨╕ ╨░╨┤╨╝╨╕╨╜╨╛╨╝, ╨╡╤Б╨╗╨╕ API ╨╜╨╡╨┤╨╛╤Б╤В╤Г╨┐╨╡╨╜).',
        reply_markup=back_to_main_kb(),
    )

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f'╨Э╨╛╨▓╨░╤П ╨╖╨░╤П╨▓╨║╨░ ╨╜╨░ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id}\n'
                f'╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М: {cb.from_user.id}\n'
                f'╨б╤Г╨╝╨╝╨░: {amount:.2f} тВ╜\n'
                f'payment_id: {payment_id or "-"}\n'
                f'╨Я╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕╤В╤М: /confirmtopup {topup_id}',
            )
        except Exception:
            pass

    await cb.answer('╨Ч╨░╤П╨▓╨║╨░ ╤Б╨╛╨╖╨┤╨░╨╜╨░')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admpanel:'))
async def admin_panel_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
        return

    action = cb.data.split(':', 1)[1]
    user_id = cb.from_user.id

    if action == 'add_product':
        admin_add_product_state[user_id] = {'step': 'category'}
        admin_action_state.pop(user_id, None)
        await cb.message.edit_text('╨Т╤Л╨▒╨╡╤А╨╕ ╨║╨░╤В╨╡╨│╨╛╤А╨╕╤О ╨┤╨╗╤П ╨╜╨╛╨▓╨╛╨│╨╛ ╤В╨╛╨▓╨░╤А╨░:', reply_markup=admin_category_kb())
    elif action == 'home':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await cb.message.edit_text('╨Я╨░╨╜╨╡╨╗╤М ╨░╨┤╨╝╨╕╨╜╨░: ╨▓╤Л╨▒╨╡╤А╨╕ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╨╡', reply_markup=admin_panel_kb())
    elif action == 'cancel_add':
        admin_add_product_state.pop(user_id, None)
        await cb.message.edit_text('╨б╨╛╨╖╨┤╨░╨╜╨╕╨╡ ╤В╨╛╨▓╨░╤А╨░ ╨╛╤В╨╝╨╡╨╜╨╡╨╜╨╛.', reply_markup=admin_panel_kb())
    elif action == 'cancel_any':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await cb.message.edit_text('╨в╨╡╨║╤Г╤Й╨╡╨╡ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╨╡ ╨╛╤В╨╝╨╡╨╜╨╡╨╜╨╛.', reply_markup=admin_panel_kb())
    elif action == 'add_balance':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'add_balance', 'step': 'user_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ ID ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П ╨┤╨╗╤П ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П ╨▒╨░╨╗╨░╨╜╤Б╨░:', reply_markup=admin_step_kb())
    elif action == 'confirm_topup':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'confirm_topup', 'step': 'topup_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ ID ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П (topup_id):', reply_markup=admin_step_kb())
    elif action == 'create_promo':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'create_promo', 'step': 'code'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ ╨║╨╛╨┤ ╨┐╤А╨╛╨╝╨╛╨║╨╛╨┤╨░ (╨╜╨░╨┐╤А╨╕╨╝╨╡╤А: BONUS50):', reply_markup=admin_step_kb())
    elif action == 'send_code':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'send_code', 'step': 'order_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ ID ╨╖╨░╨║╨░╨╖╨░:', reply_markup=admin_step_kb())
    elif action == 'refill':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'refill', 'step': 'product_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ product_id ╨┤╨╗╤П ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П ╨╛╤Б╤В╨░╤В╨║╨░:', reply_markup=admin_step_kb())
    elif action == 'set_stock':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'set_stock', 'step': 'product_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ product_id ╨┤╨╗╤П ╤Г╤Б╤В╨░╨╜╨╛╨▓╨║╨╕ ╨╛╤Б╤В╨░╤В╨║╨░:', reply_markup=admin_step_kb())
    elif action == 'delete_product':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'delete_product', 'step': 'product_id'}
        await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ product_id ╨┤╨╗╤П ╤Г╨┤╨░╨╗╨╡╨╜╨╕╤П ╤В╨╛╨▓╨░╤А╨░ ╨╕╨╖ ╨║╨░╤В╨░╨╗╨╛╨│╨░:', reply_markup=admin_step_kb())
    elif action == 'list_products':
        rows = list_all_products_admin()
        if not rows:
            await cb.message.edit_text('╨в╨╛╨▓╨░╤А╨╛╨▓ ╨┐╨╛╨║╨░ ╨╜╨╡╤В.', reply_markup=admin_panel_kb())
        else:
            lines = ['ЁЯУЛ ╨в╨╛╨▓╨░╤А╤Л:']
            for pid, title, category, price, stock, auto_restock in rows:
                auto_text = 'auto' if int(auto_restock) == 1 else 'manual'
                lines.append(f'#{pid} | {title} | {category} | {float(price):.2f}тВ╜ | stock={int(stock)} | {auto_text}')
            await cb.message.edit_text('\n'.join(lines), reply_markup=admin_panel_kb())

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admsendcode:'))
async def admin_sendcode_quick_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
        return

    try:
        order_id = int(cb.data.split(':', 1)[1])
    except Exception:
        await cb.answer('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ order_id', show_alert=True)
        return

    order = get_order(order_id)
    if not order:
        await cb.answer('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
        return

    _, _, _, _, _, status, _, _, _ = order
    if status not in ('waiting_code', 'waiting_phone'):
        await cb.answer('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╛╨╢╨╕╨┤╨░╨╡╤В ╨║╨╛╨┤', show_alert=True)
        return

    admin_action_state[cb.from_user.id] = {
        'action': 'send_code',
        'step': 'code',
        'order_id': str(order_id),
    }
    await cb.message.answer(
        f'╨Ч╨░╨║╨░╨╖ #{order_id} ╨▓╤Л╨▒╤А╨░╨╜. ╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╨║╨╛╨┤ ╤Б╨╗╨╡╨┤╤Г╤О╤Й╨╕╨╝ ╤Б╨╛╨╛╨▒╤Й╨╡╨╜╨╕╨╡╨╝.',
        reply_markup=admin_step_kb(),
    )
    await cb.answer('╨Ю╨╢╨╕╨┤╨░╤О ╨║╨╛╨┤')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('userreqcode:'))
async def user_request_code_router(cb: types.CallbackQuery):
    try:
        order_id = int(cb.data.split(':', 1)[1])
    except Exception:
        await cb.answer('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ ╨╜╨╛╨╝╨╡╤А ╨╖╨░╨║╨░╨╖╨░', show_alert=True)
        return

    order = get_order(order_id)
    if not order:
        await cb.answer('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜', show_alert=True)
        return

    _, buyer_user_id, product_id, qty, total, status, client_phone, _, _ = order
    if int(buyer_user_id) != int(cb.from_user.id):
        await cb.answer('╨н╤В╨╛ ╨╜╨╡ ╨▓╨░╤И ╨╖╨░╨║╨░╨╖', show_alert=True)
        return

    if status == 'waiting_phone':
        await cb.answer('╨б╨╜╨░╤З╨░╨╗╨░ ╨╛╤В╨┐╤А╨░╨▓╤М╤В╨╡ ╨╜╨╛╨╝╨╡╤А ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░ ╨▓ ╨░╨║╨║╨░╤Г╨╜╤В', show_alert=True)
        return

    if status == 'delivered':
        await cb.answer('╨Ъ╨╛╨┤ ╤Г╨╢╨╡ ╨▓╤Л╨┤╨░╨╜ ╨┐╨╛ ╤Н╤В╨╛╨╝╤Г ╨╖╨░╨║╨░╨╖╤Г', show_alert=True)
        return

    product = get_product(int(product_id))
    product_title = html.escape(str(product[1] if product else 'TG ╨░╨║╨║╨░╤Г╨╜╤В'))
    product_data = ''
    if product:
        product_data = format_delivery_credentials(str(product[4] or ''), int(qty))
    safe_product_data = html.escape(product_data) if product_data else '╨╜╨╡ ╤Г╨║╨░╨╖╨░╨╜╤Л'
    phone_value = html.escape(str(client_phone).strip()) if client_phone else '╨╜╨╡ ╤Г╨║╨░╨╖╨░╨╜'

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                'ЁЯУй ╨Ч╨░╨┐╤А╨╛╤Б ╨║╨╛╨┤╨░ ╨╛╤В ╨┐╨╛╨║╤Г╨┐╨░╤В╨╡╨╗╤П\n'
                f'╨Ч╨░╨║╨░╨╖: <b>#{order_id}</b>\n'
                f'╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М: <code>{buyer_user_id}</code>\n'
                f'╨в╨╛╨▓╨░╤А: <b>{product_title}</b>\n'
                f'╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛: <b>{int(qty)}</b>\n'
                f'╨б╤Г╨╝╨╝╨░: <b>{float(total):.2f} тВ╜</b>\n'
                f'╨Ф╨░╨╜╨╜╤Л╨╡ ╤В╨╛╨▓╨░╤А╨░: <code>{safe_product_data}</code>\n'
                f'╨Э╨╛╨╝╨╡╤А: <code>{phone_value}</code>\n\n'
                f'╨Ю╤В╨┐╤А╨░╨▓╨╕╤В╤М ╨║╨╛╨┤: /sendcode {order_id} 12345',
                reply_markup=admin_get_code_kb(order_id),
            )
        except Exception:
            pass

    await cb.answer('╨Ч╨░╨┐╤А╨╛╤Б ╨╛╤В╨┐╤А╨░╨▓╨╗╨╡╨╜ ╨░╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤В╨╛╤А╤Г')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddcat:'))
async def admin_add_category_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state:
        await cb.answer('╨б╨╜╨░╤З╨░╨╗╨░ ╨╛╤В╨║╤А╨╛╨╣ /adminpanel', show_alert=True)
        return

    category = cb.data.split(':', 1)[1]
    if category not in CATEGORY_NAMES:
        await cb.answer('╨Э╨╡╨▓╨╡╤А╨╜╨░╤П ╨║╨░╤В╨╡╨│╨╛╤А╨╕╤П', show_alert=True)
        return

    state['category'] = category
    state['step'] = 'title'
    await cb.message.edit_text('╨Т╨▓╨╡╨┤╨╕ ╨╜╨░╨╖╨▓╨░╨╜╨╕╨╡ ╤В╨╛╨▓╨░╤А╨░ (╨╜╨░╨┐╤А╨╕╨╝╨╡╤А: ЁЯЗйЁЯЗк ╨У╨╡╤А╨╝╨░╨╜╨╕╤П 3 ╨┤╨╜╤П):')
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddauto:'))
async def admin_add_auto_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state or state.get('step') != 'auto_restock':
        await cb.answer('╨б╨╜╨░╤З╨░╨╗╨░ ╨┐╤А╨╛╨╣╨┤╨╕ ╤И╨░╨│╨╕ ╨╝╨░╤Б╤В╨╡╤А╨░', show_alert=True)
        return

    choice = cb.data.split(':', 1)[1]
    auto_restock = 1 if choice == 'yes' else 0
    state['auto_restock'] = str(auto_restock)
    state['step'] = 'confirm'

    auto_text = '╨▓╨║╨╗╤О╤З╨╡╨╜╨╛ (+1 ╨║ ╤Б╨║╨╗╨░╨┤╤Г ╨║╨░╨╢╨┤╤Л╨╡ 30 ╤Б╨╡╨║╤Г╨╜╨┤)' if auto_restock else '╨▓╤Л╨║╨╗╤О╤З╨╡╨╜╨╛'
    await cb.message.edit_text(
        '╨Я╤А╨╛╨▓╨╡╤А╤М ╨┤╨░╨╜╨╜╤Л╨╡ ╤В╨╛╨▓╨░╤А╨░ ╨┐╨╡╤А╨╡╨┤ ╤Б╨╛╤Е╤А╨░╨╜╨╡╨╜╨╕╨╡╨╝:\n\n'
        f'╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╤П: {CATEGORY_NAMES.get(state["category"], state["category"])}\n'
        f'╨Э╨░╨╖╨▓╨░╨╜╨╕╨╡: {state["title"]}\n'
        f'╨ж╨╡╨╜╨░: {float(state["price"]):.2f} тВ╜\n'
        f'╨Ю╤Б╤В╨░╤В╨╛╨║: {int(state["stock"])}\n'
        f'╨Ф╨░╨╜╨╜╤Л╨╡: {state["credentials"]}\n'
        f'╨Ю╨┐╨╕╤Б╨░╨╜╨╕╨╡: {state["description"]}\n'
        f'╨Р╨▓╤В╨╛╨╛╨▒╨╜╨╛╨▓╨╗╨╡╨╜╨╕╨╡: {auto_text}',
        reply_markup=admin_confirm_product_kb(),
    )
    await cb.answer('╨Я╤А╨╛╨▓╨╡╤А╤М ╨╕ ╨┐╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddsave:'))
async def admin_add_save_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state or state.get('step') != 'confirm':
        await cb.answer('╨Э╨╡╤В ╨┤╨░╨╜╨╜╤Л╤Е ╨┤╨╗╤П ╤Б╨╛╤Е╤А╨░╨╜╨╡╨╜╨╕╤П', show_alert=True)
        return

    product_id = add_product(
        state['title'],
        float(state['price']),
        state['credentials'],
        state['category'],
        state['description'],
        int(state['stock']),
        auto_restock=int(state.get('auto_restock', '0')),
        restock_every_minutes=5,
        restock_amount=1,
    )
    auto_text = '╨▓╨║╨╗╤О╤З╨╡╨╜╨╛ (+1 ╨║ ╤Б╨║╨╗╨░╨┤╤Г ╨║╨░╨╢╨┤╤Л╨╡ 30 ╤Б╨╡╨║╤Г╨╜╨┤)' if int(state.get('auto_restock', '0')) == 1 else '╨▓╤Л╨║╨╗╤О╤З╨╡╨╜╨╛'
    admin_add_product_state.pop(user_id, None)
    await cb.message.edit_text(
        f'тЬЕ ╨в╨╛╨▓╨░╤А ╨┤╨╛╨▒╨░╨▓╨╗╨╡╨╜, id={product_id}\n'
        f'╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╤П: {CATEGORY_NAMES.get(state["category"], state["category"])}\n'
        f'╨Ю╤Б╤В╨░╤В╨╛╨║ ╨╜╨░ ╤Б╨║╨╗╨░╨┤╨╡: {int(state["stock"])}\n'
        f'╨Р╨▓╤В╨╛╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╤Б╨║╨╗╨░╨┤╨░: {auto_text}',
        reply_markup=admin_panel_kb(),
    )
    await cb.answer('╨б╨╛╤Е╤А╨░╨╜╨╡╨╜╨╛')


@dp.message_handler(content_types=types.ContentType.TEXT)
async def text_router(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or '').strip()

    # Fallback for clients/buttons that send plain text instead of command entity.
    lower_text = text.lower()
    if lower_text in {'старт', 'start'} or lower_text.startswith('/start'):
        pending_custom_qty_input.pop(user_id, None)
        pending_custom_topup.discard(user_id)
        pending_promo_input.discard(user_id)
        pending_tg_phone_order.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        admin_add_product_state.pop(user_id, None)
        await show_main_menu(user_id, 'Главное меню:')
        return

    if user_id in pending_custom_qty_input:
        pending_data = pending_custom_qty_input[user_id]
        try:
            qty = int(text)
        except ValueError:
            await message.answer('╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╤Ж╨╡╨╗╨╛╨╡ ╤З╨╕╤Б╨╗╨╛ ╨╛╤В 1 ╨┤╨╛ 10.')
            return

        if qty < 1 or qty > 10:
            await message.answer('╨Ь╨╛╨╢╨╜╨╛ ╨║╤Г╨┐╨╕╤В╤М ╤В╨╛╨╗╤М╨║╨╛ ╨╛╤В 1 ╨┤╨╛ 10 ╤И╤В ╨╖╨░ ╤А╨░╨╖.')
            return

        product_id = int(pending_data['product_id'])

        if user_id in active_buy_users:
            await message.answer('╨Я╨╛╨║╤Г╨┐╨║╨░ ╤Г╨╢╨╡ ╨╛╨▒╤А╨░╨▒╨░╤В╤Л╨▓╨░╨╡╤В╤Б╤П, ╨┐╨╛╨┤╨╛╨╢╨┤╨╕╤В╨╡...')
            return
        active_buy_users.add(user_id)

        try:
            product = get_product(product_id)
            if not product:
                pending_custom_qty_input.pop(user_id, None)
                await message.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜.', reply_markup=back_to_main_kb())
                return

            _, title, _, price, credentials, category, stock, *_ = product
            if qty > int(stock):
                await message.answer(
                    f'╨Э╨╡╨┤╨╛╤Б╤В╨░╤В╨╛╤З╨╜╨╛ ╤В╨╛╨▓╨░╤А╨░ ╨╜╨░ ╤Б╨║╨╗╨░╨┤╨╡. ╨Ф╨╛╤Б╤В╤Г╨┐╨╜╨╛: {int(stock)} ╤И╤В.',
                    reply_markup=back_to_main_kb(),
                )
                return

            total = float(price) * qty
            if not try_spend_balance(user_id, total):
                await message.answer(
                    f'╨Э╨╡╨┤╨╛╤Б╤В╨░╤В╨╛╤З╨╜╨╛ ╤Б╤А╨╡╨┤╤Б╤В╨▓ ╨┤╨╗╤П ╨┐╨╛╨║╤Г╨┐╨║╨╕ <b>{title}</b>.\n╨Э╤Г╨╢╨╜╨╛: {total:.2f} тВ╜',
                    reply_markup=topup_kb(),
                )
                return

            update_stock(product_id, -qty)

            if category == 'tg':
                order_id = create_order(user_id, product_id, qty, total, 'waiting_phone')
                pending_tg_phone_order[user_id] = order_id
                cashback = calculate_cashback(total)
                if cashback > 0:
                    change_balance(user_id, cashback)
                balance = get_balance(user_id)
                cashback_text = f'╨Ъ╨╡╤И╨▒╤Н╨║: +{cashback:.2f} тВ╜\n' if cashback > 0 else ''
                await message.answer(
                    f'тЬЕ ╨Ю╨┐╨╗╨░╤В╨░ ╨┐╤А╨╕╨╜╤П╤В╨░. ╨Ч╨░╨║╨░╨╖ #{order_id} ╤Б╨╛╨╖╨┤╨░╨╜.\n'
                    '╨Ю╤В╨┐╤А╨░╨▓╤М╤В╨╡ ╨╜╨╛╨╝╨╡╤А ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░ ╨▓ ╨░╨║╨║╨░╤Г╨╜╤В.\n'
                    '╨Я╨╛╤Б╨╗╨╡ ╤Н╤В╨╛╨│╨╛ ╨╜╨░╨╢╨╝╨╕╤В╨╡ ╨║╨╜╨╛╨┐╨║╤Г ┬л╨Я╨╛╨╗╤Г╤З╨╕╤В╤М ╨║╨╛╨┤ ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░┬╗.\n'
                    f'{cashback_text}'
                    f'╨Ю╤Б╤В╨░╤В╨╛╨║ ╨▒╨░╨╗╨░╨╜╤Б╨░: {balance:.2f} тВ╜',
                    reply_markup=user_get_code_kb(order_id),
                )
                pending_custom_qty_input.pop(user_id, None)
                return

            order_id = create_order(user_id, product_id, qty, total, 'delivered')
            delivery_data = format_delivery_credentials(credentials, qty)
            set_order_code(order_id, delivery_data)
            cashback = calculate_cashback(total)
            if cashback > 0:
                change_balance(user_id, cashback)
            balance = get_balance(user_id)
            cashback_text = f'╨Ъ╨╡╤И╨▒╤Н╨║: +{cashback:.2f} тВ╜\n' if cashback > 0 else ''
            await message.answer(
                f'тЬЕ ╨Ч╨░╨║╨░╨╖ #{order_id} ╤Г╤Б╨┐╨╡╤И╨╜╨╛ ╨╛╨┐╨╗╨░╤З╨╡╨╜ ╤Б ╨▒╨░╨╗╨░╨╜╤Б╨░.\n'
                f'╨в╨╛╨▓╨░╤А: <b>{title}</b>\n'
                f'╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛: {qty}\n'
                f'╨б╤Г╨╝╨╝╨░: {total:.2f} тВ╜\n'
                f'{cashback_text}'
                f'╨Ю╤Б╤В╨░╤В╨╛╨║ ╨▒╨░╨╗╨░╨╜╤Б╨░: {balance:.2f} тВ╜\n\n'
                f'╨Ф╨░╨╜╨╜╤Л╨╡:\n<code>{delivery_data}</code>',
                reply_markup=back_to_main_kb(),
            )
            pending_custom_qty_input.pop(user_id, None)
            return
        finally:
            active_buy_users.discard(user_id)

    if is_admin(user_id) and user_id in admin_action_state:
        state = admin_action_state[user_id]
        action = state.get('action')
        step = state.get('step')

        if action == 'add_balance':
            if step == 'user_id':
                try:
                    target_user_id = int(text)
                except ValueError:
                    await message.answer('ID ╨┤╨╛╨╗╨╢╨╡╨╜ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                state['user_id'] = str(target_user_id)
                state['step'] = 'amount'
                await message.answer('╨Т╨▓╨╡╨┤╨╕ ╤Б╤Г╨╝╨╝╤Г ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╤П (╨╝╨╛╨╢╨╜╨╛ ╨╛╤В╤А╨╕╤Ж╨░╤В╨╡╨╗╤М╨╜╤Г╤О ╨┤╨╗╤П ╤Б╨┐╨╕╤Б╨░╨╜╨╕╤П):')
                return

            if step == 'amount':
                try:
                    amount = float(text.replace(',', '.'))
                except ValueError:
                    await message.answer('╨б╤Г╨╝╨╝╨░ ╨┤╨╛╨╗╨╢╨╜╨░ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                target_user_id = int(state['user_id'])
                change_balance(target_user_id, amount)
                balance = get_balance(target_user_id)
                admin_action_state.pop(user_id, None)
                await message.answer(
                    f'тЬЕ ╨С╨░╨╗╨░╨╜╤Б ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П {target_user_id} ╨╕╨╖╨╝╨╡╨╜╨╡╨╜ ╨╜╨░ {amount:.2f} тВ╜.\n╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜',
                    reply_markup=admin_panel_kb(),
                )
                try:
                    await bot.send_message(target_user_id, f'тЬЕ ╨Т╨░╤И ╨▒╨░╨╗╨░╨╜╤Б ╨╕╨╖╨╝╨╡╨╜╨╡╨╜ ╨╜╨░ {amount:.2f} тВ╜.\n╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜')
                except Exception:
                    pass
                return

        if action == 'confirm_topup' and step == 'topup_id':
            try:
                topup_id = int(text)
            except ValueError:
                await message.answer('topup_id ╨┤╨╛╨╗╨╢╨╡╨╜ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                return

            result = confirm_topup(topup_id)
            if not result:
                await message.answer('╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜╨╛. ╨Т╨▓╨╡╨┤╨╕ ╨┤╤А╤Г╨│╨╛╨╣ topup_id:')
                return

            _, target_user_id, amount, _ = result
            balance = get_balance(target_user_id)
            admin_action_state.pop(user_id, None)
            await message.answer(
                f'тЬЕ ╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id} ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╛ ╨╜╨░ {float(amount):.2f} тВ╜.',
                reply_markup=admin_panel_kb(),
            )
            try:
                await bot.send_message(target_user_id, f'тЬЕ ╨Т╨░╤И ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜ ╨╜╨░ {float(amount):.2f} тВ╜.\n╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜')
            except Exception:
                pass
            return

        if action == 'create_promo':
            if step == 'code':
                state['code'] = text.upper()
                state['step'] = 'amount'
                await message.answer('╨Т╨▓╨╡╨┤╨╕ ╤Б╤Г╨╝╨╝╤Г ╨┐╤А╨╛╨╝╨╛╨║╨╛╨┤╨░:')
                return
            if step == 'amount':
                try:
                    amount = float(text.replace(',', '.'))
                    if amount <= 0:
                        raise ValueError
                except ValueError:
                    await message.answer('╨б╤Г╨╝╨╝╨░ ╨┤╨╛╨╗╨╢╨╜╨░ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝ > 0. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                state['amount'] = str(amount)
                state['step'] = 'uses'
                await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨░╨║╤В╨╕╨▓╨░╤Ж╨╕╨╣:')
                return
            if step == 'uses':
                try:
                    uses = int(text)
                    if uses <= 0:
                        raise ValueError
                except ValueError:
                    await message.answer('╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨░╨║╤В╨╕╨▓╨░╤Ж╨╕╨╣ ╨┤╨╛╨╗╨╢╨╜╨╛ ╨▒╤Л╤В╤М ╤Ж╨╡╨╗╤Л╨╝ > 0. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                create_promo(state['code'], float(state['amount']), uses)
                admin_action_state.pop(user_id, None)
                await message.answer(
                    f'тЬЕ ╨Я╤А╨╛╨╝╨╛╨║╨╛╨┤ {state["code"]} ╤Б╨╛╨╖╨┤╨░╨╜: {float(state["amount"]):.2f} тВ╜, ╨░╨║╤В╨╕╨▓╨░╤Ж╨╕╨╣ {uses}',
                    reply_markup=admin_panel_kb(),
                )
                return

        if action == 'send_code':
            if step == 'order_id':
                try:
                    order_id = int(text)
                except ValueError:
                    await message.answer('order_id ╨┤╨╛╨╗╨╢╨╡╨╜ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                order = get_order(order_id)
                if not order:
                    await message.answer('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜. ╨Т╨▓╨╡╨┤╨╕ ╨┤╤А╤Г╨│╨╛╨╣ order_id:')
                    return
                state['order_id'] = str(order_id)
                state['step'] = 'code'
                await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨║╨╛╨┤ ╨┤╨╗╤П ╨╛╤В╨┐╤А╨░╨▓╨║╨╕ ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤О:')
                return
            if step == 'code':
                order_id = int(state['order_id'])
                code = text
                order = get_order(order_id)
                if not order:
                    admin_action_state.pop(user_id, None)
                    await message.answer('╨Ч╨░╨║╨░╨╖ ╤Г╨╢╨╡ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜.', reply_markup=admin_panel_kb())
                    return
                _, target_user_id, _, _, _, status, _, _, _ = order
                if status not in ('waiting_code', 'waiting_phone'):
                    admin_action_state.pop(user_id, None)
                    await message.answer('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╛╨╢╨╕╨┤╨░╨╡╤В ╨║╨╛╨┤.', reply_markup=admin_panel_kb())
                    return
                set_order_code(order_id, code)
                admin_action_state.pop(user_id, None)
                await message.answer('тЬЕ ╨Ъ╨╛╨┤ ╨╛╤В╨┐╤А╨░╨▓╨╗╨╡╨╜ ╨║╨╗╨╕╨╡╨╜╤В╤Г.', reply_markup=admin_panel_kb())
                await bot.send_message(target_user_id, f'╨Ъ╨╛╨┤ ╨┤╨╗╤П ╨╖╨░╨║╨░╨╖╨░ #{order_id}: <code>{code}</code>')
                return

        if action in ('refill', 'set_stock'):
            if step == 'product_id':
                try:
                    product_id = int(text)
                except ValueError:
                    await message.answer('product_id ╨┤╨╛╨╗╨╢╨╡╨╜ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                state['product_id'] = str(product_id)
                state['step'] = 'qty'
                if action == 'refill':
                    await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨┤╨╗╤П ╨┤╨╛╨▒╨░╨▓╨╗╨╡╨╜╨╕╤П (+):')
                else:
                    await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨╕╤В╨╛╨│╨╛╨▓╨╛╨╡ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨╜╨░ ╤Б╨║╨╗╨░╨┤╨╡:')
                return
            if step == 'qty':
                try:
                    qty = int(text)
                except ValueError:
                    await message.answer('╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨┤╨╛╨╗╨╢╨╜╨╛ ╨▒╤Л╤В╤М ╤Ж╨╡╨╗╤Л╨╝ ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                    return
                product_id = int(state['product_id'])
                if action == 'refill':
                    update_stock(product_id, qty)
                    message_text = f'тЬЕ ╨Ю╤Б╤В╨░╤В╨╛╨║ ╤В╨╛╨▓╨░╤А╨░ #{product_id} ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜ ╨╜╨░ {qty}.'
                else:
                    set_stock(product_id, qty)
                    message_text = f'тЬЕ ╨Ю╤Б╤В╨░╤В╨╛╨║ ╤В╨╛╨▓╨░╤А╨░ #{product_id} ╤Г╤Б╤В╨░╨╜╨╛╨▓╨╗╨╡╨╜: {qty}.'
                admin_action_state.pop(user_id, None)
                await message.answer(message_text, reply_markup=admin_panel_kb())
                return

        if action == 'delete_product' and step == 'product_id':
            try:
                product_id = int(text)
            except ValueError:
                await message.answer('product_id ╨┤╨╛╨╗╨╢╨╡╨╜ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:')
                return

            deleted = deactivate_product(product_id)
            admin_action_state.pop(user_id, None)
            if deleted:
                await message.answer(f'тЬЕ ╨в╨╛╨▓╨░╤А #{product_id} ╤Г╨┤╨░╨╗╨╡╨╜ ╨╕╨╖ ╨║╨░╤В╨░╨╗╨╛╨│╨░.', reply_markup=admin_panel_kb())
            else:
                await message.answer('╨в╨╛╨▓╨░╤А ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜ ╨╕╨╗╨╕ ╤Г╨╢╨╡ ╤Г╨┤╨░╨╗╨╡╨╜.', reply_markup=admin_panel_kb())
            return

    if is_admin(user_id) and user_id in admin_add_product_state:
        state = admin_add_product_state[user_id]
        step = state.get('step')

        if step == 'title':
            state['title'] = text
            state['step'] = 'price'
            await message.answer('╨в╨╡╨┐╨╡╤А╤М ╨▓╨▓╨╡╨┤╨╕ ╤Ж╨╡╨╜╤Г (╨╜╨░╨┐╤А╨╕╨╝╨╡╤А: 8.8)', reply_markup=admin_step_kb())
            return

        if step == 'price':
            try:
                price = float(text.replace(',', '.'))
                if price <= 0:
                    raise ValueError
            except ValueError:
                await message.answer('╨ж╨╡╨╜╨░ ╨┤╨╛╨╗╨╢╨╜╨░ ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨╛╨╝ ╨▒╨╛╨╗╤М╤И╨╡ 0. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:', reply_markup=admin_step_kb())
                return
            state['price'] = str(price)
            state['step'] = 'stock'
            await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨║╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨╜╨░ ╤Б╨║╨╗╨░╨┤╨╡ (stock):', reply_markup=admin_step_kb())
            return

        if step == 'stock':
            try:
                stock = int(text)
                if stock < 0:
                    raise ValueError
            except ValueError:
                await message.answer('╨Ъ╨╛╨╗╨╕╤З╨╡╤Б╤В╨▓╨╛ ╨┤╨╛╨╗╨╢╨╜╨╛ ╨▒╤Л╤В╤М ╤Ж╨╡╨╗╤Л╨╝ ╤З╨╕╤Б╨╗╨╛╨╝ 0 ╨╕╨╗╨╕ ╨▒╨╛╨╗╤М╤И╨╡. ╨Т╨▓╨╡╨┤╨╕ ╤Б╨╜╨╛╨▓╨░:', reply_markup=admin_step_kb())
                return
            state['stock'] = str(stock)
            state['step'] = 'credentials'
            await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨┤╨░╨╜╨╜╤Л╨╡ ╤В╨╛╨▓╨░╤А╨░ (╨╗╨╛╨│╨╕╨╜:╨┐╨░╤А╨╛╨╗╤М / ╨┐╤А╨╛╨║╤Б╨╕ / ╨╜╨╛╨╝╨╡╤А ╨╕ ╤В.╨┤.):', reply_markup=admin_step_kb())
            return

        if step == 'credentials':
            state['credentials'] = text
            state['step'] = 'description'
            await message.answer('╨Т╨▓╨╡╨┤╨╕ ╨╛╨┐╨╕╤Б╨░╨╜╨╕╨╡ ╤В╨╛╨▓╨░╤А╨░:', reply_markup=admin_step_kb())
            return

        if step == 'description':
            state['description'] = text
            state['step'] = 'auto_restock'
            await message.answer(
                '╨Т╨║╨╗╤О╤З╨╕╤В╤М ╨░╨▓╤В╨╛╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ ╤Б╨║╨╗╨░╨┤╨░?\n╨Х╤Б╨╗╨╕ ╨▓╨║╨╗╤О╤З╨╕╤В╤М, ╤Б╨║╨╗╨░╨┤ ╨▒╤Г╨┤╨╡╤В ╤Г╨▓╨╡╨╗╨╕╤З╨╕╨▓╨░╤В╤М╤Б╤П ╨╜╨░ +1 ╨║╨░╨╢╨┤╤Л╨╡ 30 ╤Б╨╡╨║╤Г╨╜╨┤.',
                reply_markup=admin_auto_restock_kb(),
            )
            return

    if user_id in pending_promo_input:
        pending_promo_input.discard(user_id)
        ok, msg, amount = activate_promo(user_id, text)
        if ok:
            bal = get_balance(user_id)
            await message.answer(f'тЬЕ {msg}\n╨Э╨░╤З╨╕╤Б╨╗╨╡╨╜╨╛: {amount:.2f} тВ╜\n╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {bal:.2f} тВ╜', reply_markup=main_menu_kb(user_id))
        else:
            await message.answer(f'тЭМ {msg}', reply_markup=main_menu_kb(user_id))
        return

    if user_id in pending_custom_topup:
        try:
            amount = float(text.replace(',', '.'))
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer('╨Т╨▓╨╡╨┤╨╕╤В╨╡ ╨║╨╛╤А╤А╨╡╨║╤В╨╜╤Г╤О ╤Б╤Г╨╝╨╝╤Г ╤З╨╕╤Б╨╗╨╛╨╝.')
            return

        pending_custom_topup.discard(user_id)
        topup_id = create_topup(user_id, amount, '')
        payment_link, payment_id = create_funpay_payment(amount, user_id, topup_id)
        set_topup_payment_data(topup_id, payment_link, payment_id)
        if payment_id:
            set_topup_external_status(topup_id, 'created')

        await message.answer(
            f'╨Ч╨░╤П╨▓╨║╨░ ╨╜╨░ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id} ╤Б╨╛╨╖╨┤╨░╨╜╨░ ╨╜╨░ {amount:.2f} тВ╜\n'
            f'1) ╨Ю╨┐╨╗╨░╤В╨╕╤В╨╡ ╤В╨╛╨▓╨░╤А ╨┐╨╛ ╤Б╤Б╤Л╨╗╨║╨╡: {payment_link}\n'
            f'2) ╨Т ╨║╨╛╨╝╨╝╨╡╨╜╤В╨░╤А╨╕╨╕ ╨║ ╨╖╨░╨║╨░╨╖╤Г ╤Г╨║╨░╨╢╨╕╤В╨╡ ╨║╨╛╨┤: <code>topup_{topup_id}_{user_id}</code>\n'
            f'3) ╨б╤Г╨╝╨╝╨░ ╨╛╨┐╨╗╨░╤В╤Л ╨┤╨╛╨╗╨╢╨╜╨░ ╨▒╤Л╤В╤М: {amount:.2f} тВ╜\n'
            '╨Я╨╛╤Б╨╗╨╡ ╨╛╨┐╨╗╨░╤В╤Л ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕╤В╤Б╤П ╨░╨▓╤В╨╛╨╝╨░╤В╨╕╤З╨╡╤Б╨║╨╕ (╨╕╨╗╨╕ ╨░╨┤╨╝╨╕╨╜╨╛╨╝, ╨╡╤Б╨╗╨╕ API ╨╜╨╡╨┤╨╛╤Б╤В╤Г╨┐╨╡╨╜).',
            reply_markup=main_menu_kb(user_id),
        )

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f'╨Э╨╛╨▓╨░╤П ╨╖╨░╤П╨▓╨║╨░ ╨╜╨░ ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id}\n'
                    f'╨Я╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤М: {user_id}\n'
                    f'╨б╤Г╨╝╨╝╨░: {amount:.2f} тВ╜\n'
                    f'payment_id: {payment_id or "-"}\n'
                    f'╨Я╨╛╨┤╤В╨▓╨╡╤А╨┤╨╕╤В╤М: /confirmtopup {topup_id}',
                )
            except Exception:
                pass
        return

    if user_id in pending_tg_phone_order:
        order_id = pending_tg_phone_order.pop(user_id)
        set_order_phone(order_id, text)
        set_order_status(order_id, 'waiting_code')
        order = get_order(order_id)
        product_data = ''
        if order:
            _, _, product_id, qty, _, _, _, _, _ = order
            product = get_product(int(product_id))
            if product:
                product_data = format_delivery_credentials(str(product[4] or ''), int(qty))
        safe_product_data = html.escape(product_data) if product_data else '╨╜╨╡ ╤Г╨║╨░╨╖╨░╨╜╤Л'

        await message.answer(
            f'╨Э╨╛╨╝╨╡╤А ╤Б╨╛╤Е╤А╨░╨╜╤С╨╜ ╨┤╨╗╤П ╨╖╨░╨║╨░╨╖╨░ #{order_id}.\n'
            f'╨Ф╨░╨╜╨╜╤Л╨╡ ╤В╨╛╨▓╨░╤А╨░ ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░: <code>{safe_product_data}</code>\n'
            '╨Э╨░╨╢╨╝╨╕╤В╨╡ ╨║╨╜╨╛╨┐╨║╤Г ┬л╨Я╨╛╨╗╤Г╤З╨╕╤В╤М ╨║╨╛╨┤ ╨┤╨╗╤П ╨▓╤Е╨╛╨┤╨░┬╗, ╤З╤В╨╛╨▒╤Л ╨╛╤В╨┐╤А╨░╨▓╨╕╤В╤М ╨╖╨░╨┐╤А╨╛╤Б ╨░╨┤╨╝╨╕╨╜╨╕╤Б╤В╤А╨░╤В╨╛╤А╤Г.',
            reply_markup=user_get_code_kb(order_id),
        )
        return


@dp.message_handler(commands=['admin'])
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.reply(
        '╨Р╨┤╨╝╨╕╨╜ ╨║╨╛╨╝╨░╨╜╨┤╤Л:\n'
        '/adminpanel тАФ ╨╝╨░╤Б╤В╨╡╤А ╨┤╨╛╨▒╨░╨▓╨╗╨╡╨╜╨╕╤П ╤В╨╛╨▓╨░╤А╨░\n'
        '/addproduct category|title|price|stock|credentials|description\n'
        '/refill <product_id> <qty>\n'
        '/setstock <product_id> <qty>\n'
        '/confirmtopup <topup_id>\n'
        '/createpromo <CODE> <amount> <uses>\n'
        '/sendcode <order_id> <code>\n'
        '/addbalance <user_id> <amount>'
    )


@dp.message_handler(commands=['adminpanel'])
async def cmd_adminpanel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return
    admin_add_product_state.pop(message.from_user.id, None)
    admin_action_state.pop(message.from_user.id, None)
    await message.reply('╨Я╨░╨╜╨╡╨╗╤М ╨░╨┤╨╝╨╕╨╜╨░: ╨▓╤Л╨▒╨╡╤А╨╕ ╨┤╨╡╨╣╤Б╤В╨▓╨╕╨╡', reply_markup=admin_panel_kb())


@dp.message_handler(commands=['addproduct'])
async def cmd_addproduct(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split('|')
    if len(parts) < 6:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /addproduct category|title|price|stock|credentials|description')
        return

    category = parts[0].strip().lower()
    title = parts[1].strip()
    try:
        price = float(parts[2].strip().replace(',', '.'))
        stock = int(parts[3].strip())
    except ValueError:
        await message.reply('price ╨╕ stock ╨┤╨╛╨╗╨╢╨╜╤Л ╨▒╤Л╤В╤М ╤З╨╕╤Б╨╗╨░╨╝╨╕')
        return

    credentials = parts[4].strip()
    description = parts[5].strip()

    if category not in CATEGORY_NAMES:
        await message.reply('╨Ъ╨░╤В╨╡╨│╨╛╤А╨╕╨╕: proxy, tg, email')
        return

    product_id = add_product(title, price, credentials, category, description, stock, auto_restock=0)
    await message.reply(f'╨в╨╛╨▓╨░╤А ╨┤╨╛╨▒╨░╨▓╨╗╨╡╨╜, id={product_id}')


@dp.message_handler(commands=['refill'])
async def cmd_refill(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /refill <product_id> <qty>')
        return

    try:
        product_id = int(parts[0])
        qty = int(parts[1])
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ ╤Д╨╛╤А╨╝╨░╤В')
        return

    update_stock(product_id, qty)
    await message.reply('╨Ю╤Б╤В╨░╤В╨╛╨║ ╨╛╨▒╨╜╨╛╨▓╨╗╨╡╨╜')


@dp.message_handler(commands=['setstock'])
async def cmd_setstock(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /setstock <product_id> <qty>')
        return

    try:
        product_id = int(parts[0])
        qty = int(parts[1])
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ ╤Д╨╛╤А╨╝╨░╤В')
        return

    set_stock(product_id, qty)
    await message.reply('╨Ю╤Б╤В╨░╤В╨╛╨║ ╤Г╤Б╤В╨░╨╜╨╛╨▓╨╗╨╡╨╜')


@dp.message_handler(commands=['confirmtopup'])
async def cmd_confirm_topup(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    args = message.get_args().strip()
    if not args:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /confirmtopup <topup_id>')
        return

    try:
        topup_id = int(args)
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ id')
        return

    result = confirm_topup(topup_id)
    if not result:
        await message.reply('╨Ч╨░╤П╨▓╨║╨░ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜╨░')
        return

    _, user_id, amount, status = result
    await message.reply(f'╨Я╨╛╨┐╨╛╨╗╨╜╨╡╨╜╨╕╨╡ #{topup_id} ╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╛: {amount:.2f} тВ╜')
    try:
        balance = get_balance(user_id)
        await bot.send_message(
            user_id,
            f'тЬЕ ╨Т╨░╤И ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜ ╨╜╨░ {float(amount):.2f} тВ╜.\n╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜',
        )
    except Exception:
        pass


@dp.message_handler(commands=['createpromo'])
async def cmd_createpromo(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split()
    if len(parts) != 3:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /createpromo <CODE> <amount> <uses>')
        return

    code = parts[0].upper()
    try:
        amount = float(parts[1].replace(',', '.'))
        uses = int(parts[2])
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ ╤Д╨╛╤А╨╝╨░╤В amount/uses')
        return

    create_promo(code, amount, uses)
    await message.reply(f'╨Я╤А╨╛╨╝╨╛╨║╨╛╨┤ {code} ╤Б╨╛╨╖╨┤╨░╨╜: {amount:.2f} тВ╜, ╨░╨║╤В╨╕╨▓╨░╤Ж╨╕╨╣ {uses}')


@dp.message_handler(commands=['sendcode'])
async def cmd_sendcode(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split(maxsplit=1)
    if len(parts) != 2:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /sendcode <order_id> <code>')
        return

    try:
        order_id = int(parts[0])
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ order_id')
        return

    code = parts[1].strip()
    order = get_order(order_id)
    if not order:
        await message.reply('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜')
        return

    _, user_id, _, _, _, status, _, _, _ = order
    if status not in ('waiting_code', 'waiting_phone'):
        await message.reply('╨Ч╨░╨║╨░╨╖ ╨╜╨╡ ╨╛╨╢╨╕╨┤╨░╨╡╤В ╨║╨╛╨┤')
        return

    set_order_code(order_id, code)
    await message.reply('╨Ъ╨╛╨┤ ╨╛╤В╨┐╤А╨░╨▓╨╗╨╡╨╜ ╨║╨╗╨╕╨╡╨╜╤В╤Г')
    await bot.send_message(user_id, f'╨Ъ╨╛╨┤ ╨┤╨╗╤П ╨╖╨░╨║╨░╨╖╨░ #{order_id}: <code>{code}</code>')


@dp.message_handler(commands=['addbalance'])
async def cmd_addbalance(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('╨в╨╛╨╗╤М╨║╨╛ ╨░╨┤╨╝╨╕╨╜')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('╨д╨╛╤А╨╝╨░╤В: /addbalance <user_id> <amount>')
        return

    try:
        user_id = int(parts[0])
        amount = float(parts[1].replace(',', '.'))
    except ValueError:
        await message.reply('╨Э╨╡╨▓╨╡╤А╨╜╤Л╨╣ ╤Д╨╛╤А╨╝╨░╤В')
        return

    change_balance(user_id, amount)
    bal = get_balance(user_id)
    await message.reply(f'╨С╨░╨╗╨░╨╜╤Б ╨┐╨╛╨╗╤М╨╖╨╛╨▓╨░╤В╨╡╨╗╤П {user_id} ╨╕╨╖╨╝╨╡╨╜╨╡╨╜ ╨╜╨░ {amount:.2f} тВ╜. ╨в╨╡╨║╤Г╤Й╨╕╨╣: {bal:.2f} тВ╜')


async def restock_worker() -> None:
    while True:
        try:
            updated = apply_auto_restock()
            if updated:
                logging.info('Auto-restock completed for %s products', updated)
        except Exception as error:
            logging.exception('Auto-restock failed: %s', error)
        await asyncio.sleep(30)


async def auto_confirm_funpay_worker() -> None:
    global funpay_events_queue
    global funpay_listener_started

    if not FUNPAY_GOLDEN_KEY:
        logging.info('FunPay auto-confirm disabled: set FUNPAY_GOLDEN_KEY')
        return
    if not FUNPAY_API_AVAILABLE:
        logging.warning('FunPay auto-confirm disabled: install FunPayAPI package')
        return

    if funpay_events_queue is None:
        funpay_events_queue = asyncio.Queue(maxsize=500)

    if not funpay_listener_started:
        loop = asyncio.get_running_loop()
        listener_thread = threading.Thread(
            target=_funpay_listener_thread,
            args=(loop, funpay_events_queue),
            daemon=True,
            name='funpay-listener',
        )
        listener_thread.start()
        funpay_listener_started = True
        logging.info('FunPay listener started')

    while True:
        try:
            payload = await asyncio.wait_for(funpay_events_queue.get(), timeout=5)
        except asyncio.TimeoutError:
            await asyncio.sleep(1)
            continue
        except Exception as error:
            logging.exception('FunPay queue read failed: %s', error)
            await asyncio.sleep(3)
            continue

        try:
            topup_id = int(payload.get('topup_id'))
            user_id = int(payload.get('user_id'))
            order_amount = payload.get('amount')
            order_ref = str(payload.get('order_ref') or '').strip()

            pending = list_pending_topups_for_auto(limit=200)
            target = next(
                (row for row in pending if int(row[0]) == topup_id and int(row[1]) == user_id),
                None,
            )
            if not target:
                continue

            _, confirmed_user_id, expected_amount, _ = target

            if FUNPAY_MATCH_BY_AMOUNT and order_amount is not None:
                try:
                    if abs(float(order_amount) - float(expected_amount)) > 0.01:
                        continue
                except Exception:
                    pass

            if order_ref:
                set_topup_external_status(topup_id, f'funpay:{order_ref}')
            else:
                set_topup_external_status(topup_id, 'funpay:paid')

            result = confirm_topup(topup_id)
            if not result:
                continue

            _, _, credited_amount, _ = result
            balance = get_balance(int(confirmed_user_id))

            try:
                await bot.send_message(
                    int(confirmed_user_id),
                    f'тЬЕ ╨Т╨░╤И ╨▒╨░╨╗╨░╨╜╤Б ╨┐╨╛╨┐╨╛╨╗╨╜╨╡╨╜ ╨╜╨░ {float(credited_amount):.2f} тВ╜.\n'
                    f'╨в╨╡╨║╤Г╤Й╨╕╨╣ ╨▒╨░╨╗╨░╨╜╤Б: {balance:.2f} тВ╜',
                )
            except Exception:
                pass

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f'╨Р╨▓╤В╨╛╨┐╨╛╨┤╤В╨▓╨╡╤А╨╢╨┤╨╡╨╜╨╕╨╡ FunPay: topup #{topup_id}, user={confirmed_user_id}, amount={float(credited_amount):.2f} тВ╜',
                    )
                except Exception:
                    pass
        except Exception as error:
            logging.exception('FunPay auto-confirm handler failed: %s', error)
            await asyncio.sleep(2)


async def on_startup(_: Dispatcher):
    asyncio.create_task(restock_worker())
    asyncio.create_task(auto_confirm_funpay_worker())
    asyncio.create_task(github_backup_worker())


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
