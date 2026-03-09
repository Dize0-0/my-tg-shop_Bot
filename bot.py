


import asyncio
import atexit
import base64
import datetime
import hashlib
import os
import threading
import logging
import html
import json
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from typing import Any, Optional, Set, Dict
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Вспомогательные переменные и объекты для запуска ---
import re
topup_marker_re = re.compile(r"topup_(\\d+)_(\\d+)")
funpay_enums = None
FUNPAY_API_AVAILABLE = False
class FunPayAccount:
    def __init__(self, key): pass
    def get(self): return None
class FunPayRunner:
    def __init__(self, account): pass
    def listen(self, requests_delay): return []
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_TOKEN = os.getenv('TG_BOT_TOKEN', 'YOUR_BOT_TOKEN')
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
admin_action_state = {}
admin_add_product_state = {}
pending_custom_qty_input = {}
pending_review_input = {}
pending_promo_input = set()
pending_custom_topup = set()
active_buy_users = set()
pending_tg_phone_order = {}

# Заглушки для функций, которые должны быть реализованы в проекте
def env_or_default(key, default):
    return os.getenv(key, default)
def build_main_menu_text(user_id, text):
    return text
def main_menu_kb(user_id):
    return InlineKeyboardMarkup()
def catalog_kb():
    return InlineKeyboardMarkup()
def profile_kb():
    return InlineKeyboardMarkup()
def back_to_main_kb():
    return InlineKeyboardMarkup()
def update_stock(product_id, qty):
    pass
def try_spend_balance(user_id, total):
    return True
def set_topup_payment_data(topup_id, payment_link, payment_id):
    pass
def set_topup_external_status(topup_id, status):
    pass
def get_catalog_products(category):
    return []
def category_products_kb(category, page=0):
    return InlineKeyboardMarkup()
def category_products_text(category, page=0):
    return ""
def reviews_nav_kb(page, total_count):
    return InlineKeyboardMarkup()
def quantity_kb(product_id, catalog_key, stock, page=0):
    return InlineKeyboardMarkup()
def set_order_status(order_id, status):
    pass
def build_profile_hub_text(user_id):
    return ""
def proxy_sections_kb():
    return InlineKeyboardMarkup()
def release_lock():
    pass
REVIEWS_PHOTO_URL_2 = os.getenv('REVIEWS_PHOTO_URL_2', 'https://i.postimg.cc/wMyfFw4J/EB328F27-0B7A-4338-A923-2BF9D774A300.png')
WEB_SHOP_URL = os.getenv('WEB_SHOP_URL', '').strip()
AGREEMENT_TEXT = 'Пользовательское соглашение...'
CATEGORY_NAMES = {
    'proxy': '🌐 Прокси',
    'tg': '🤖 TG аккаунты',
    'email': '✉️ Почты',
}
CATALOG_VIEW_NAMES = {
    'proxy': '🌐 Прокси',
    'proxy_de': '🇩🇪 Прокси Германия',
    'proxy_us': '🇺🇸 Прокси США',
    'tg': '🤖 TG аккаунты',
    'email': '✉️ Почты',
}
PRODUCTS_PAGE_SIZE = 5
REVIEW_REWARD_RUB = 1.0
REVIEWS_PAGE_SIZE = 3





from db import (
    activate_promo,
    add_product,
    append_product_credentials_with_stock,
    apply_auto_restock,
    change_balance,
    confirm_topup,
    create_order,
    consume_product_credentials,
    create_review,
    create_promo,
    create_topup,
    delete_review,
    deactivate_product,
    get_balance,
    get_order,
    get_product,
    init_db,
    get_reviews_stats,
    list_admin_logs,
    list_reviews,
    list_reviews_page,
    list_products,
    list_pending_topups_for_auto,
    list_all_products_admin,
    list_user_orders,
    list_user_topups,
    log_admin_action,
    seed_products,
    set_order_code,
    set_order_phone,
    set_order_status,
    set_stock,
)

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





ADMIN_IDS: Set[int] = set(
    int(value.strip())
    for value in os.getenv('ADMIN_IDS', '8594771951,8466199706').split(',')
    if value.strip()
)

FUNPAY_PAYMENT_URL = env_or_default('FUNPAY_PAYMENT_URL', 'https://funpay.com/')
FUNPAY_TOPUP_LOT_URL = env_or_default('FUNPAY_TOPUP_LOT_URL', '')
FUNPAY_LOT_MAP = os.getenv(
    'FUNPAY_LOT_MAP',
    '50=https://funpay.com/lots/offer?id=65219645;100=https://funpay.com/lots/offer?id=65219541',
).strip()
FUNPAY_LOT_URL_50 = env_or_default('FUNPAY_LOT_URL_50', 'https://funpay.com/lots/offer?id=65219645')
FUNPAY_LOT_URL_100 = env_or_default('FUNPAY_LOT_URL_100', 'https://funpay.com/lots/offer?id=65219541')
FUNPAY_GOLDEN_KEY = os.getenv('FUNPAY_GOLDEN_KEY', '').strip()
FUNPAY_LISTEN_DELAY = env_int_or_default('FUNPAY_LISTEN_DELAY', 4)
FUNPAY_MATCH_BY_AMOUNT = env_or_default('FUNPAY_MATCH_BY_AMOUNT', '1').lower() in {'1', 'true', 'yes', 'on'}
PURCHASE_CASHBACK_PERCENT = env_float_or_default('PURCHASE_CASHBACK_PERCENT', 2.0)
GITHUB_BACKUP_ENABLED = env_bool_or_default('GITHUB_BACKUP_ENABLED', False)
GITHUB_BACKUP_INTERVAL_SECONDS = max(60, env_int_or_default('GITHUB_BACKUP_INTERVAL_SECONDS', 300))
GITHUB_BACKUP_FILES = [
    item.strip()
    for item in env_or_default('GITHUB_BACKUP_FILES', 'products.db').split(',')
    if item.strip()
]
GITHUB_BACKUP_REPO = os.getenv('GITHUB_BACKUP_REPO', 'Dize0-0/my-tg-shop_Bot').strip()
GITHUB_BACKUP_BRANCH = env_or_default('GITHUB_BACKUP_BRANCH', 'main')
GITHUB_BACKUP_TOKEN = os.getenv('GITHUB_BACKUP_TOKEN', '').strip()
GITHUB_BACKUP_REMOTE_DIR = env_or_default('GITHUB_BACKUP_REMOTE_DIR', 'backups')
MAIN_MENU_PHOTO_URL = os.getenv('MAIN_MENU_PHOTO_URL', 'https://i.postimg.cc/qM2NwggQ/db0f16d54211312d8d0fe9e7b6aa95b7.jpg')
CHANNEL_URL = os.getenv('CHANNEL_URL', 'https://t.me/H0MER0K')
REVIEWS_URL = os.getenv('REVIEWS_URL', 'https://t.me/Lune_shop_bot_0')
TG_CATEGORY_PHOTO_URL = os.getenv('TG_CATEGORY_PHOTO_URL', 'https://i.postimg.cc/QM32CZ4z/8BEC6871-6346-4FBE-AB56-1BE98473650D.png')
PROXY_CATEGORY_PHOTO_URL = os.getenv('PROXY_CATEGORY_PHOTO_URL', MAIN_MENU_PHOTO_URL)
EMAIL_CATEGORY_PHOTO_URL = os.getenv('EMAIL_CATEGORY_PHOTO_URL', MAIN_MENU_PHOTO_URL)
REVIEWS_PHOTO_URL_1 = os.getenv('REVIEWS_PHOTO_URL_1', 'https://i.postimg.cc/DfMRsXp7/a33681df3b4a34e1706da52d4484ab7e.jpg')
REVIEWS_PHOTO_URL_2 = os.getenv('REVIEWS_PHOTO_URL_2', 'https://i.postimg.cc/wMyfFw4J/EB328F27-0B7A-4338-A923-2BF9D774A300.png')
WEB_SHOP_URL = os.getenv('WEB_SHOP_URL', '').strip()
AGREEMENT_TEXT = (
    'Пользовательское соглашение\n\n'
    '1) Общие положения\n'
    '1.1. Используя бота, пользователь подтверждает, что ознакомился с условиями и полностью их принимает.\n'
    '1.2. Все товары и услуги в боте являются цифровыми и предоставляются в электронном виде.\n'
    '1.3. Факт оплаты (товара или пополнения) автоматически означает акцепт настоящего соглашения без дополнительных подтверждений.\n\n'
    '2) Личная ответственность пользователя\n'
    '2.1. Пользователь несет полную и персональную ответственность за выбор товара, ввод реквизитов, комментариев и кода оплаты.\n'
    '2.2. Пользователь самостоятельно несет ответственность за дальнейшее использование полученных данных (аккаунты, прокси, почты, коды и иные цифровые данные).\n'
    '2.3. Пользователь самостоятельно отвечает за соблюдение правил сторонних сервисов, платформ, игр, мессенджеров и применимого законодательства.\n'
    '2.4. Все риски, связанные с блокировками, ограничениями, санкциями, изменением правил сторонних сервисов, пользователь принимает на себя.\n'
    '2.5. Передача полученных данных третьим лицам, разглашение, утрата доступа, компрометация по вине пользователя не относится к зоне ответственности магазина.\n\n'
    '3) Оплата, пополнение и баланс\n'
    '3.1. Баланс бота является внутренней расчетной единицей сервиса и используется только для оплаты внутри бота.\n'
    '3.2. Пополнение засчитывается только после подтверждения платежа системой/провайдером или администратором.\n'
    '3.3. При оплате пользователь обязан указывать корректный код платежа (если он требуется в инструкции).\n'
    '3.4. Ошибки в сумме, комментарии или реквизитах, допущенные пользователем, могут привести к задержке/отказу в зачислении и рассматриваются индивидуально.\n'
    '3.5. Администрация вправе запросить подтверждение оплаты при спорных ситуациях (чек, номер заказа, скриншот и т.д.).\n\n'
    '4) Выдача цифрового товара\n'
    '4.1. Товар считается выданным надлежащим образом с момента отображения данных в чате бота или в истории заказа.\n'
    '4.2. Пользователь обязан сразу проверить полученные данные после выдачи.\n'
    '4.3. Претензии по товару принимаются при наличии объективных подтверждений и в разумный срок после покупки.\n\n'
    '5) Возвраты и отмены\n'
    '5.1. Возврат возможен только если товар не был выдан по вине магазина или подтверждена техническая ошибка на стороне сервиса.\n'
    '5.2. Если товар выдан, но не подошел по личным причинам пользователя, возврат не производится.\n'
    '5.3. Возврат не производится при нарушении пользователем инструкции, правил использования товара или условий сторонних платформ.\n\n'
    '6) Ограничения и право отказа\n'
    '6.1. Администрация вправе отказать в обслуживании при подозрении на мошенничество, злоупотребление, попытки взлома, спам или иные недобросовестные действия.\n'
    '6.2. Администрация вправе временно ограничить функциональность бота при технических работах, форс-мажорах и изменениях у платежных/сторонних сервисов.\n\n'
    '7) Заключительные положения\n'
    '7.1. Условия соглашения могут быть изменены без предварительного персонального уведомления.\n'
    '7.2. Актуальная редакция соглашения публикуется в боте и вступает в силу с момента публикации.\n'
    '7.3. Продолжение использования бота после публикации изменений означает полное согласие пользователя с обновленными условиями.'
)

CATEGORY_NAMES = {
    'proxy': '🌐 Прокси',
    'tg': '🤖 TG аккаунты',
    'email': '✉️ Почты',
}
CATALOG_VIEW_NAMES = {
    'proxy': '🌐 Прокси',
    'proxy_de': '🇩🇪 Прокси Германия',
    'proxy_us': '🇺🇸 Прокси США',
    'tg': '🤖 TG аккаунты',
    'email': '✉️ Почты',
}
PRODUCTS_PAGE_SIZE = 5
REVIEW_REWARD_RUB = 1.0
REVIEWS_PAGE_SIZE = 3


init_db()
seed_products()





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

    if amount_key == '50.00' and FUNPAY_LOT_URL_50.startswith('http'):
        return FUNPAY_LOT_URL_50
    if amount_key == '100.00' and FUNPAY_LOT_URL_100.startswith('http'):
        return FUNPAY_LOT_URL_100

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
    return ''


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS





def _filter_proxy_products_by_region(region_key: str) -> list[tuple]:
    proxy_products = list_products('proxy')
    if region_key == 'proxy_de':
        region_tokens = ('герм', 'german', 'germany', 'deutsch', '🇩🇪')
    elif region_key == 'proxy_us':
        region_tokens = ('сша', 'usa', 'america', 'америк', '🇺🇸')
    else:
        return proxy_products

    filtered = []
    for row in proxy_products:
        title = str(row[1] or '').lower()
        description = str(row[2] or '').lower()
        if region_key == 'proxy_de' and '[proxy_region:de]' in description:
            filtered.append(row)
            continue
        if region_key == 'proxy_us' and '[proxy_region:us]' in description:
            filtered.append(row)
            continue

def product_card_text(title: str, category: str, price: float, stock: int, description: str) -> str:
    category_title = CATEGORY_NAMES.get(category, category)
    text = (
        '╭──── 🛒 <b>Карточка товара</b>\n'
        f'├ Категория: <b>{category_title}</b>\n'
        f'├ Позиция: <b>{title}</b>\n'
        f'├ Стоимость: <b>{price:.2f} ₽</b>\n'
        f'╰ В наличии: <b>{stock} шт.</b>'
    )
    if description and description.strip():
        text += f"\n\n<b>Описание:</b> {description.strip()}"
    return text


def split_credentials_items(credentials: str) -> list[str]:
    items: list[str] = []
    for raw_line in (credentials or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if '|' in line:
            items.extend(chunk.strip() for chunk in line.split('|') if chunk.strip())
        else:
            items.append(line)
    return items


def format_delivery_credentials(credentials: str, qty: int) -> str:
    if qty <= 0:
        return ''

    items = split_credentials_items(credentials)

    if not items:
        return 'Данные отсутствуют. Обратитесь к администратору.'

    if len(items) >= qty:
        selected = items[:qty]
    else:
        selected = items + [items[-1]] * (qty - len(items))

    return '\n'.join(f'{index}. {value}' for index, value in enumerate(selected, start=1))


def topup_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('💸 50₽', callback_data='topup:50'),
        InlineKeyboardButton('💸 100₽', callback_data='topup:100'),
    )
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
    return kb


def review_offer_kb(order_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('✍️ Оставить отзыв', callback_data=f'review:start:{order_id}'))
    kb.add(InlineKeyboardButton('🔙 В меню', callback_data='menu:main'))
    return kb


def review_rating_kb(order_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=5)
    kb.add(
        InlineKeyboardButton('1⭐', callback_data=f'review:rate:{order_id}:1'),
        InlineKeyboardButton('2⭐', callback_data=f'review:rate:{order_id}:2'),
        InlineKeyboardButton('3⭐', callback_data=f'review:rate:{order_id}:3'),
        InlineKeyboardButton('4⭐', callback_data=f'review:rate:{order_id}:4'),
        InlineKeyboardButton('5⭐', callback_data=f'review:rate:{order_id}:5'),
    )
    kb.add(InlineKeyboardButton('🔙 В меню', callback_data='menu:main'))
    return kb


def reviews_nav_kb(page: int, total_count: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    max_page = max((max(0, total_count) - 1) // REVIEWS_PAGE_SIZE, 0)
    safe_page = min(max(0, int(page)), max_page)

    nav_row = []
    if safe_page > 0:
        nav_row.append(InlineKeyboardButton('⬅️ Предыдущий', callback_data=f'reviews:page:{safe_page - 1}'))
    if safe_page < max_page:
        nav_row.append(InlineKeyboardButton('➡️ Следующий', callback_data=f'reviews:page:{safe_page + 1}'))
    if nav_row:
        kb.row(*nav_row)

    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
    return kb


def _avg_rating_stars(avg_rating: float) -> str:
    rounded = int(round(max(0.0, min(5.0, float(avg_rating)))))
    return '★' * rounded + '☆' * (5 - rounded)


def reviews_text_page(page: int = 0) -> tuple[str, int]:
    rows, total_count = list_reviews_page(page=page, page_size=REVIEWS_PAGE_SIZE, active_only=True)
    reviews_count, avg_rating = get_reviews_stats(active_only=True)
    stars = _avg_rating_stars(avg_rating)

    lines = [
        '⭐ <b>Отзывы покупателей</b>',
        f'Общая оценка: <b>{avg_rating:.2f}/5</b> {stars}',
        f'Всего отзывов: <b>{reviews_count}</b>',
        '',
    ]

    if not rows:
        lines.append('Пока отзывов нет.')
    else:
        for review_id, user_id, username, order_id, review_text, rating, _, created_at in rows:
            safe_text = html.escape(str(review_text or '').strip())
            if len(safe_text) > 260:
                safe_text = safe_text[:260] + '...'
            rating_int = min(5, max(1, int(rating)))
            rating_stars = '★' * rating_int + '☆' * (5 - rating_int)
            author = f'@{username}' if str(username or '').strip() else f'ID {user_id}'
            lines.append(f'╭ Отзыв #{review_id} • заказ #{order_id}')
            lines.append(f'├ Автор: {html.escape(author)}')
            lines.append(f'├ Оценка: {rating_stars}')
            lines.append(f'├ Текст: {safe_text}')
            lines.append(f'╰ 🕒 {created_at}')
            lines.append('')

    text = '\n'.join(lines).strip()
    if len(text) > 3900:
        text = text[:3900] + '\n...'
    return text, total_count


async def send_review_offer_message(user_id: int, order_id: int) -> None:
    return


async def notify_admins_about_purchase(
    buyer: types.User,
    order_id: int,
    title: str,
    qty: int,
    total: float,
    status: str,
) -> None:
    username = str(getattr(buyer, 'username', '') or '').strip()
    buyer_label = f'@{username}' if username else html.escape(str(getattr(buyer, 'full_name', '') or '').strip() or '-')
    status_label = {
        'waiting_phone': 'ожидает номер',
        'waiting_code': 'ожидает код',
        'delivered': 'выдан',
    }.get(status, status)

    text = (
        f'🛒 Новая покупка #{order_id}\n'
        f'Пользователь: {buyer.id} ({buyer_label})\n'
        f'Товар: {html.escape(str(title))}\n'
        f'Количество: {int(qty)}\n'
        f'Сумма: {float(total):.2f} ₽\n'
        f'Статус: {status_label}'
    )

    # Кнопка "Выдать код по заказу"
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f'Выдать код по заказу #{order_id}', callback_data=f'admin_sendcode:{order_id}'))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            pass
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admin:issue_code:'))
async def admin_issue_code_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только для админов', show_alert=True)
        return
    try:
        order_id = int(cb.data.split(':')[2])
    except Exception:
        await cb.answer('Некорректный ID заказа', show_alert=True)
        return
    order = get_order(order_id)
    if not order:
        await cb.answer('Заказ не найден', show_alert=True)
        return
    # Сохраняем состояние для ожидания кода
    admin_action_state[cb.from_user.id] = {'action': 'issue_code', 'step': 'wait_code', 'order_id': str(order_id)}
    await bot.send_message(cb.from_user.id, f'Пожалуйста, введите код для выдачи по заказу #{order_id}:')
    await cb.answer('Ожидаю код для выдачи')

# Обработчик ввода кода админом после нажатия кнопки
@dp.message_handler(lambda m: is_admin(m.from_user.id) and admin_action_state.get(m.from_user.id, {}).get('action') == 'issue_code' and admin_action_state.get(m.from_user.id, {}).get('step') == 'wait_code')
async def admin_receive_code_for_order(message: types.Message):
    state = admin_action_state.get(message.from_user.id)
    if not state:
        return
    order_id = int(state.get('order_id'))
    code = message.text.strip()
    order = get_order(order_id)
    if not order:
        await message.reply('Заказ не найден.')
        admin_action_state.pop(message.from_user.id, None)
        return
    user_id = order[1]
    set_order_code(order_id, code)
    await bot.send_message(user_id, f'🔑 Вам выдан код по вашему заказу #{order_id}:\n<code>{html.escape(code)}</code>')
    await message.reply(f'Код успешно выдан пользователю (ID {user_id}) по заказу #{order_id}.')
    admin_action_state.pop(message.from_user.id, None)


def admin_panel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton('🆕 Добавить товар', callback_data='admpanel:add_product'))
    kb.add(
        InlineKeyboardButton('📦 Товары', callback_data='admpanel:section_products'),
        InlineKeyboardButton('💳 Финансы', callback_data='admpanel:section_finance'),
    )
    kb.add(InlineKeyboardButton('🧩 Прочее', callback_data='admpanel:section_other'))
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_any'))
    kb.add(InlineKeyboardButton('🔙 В главное меню', callback_data='menu:main'))
    return kb


def admin_products_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('➕ Добавить данные в товар', callback_data='admpanel:append_credentials'))
    kb.add(InlineKeyboardButton('📄 Список товаров', callback_data='admpanel:list_products'))
    kb.add(InlineKeyboardButton('🗑 Удалить товар', callback_data='admpanel:delete_product'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='admpanel:home'))
    return kb


def admin_finance_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('💸 Пополнить баланс', callback_data='admpanel:add_balance'))
    kb.add(InlineKeyboardButton('✅ Подтвердить пополнение', callback_data='admpanel:confirm_topup'))
    kb.add(InlineKeyboardButton('🎟 Создать промокод', callback_data='admpanel:create_promo'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='admpanel:home'))
    return kb


def admin_other_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('🧾 Админ логи', callback_data='admpanel:view_logs'))
    kb.add(InlineKeyboardButton('📦 Пополнить остаток', callback_data='admpanel:refill'))
    kb.add(InlineKeyboardButton('🧮 Установить остаток', callback_data='admpanel:set_stock'))
    kb.add(InlineKeyboardButton('📨 Выдать код по заказу', callback_data='admpanel:send_code'))
    kb.add(InlineKeyboardButton('🗑 Удалить отзыв', callback_data='admpanel:delete_review'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='admpanel:home'))
    return kb


def admin_panel_text(user_id: int) -> str:
    return (
        '╭──── 🛠 <b>Админ-панель</b>\n'
        f'├ Админ ID: <code>{user_id}</code>\n'
        '├ Верхняя кнопка: добавить товар\n'
        '├ Разделы: товары / финансы / прочее\n'
        '╰ Выбери нужный раздел'
    )


def admin_logs_filter_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('🧾 Все админы', callback_data='admlogs:all'))
    for admin_id in sorted(ADMIN_IDS):
        kb.add(InlineKeyboardButton(f'👤 {admin_id}', callback_data=f'admlogs:{admin_id}'))
    kb.add(InlineKeyboardButton('🔙 В админ панель', callback_data='admpanel:home'))
    return kb


def admin_logs_text(limit: int = 30, admin_id: Optional[int] = None) -> str:
    rows = list_admin_logs(limit=max(200, int(limit) * 8))
    if admin_id is not None:
        rows = [row for row in rows if int(row[1]) == int(admin_id)]
    rows = rows[: max(1, int(limit))]

    if not rows:
        if admin_id is None:
            return '🧾 Логи пока пустые.'
        return f'🧾 Логи для admin_id={admin_id} пока пустые.'

    if admin_id is None:
        lines = ['🧾 Последние действия админов:']
    else:
        lines = [f'🧾 Действия admin_id={admin_id}:']

    for log_id, admin_id, action, details, created_at in rows:
        safe_action = html.escape(str(action or 'action'))
        safe_details = html.escape(str(details or '').strip())
        if len(safe_details) > 140:
            safe_details = safe_details[:140] + '...'
        if safe_details:
            lines.append(f'#{log_id} | admin={admin_id} | {safe_action} | {safe_details} | {created_at}')
        else:
            lines.append(f'#{log_id} | admin={admin_id} | {safe_action} | {created_at}')

    text = '\n'.join(lines)
    if len(text) > 3800:
        text = text[:3800] + '\n...'
    return text


def admin_owned_product_ids(user_id: int) -> set[int]:
    rows = list_all_products_admin(admin_id=user_id)
    ids: set[int] = set()
    for row in rows:
        try:
            ids.add(int(row[0]))
        except Exception:
            continue
    return ids


def admin_product_id_prompt_text(user_id: int, title: str) -> str:
    rows = list_all_products_admin(admin_id=user_id)
    lines = [title, '', f'Твои товары (admin_id={user_id}):']
    if not rows:
        lines.append('— У тебя пока нет товаров')
    else:
        for pid, name, category, price, stock, _ in rows[:20]:
            lines.append(f'#{int(pid)} | {name} | {category} | {float(price):.2f}₽ | stock={int(stock)}')
        if len(rows) > 20:
            lines.append('... список сокращен, используй "Список товаров" для полного списка')
    lines.append('')
    lines.append('Нажми кнопку товара ниже или введи число product_id вручную.')
    return '\n'.join(lines)


def admin_product_select_kb(user_id: int, action: str) -> InlineKeyboardMarkup:
    rows = list_all_products_admin(admin_id=user_id)
    action_code_map = {
        'append_credentials': 'app',
        'refill': 'ref',
        'set_stock': 'set',
        'delete_product': 'del',
    }
    action_code = action_code_map.get(action, '')

    kb = InlineKeyboardMarkup(row_width=1)
    if action_code:
        for pid, name, _, _, _, _ in rows[:25]:
            safe_name = str(name or 'Товар').strip()
            if len(safe_name) > 34:
                safe_name = safe_name[:34] + '...'
            kb.add(
                InlineKeyboardButton(
                    f'#{int(pid)} • {safe_name}',
                    callback_data=f'admpsel:{action_code}:{int(pid)}',
                )
            )
    kb.add(InlineKeyboardButton('🔙 В админ панель', callback_data='admpanel:home'))
    return kb


def admin_category_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['proxy'], callback_data='admaddcat:proxy'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['tg'], callback_data='admaddcat:tg'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['email'], callback_data='admaddcat:email'))
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_add'))
    return kb


def admin_proxy_section_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('🇩🇪 Германия', callback_data='admaddproxy:de'))
    kb.add(InlineKeyboardButton('🇺🇸 США', callback_data='admaddproxy:us'))
    kb.add(InlineKeyboardButton('🔙 Назад к категориям', callback_data='admpanel:add_product'))
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_add'))
    return kb


def apply_proxy_region_title(title: str, proxy_region: str) -> str:
    raw_title = str(title or '').strip()
    if not raw_title:
        return raw_title

    if proxy_region == 'de':
        if '🇩🇪' in raw_title:
            return raw_title
        return f'🇩🇪 {raw_title}'
    if proxy_region == 'us':
        if '🇺🇸' in raw_title:
            return raw_title
        return f'🇺🇸 {raw_title}'
    return raw_title


def apply_proxy_region_description(description: str, proxy_region: str) -> str:
    raw_description = str(description or '').strip()
    if proxy_region not in {'de', 'us'}:
        return raw_description

    marker = f'[proxy_region:{proxy_region}]'
    if marker in raw_description.lower():
        return raw_description
    if raw_description:
        return f'{marker} {raw_description}'
    return marker


def admin_auto_restock_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('✅ Да, автообновление', callback_data='admaddauto:yes'),
        InlineKeyboardButton('❌ Нет', callback_data='admaddauto:no'),
    )
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_add'))
    return kb


def admin_step_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('🔙 В админ панель', callback_data='admpanel:home'),
        InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_any'),
    )
    return kb


def admin_confirm_product_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('✅ Сохранить товар', callback_data='admaddsave:yes'),
        InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_add'),
    )
    return kb


async def show_main_menu(user_id: int, text: str = 'Главное меню:') -> None:
    pretty_text = build_main_menu_text(user_id, text)
    try:
        await bot.send_photo(
            user_id,
            photo=MAIN_MENU_PHOTO_URL,
            caption=pretty_text,
            reply_markup=main_menu_kb(user_id),
        )
    except Exception:
        await bot.send_message(user_id, pretty_text, reply_markup=main_menu_kb(user_id))


async def safe_edit_text(message: types.Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        try:
            await message.edit_caption(caption=text, reply_markup=reply_markup)
        except Exception:
            try:
                await bot.send_message(message.chat.id, text, reply_markup=reply_markup)
            except Exception:
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


def _github_api_headers(token: str) -> Dict[str, str]:
    return {
        'Accept': 'application/vnd.github+json',
        'Authorization': f'Bearer {token}',
        'X-GitHub-Api-Version': '2022-11-28',
        'Content-Type': 'application/json',
        'User-Agent': 'lune-shop-bot-backup',
    }


def _github_get_contents_sha(repo: str, branch: str, remote_path: str, token: str) -> Optional[str]:
    encoded_path = quote(remote_path, safe='/')
    url = f'https://api.github.com/repos/{repo}/contents/{encoded_path}?ref={quote(branch, safe="")}'
    request = Request(url, headers=_github_api_headers(token), method='GET')
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode('utf-8'))
            if isinstance(payload, dict):
                sha = payload.get('sha')
                return str(sha) if sha else None
            return None
    except HTTPError as error:
        if error.code == 404:
            return None
        detail = error.read().decode('utf-8', errors='ignore')
        raise RuntimeError(f'GitHub GET contents failed ({error.code}): {detail}')


def _github_put_contents(
    repo: str,
    branch: str,
    remote_path: str,
    token: str,
    content_bytes: bytes,
    message: str,
    current_sha: Optional[str],
) -> tuple[bool, str]:
    encoded_path = quote(remote_path, safe='/')
    url = f'https://api.github.com/repos/{repo}/contents/{encoded_path}'
    payload: Dict[str, Any] = {
        'message': message,
        'branch': branch,
        'content': base64.b64encode(content_bytes).decode('ascii'),
    }
    if current_sha:
        payload['sha'] = current_sha

    data = json.dumps(payload).encode('utf-8')
    request = Request(url, data=data, headers=_github_api_headers(token), method='PUT')
    try:
        with urlopen(request, timeout=60) as response:
            response_payload = response.read().decode('utf-8', errors='ignore')
            return True, response_payload
    except HTTPError as error:
        detail = error.read().decode('utf-8', errors='ignore')
        return False, f'GitHub PUT contents failed ({error.code}): {detail}'


async def _github_backup_worker_api(tracked_files: list[str]) -> None:
    if not GITHUB_BACKUP_TOKEN:
        logging.warning('GitHub backup worker: .git not found and GITHUB_BACKUP_TOKEN is empty, API mode disabled.')
        return
    if not GITHUB_BACKUP_REPO:
        logging.warning('GitHub backup worker: .git not found and GITHUB_BACKUP_REPO is empty, API mode disabled.')
        return

    remote_root = GITHUB_BACKUP_REMOTE_DIR.strip().strip('/').replace('\\', '/')
    last_uploaded_hashes: Dict[str, str] = {}

    logging.info(
        'GitHub backup worker (API mode) enabled: repo=%s, branch=%s, dir=%s, interval=%ss, files=%s',
        GITHUB_BACKUP_REPO,
        GITHUB_BACKUP_BRANCH,
        remote_root or '.',
        GITHUB_BACKUP_INTERVAL_SECONDS,
        tracked_files,
    )

    def _read_file_bytes(path: str) -> bytes:
        with open(path, 'rb') as file:
            return file.read()

    while True:
        try:
            changed = False
            for rel_path in tracked_files:
                abs_path = os.path.join(BASE_DIR, rel_path)
                if not os.path.exists(abs_path):
                    continue

                content_bytes = await asyncio.to_thread(_read_file_bytes, abs_path)
                content_hash = hashlib.sha256(content_bytes).hexdigest()
                if last_uploaded_hashes.get(rel_path) == content_hash:
                    continue

                normalized_rel = rel_path.replace('\\', '/').lstrip('/')
                remote_path = f'{remote_root}/{normalized_rel}' if remote_root else normalized_rel
                current_sha = await asyncio.to_thread(
                    _github_get_contents_sha,
                    GITHUB_BACKUP_REPO,
                    GITHUB_BACKUP_BRANCH,
                    remote_path,
                    GITHUB_BACKUP_TOKEN,
                )

                timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                message = f'Auto backup {normalized_rel} {timestamp}'
                ok, result = await asyncio.to_thread(
                    _github_put_contents,
                    GITHUB_BACKUP_REPO,
                    GITHUB_BACKUP_BRANCH,
                    remote_path,
                    GITHUB_BACKUP_TOKEN,
                    content_bytes,
                    message,
                    current_sha,
                )
                if not ok:
                    logging.warning('GitHub backup worker (API mode): %s', result[:500])
                    continue

                last_uploaded_hashes[rel_path] = content_hash
                changed = True
                logging.info('GitHub backup worker (API mode): uploaded %s -> %s', rel_path, remote_path)

            if not changed:
                logging.debug('GitHub backup worker (API mode): no changes detected.')
        except Exception as error:
            logging.exception('GitHub backup worker (API mode) failed: %s', error)

        await asyncio.sleep(GITHUB_BACKUP_INTERVAL_SECONDS)


async def github_backup_worker() -> None:
    if not GITHUB_BACKUP_ENABLED:
        logging.info('GitHub backup worker is disabled (set GITHUB_BACKUP_ENABLED=1 to enable).')
        return

    tracked_files = [path for path in GITHUB_BACKUP_FILES if os.path.exists(os.path.join(BASE_DIR, path))]
    if not tracked_files:
        logging.warning('GitHub backup worker: no existing files from GITHUB_BACKUP_FILES=%s', GITHUB_BACKUP_FILES)
        return

    if not os.path.exists(os.path.join(BASE_DIR, '.git')):
        logging.warning('GitHub backup worker: .git directory not found in %s, switching to GitHub API mode.', BASE_DIR)
        await _github_backup_worker_api(tracked_files)
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
    await show_main_menu(message.from_user.id, 'Добро пожаловать в магазин цифровых товаров!')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('menu:'))
async def menu_router(cb: types.CallbackQuery):
    pending_custom_qty_input.pop(cb.from_user.id, None)
    action = cb.data.split(':', 1)[1]

    if action == 'main':
        try:
            await cb.message.delete()
        except Exception:
            pass
        await show_main_menu(cb.from_user.id, 'Главное меню:')
    elif action == 'adminpanel':
        if not is_admin(cb.from_user.id):
            await cb.answer('Только админ', show_alert=True)
            return
        admin_add_product_state.pop(cb.from_user.id, None)
        await safe_edit_text(cb.message, admin_panel_text(cb.from_user.id), reply_markup=admin_panel_kb())
    elif action == 'catalog':
        await safe_edit_text(cb.message, '🧩 Выберите категорию:', reply_markup=catalog_kb())
    elif action == 'profile':
        await safe_edit_text(cb.message, build_profile_hub_text(cb.from_user.id), reply_markup=profile_kb())
    elif action == 'agreement':
        try:
            await cb.message.delete()
        except Exception:
            pass
        await bot.send_message(cb.from_user.id, AGREEMENT_TEXT, reply_markup=back_to_main_kb())
    elif action == 'topup':
        await safe_edit_text(
            cb.message,
            '╭──── 💳 <b>Пополнение баланса</b>\n'
            '├ Способ: FunPay\n'
            '╰ Доступные суммы: <b>50 ₽</b> и <b>100 ₽</b>',
            reply_markup=topup_kb(),
        )
    elif action == 'reviews':
        try:
            await cb.message.delete()
        except Exception:
            pass
        text, total_count = reviews_text_page(page=0)
        await bot.send_message(cb.from_user.id, text, reply_markup=reviews_nav_kb(0, total_count))
    elif action == 'channel':
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton('Открыть канал', url=CHANNEL_URL))
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
        await safe_edit_text(cb.message, '📢 Наш канал:', reply_markup=kb)

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('review:'))
async def review_router(cb: types.CallbackQuery):
    parts = cb.data.split(':')
    if len(parts) < 3:
        await cb.answer('Неверные данные', show_alert=True)
        return

    action = parts[1]
    if action not in {'start', 'rate'}:
        await cb.answer('Неверное действие', show_alert=True)
        return

    try:
        order_id = int(parts[2])
    except ValueError:
        await cb.answer('Неверный заказ', show_alert=True)
        return

    order = get_order(order_id)
    if not order:
        await cb.answer('Заказ не найден', show_alert=True)
        return

    _, user_id, _, _, _, _, _, _, _ = order
    if int(user_id) != int(cb.from_user.id):
        await cb.answer('Это не ваш заказ', show_alert=True)
        return

    if action == 'start':
        await safe_edit_text(
            cb.message,
            f'⭐ Выберите оценку для заказа #{order_id}:',
            reply_markup=review_rating_kb(order_id),
        )
        await cb.answer('Выберите оценку')
        return

    if len(parts) < 4:
        await cb.answer('Неверная оценка', show_alert=True)
        return

    try:
        rating = int(parts[3])
    except ValueError:
        await cb.answer('Неверная оценка', show_alert=True)
        return

    if rating < 1 or rating > 5:
        await cb.answer('Оценка должна быть от 1 до 5', show_alert=True)
        return

    pending_review_input[cb.from_user.id] = {'order_id': order_id, 'rating': rating}
    await safe_edit_text(
        cb.message,
        f'✍️ Напишите текст отзыва для заказа #{order_id} одним сообщением.\n'
        f'Ваша оценка: <b>{rating}⭐</b>\n'
        f'После отправки начислим <b>{REVIEW_REWARD_RUB:.0f} ₽</b> на баланс.',
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main')),
    )
    await cb.answer('Жду ваш отзыв')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('reviews:page:'))
async def reviews_page_router(cb: types.CallbackQuery):
    try:
        page = int(cb.data.split(':')[2])
    except Exception:
        await cb.answer('Неверная страница', show_alert=True)
        return

    text, total_count = reviews_text_page(page=page)
    await safe_edit_text(cb.message, text, reply_markup=reviews_nav_kb(page, total_count))
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('profile:'))
async def profile_router(cb: types.CallbackQuery):
    action = cb.data.split(':', 1)[1]

    if action == 'hub':
        await safe_edit_text(cb.message, build_profile_hub_text(cb.from_user.id), reply_markup=profile_kb())
        await cb.answer()
        return

    if action == 'orders':
        rows = list_user_orders(cb.from_user.id, limit=10)
        if not rows:
            text = '╭──── 🧾 <b>История покупок</b>\n╰ Покупок пока нет.'
        else:
            lines = ['╭──── 🧾 <b>История покупок</b>']
            max_text_len = 3500
            for order_id, title, qty, total, status, code_value, created in rows:
                safe_title = html.escape(str(title or 'Товар'))
                safe_status = html.escape(str(status or 'unknown'))
                entry_lines = [
                    f'├ <b>#{order_id}</b> • {safe_title} • x{qty} • {total:.2f} ₽',
                    f'├ Статус: {safe_status} • {created}',
                ]
                if code_value:
                    safe_value = html.escape(str(code_value).strip())
                    if len(safe_value) > 200:
                        safe_value = safe_value[:200] + ' ...'
                    entry_lines.append(f'├ Данные: <code>{safe_value}</code>')

                projected = '\n'.join(lines + entry_lines + ['╰ Конец списка'])
                if len(projected) > max_text_len:
                    lines.append('├ ... список сокращен')
                    break

                lines.extend(entry_lines)
            lines.append('╰ Конец списка')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'topups':
        rows = list_user_topups(cb.from_user.id, limit=15)
        if not rows:
            text = '╭──── 💸 <b>История пополнений</b>\n╰ Пополнений пока нет.'
        else:
            lines = ['╭──── 💸 <b>История пополнений</b>']
            for topup_id, amount, status, created in rows:
                safe_status = html.escape(str(status or 'unknown'))
                lines.append(f'├ <b>#{topup_id}</b> • {amount:.2f} ₽ • {safe_status} • {created}')
            lines.append('╰ Конец списка')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'promo':
        pending_promo_input.add(cb.from_user.id)
        await safe_edit_text(cb.message, 'Введите промокод одним сообщением:', reply_markup=back_to_main_kb())

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cat:'))
async def category_router(cb: types.CallbackQuery):
    category = cb.data.split(':', 1)[1]
    if category == 'proxy':
        await safe_edit_text(
            cb.message,
            '🌐 <b>Прокси</b>\n\nВыберите раздел:',
            reply_markup=proxy_sections_kb(),
        )
        await cb.answer()
        return

    if category not in CATEGORY_NAMES and category not in {'proxy_de', 'proxy_us'}:
        await cb.answer('Категория не найдена', show_alert=True)
        return

    products = get_catalog_products(category)
    if not products:
        await safe_edit_text(
            cb.message,
            f'В категории {CATALOG_VIEW_NAMES.get(category, category)} пока нет товаров в наличии.',
            reply_markup=back_to_main_kb(),
        )
        await cb.answer()
        return

    kb = category_products_kb(category, page=0)
    await safe_edit_text(cb.message, category_products_text(category, page=0), reply_markup=kb)
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('catpage:'))
async def category_page_router(cb: types.CallbackQuery):
    _, category, page_str = cb.data.split(':')
    page = int(page_str)
    products = get_catalog_products(category)
    if not products:
        await safe_edit_text(
            cb.message,
            f'В категории {CATALOG_VIEW_NAMES.get(category, category)} пока нет товаров в наличии.',
            reply_markup=back_to_main_kb(),
        )
        await cb.answer()
        return

    max_page = max((len(products) - 1) // PRODUCTS_PAGE_SIZE, 0)
    safe_page = min(max(page, 0), max_page)
    await safe_edit_text(
        cb.message,
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
    catalog_key = parts[3] if len(parts) > 3 else None
    product = get_product(product_id)
    if not product:
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, description, price, _, category, stock, *_ = product
    view_category = catalog_key or category
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    text = product_card_text(title, category, float(price), int(stock), description)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('💰 Купить товар', callback_data=f'qtymenu:{product_id}:{page}:{view_category}'))
    kb.add(InlineKeyboardButton('↩️ Назад', callback_data=f'catpage:{view_category}:{page}'))
    await safe_edit_text(cb.message, text, reply_markup=kb)
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qtymenu:'))
async def qty_menu_router(cb: types.CallbackQuery):
    pending_custom_qty_input.pop(cb.from_user.id, None)
    parts = cb.data.split(':')
    _, product_id_str, page_str = parts[:3]
    view_category = parts[3] if len(parts) > 3 else None
    product_id = int(product_id_str)
    page = int(page_str)
    product = get_product(product_id)
    if not product:
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, _, price, _, category, stock, *_ = product
    catalog_key = view_category or category
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    text = (
        '╭──── 🧾 <b>Оформление заказа</b>\n'
        f'├ Товар: <b>{title}</b>\n'
        f'├ Цена за 1 шт: <b>{float(price):.2f} ₽</b>\n'
        f'├ В наличии: <b>{int(stock)} шт.</b>\n'
        '╰ Выберите количество:'
    )
    await safe_edit_text(cb.message, text, reply_markup=quantity_kb(product_id, catalog_key, int(stock), page=page))
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qty:'))
async def qty_router(cb: types.CallbackQuery):
    _, product_id_str, qty_str = cb.data.split(':')
    product_id = int(product_id_str)
    qty = int(qty_str)
    user_id = cb.from_user.id

    if user_id in active_buy_users:
        await cb.answer('Покупка уже обрабатывается, подождите...', show_alert=True)
        return
    active_buy_users.add(user_id)

    try:
        product = get_product(product_id)
        if not product:
            await cb.answer('Товар не найден', show_alert=True)
            return

        _, title, _, price, credentials, category, stock, *_ = product

        if qty <= 0 or qty > 10:
            await cb.answer('Можно купить от 1 до 10 шт за раз', show_alert=True)
            return

        if qty > stock:
            await cb.answer('Недостаточно товара на складе', show_alert=True)
            return

        total = float(price) * qty

        if category != 'tg':
            available_items = len(split_credentials_items(credentials))
            if int(stock) != available_items:
                try:
                    set_stock(product_id, available_items)
                except Exception:
                    pass
            if available_items < qty:
                await cb.answer('Товар временно недоступен: закончились данные', show_alert=True)
                await safe_edit_text(
                    cb.message,
                    '⛔ Покупка временно недоступна.\n'
                    f'Доступно данных для выдачи: <b>{available_items}</b>.\n'
                    'Напишите в поддержку или выберите другой товар.',
                    reply_markup=back_to_main_kb(),
                )
                return

        if not try_spend_balance(user_id, total):
            await cb.answer('Недостаточно баланса', show_alert=True)
            await safe_edit_text(
                cb.message,
                '╭──── ❌ <b>Недостаточно средств</b>\n'
                f'├ Товар: <b>{title}</b>\n'
                f'├ Нужно: <b>{total:.2f} ₽</b>\n'
                '╰ Пополните баланс и повторите попытку',
                reply_markup=topup_kb(),
            )
            return

        if category == 'tg':
            # Автоматическая выдача TG-аккаунта (номер из базы)
            delivery_data, _ = consume_product_credentials(product_id, qty)
            if not delivery_data:
                change_balance(user_id, total)
                await cb.answer('Товар закончился во время покупки, деньги возвращены', show_alert=True)
                await safe_edit_text(
                    cb.message,
                    '⛔ Во время оформления товар закончился, сумма возвращена на баланс.\n'
                    'Обновите каталог и попробуйте снова.',
                    reply_markup=back_to_main_kb(),
                )
                return

            update_stock(product_id, -qty)
            order_id = create_order(user_id, product_id, qty, total, 'delivered')
            set_order_code(order_id, delivery_data)
            await notify_admins_about_purchase(cb.from_user, order_id, title, qty, total, 'delivered')
            cashback = calculate_cashback(total)
            if cashback > 0:
                change_balance(user_id, cashback)
            balance = get_balance(user_id)
            cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
            await safe_edit_text(
                cb.message,
                f'╭──── ✅ <b>Заказ #{order_id} оплачен</b>\n'
                f'├ Товар: <b>{title}</b>\n'
                f'├ Количество: {qty}\n'
                f'├ Сумма: {total:.2f} ₽\n'
                f'{cashback_text}'
                f'├ Баланс: <b>{balance:.2f} ₽</b>\n'
                f'╰ Ваш номер для входа:\n<code>{delivery_data}</code>',
                reply_markup=review_offer_kb(order_id),
            )
            await cb.answer('TG-аккаунт выдан')
            return

        delivery_data, _ = consume_product_credentials(product_id, qty)
        if not delivery_data:
            change_balance(user_id, total)
            await cb.answer('Товар закончился во время покупки, деньги возвращены', show_alert=True)
            await safe_edit_text(
                cb.message,
                '⛔ Во время оформления товар закончился, сумма возвращена на баланс.\n'
                'Обновите каталог и попробуйте снова.',
                reply_markup=back_to_main_kb(),
            )
            return

        order_id = create_order(user_id, product_id, qty, total, 'delivered')
        set_order_code(order_id, delivery_data)
        await notify_admins_about_purchase(cb.from_user, order_id, title, qty, total, 'delivered')
        cashback = calculate_cashback(total)
        if cashback > 0:
            change_balance(user_id, cashback)
        balance = get_balance(user_id)
        cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
        deliver_text = (
            f'╭──── ✅ <b>Заказ #{order_id} оплачен</b>\n'
            f'├ Товар: <b>{title}</b>\n'
            f'├ Количество: <b>{qty}</b>\n'
            f'├ Сумма: <b>{total:.2f} ₽</b>\n'
            f'{cashback_text}'
            f'├ Баланс: <b>{balance:.2f} ₽</b>\n'
            f'├ Данные:\n<code>{delivery_data}</code>\n'
            '╰ Техподдержка находится в профиле'
        )
        await safe_edit_text(cb.message, deliver_text, reply_markup=review_offer_kb(order_id))
        await cb.answer('Покупка успешна')
    finally:
        active_buy_users.discard(user_id)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qtycustom:'))
async def qty_custom_router(cb: types.CallbackQuery):
    parts = cb.data.split(':')
    _, product_id_str, page_str = parts[:3]
    view_category = parts[3] if len(parts) > 3 else None
    product_id = int(product_id_str)
    page = int(page_str)
    product = get_product(product_id)
    if not product:
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, _, _, _, category, stock, *_ = product
    catalog_key = view_category or category
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    pending_custom_qty_input[cb.from_user.id] = {'product_id': product_id, 'page': page, 'catalog_key': catalog_key}
    await safe_edit_text(
        cb.message,
        f'Введите количество для <b>{title}</b> (от 1 до 10).\n'
        f'Сейчас доступно: <b>{int(stock)} шт.</b>',
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton('↩️ Назад', callback_data=f'qtymenu:{product_id}:{page}:{catalog_key}')
        ),
    )
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('topup:'))
async def topup_router(cb: types.CallbackQuery):
    action = cb.data.split(':', 1)[1]

    if action == 'custom':
        pending_custom_topup.add(cb.from_user.id)
        await safe_edit_text(cb.message, 'Введите сумму пополнения числом (например 750):', reply_markup=back_to_main_kb())
        await cb.answer()
        return

    amount = float(action)
    lot_url = resolve_funpay_lot_url(amount)
    if not lot_url:
        await safe_edit_text(
            cb.message,
            '╭──── ⛔ <b>Пополнение недоступно</b>\n'
            '├ Для этой суммы нет ссылки на лот FunPay\n'
            '╰ Настройте FUNPAY_LOT_MAP или FUNPAY_TOPUP_LOT_URL',
            reply_markup=back_to_main_kb(),
        )
        await cb.answer('Лот не настроен', show_alert=True)
        return

    topup_id = create_topup(cb.from_user.id, amount, '')
    payment_link, payment_id = lot_url, f'fp_{topup_id}'
    set_topup_payment_data(topup_id, payment_link, payment_id)
    if payment_id:
        set_topup_external_status(topup_id, 'created')

    await safe_edit_text(
        cb.message,
        f'╭──── ✅ <b>Заявка #{topup_id} создана</b>\n'
        f'├ Сумма: <b>{amount:.2f} ₽</b>\n'
        f'├ Ссылка: {payment_link}\n'
        f'├ Код в комментарий: <code>topup_{topup_id}_{cb.from_user.id}</code>\n'
        f'├ Оплатить ровно: <b>{amount:.2f} ₽</b>\n'
        '╰ После оплаты баланс зачислится автоматически',
        reply_markup=back_to_main_kb(),
    )

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f'Новая заявка на пополнение #{topup_id}\n'
                f'Пользователь: {cb.from_user.id}\n'
                f'Сумма: {amount:.2f} ₽\n'
                f'payment_id: {payment_id or "-"}\n'
                f'Подтвердить: /confirmtopup {topup_id}',
            )
        except Exception:
            pass

    await cb.answer('Заявка создана')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admpanel:'))
async def admin_panel_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    action = cb.data.split(':', 1)[1]
    user_id = cb.from_user.id

    if action == 'add_product':
        admin_add_product_state[user_id] = {'step': 'category'}
        admin_action_state.pop(user_id, None)
        await safe_edit_text(cb.message, 'Выбери категорию для нового товара:', reply_markup=admin_category_kb())
    elif action == 'section_products':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await safe_edit_text(
            cb.message,
            '📦 <b>Раздел: Товары</b>\nВыбери действие:',
            reply_markup=admin_products_kb(),
        )
    elif action == 'section_finance':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await safe_edit_text(
            cb.message,
            '💳 <b>Раздел: Финансы</b>\nВыбери действие:',
            reply_markup=admin_finance_kb(),
        )
    elif action == 'section_other':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await safe_edit_text(
            cb.message,
            '🧩 <b>Раздел: Прочее</b>\nЛоги и склад:',
            reply_markup=admin_other_kb(),
        )
    elif action == 'append_credentials':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'append_credentials', 'step': 'product_id'}
        await safe_edit_text(
            cb.message,
            admin_product_id_prompt_text(user_id, 'Выбери product_id товара, в который нужно добавить данные:'),
            reply_markup=admin_product_select_kb(user_id, 'append_credentials'),
        )
    elif action == 'home':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await safe_edit_text(cb.message, admin_panel_text(cb.from_user.id), reply_markup=admin_panel_kb())
    elif action == 'cancel_add':
        admin_add_product_state.pop(user_id, None)
        await safe_edit_text(cb.message, 'Создание товара отменено.', reply_markup=admin_panel_kb())
    elif action == 'cancel_any':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await safe_edit_text(cb.message, 'Текущее действие отменено.', reply_markup=admin_panel_kb())
    elif action == 'add_balance':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'add_balance', 'step': 'user_id'}
        await safe_edit_text(cb.message, 'Введи ID пользователя для пополнения баланса:', reply_markup=admin_step_kb())
    elif action == 'confirm_topup':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'confirm_topup', 'step': 'topup_id'}
        await safe_edit_text(cb.message, 'Введи ID пополнения (topup_id):', reply_markup=admin_step_kb())
    elif action == 'create_promo':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'create_promo', 'step': 'code'}
        await safe_edit_text(cb.message, 'Введи код промокода (например: BONUS50):', reply_markup=admin_step_kb())
    elif action == 'send_code':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'send_code', 'step': 'order_id'}
        await safe_edit_text(cb.message, 'Введи ID заказа:', reply_markup=admin_step_kb())
    elif action == 'delete_review':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'delete_review', 'step': 'review_id'}
        rows = list_reviews(limit=10, active_only=True)
        if not rows:
            await safe_edit_text(
                cb.message,
                'Активных отзывов пока нет.',
                reply_markup=admin_other_kb(),
            )
        else:
            lines = ['🗑 <b>Удаление отзыва</b>', 'Введи review_id для удаления:', '']
            for review_id, target_user_id, target_username, order_id, review_text, rating, _, _ in rows:
                short_text = html.escape(str(review_text or '').strip())
                if len(short_text) > 55:
                    short_text = short_text[:55] + '...'
                display_name = f'@{target_username}' if str(target_username or '').strip() else f'ID {target_user_id}'
                lines.append(f'#{review_id} | заказ #{order_id} | {html.escape(display_name)} | {int(rating)}⭐ | {short_text}')
            await safe_edit_text(cb.message, '\n'.join(lines), reply_markup=admin_step_kb())
    elif action == 'refill':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'refill', 'step': 'product_id'}
        await safe_edit_text(
            cb.message,
            admin_product_id_prompt_text(user_id, 'Выбери product_id для пополнения остатка:'),
            reply_markup=admin_product_select_kb(user_id, 'refill'),
        )
    elif action == 'set_stock':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'set_stock', 'step': 'product_id'}
        await safe_edit_text(
            cb.message,
            admin_product_id_prompt_text(user_id, 'Выбери product_id для установки остатка:'),
            reply_markup=admin_product_select_kb(user_id, 'set_stock'),
        )
    elif action == 'delete_product':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'delete_product', 'step': 'product_id'}
        await safe_edit_text(
            cb.message,
            admin_product_id_prompt_text(user_id, 'Выбери product_id для удаления товара из каталога:'),
            reply_markup=admin_product_select_kb(user_id, 'delete_product'),
        )
    elif action == 'list_products':
        rows = list_all_products_admin(admin_id=user_id)
        if not rows:
            await safe_edit_text(cb.message, 'У тебя пока нет добавленных товаров.', reply_markup=admin_panel_kb())
        else:
            lines = [f'📋 Твои товары (admin_id={user_id}):']
            for pid, title, category, price, stock, auto_restock in rows:
                auto_text = 'auto' if int(auto_restock) == 1 else 'manual'
                lines.append(f'#{pid} | {title} | {category} | {float(price):.2f}₽ | stock={int(stock)} | {auto_text}')
            await safe_edit_text(cb.message, '\n'.join(lines), reply_markup=admin_panel_kb())
    elif action == 'view_logs':
        await safe_edit_text(
            cb.message,
            '🧾 Выбери админа, чьи логи показать:',
            reply_markup=admin_logs_filter_kb(),
        )

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admpsel:'))
async def admin_product_select_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    parts = cb.data.split(':', 2)
    if len(parts) != 3:
        await cb.answer('Неверные данные', show_alert=True)
        return

    code = parts[1].strip()
    try:
        product_id = int(parts[2].strip())
    except ValueError:
        await cb.answer('Неверный product_id', show_alert=True)
        return

    action_map = {
        'app': 'append_credentials',
        'ref': 'refill',
        'set': 'set_stock',
        'del': 'delete_product',
    }
    action = action_map.get(code)
    user_id = cb.from_user.id
    if not action:
        await cb.answer('Неверное действие', show_alert=True)
        return

    if product_id not in admin_owned_product_ids(user_id):
        await cb.answer('Этот товар не из твоих', show_alert=True)
        return

    admin_add_product_state.pop(user_id, None)

    if action in {'refill', 'set_stock'}:
        admin_action_state[user_id] = {'action': action, 'step': 'qty', 'product_id': str(product_id)}
        prompt = 'Введи количество для добавления (+):' if action == 'refill' else 'Введи итоговое количество на складе:'
        await safe_edit_text(cb.message, f'Выбран товар #{product_id}.\n{prompt}', reply_markup=admin_step_kb())
        await cb.answer('Товар выбран')
        return

    if action == 'append_credentials':
        admin_action_state[user_id] = {'action': action, 'step': 'credentials', 'product_id': str(product_id)}
        await safe_edit_text(
            cb.message,
            f'Выбран товар #{product_id}.\n'
            'Отправь данные для добавления (каждая строка — отдельная единица товара).\n'
            'Пример:\nmail1@example.com:pass1\nmail2@example.com:pass2',
            reply_markup=admin_step_kb(),
        )
        await cb.answer('Товар выбран')
        return

    deleted = deactivate_product(product_id)
    admin_action_state.pop(user_id, None)
    if deleted:
        log_admin_action(user_id, 'delete_product', f'product_id={product_id}')
        await safe_edit_text(cb.message, f'✅ Товар #{product_id} удален из каталога.', reply_markup=admin_panel_kb())
        await cb.answer('Удалено')
    else:
        await safe_edit_text(cb.message, 'Товар не найден или уже удален.', reply_markup=admin_panel_kb())
        await cb.answer('Не найдено')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admlogs:'))
async def admin_logs_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    payload = cb.data.split(':', 1)[1].strip()
    target_admin_id: Optional[int] = None
    if payload != 'all':
        try:
            target_admin_id = int(payload)
        except ValueError:
            await cb.answer('Неверный admin_id', show_alert=True)
            return

    await safe_edit_text(
        cb.message,
        admin_logs_text(limit=40, admin_id=target_admin_id),
        reply_markup=admin_logs_filter_kb(),
    )
    await cb.answer('Логи обновлены')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddcat:'))
async def admin_add_category_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state:
        await cb.answer('Сначала открой /adminpanel', show_alert=True)
        return

    category = cb.data.split(':', 1)[1]
    if category not in CATEGORY_NAMES:
        await cb.answer('Неверная категория', show_alert=True)
        return

    if category == 'proxy':
        state['category'] = 'proxy'
        state['step'] = 'proxy_section'
        await safe_edit_text(cb.message, 'Выбери раздел для прокси:', reply_markup=admin_proxy_section_kb())
        await cb.answer()
        return

    state['category'] = category
    state.pop('proxy_region', None)
    state['step'] = 'title'
    await safe_edit_text(cb.message, 'Введи название товара (например: 🇩🇪 Германия 3 дня):')
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddproxy:'))
async def admin_add_proxy_section_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state or state.get('step') != 'proxy_section':
        await cb.answer('Сначала открой мастер добавления', show_alert=True)
        return

    section = cb.data.split(':', 1)[1]
    if section not in {'de', 'us'}:
        await cb.answer('Неверный раздел', show_alert=True)
        return

    state['category'] = 'proxy'
    state['proxy_region'] = section
    state['step'] = 'title'
    region_label = '🇩🇪 Германия' if section == 'de' else '🇺🇸 США'
    await safe_edit_text(
        cb.message,
        f'Раздел выбран: {region_label}\n'
        'Введи название товара (флаг добавится автоматически, если его нет):',
    )
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddauto:'))
async def admin_add_auto_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state or state.get('step') != 'auto_restock':
        await cb.answer('Сначала пройди шаги мастера', show_alert=True)
        return

    choice = cb.data.split(':', 1)[1]
    auto_restock = 1 if choice == 'yes' else 0
    state['auto_restock'] = str(auto_restock)
    state['step'] = 'confirm'

    auto_text = 'включено (+1 к складу каждые 30 секунд)' if auto_restock else 'выключено'
    category_label = CATEGORY_NAMES.get(state['category'], state['category'])
    if state.get('category') == 'proxy' and state.get('proxy_region') in {'de', 'us'}:
        region_label = '🇩🇪 Германия' if state.get('proxy_region') == 'de' else '🇺🇸 США'
        category_label = f'{category_label} / {region_label}'
    await safe_edit_text(
        cb.message,
        'Проверь данные товара перед сохранением:\n\n'
        f'Категория: {category_label}\n'
        f'Название: {state["title"]}\n'
        f'Цена: {float(state["price"]):.2f} ₽\n'
        f'Остаток: {int(state["stock"])}\n'
        f'Данные: {state["credentials"]}\n'
        f'Описание: {state["description"]}\n'
        f'Автообновление: {auto_text}',
        reply_markup=admin_confirm_product_kb(),
    )
    await cb.answer('Проверь и подтверди')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admaddsave:'))
async def admin_add_save_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    user_id = cb.from_user.id
    state = admin_add_product_state.get(user_id)
    if not state or state.get('step') != 'confirm':
        await cb.answer('Нет данных для сохранения', show_alert=True)
        return

    final_title = state['title']
    final_description = state['description']
    if state.get('category') == 'proxy':
        final_title = apply_proxy_region_title(final_title, state.get('proxy_region', ''))
        final_description = apply_proxy_region_description(final_description, state.get('proxy_region', ''))

    product_id = add_product(
        final_title,
        float(state['price']),
        state['credentials'],
        state['category'],
        final_description,
        int(state['stock']),
        auto_restock=int(state.get('auto_restock', '0')),
        restock_every_minutes=5,
        restock_amount=1,
        created_by_admin=user_id,
    )
    auto_text = 'включено (+1 к складу каждые 30 секунд)' if int(state.get('auto_restock', '0')) == 1 else 'выключено'
    category_label = CATEGORY_NAMES.get(state['category'], state['category'])
    if state.get('category') == 'proxy' and state.get('proxy_region') in {'de', 'us'}:
        region_label = '🇩🇪 Германия' if state.get('proxy_region') == 'de' else '🇺🇸 США'
        category_label = f'{category_label} / {region_label}'
    log_admin_action(
        user_id,
        'add_product',
        f'product_id={product_id}; category={state["category"]}; title={final_title}; stock={int(state["stock"])}',
    )
    admin_add_product_state.pop(user_id, None)
    await safe_edit_text(
        cb.message,
        f'✅ Товар добавлен, id={product_id}\n'
        f'Категория: {category_label}\n'
        f'Остаток на складе: {int(state["stock"])}\n'
        f'Автопополнение склада: {auto_text}',
        reply_markup=admin_panel_kb(),
    )
    await cb.answer('Сохранено')


@dp.message_handler(content_types=types.ContentType.TEXT)
async def text_router(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or '').strip()

    if user_id in pending_custom_qty_input:
        pending_data = pending_custom_qty_input[user_id]
        try:
            qty = int(text)
        except ValueError:
            await message.answer('Введите целое число от 1 до 10.')
            return

        if qty < 1 or qty > 10:
            await message.answer('Можно купить только от 1 до 10 шт за раз.')
            return

        product_id = int(pending_data['product_id'])

        if user_id in active_buy_users:
            await message.answer('Покупка уже обрабатывается, подождите...')
            return
        active_buy_users.add(user_id)

        try:
            product = get_product(product_id)
            if not product:
                pending_custom_qty_input.pop(user_id, None)
                await message.answer('Товар не найден.', reply_markup=back_to_main_kb())
                return

            _, title, _, price, credentials, category, stock, *_ = product
            if qty > int(stock):
                await message.answer(
                    f'Недостаточно товара на складе. Доступно: {int(stock)} шт.',
                    reply_markup=back_to_main_kb(),
                )
                return

            total = float(price) * qty

            if category != 'tg':
                available_items = len(split_credentials_items(credentials))
                if int(stock) != available_items:
                    try:
                        set_stock(product_id, available_items)
                    except Exception:
                        pass
                if available_items < qty:
                    await message.answer(
                        '⛔ Покупка временно недоступна.\n'
                        f'Доступно данных для выдачи: <b>{available_items}</b>.\n'
                        'Напишите в поддержку или выберите другой товар.',
                        reply_markup=back_to_main_kb(),
                    )
                    return

            if not try_spend_balance(user_id, total):
                await message.answer(
                    f'Недостаточно средств для покупки <b>{title}</b>.\nНужно: {total:.2f} ₽',
                    reply_markup=topup_kb(),
                )
                return

            if category == 'tg':
                # Автоматическая выдача TG-аккаунта (номер из базы)
                delivery_data, _ = consume_product_credentials(product_id, qty)
                if not delivery_data:
                    change_balance(user_id, total)
                    await message.answer(
                        '⛔ Во время оформления товар закончился, сумма возвращена на баланс.\n'
                        'Обновите каталог и попробуйте снова.',
                        reply_markup=back_to_main_kb(),
                    )
                    pending_custom_qty_input.pop(user_id, None)
                    return

                update_stock(product_id, -qty)
                order_id = create_order(user_id, product_id, qty, total, 'delivered')
                set_order_code(order_id, delivery_data)
                await notify_admins_about_purchase(message.from_user, order_id, title, qty, total, 'delivered')
                cashback = calculate_cashback(total)
                if cashback > 0:
                    change_balance(user_id, cashback)
                balance = get_balance(user_id)
                cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
                await message.answer(
                    f'✅ Заказ #{order_id} успешно оплачен с баланса.\n'
                    f'Товар: <b>{title}</b>\n'
                    f'Количество: {qty}\n'
                    f'Сумма: {total:.2f} ₽\n'
                    f'{cashback_text}'
                    f'Остаток баланса: {balance:.2f} ₽\n\n'
                    f'Ваш номер для входа:\n<code>{delivery_data}</code>',
                    reply_markup=review_offer_kb(order_id),
                )
                pending_custom_qty_input.pop(user_id, None)
                return

            delivery_data, _ = consume_product_credentials(product_id, qty)
            if not delivery_data:
                change_balance(user_id, total)
                await message.answer(
                    '⛔ Во время оформления товар закончился, сумма возвращена на баланс.\n'
                    'Обновите каталог и попробуйте снова.',
                    reply_markup=back_to_main_kb(),
                )
                pending_custom_qty_input.pop(user_id, None)
                return

            order_id = create_order(user_id, product_id, qty, total, 'delivered')
            set_order_code(order_id, delivery_data)
            await notify_admins_about_purchase(message.from_user, order_id, title, qty, total, 'delivered')
            cashback = calculate_cashback(total)
            if cashback > 0:
                change_balance(user_id, cashback)
            balance = get_balance(user_id)
            cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
            await message.answer(
                f'✅ Заказ #{order_id} успешно оплачен с баланса.\n'
                f'Товар: <b>{title}</b>\n'
                f'Количество: {qty}\n'
                f'Сумма: {total:.2f} ₽\n'
                f'{cashback_text}'
                f'Остаток баланса: {balance:.2f} ₽\n\n'
                f'Данные:\n<code>{delivery_data}</code>',
                reply_markup=review_offer_kb(order_id),
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
                    await message.answer('ID должен быть числом. Введи снова:')
                    return
                state['user_id'] = str(target_user_id)
                state['step'] = 'amount'
                await message.answer('Введи сумму пополнения (можно отрицательную для списания):')
                return

            if step == 'amount':
                try:
                    amount = float(text.replace(',', '.'))
                except ValueError:
                    await message.answer('Сумма должна быть числом. Введи снова:')
                    return
                target_user_id = int(state['user_id'])
                change_balance(target_user_id, amount)
                balance = get_balance(target_user_id)
                log_admin_action(user_id, 'add_balance', f'user_id={target_user_id}; amount={amount:.2f}; balance={balance:.2f}')
                admin_action_state.pop(user_id, None)
                await message.answer(
                    f'✅ Баланс пользователя {target_user_id} изменен на {amount:.2f} ₽.\nТекущий баланс: {balance:.2f} ₽',
                    reply_markup=admin_panel_kb(),
                )
                try:
                    await bot.send_message(target_user_id, f'✅ Ваш баланс изменен на {amount:.2f} ₽.\nТекущий баланс: {balance:.2f} ₽')
                except Exception:
                    pass
                return

        if action == 'confirm_topup' and step == 'topup_id':
            try:
                topup_id = int(text)
            except ValueError:
                await message.answer('topup_id должен быть числом. Введи снова:')
                return

            result = confirm_topup(topup_id)
            if not result:
                await message.answer('Пополнение не найдено. Введи другой topup_id:')
                return

            _, target_user_id, amount, _ = result
            balance = get_balance(target_user_id)
            log_admin_action(user_id, 'confirm_topup', f'topup_id={topup_id}; user_id={target_user_id}; amount={float(amount):.2f}')
            admin_action_state.pop(user_id, None)
            await message.answer(
                f'✅ Пополнение #{topup_id} подтверждено на {float(amount):.2f} ₽.',
                reply_markup=admin_panel_kb(),
            )
            try:
                await bot.send_message(target_user_id, f'✅ Ваш баланс пополнен на {float(amount):.2f} ₽.\nТекущий баланс: {balance:.2f} ₽')
            except Exception:
                pass
            return

        if action == 'create_promo':
            if step == 'code':
                state['code'] = text.upper()
                state['step'] = 'amount'
                await message.answer('Введи сумму промокода:')
                return
            if step == 'amount':
                try:
                    amount = float(text.replace(',', '.'))
                    if amount <= 0:
                        raise ValueError
                except ValueError:
                    await message.answer('Сумма должна быть числом > 0. Введи снова:')
                    return
                state['amount'] = str(amount)
                state['step'] = 'uses'
                await message.answer('Введи количество активаций:')
                return
            if step == 'uses':
                try:
                    uses = int(text)
                    if uses <= 0:
                        raise ValueError
                except ValueError:
                    await message.answer('Количество активаций должно быть целым > 0. Введи снова:')
                    return
                create_promo(state['code'], float(state['amount']), uses)
                log_admin_action(
                    user_id,
                    'create_promo',
                    f'code={state["code"]}; amount={float(state["amount"]):.2f}; uses={uses}',
                )
                admin_action_state.pop(user_id, None)
                await message.answer(
                    f'✅ Промокод {state["code"]} создан: {float(state["amount"]):.2f} ₽, активаций {uses}',
                    reply_markup=admin_panel_kb(),
                )
                return

        if action == 'send_code':
            if step == 'order_id':
                try:
                    order_id = int(text)
                except ValueError:
                    await message.answer('order_id должен быть числом. Введи снова:')
                    return
                order = get_order(order_id)
                if not order:
                    await message.answer('Заказ не найден. Введи другой order_id:')
                    return
                state['order_id'] = str(order_id)
                state['step'] = 'code'
                await message.answer('Введи код для отправки пользователю:')
                return
            if step == 'code':
                order_id = int(state['order_id'])
                code = text
                order = get_order(order_id)
                if not order:
                    admin_action_state.pop(user_id, None)
                    await message.answer('Заказ уже не найден.', reply_markup=admin_panel_kb())
                    return
                _, target_user_id, _, _, _, status, _, _, _ = order
                if status not in ('waiting_code', 'waiting_phone'):
                    admin_action_state.pop(user_id, None)
                    await message.answer('Заказ не ожидает код.', reply_markup=admin_panel_kb())
                    return
                set_order_code(order_id, code)
                log_admin_action(user_id, 'send_code', f'order_id={order_id}; target_user_id={target_user_id}')
                admin_action_state.pop(user_id, None)
                await message.answer('✅ Код отправлен клиенту.', reply_markup=admin_panel_kb())
                await bot.send_message(target_user_id, f'Код для заказа #{order_id}: <code>{code}</code>')
                return

        if action in ('refill', 'set_stock'):
            if step == 'product_id':
                try:
                    product_id = int(text)
                except ValueError:
                    await message.answer(
                        admin_product_id_prompt_text(user_id, 'product_id должен быть числом. Выбери из списка:'),
                        reply_markup=admin_product_select_kb(user_id, action),
                    )
                    return
                if product_id not in admin_owned_product_ids(user_id):
                    await message.answer(
                        admin_product_id_prompt_text(user_id, 'Этот product_id не из твоих товаров. Выбери из списка:'),
                        reply_markup=admin_product_select_kb(user_id, action),
                    )
                    return
                state['product_id'] = str(product_id)
                state['step'] = 'qty'
                if action == 'refill':
                    await message.answer('Введи количество для добавления (+):')
                else:
                    await message.answer('Введи итоговое количество на складе:')
                return
            if step == 'qty':
                try:
                    qty = int(text)
                except ValueError:
                    await message.answer('Количество должно быть целым числом. Введи снова:')
                    return
                product_id = int(state['product_id'])
                if action == 'refill':
                    update_stock(product_id, qty)
                    message_text = f'✅ Остаток товара #{product_id} пополнен на {qty}.'
                    log_admin_action(user_id, 'refill_stock', f'product_id={product_id}; delta={qty}')
                else:
                    set_stock(product_id, qty)
                    message_text = f'✅ Остаток товара #{product_id} установлен: {qty}.'
                    log_admin_action(user_id, 'set_stock', f'product_id={product_id}; stock={qty}')
                admin_action_state.pop(user_id, None)
                await message.answer(message_text, reply_markup=admin_panel_kb())
                return

        if action == 'append_credentials':
            if step == 'product_id':
                try:
                    product_id = int(text)
                except ValueError:
                    await message.answer(
                        admin_product_id_prompt_text(user_id, 'product_id должен быть числом. Выбери из списка:'),
                        reply_markup=admin_product_select_kb(user_id, action),
                    )
                    return
                if product_id not in admin_owned_product_ids(user_id):
                    await message.answer(
                        admin_product_id_prompt_text(user_id, 'Этот product_id не из твоих товаров. Выбери из списка:'),
                        reply_markup=admin_product_select_kb(user_id, action),
                    )
                    return
                state['product_id'] = str(product_id)
                state['step'] = 'credentials'
                await message.answer(
                    'Отправь данные для добавления (каждая строка — отдельная единица товара).\n'
                    'Пример:\nmail1@example.com:pass1\nmail2@example.com:pass2'
                )
                return
            if step == 'credentials':
                product_id = int(state['product_id'])
                result = append_product_credentials_with_stock(product_id, text)
                if not result:
                    await message.answer('Не удалось добавить данные. Проверь product_id и формат данных.')
                    return
                added_count, new_stock, _ = result
                log_admin_action(
                    user_id,
                    'append_credentials',
                    f'product_id={product_id}; added={added_count}; stock={new_stock}',
                )
                admin_action_state.pop(user_id, None)
                await message.answer(
                    f'✅ Данные добавлены в товар #{product_id}.\nДобавлено позиций: {added_count}\nНовый остаток: {new_stock}',
                    reply_markup=admin_panel_kb(),
                )
                return

        if action == 'delete_product' and step == 'product_id':
            try:
                product_id = int(text)
            except ValueError:
                await message.answer(
                    admin_product_id_prompt_text(user_id, 'product_id должен быть числом. Выбери из списка:'),
                    reply_markup=admin_product_select_kb(user_id, action),
                )
                return
            if product_id not in admin_owned_product_ids(user_id):
                await message.answer(
                    admin_product_id_prompt_text(user_id, 'Этот product_id не из твоих товаров. Выбери из списка:'),
                    reply_markup=admin_product_select_kb(user_id, action),
                )
                return

            deleted = deactivate_product(product_id)
            admin_action_state.pop(user_id, None)
            if deleted:
                log_admin_action(user_id, 'delete_product', f'product_id={product_id}')
                await message.answer(f'✅ Товар #{product_id} удален из каталога.', reply_markup=admin_panel_kb())
            else:
                await message.answer('Товар не найден или уже удален.', reply_markup=admin_panel_kb())
            return

        if action == 'delete_review' and step == 'review_id':
            try:
                review_id = int(text)
            except ValueError:
                await message.answer('review_id должен быть числом. Введи снова:')
                return

            deleted = delete_review(review_id)
            admin_action_state.pop(user_id, None)
            if deleted:
                log_admin_action(user_id, 'delete_review', f'review_id={review_id}')
                await message.answer(f'✅ Отзыв #{review_id} удален.', reply_markup=admin_panel_kb())
            else:
                await message.answer('Отзыв не найден или уже удален.', reply_markup=admin_panel_kb())
            return

    if is_admin(user_id) and user_id in admin_add_product_state:
        state = admin_add_product_state[user_id]
        step = state.get('step')

        if step == 'title':
            state['title'] = text
            state['step'] = 'price'
            await message.answer('Теперь введи цену (например: 8.8)', reply_markup=admin_step_kb())
            return

        if step == 'price':
            try:
                price = float(text.replace(',', '.'))
                if price <= 0:
                    raise ValueError
            except ValueError:
                await message.answer('Цена должна быть числом больше 0. Введи снова:', reply_markup=admin_step_kb())
                return
            state['price'] = str(price)
            state['step'] = 'stock'
            await message.answer('Введи количество на складе (stock):', reply_markup=admin_step_kb())
            return

        if step == 'stock':
            try:
                stock = int(text)
                if stock < 0:
                    raise ValueError
            except ValueError:
                await message.answer('Количество должно быть целым числом 0 или больше. Введи снова:', reply_markup=admin_step_kb())
                return
            state['stock'] = str(stock)
            state['step'] = 'credentials'
            await message.answer('Введи данные товара (логин:пароль / прокси / номер и т.д.):', reply_markup=admin_step_kb())
            return

        if step == 'credentials':
            state['credentials'] = text
            state['step'] = 'description'
            await message.answer('Введи описание товара:', reply_markup=admin_step_kb())
            return

        if step == 'description':
            state['description'] = text
            state['step'] = 'auto_restock'
            await message.answer(
                'Включить автопополнение склада?\nЕсли включить, склад будет увеличиваться на +1 каждые 30 секунд.',
                reply_markup=admin_auto_restock_kb(),
            )
            return

    if user_id in pending_promo_input:
        pending_promo_input.discard(user_id)
        ok, msg, amount = activate_promo(user_id, text)
        if ok:
            bal = get_balance(user_id)
            await message.answer(f'✅ {msg}\nНачислено: {amount:.2f} ₽\nТекущий баланс: {bal:.2f} ₽', reply_markup=main_menu_kb(user_id))
        else:
            await message.answer(f'❌ {msg}', reply_markup=main_menu_kb(user_id))
        return

    if user_id in pending_custom_topup:
        try:
            amount = float(text.replace(',', '.'))
            if amount <= 0:
                raise ValueError
        except ValueError:
            await message.answer('Введите корректную сумму числом.')
            return

        pending_custom_topup.discard(user_id)
        lot_url = resolve_funpay_lot_url(amount)
        if not lot_url:
            await message.answer(
                '⛔ Пополнение временно недоступно.\n\n'
                'Для этой суммы не настроен лот FunPay.\n'
                'Админу нужно задать `FUNPAY_LOT_MAP` или `FUNPAY_TOPUP_LOT_URL`.',
                reply_markup=main_menu_kb(user_id),
            )
            return

        topup_id = create_topup(user_id, amount, '')
        payment_link, payment_id = lot_url, f'fp_{topup_id}'
        set_topup_payment_data(topup_id, payment_link, payment_id)
        if payment_id:
            set_topup_external_status(topup_id, 'created')

        await message.answer(
            f'Заявка на пополнение #{topup_id} создана на {amount:.2f} ₽\n'
            f'1) Оплатите товар по ссылке: {payment_link}\n'
            f'2) В комментарии к заказу укажите код: <code>topup_{topup_id}_{user_id}</code>\n'
            f'3) Сумма оплаты должна быть: {amount:.2f} ₽\n'
            'После оплаты баланс подтвердится автоматически (или админом, если API недоступен).',
            reply_markup=main_menu_kb(user_id),
        )

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f'Новая заявка на пополнение #{topup_id}\n'
                    f'Пользователь: {user_id}\n'
                    f'Сумма: {amount:.2f} ₽\n'
                    f'payment_id: {payment_id or "-"}\n'
                    f'Подтвердить: /confirmtopup {topup_id}',
                )
            except Exception:
                pass
        return

    if user_id in pending_review_input:
        review_state = pending_review_input.pop(user_id)
        order_id = int(review_state.get('order_id', 0))
        rating = int(review_state.get('rating', 5))
        order = get_order(order_id)
        if not order:
            await message.answer('Заказ для отзыва не найден.', reply_markup=main_menu_kb(user_id))
            return

        _, order_user_id, _, _, _, _, _, _, _ = order
        if int(order_user_id) != int(user_id):
            await message.answer('Нельзя оставить отзыв к чужому заказу.', reply_markup=main_menu_kb(user_id))
            return

        ok, msg, reward, review_id = create_review(
            user_id,
            order_id,
            text,
            REVIEW_REWARD_RUB,
            rating=rating,
            username=(message.from_user.username or ''),
        )
        if not ok:
            await message.answer(f'❌ {msg}', reply_markup=main_menu_kb(user_id))
            return

        balance = get_balance(user_id)
        await message.answer(
            f'✅ Отзыв #{review_id} сохранен!\n'
            f'Оценка: {rating}⭐\n'
            f'Начислено за отзыв: {reward:.2f} ₽\n'
            f'Текущий баланс: {balance:.2f} ₽',
            reply_markup=main_menu_kb(user_id),
        )
        return

    if user_id in pending_tg_phone_order:
        order_id = pending_tg_phone_order.pop(user_id)
        set_order_phone(order_id, text)
        set_order_status(order_id, 'waiting_code')

        await message.answer(
            f'Номер сохранён для заказа #{order_id}.\n'
            'Ожидайте код подтверждения от администратора.',
            reply_markup=main_menu_kb(user_id),
        )

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f'Заказ #{order_id} ожидает код.\n'
                    f'Пользователь: {user_id}\n'
                    f'Номер: {text}\n\n'
                    f'Отправить код: /sendcode {order_id} 12345',
                )
            except Exception:
                pass
        return


@dp.message_handler(commands=['admin'])
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.reply(
        'Админ команды:\n'
        '/adminpanel — мастер добавления товара\n'
        '/addproduct category|title|price|stock|credentials|description\n'
        '/refill <product_id> <qty>\n'
        '/setstock <product_id> <qty>\n'
        '/confirmtopup <topup_id>\n'
        '/createpromo <CODE> <amount> <uses>\n'
        '/sendcode <order_id> <code>\n'
        '/addbalance <user_id> <amount>\n'
        '/deletereview <review_id>'
    )


@dp.message_handler(commands=['adminpanel'])
async def cmd_adminpanel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return
    admin_add_product_state.pop(message.from_user.id, None)
    admin_action_state.pop(message.from_user.id, None)
    await message.reply(admin_panel_text(message.from_user.id), reply_markup=admin_panel_kb())


@dp.message_handler(commands=['addproduct'])
async def cmd_addproduct(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split('|')
    if len(parts) < 6:
        await message.reply('Формат: /addproduct category|title|price|stock|credentials|description')
        return

    category = parts[0].strip().lower()
    title = parts[1].strip()
    try:
        price = float(parts[2].strip().replace(',', '.'))
        stock = int(parts[3].strip())
    except ValueError:
        await message.reply('price и stock должны быть числами')
        return

    credentials = parts[4].strip()
    description = parts[5].strip()

    proxy_region = ''
    if category in {'proxy_de', 'proxy_us'}:
        proxy_region = 'de' if category == 'proxy_de' else 'us'
        category = 'proxy'

    if category not in CATEGORY_NAMES:
        await message.reply('Категории: proxy, proxy_de, proxy_us, tg, email')
        return

    if category == 'proxy' and proxy_region:
        title = apply_proxy_region_title(title, proxy_region)
        description = apply_proxy_region_description(description, proxy_region)

    product_id = add_product(
        title,
        price,
        credentials,
        category,
        description,
        stock,
        auto_restock=0,
        created_by_admin=message.from_user.id,
    )
    log_admin_action(
        message.from_user.id,
        'add_product_cmd',
        f'product_id={product_id}; category={category}; title={title}; stock={stock}',
    )
    await message.reply(f'Товар добавлен, id={product_id}')


@dp.message_handler(commands=['refill'])
async def cmd_refill(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('Формат: /refill <product_id> <qty>')
        return

    try:
        product_id = int(parts[0])
        qty = int(parts[1])
    except ValueError:
        await message.reply('Неверный формат')
        return

    update_stock(product_id, qty)
    log_admin_action(message.from_user.id, 'refill_stock_cmd', f'product_id={product_id}; delta={qty}')
    await message.reply('Остаток обновлен')


@dp.message_handler(commands=['setstock'])
async def cmd_setstock(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('Формат: /setstock <product_id> <qty>')
        return

    try:
        product_id = int(parts[0])
        qty = int(parts[1])
    except ValueError:
        await message.reply('Неверный формат')
        return

    set_stock(product_id, qty)
    log_admin_action(message.from_user.id, 'set_stock_cmd', f'product_id={product_id}; stock={qty}')
    await message.reply('Остаток установлен')


@dp.message_handler(commands=['confirmtopup'])
async def cmd_confirm_topup(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    args = message.get_args().strip()
    if not args:
        await message.reply('Формат: /confirmtopup <topup_id>')
        return

    try:
        topup_id = int(args)
    except ValueError:
        await message.reply('Неверный id')
        return

    result = confirm_topup(topup_id)
    if not result:
        await message.reply('Заявка не найдена')
        return

    _, user_id, amount, status = result
    log_admin_action(message.from_user.id, 'confirm_topup_cmd', f'topup_id={topup_id}; user_id={user_id}; amount={float(amount):.2f}')
    await message.reply(f'Пополнение #{topup_id} подтверждено: {amount:.2f} ₽')
    try:
        balance = get_balance(user_id)
        await bot.send_message(
            user_id,
            f'✅ Ваш баланс пополнен на {float(amount):.2f} ₽.\nТекущий баланс: {balance:.2f} ₽',
        )
    except Exception:
        pass


@dp.message_handler(commands=['createpromo'])
async def cmd_createpromo(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split()
    if len(parts) != 3:
        await message.reply('Формат: /createpromo <CODE> <amount> <uses>')
        return

    code = parts[0].upper()
    try:
        amount = float(parts[1].replace(',', '.'))
        uses = int(parts[2])
    except ValueError:
        await message.reply('Неверный формат amount/uses')
        return

    create_promo(code, amount, uses)
    log_admin_action(message.from_user.id, 'create_promo_cmd', f'code={code}; amount={amount:.2f}; uses={uses}')
    await message.reply(f'Промокод {code} создан: {amount:.2f} ₽, активаций {uses}')


@dp.message_handler(commands=['sendcode'])
async def cmd_sendcode(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split(maxsplit=1)
    if len(parts) != 2:
        await message.reply('Формат: /sendcode <order_id> <code>')
        return

    try:
        order_id = int(parts[0])
    except ValueError:
        await message.reply('Неверный order_id')
        return

    code = parts[1].strip()
    order = get_order(order_id)
    if not order:
        await message.reply('Заказ не найден')
        return

    _, user_id, _, _, _, status, _, _, _ = order
    if status not in ('waiting_code', 'waiting_phone'):
        await message.reply('Заказ не ожидает код')
        return

    set_order_code(order_id, code)
    log_admin_action(message.from_user.id, 'send_code_cmd', f'order_id={order_id}; user_id={user_id}')
    await message.reply('Код отправлен клиенту')
    await bot.send_message(
        user_id,
        f'Код для заказа #{order_id}: <code>{code}</code>',
        reply_markup=review_offer_kb(order_id),
    )


@dp.message_handler(commands=['addbalance'])
async def cmd_addbalance(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    parts = message.get_args().split()
    if len(parts) != 2:
        await message.reply('Формат: /addbalance <user_id> <amount>')
        return

    try:
        user_id = int(parts[0])
        amount = float(parts[1].replace(',', '.'))
    except ValueError:
        await message.reply('Неверный формат')
        return

    change_balance(user_id, amount)
    bal = get_balance(user_id)
    log_admin_action(message.from_user.id, 'add_balance_cmd', f'user_id={user_id}; amount={amount:.2f}; balance={bal:.2f}')
    await message.reply(f'Баланс пользователя {user_id} изменен на {amount:.2f} ₽. Текущий: {bal:.2f} ₽')


@dp.message_handler(commands=['deletereview'])
async def cmd_deletereview(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return

    args = message.get_args().strip()
    if not args:
        await message.reply('Формат: /deletereview <review_id>')
        return

    try:
        review_id = int(args)
    except ValueError:
        await message.reply('Неверный review_id')
        return

    deleted = delete_review(review_id)
    if not deleted:
        await message.reply('Отзыв не найден или уже удален')
        return

    log_admin_action(message.from_user.id, 'delete_review_cmd', f'review_id={review_id}')
    await message.reply(f'✅ Отзыв #{review_id} удален')


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
                    f'✅ Ваш баланс пополнен на {float(credited_amount):.2f} ₽.\n'
                    f'Текущий баланс: {balance:.2f} ₽',
                )
            except Exception:
                pass

            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f'Автоподтверждение FunPay: topup #{topup_id}, user={confirmed_user_id}, amount={float(credited_amount):.2f} ₽',
                    )
                except Exception:
                    pass
        except Exception as error:
            logging.exception('FunPay auto-confirm handler failed: %s', error)
            await asyncio.sleep(2)


async def on_startup(_: Dispatcher):
    logging.info('Bot starting up...')
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info('Webhook deleted and pending updates dropped')
    except Exception as e:
        logging.warning(f'Failed to delete webhook: {e}')
    
    asyncio.create_task(restock_worker())
    asyncio.create_task(auto_confirm_funpay_worker())
    
    if GITHUB_BACKUP_ENABLED:
        asyncio.create_task(github_backup_worker())
        logging.info('GitHub backup worker started')


async def on_shutdown(_: Dispatcher):
    logging.info('Bot shutting down...')
    release_lock()


