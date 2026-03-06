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
        InlineKeyboardButton('📦 Категории', callback_data='menu:catalog'),
        InlineKeyboardButton('👤 Профиль', callback_data='menu:profile'),
    )
    kb.add(
        InlineKeyboardButton('📄 Пользовательское соглашение', callback_data='menu:agreement'),
        InlineKeyboardButton('💳 Пополнить баланс', callback_data='menu:topup'),
    )
    kb.add(
        InlineKeyboardButton('⭐ Отзывы', callback_data='menu:reviews'),
        InlineKeyboardButton('📣 Канал', callback_data='menu:channel'),
    )
    if is_admin(user_id):
        kb.add(InlineKeyboardButton('🛠 Админ панель', callback_data='menu:adminpanel'))
    return kb


def back_to_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup().add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))


def user_get_code_kb(order_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('🔐 Получить код для входа', callback_data=f'userreqcode:{order_id}'))
    kb.add(InlineKeyboardButton('⭐ Оставить отзыв', callback_data='menu:reviews'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
    return kb


def admin_get_code_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton('🔐 Получить код', callback_data=f'admsendcode:{order_id}')
    )


def profile_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('📊 Центр уведомлений', callback_data='profile:hub'))
    kb.add(InlineKeyboardButton('🧾 История покупок', callback_data='profile:orders'))
    kb.add(InlineKeyboardButton('💸 История пополнений', callback_data='profile:topups'))
    kb.add(InlineKeyboardButton('🎯 Ежедневный бонус', callback_data='profile:dailybonus'))
    kb.add(InlineKeyboardButton('🎁 Активировать промо', callback_data='profile:promo'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
    return kb


def build_profile_hub_text(user_id: int) -> str:
    balance = get_balance(user_id)
    bonus_left = get_daily_bonus_remaining_seconds(user_id, cooldown_hours=DAILY_BONUS_COOLDOWN_HOURS)
    if bonus_left <= 0:
        bonus_text = 'доступен сейчас ✅'
    else:
        hours_left = bonus_left // 3600
        minutes_left = (bonus_left % 3600) // 60
        bonus_text = f'через {hours_left} ч {minutes_left} мин'

    rows = list_user_orders(user_id, limit=3)
    if rows:
        last_orders = []
        for order_id, title, qty, total, status, _, _ in rows:
            safe_title = html.escape((title or 'Товар').strip())
            safe_status = html.escape(str(status or 'unknown'))
            last_orders.append(f'#{order_id} {safe_title} x{qty} | {total:.2f} ₽ | {safe_status}')
        orders_text = '\n'.join(last_orders)
    else:
        orders_text = 'Покупок пока нет.'

    return (
        '📊 Центр уведомлений\n\n'
        f'ID: <code>{user_id}</code>\n'
        f'Баланс: <b>{balance:.2f} ₽</b>\n'
        f'Кешбэк: <b>{PURCHASE_CASHBACK_PERCENT:.2f}%</b>\n'
        f'Ежедневный бонус: <b>{bonus_text}</b>\n\n'
        'Последние покупки:\n'
        f'{orders_text}'
    )


def catalog_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['proxy'], callback_data='cat:proxy'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['tg'], callback_data='cat:tg'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['email'], callback_data='cat:email'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
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
                f'{title} — {price:.0f} ₽ — {stock} шт.',
                callback_data=f'buy:{product_id}:{page}',
            )
        )

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('<< Предыдущая', callback_data=f'catpage:{category}:{page - 1}'))
    if end < total:
        nav_buttons.append(InlineKeyboardButton('Следующая >>', callback_data=f'catpage:{category}:{page + 1}'))
    if nav_buttons:
        kb.row(*nav_buttons)

    kb.add(InlineKeyboardButton('↩️ Назад', callback_data='menu:catalog'))
    return kb


def category_products_text(category: str, page: int = 0) -> str:
    products = list_products(category)
    total = len(products)
    start = page * PRODUCTS_PAGE_SIZE
    end = min(start + PRODUCTS_PAGE_SIZE, total)
    category_title = CATEGORY_NAMES.get(category, category)
    page_label = f'{start + 1}-{end}' if total > 0 else '0-0'
    return (
        '🛒 Купить товар\n'
        f'├ Категория: {category_title}\n'
        '└ Позиция: Не выбрана\n\n'
        f'📌 Выберите один из предложенных вариантов ({page_label} из {total}):'
    )


def quantity_kb(product_id: int, category: str, max_qty: int, page: int = 0) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=4)
    safe_max = min(max_qty, 10) if max_qty > 0 else 1
    for qty in range(1, safe_max + 1):
        kb.insert(InlineKeyboardButton(str(qty), callback_data=f'qty:{product_id}:{qty}'))
    kb.add(InlineKeyboardButton('✍️ Свое количество', callback_data=f'qtycustom:{product_id}:{page}'))
    kb.add(InlineKeyboardButton('↩️ Назад', callback_data=f'buy:{product_id}:{page}'))
    return kb


def product_card_text(title: str, category: str, price: float, stock: int, description: str) -> str:
    category_title = CATEGORY_NAMES.get(category, category)
    format_line = description if description else 'Логин:Пароль'
    return (
        '🛒 Купить товар\n'
        f'├ Категория: {category_title}\n'
        f'└ Позиция: {title}\n\n'
        f'├ Стоимость: {price:.2f} ₽\n'
        f'└ Количество: {stock} шт.\n'
        '────────────────────\n'
        f'Формат выдачи: {format_line}'
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
        return 'Данные отсутствуют. Обратитесь к администратору.'

    if len(items) >= qty:
        selected = items[:qty]
    else:
        selected = items + [items[-1]] * (qty - len(items))

    return '\n'.join(f'{index}. {value}' for index, value in enumerate(selected, start=1))


def topup_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('50₽', callback_data='topup:50'),
        InlineKeyboardButton('100₽', callback_data='topup:100'),
    )
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
    return kb


def admin_panel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('➕ Загрузить товар (мастер)', callback_data='admpanel:add_product'))
    kb.add(InlineKeyboardButton('💳 Пополнить баланс пользователя', callback_data='admpanel:add_balance'))
    kb.add(InlineKeyboardButton('✅ Подтвердить пополнение', callback_data='admpanel:confirm_topup'))
    kb.add(InlineKeyboardButton('🎁 Создать промокод', callback_data='admpanel:create_promo'))
    kb.add(InlineKeyboardButton('📨 Отправить код по заказу', callback_data='admpanel:send_code'))
    kb.add(InlineKeyboardButton('📥 Пополнить остаток', callback_data='admpanel:refill'))
    kb.add(InlineKeyboardButton('🧮 Установить остаток', callback_data='admpanel:set_stock'))
    kb.add(InlineKeyboardButton('🗑 Удалить товар', callback_data='admpanel:delete_product'))
    kb.add(InlineKeyboardButton('📋 Список товаров', callback_data='admpanel:list_products'))
    kb.add(InlineKeyboardButton('❌ Отмена действия', callback_data='admpanel:cancel_any'))
    kb.add(InlineKeyboardButton('🔙 Главное меню', callback_data='menu:main'))
    return kb


def admin_category_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['proxy'], callback_data='admaddcat:proxy'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['tg'], callback_data='admaddcat:tg'))
    kb.add(InlineKeyboardButton(CATEGORY_NAMES['email'], callback_data='admaddcat:email'))
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='admpanel:cancel_add'))
    return kb


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


async def notify_admins_about_tg_order(
    order_id: int,
    buyer_user_id: int,
    product_title: str,
    quantity: int,
    total: float,
    phone: Optional[str] = None,
) -> None:
    safe_title = html.escape(str(product_title or 'TG аккаунт'))
    phone_value = html.escape(str(phone).strip()) if phone else 'еще не указан'
    text = (
        '💸 Оплачен TG заказ\n'
        f'Номер заказа: <b>#{order_id}</b>\n'
        f'Номер аккаунта (ID): <code>{buyer_user_id}</code>\n'
        f'Товар: <b>{safe_title}</b>\n'
        f'Количество: <b>{int(quantity)}</b>\n'
        f'Сумма: <b>{float(total):.2f} ₽</b>\n'
        f'Номер для кода: <code>{phone_value}</code>'
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            pass


async def show_main_menu(user_id: int, text: str = 'Главное меню:') -> None:
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
        await cb.message.edit_text('Панель админа: выбери действие', reply_markup=admin_panel_kb())
    elif action == 'catalog':
        await cb.message.edit_text('Выберите категорию:', reply_markup=catalog_kb())
    elif action == 'profile':
        await safe_edit_text(cb.message, build_profile_hub_text(cb.from_user.id), reply_markup=profile_kb())
    elif action == 'agreement':
        await cb.message.edit_text(AGREEMENT_TEXT, reply_markup=back_to_main_kb())
    elif action == 'topup':
        await cb.message.edit_text(
            '⚠️ По вынужденным ситуациям пока доступен только такой способ пополнения.\n'
            'Пополнение через FunPay\n'
            'Доступные суммы: 50₽ и 100₽.',
            reply_markup=topup_kb(),
        )
    elif action == 'reviews':
        try:
            await cb.message.delete()
        except Exception:
            pass
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
        await bot.send_message(cb.from_user.id, 'Отзывы покупателей:', reply_markup=kb)
    elif action == 'channel':
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton('Открыть канал', url=CHANNEL_URL))
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='menu:main'))
        await cb.message.edit_text('Наш канал:', reply_markup=kb)

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
            text = 'История покупок пуста.'
        else:
            lines = ['🧾 История покупок:']
            for order_id, title, qty, total, status, code_value, created in rows:
                safe_title = html.escape(str(title or 'Товар'))
                safe_status = html.escape(str(status or 'unknown'))
                lines.append(f'#{order_id} | {safe_title} | x{qty} | {total:.2f}₽ | {safe_status} | {created}')
                if code_value and str(status).lower() == 'delivered':
                    safe_value = html.escape(str(code_value).strip())
                    if len(safe_value) > 800:
                        safe_value = safe_value[:800] + ' ...'
                    lines.append(f'Данные: <code>{safe_value}</code>')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'topups':
        rows = list_user_topups(cb.from_user.id, limit=15)
        if not rows:
            text = 'История пополнений пуста.'
        else:
            lines = ['💸 История пополнений:']
            for topup_id, amount, status, created in rows:
                lines.append(f'#{topup_id} | {amount:.2f}₽ | {status} | {created}')
            text = '\n'.join(lines)
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    elif action == 'promo':
        pending_promo_input.add(cb.from_user.id)
        await safe_edit_text(cb.message, 'Введите промокод одним сообщением:', reply_markup=back_to_main_kb())

    elif action == 'dailybonus':
        ok, credited, seconds_left = claim_daily_bonus(
            cb.from_user.id,
            DAILY_BONUS_AMOUNT,
            cooldown_hours=DAILY_BONUS_COOLDOWN_HOURS,
        )
        if ok:
            balance = get_balance(cb.from_user.id)
            text = (
                f'🎉 Ежедневный бонус получен: +{credited:.2f} ₽\n'
                f'Текущий баланс: {balance:.2f} ₽\n\n'
                f'Следующий бонус будет доступен через {DAILY_BONUS_COOLDOWN_HOURS} ч.'
            )
        else:
            hours_left = seconds_left // 3600
            minutes_left = (seconds_left % 3600) // 60
            text = (
                '⏳ Бонус уже получен.\n'
                f'Следующая попытка через: {hours_left} ч {minutes_left} мин.'
            )
        await safe_edit_text(cb.message, text, reply_markup=profile_kb())

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cat:'))
async def category_router(cb: types.CallbackQuery):
    category = cb.data.split(':', 1)[1]
    if category not in CATEGORY_NAMES:
        await cb.answer('Категория не найдена', show_alert=True)
        return

    products = list_products(category)
    if not products:
        await cb.message.edit_text(
            f'В категории {CATEGORY_NAMES[category]} пока нет товаров в наличии.',
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
            f'В категории {CATEGORY_NAMES.get(category, category)} пока нет товаров в наличии.',
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
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, description, price, _, category, stock, *_ = product
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    text = product_card_text(title, category, float(price), int(stock), description)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton('💰 Купить товар', callback_data=f'qtymenu:{product_id}:{page}'))
    kb.add(InlineKeyboardButton('↩️ Назад', callback_data=f'catpage:{category}:{page}'))
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
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, _, price, _, category, stock, *_ = product
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    text = (
        f'🧾 Покупка: <b>{title}</b>\n'
        f'Цена: <b>{float(price):.2f} ₽</b>\n'
        f'Доступно: <b>{int(stock)} шт.</b>\n\n'
        'Выберите количество:'
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
        if not try_spend_balance(user_id, total):
            await cb.answer('Недостаточно баланса', show_alert=True)
            await cb.message.edit_text(
                f'Недостаточно средств для покупки <b>{title}</b>.\nНужно: {total:.2f} ₽',
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
            cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
            await cb.message.edit_text(
                f'✅ Оплата принята. Заказ #{order_id} создан.\n'
                'Отправьте номер для входа в аккаунт.\n'
                'После этого нажмите кнопку «Получить код для входа».\n'
                f'{cashback_text}'
                f'Остаток баланса: {balance:.2f} ₽',
                reply_markup=user_get_code_kb(order_id),
            )
            await cb.answer('Ожидаю номер')
            return

        order_id = create_order(user_id, product_id, qty, total, 'delivered')
        delivery_data = format_delivery_credentials(credentials, qty)
        set_order_code(order_id, delivery_data)
        cashback = calculate_cashback(total)
        if cashback > 0:
            change_balance(user_id, cashback)
        balance = get_balance(user_id)
        cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
        deliver_text = (
            f'✅ Заказ #{order_id} успешно оплачен с баланса.\n'
            f'Товар: <b>{title}</b>\n'
            f'Количество: {qty}\n'
            f'Сумма: {total:.2f} ₽\n'
            f'{cashback_text}'
            f'Остаток баланса: {balance:.2f} ₽\n\n'
            f'Данные:\n<code>{delivery_data}</code>'
        )
        await cb.message.edit_text(deliver_text, reply_markup=back_to_main_kb())
        await cb.answer('Покупка успешна')
    finally:
        active_buy_users.discard(user_id)


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('qtycustom:'))
async def qty_custom_router(cb: types.CallbackQuery):
    _, product_id_str, page_str = cb.data.split(':')
    product_id = int(product_id_str)
    page = int(page_str)
    product = get_product(product_id)
    if not product:
        await cb.answer('Товар не найден', show_alert=True)
        return

    _, title, _, _, _, _, stock, *_ = product
    if stock <= 0:
        await cb.answer('Нет в наличии', show_alert=True)
        return

    pending_custom_qty_input[cb.from_user.id] = {'product_id': product_id, 'page': page}
    await cb.message.edit_text(
        f'Введите количество для <b>{title}</b> (от 1 до 10).\n'
        f'Сейчас доступно: <b>{int(stock)} шт.</b>',
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton('↩️ Назад', callback_data=f'qtymenu:{product_id}:{page}')
        ),
    )
    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('topup:'))
async def topup_router(cb: types.CallbackQuery):
    action = cb.data.split(':', 1)[1]

    if action == 'custom':
        pending_custom_topup.add(cb.from_user.id)
        await cb.message.edit_text('Введите сумму пополнения числом (например 750):', reply_markup=back_to_main_kb())
        await cb.answer()
        return

    amount = float(action)
    topup_id = create_topup(cb.from_user.id, amount, '')
    payment_link, payment_id = create_funpay_payment(amount, cb.from_user.id, topup_id)
    set_topup_payment_data(topup_id, payment_link, payment_id)
    if payment_id:
        set_topup_external_status(topup_id, 'created')

    await cb.message.edit_text(
        f'Заявка на пополнение #{topup_id} создана на {amount:.2f} ₽\n'
        f'1) Оплатите товар по ссылке: {payment_link}\n'
        f'2) В комментарии к заказу укажите код: <code>topup_{topup_id}_{cb.from_user.id}</code>\n'
        f'3) Сумма оплаты должна быть: {amount:.2f} ₽\n\n'
        'После оплаты баланс подтвердится автоматически (или админом, если API недоступен).',
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
        await cb.message.edit_text('Выбери категорию для нового товара:', reply_markup=admin_category_kb())
    elif action == 'home':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await cb.message.edit_text('Панель админа: выбери действие', reply_markup=admin_panel_kb())
    elif action == 'cancel_add':
        admin_add_product_state.pop(user_id, None)
        await cb.message.edit_text('Создание товара отменено.', reply_markup=admin_panel_kb())
    elif action == 'cancel_any':
        admin_add_product_state.pop(user_id, None)
        admin_action_state.pop(user_id, None)
        await cb.message.edit_text('Текущее действие отменено.', reply_markup=admin_panel_kb())
    elif action == 'add_balance':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'add_balance', 'step': 'user_id'}
        await cb.message.edit_text('Введи ID пользователя для пополнения баланса:', reply_markup=admin_step_kb())
    elif action == 'confirm_topup':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'confirm_topup', 'step': 'topup_id'}
        await cb.message.edit_text('Введи ID пополнения (topup_id):', reply_markup=admin_step_kb())
    elif action == 'create_promo':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'create_promo', 'step': 'code'}
        await cb.message.edit_text('Введи код промокода (например: BONUS50):', reply_markup=admin_step_kb())
    elif action == 'send_code':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'send_code', 'step': 'order_id'}
        await cb.message.edit_text('Введи ID заказа:', reply_markup=admin_step_kb())
    elif action == 'refill':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'refill', 'step': 'product_id'}
        await cb.message.edit_text('Введи product_id для пополнения остатка:', reply_markup=admin_step_kb())
    elif action == 'set_stock':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'set_stock', 'step': 'product_id'}
        await cb.message.edit_text('Введи product_id для установки остатка:', reply_markup=admin_step_kb())
    elif action == 'delete_product':
        admin_add_product_state.pop(user_id, None)
        admin_action_state[user_id] = {'action': 'delete_product', 'step': 'product_id'}
        await cb.message.edit_text('Введи product_id для удаления товара из каталога:', reply_markup=admin_step_kb())
    elif action == 'list_products':
        rows = list_all_products_admin()
        if not rows:
            await cb.message.edit_text('Товаров пока нет.', reply_markup=admin_panel_kb())
        else:
            lines = ['📋 Товары:']
            for pid, title, category, price, stock, auto_restock in rows:
                auto_text = 'auto' if int(auto_restock) == 1 else 'manual'
                lines.append(f'#{pid} | {title} | {category} | {float(price):.2f}₽ | stock={int(stock)} | {auto_text}')
            await cb.message.edit_text('\n'.join(lines), reply_markup=admin_panel_kb())

    await cb.answer()


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('admsendcode:'))
async def admin_sendcode_quick_router(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer('Только админ', show_alert=True)
        return

    try:
        order_id = int(cb.data.split(':', 1)[1])
    except Exception:
        await cb.answer('Неверный order_id', show_alert=True)
        return

    order = get_order(order_id)
    if not order:
        await cb.answer('Заказ не найден', show_alert=True)
        return

    _, _, _, _, _, status, _, _, _ = order
    if status not in ('waiting_code', 'waiting_phone'):
        await cb.answer('Заказ не ожидает код', show_alert=True)
        return

    admin_action_state[cb.from_user.id] = {
        'action': 'send_code',
        'step': 'code',
        'order_id': str(order_id),
    }
    await cb.message.answer(
        f'Заказ #{order_id} выбран. Введите код следующим сообщением.',
        reply_markup=admin_step_kb(),
    )
    await cb.answer('Ожидаю код')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('userreqcode:'))
async def user_request_code_router(cb: types.CallbackQuery):
    try:
        order_id = int(cb.data.split(':', 1)[1])
    except Exception:
        await cb.answer('Неверный номер заказа', show_alert=True)
        return

    order = get_order(order_id)
    if not order:
        await cb.answer('Заказ не найден', show_alert=True)
        return

    _, buyer_user_id, product_id, qty, total, status, client_phone, _, _ = order
    if int(buyer_user_id) != int(cb.from_user.id):
        await cb.answer('Это не ваш заказ', show_alert=True)
        return

    if status == 'waiting_phone':
        await cb.answer('Сначала отправьте номер для входа в аккаунт', show_alert=True)
        return

    if status == 'delivered':
        await cb.answer('Код уже выдан по этому заказу', show_alert=True)
        return

    product = get_product(int(product_id))
    product_title = html.escape(str(product[1] if product else 'TG аккаунт'))
    product_data = ''
    if product:
        product_data = format_delivery_credentials(str(product[4] or ''), int(qty))
    safe_product_data = html.escape(product_data) if product_data else 'не указаны'
    phone_value = html.escape(str(client_phone).strip()) if client_phone else 'не указан'

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                '📩 Запрос кода от покупателя\n'
                f'Заказ: <b>#{order_id}</b>\n'
                f'Пользователь: <code>{buyer_user_id}</code>\n'
                f'Товар: <b>{product_title}</b>\n'
                f'Количество: <b>{int(qty)}</b>\n'
                f'Сумма: <b>{float(total):.2f} ₽</b>\n'
                f'Данные товара: <code>{safe_product_data}</code>\n'
                f'Номер: <code>{phone_value}</code>\n\n'
                f'Отправить код: /sendcode {order_id} 12345',
                reply_markup=admin_get_code_kb(order_id),
            )
        except Exception:
            pass

    await cb.answer('Запрос отправлен администратору')


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

    state['category'] = category
    state['step'] = 'title'
    await cb.message.edit_text('Введи название товара (например: 🇩🇪 Германия 3 дня):')
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
    await cb.message.edit_text(
        'Проверь данные товара перед сохранением:\n\n'
        f'Категория: {CATEGORY_NAMES.get(state["category"], state["category"])}\n'
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
    auto_text = 'включено (+1 к складу каждые 30 секунд)' if int(state.get('auto_restock', '0')) == 1 else 'выключено'
    admin_add_product_state.pop(user_id, None)
    await cb.message.edit_text(
        f'✅ Товар добавлен, id={product_id}\n'
        f'Категория: {CATEGORY_NAMES.get(state["category"], state["category"])}\n'
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
            if not try_spend_balance(user_id, total):
                await message.answer(
                    f'Недостаточно средств для покупки <b>{title}</b>.\nНужно: {total:.2f} ₽',
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
                cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
                await message.answer(
                    f'✅ Оплата принята. Заказ #{order_id} создан.\n'
                    'Отправьте номер для входа в аккаунт.\n'
                    'После этого нажмите кнопку «Получить код для входа».\n'
                    f'{cashback_text}'
                    f'Остаток баланса: {balance:.2f} ₽',
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
            cashback_text = f'Кешбэк: +{cashback:.2f} ₽\n' if cashback > 0 else ''
            await message.answer(
                f'✅ Заказ #{order_id} успешно оплачен с баланса.\n'
                f'Товар: <b>{title}</b>\n'
                f'Количество: {qty}\n'
                f'Сумма: {total:.2f} ₽\n'
                f'{cashback_text}'
                f'Остаток баланса: {balance:.2f} ₽\n\n'
                f'Данные:\n<code>{delivery_data}</code>',
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
                admin_action_state.pop(user_id, None)
                await message.answer('✅ Код отправлен клиенту.', reply_markup=admin_panel_kb())
                await bot.send_message(target_user_id, f'Код для заказа #{order_id}: <code>{code}</code>')
                return

        if action in ('refill', 'set_stock'):
            if step == 'product_id':
                try:
                    product_id = int(text)
                except ValueError:
                    await message.answer('product_id должен быть числом. Введи снова:')
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
                else:
                    set_stock(product_id, qty)
                    message_text = f'✅ Остаток товара #{product_id} установлен: {qty}.'
                admin_action_state.pop(user_id, None)
                await message.answer(message_text, reply_markup=admin_panel_kb())
                return

        if action == 'delete_product' and step == 'product_id':
            try:
                product_id = int(text)
            except ValueError:
                await message.answer('product_id должен быть числом. Введи снова:')
                return

            deleted = deactivate_product(product_id)
            admin_action_state.pop(user_id, None)
            if deleted:
                await message.answer(f'✅ Товар #{product_id} удален из каталога.', reply_markup=admin_panel_kb())
            else:
                await message.answer('Товар не найден или уже удален.', reply_markup=admin_panel_kb())
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
        topup_id = create_topup(user_id, amount, '')
        payment_link, payment_id = create_funpay_payment(amount, user_id, topup_id)
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
        safe_product_data = html.escape(product_data) if product_data else 'не указаны'

        await message.answer(
            f'Номер сохранён для заказа #{order_id}.\n'
            f'Данные товара для входа: <code>{safe_product_data}</code>\n'
            'Нажмите кнопку «Получить код для входа», чтобы отправить запрос администратору.',
            reply_markup=user_get_code_kb(order_id),
        )
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
        '/addbalance <user_id> <amount>'
    )


@dp.message_handler(commands=['adminpanel'])
async def cmd_adminpanel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply('Только админ')
        return
    admin_add_product_state.pop(message.from_user.id, None)
    admin_action_state.pop(message.from_user.id, None)
    await message.reply('Панель админа: выбери действие', reply_markup=admin_panel_kb())


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

    if category not in CATEGORY_NAMES:
        await message.reply('Категории: proxy, tg, email')
        return

    product_id = add_product(title, price, credentials, category, description, stock, auto_restock=0)
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
    await message.reply('Код отправлен клиенту')
    await bot.send_message(user_id, f'Код для заказа #{order_id}: <code>{code}</code>')


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
    await message.reply(f'Баланс пользователя {user_id} изменен на {amount:.2f} ₽. Текущий: {bal:.2f} ₽')


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
    asyncio.create_task(restock_worker())
    asyncio.create_task(auto_confirm_funpay_worker())
    asyncio.create_task(github_backup_worker())


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
