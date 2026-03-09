import http.server
import http.server
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from db import (
    add_product,
    create_category,
    append_product_credentials_with_stock,
    list_all_products_admin,
    deactivate_product,
)

import sys
BASE_DIR = Path(os.getcwd())
INDEX_PATH = BASE_DIR / "index.html"
HOST = os.getenv("WEB_HOST", "127.0.0.1")
PORT = int(os.getenv("WEB_PORT", "8080"))

class ShopHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write("%s - - [%s] %s\n" % (self.client_address[0], self.log_date_time_string(), format%args))

    def _json_response(self, data, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def do_GET(self):
        if self.path == '/api/logs':
            from db import list_admin_logs
            logs = list_admin_logs(50)
            self._json_response({'ok': True, 'logs': logs})
            return
        if self.path == '/api/profile':
            self._json_response({'ok': True, 'id': 1, 'login': 'admin', 'role': 'superadmin'})
            return
        if self.path == '/api/stats':
            from db import list_all_products_admin, list_users_page, list_orders_page
            products = list_all_products_admin()
            users, user_count = list_users_page()
            orders, order_count = list_orders_page()
            self._json_response({
                'ok': True,
                'products_count': len(products),
                'users_count': user_count,
                'orders_count': order_count
            })
            return
        if self.path == '/api/users':
            from db import list_users_page
            users, user_count = list_users_page()
            self._json_response({'ok': True, 'users': users, 'count': user_count})
            return
        if self.path == '/' or self.path.startswith('/index.html'):
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            with open(INDEX_PATH, 'r', encoding='utf-8') as f:
                self.wfile.write(f.read().encode('utf-8'))
            return
        elif self.path.startswith('/api/admin_products'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            products = list_all_products_admin()
            result = {'ok': True, 'products': [
                {'id': p[0], 'title': p[1], 'category': p[2], 'price': p[3], 'stock': p[4]} for p in products
            ]}
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))
            return
        # Serve static files (js, css, images, etc.) from BASE_DIR and subfolders
        else:
            rel_path = self.path.lstrip('/')
            static_path = (BASE_DIR / rel_path).resolve()
            # Логируем путь для диагностики
            print(f"[STATIC] Запрошен: {self.path} → {static_path}")
            if not str(static_path).startswith(str(BASE_DIR)):
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b'Forbidden')
                print(f"[STATIC] Отклонено (выход за BASE_DIR): {static_path}")
                return
            if static_path.exists() and static_path.is_file():
                if static_path.suffix == '.js':
                    content_type = 'application/javascript; charset=utf-8'
                elif static_path.suffix == '.css':
                    content_type = 'text/css; charset=utf-8'
                elif static_path.suffix in ['.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg']:
                    if static_path.suffix == '.svg':
                        content_type = 'image/svg+xml'
                    else:
                        content_type = f'image/{static_path.suffix[1:]}'
                else:
                    content_type = 'application/octet-stream'
                self.send_response(200)
                self.send_header('Content-type', content_type)
                self.end_headers()
                with open(static_path, 'rb') as f:
                    self.wfile.write(f.read())
                print(f"[STATIC] Отдан: {static_path}")
                return
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
            print(f"[STATIC] Не найден: {static_path}")

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length).decode('utf-8') if length else ''
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}

        if self.path == '/api/broadcast':
            msg = data.get('message', '').strip()
            if not msg:
                self._json_response({'ok': False, 'error': 'Пустое сообщение'})
                return
            try:
                import asyncio
                from bot import broadcast_message_to_all_users
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Если сервер уже в асинхронном режиме (редко)
                    fut = asyncio.ensure_future(broadcast_message_to_all_users(msg))
                    sent = 0
                else:
                    sent = loop.run_until_complete(broadcast_message_to_all_users(msg))
                self._json_response({'ok': True, 'sent': sent})
            except Exception as e:
                self._json_response({'ok': False, 'error': str(e)})
            return

        if self.path.startswith('/api/admin_add_product'):
            title = data.get('title', '').strip()
            category = data.get('category', '').strip()
            price = float(data.get('price', 0))
            stock = int(data.get('stock', 0))
            credentials = data.get('credentials', '').strip()
            description = data.get('description', '').strip()
            product_id = add_product(title, price, credentials, category, description, stock)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': True, 'product_id': product_id}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_add_creds'):
            product_id = int(data.get('product_id', 0))
            credentials = data.get('credentials', '').strip()
            result = append_product_credentials_with_stock(product_id, credentials)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': bool(result)}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_deactivate_product'):
            product_id = int(data.get('product_id', 0))
            ok = deactivate_product(product_id)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': ok}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_add_category'):
            slug = data.get('slug', '').strip()
            title = data.get('title', '').strip()
            ok, msg = create_category(slug, title)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': ok, 'msg': msg}, ensure_ascii=False).encode('utf-8'))
            return
    def do_GET(self):
        if self.path == '/' or self.path.startswith('/index.html'):
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            with open(INDEX_PATH, 'r', encoding='utf-8') as f:
                self.wfile.write(f.read().encode('utf-8'))
            return
        elif self.path.startswith('/api/admin_products'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            products = list_all_products_admin()
            result = {'ok': True, 'products': [
                {'id': p[0], 'title': p[1], 'category': p[2], 'price': p[3], 'stock': p[4]} for p in products
            ]}
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))
            return
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length).decode('utf-8') if length else ''
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}

        if self.path.startswith('/api/admin_add_product'):
            title = data.get('title', '').strip()
            category = data.get('category', '').strip()
            price = float(data.get('price', 0))
            stock = int(data.get('stock', 0))
            credentials = data.get('credentials', '').strip()
            description = data.get('description', '').strip()
            product_id = add_product(title, price, credentials, category, description, stock)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': True, 'product_id': product_id}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_add_creds'):
            product_id = int(data.get('product_id', 0))
            credentials = data.get('credentials', '').strip()
            result = append_product_credentials_with_stock(product_id, credentials)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': bool(result)}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_deactivate_product'):
            product_id = int(data.get('product_id', 0))
            ok = deactivate_product(product_id)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': ok}, ensure_ascii=False).encode('utf-8'))
            return

        if self.path.startswith('/api/admin_add_category'):
            slug = data.get('slug', '').strip()
            title = data.get('title', '').strip()
            ok, msg = create_category(slug, title)
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': ok, 'msg': msg}, ensure_ascii=False).encode('utf-8'))
            return

if __name__ == '__main__':
    server = ThreadingHTTPServer((HOST, PORT), ShopHandler)
    print(f'Server running at http://{HOST}:{PORT}/')
    server.serve_forever()
import json
import logging
import os
import secrets
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import parse_qs, urlparse
from urllib.parse import quote
from urllib.request import Request, urlopen

from db import (
    activate_promo,
    add_product,
    create_category,
    create_promo,
    change_balance,
    append_product_credentials_with_stock,
    confirm_topup,
    consume_product_credentials,
    create_order,
    create_topup,
    get_balance,
    get_product,
    init_db,
    list_categories,
    list_categories_admin,
    list_admin_logs,
    list_all_products_admin,
    list_orders_page,
    list_pending_topups_for_auto,
    list_promo_codes,
    list_users_page,
    list_web_products,
    log_admin_action,
    set_product_active,
    set_order_code,
    set_stock,
    try_spend_balance,
    update_category,
    update_product_fields,
    update_stock,
    deactivate_product,
)

def load_local_env_file(file_path: str = ".env") -> None:
    if not os.path.exists(file_path):
        return
    try:
        with open(file_path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, value)
    except Exception as error:
        logging.warning("Failed to read %s: %s", file_path, error)

load_local_env_file(".env")
load_local_env_file(".env.example")

BASE_DIR = Path(__file__).parent
INDEX_PATH = BASE_DIR / "index.html"
HOST = os.getenv("WEB_HOST", "127.0.0.1")
PORT = int(os.getenv("WEB_PORT", "8080"))
PURCHASE_CASHBACK_PERCENT = float(os.getenv("PURCHASE_CASHBACK_PERCENT", "2.0"))
MAX_QTY_PER_ORDER = 10
SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@your_support_user").strip() or "@your_support_user"
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
FUNPAY_PAYMENT_URL = os.getenv("FUNPAY_PAYMENT_URL", "https://funpay.com/").strip() or "https://funpay.com/"
FUNPAY_TOPUP_LOT_URL = os.getenv("FUNPAY_TOPUP_LOT_URL", "").strip()
ADMIN_LOGIN = os.getenv("WEB_ADMIN_LOGIN", "Dize").strip()
ADMIN_PASSWORD = os.getenv("WEB_ADMIN_PASSWORD", "gorzfmyoX!5516").strip()
ADMIN_TOKEN_TTL_SECONDS = max(600, int(os.getenv("WEB_ADMIN_TOKEN_TTL_SECONDS", "7200")))
ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "8594771951,8466199706").split(",")
    if value.strip().isdigit()
}
ADMIN_TOKENS: Dict[str, Dict[str, Any]] = {}
AGREEMENT_TEXT = (
    "Пользовательское соглашение\n\n"
    "1) Общие положения\n"
    "1.1. Используя магазин, пользователь подтверждает, что ознакомился с условиями и полностью их принимает.\n"
    "1.2. Все товары и услуги являются цифровыми и предоставляются в электронном виде.\n"
    "1.3. Факт оплаты товара или пополнения означает акцепт соглашения.\n\n"
    "2) Личная ответственность пользователя\n"
    "2.1. Пользователь несет ответственность за выбор товара, ввод реквизитов и комментариев.\n"
    "2.2. Пользователь самостоятельно отвечает за использование полученных данных.\n"
    "2.3. Пользователь обязан соблюдать правила сторонних сервисов и законодательство.\n"
    "2.4. Риски блокировок и ограничений со стороны сторонних платформ пользователь принимает на себя.\n"
    "2.5. Передача данных третьим лицам и утрата доступа по вине пользователя не является ответственностью магазина.\n\n"
    "3) Оплата, пополнение и баланс\n"
    "3.1. Баланс является внутренней единицей и используется только внутри сервиса.\n"
    "3.2. Пополнение засчитывается после подтверждения системой или админом.\n"
    "3.3. При оплате пользователь обязан указывать корректные данные платежа.\n"
    "3.4. Ошибки в сумме или реквизитах могут привести к задержке или отказу в зачислении.\n"
    "3.5. Администрация может запросить подтверждение оплаты в спорных случаях.\n\n"
    "4) Выдача цифрового товара\n"
    "4.1. Товар считается выданным с момента отображения данных в заказе.\n"
    "4.2. Пользователь обязан сразу проверить полученные данные.\n"
    "4.3. Претензии принимаются при объективных подтверждениях и в разумный срок.\n\n"
    "5) Возвраты и отмены\n"
    "5.1. Возврат возможен, если товар не был выдан по вине магазина или подтверждена техническая ошибка.\n"
    "5.2. Если товар выдан, но не подошел по личным причинам, возврат не производится.\n"
    "5.3. Возврат не производится при нарушении инструкции использования товара.\n\n"
    "6) Ограничения и право отказа\n"
    "6.1. Администрация вправе отказать в обслуживании при подозрении на мошенничество, взлом или спам.\n"
    "6.2. Администрация вправе ограничить функциональность сервиса при технических работах и форс-мажорах.\n\n"
    "7) Заключительные положения\n"
    "7.1. Условия соглашения могут быть изменены без персонального уведомления.\n"
    "7.2. Актуальная редакция публикуется в сервисе и действует с момента публикации.\n"
    "7.3. Продолжение использования сервиса означает согласие с обновленными условиями."
)

def calculate_cashback(total_amount: float) -> float:
    if PURCHASE_CASHBACK_PERCENT <= 0:
        return 0.0
    cashback = round(float(total_amount) * PURCHASE_CASHBACK_PERCENT / 100.0, 2)
    return cashback if cashback > 0 else 0.0

def split_credentials_items(credentials: str) -> list[str]:
    items: list[str] = []
    for raw_line in (credentials or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "|" in line:
            items.extend(chunk.strip() for chunk in line.split("|") if chunk.strip())
        else:
            items.append(line)
    return items

def make_error(message: str, code: int = 400) -> Tuple[int, Dict[str, Any]]:
    return code, {"ok": False, "error": message}

def _clean_expired_admin_tokens() -> None:
    now = int(time.time())
    expired_tokens = [token for token, payload in ADMIN_TOKENS.items() if int(payload.get("exp", 0)) <= now]
    for token in expired_tokens:
        ADMIN_TOKENS.pop(token, None)

def _create_admin_token(user_id: int, login: str) -> str:
    _clean_expired_admin_tokens()
    token = secrets.token_urlsafe(32)
    ADMIN_TOKENS[token] = {
        "user_id": int(user_id),
        "login": str(login),
        "exp": int(time.time()) + ADMIN_TOKEN_TTL_SECONDS,
    }
    return token

def _extract_admin_token(headers: Any) -> str:
    auth = str(headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""

def _require_admin(headers: Any) -> Tuple[bool, Dict[str, Any]]:
    _clean_expired_admin_tokens()
    token = _extract_admin_token(headers)
    if not token:
        return False, {"ok": False, "error": "admin token is required"}
    payload = ADMIN_TOKENS.get(token)
    if not payload:
        return False, {"ok": False, "error": "invalid or expired admin token"}
    return True, payload
# ...existing code...
