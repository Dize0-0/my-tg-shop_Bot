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
    "3.2. Пополнение засчитывается после подтверждения системой или администратором.\n"
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


def _build_funpay_payment_link(amount: float, user_id: int, topup_id: int) -> str:
    marker = f"topup_{topup_id}_{user_id}"
    base = FUNPAY_TOPUP_LOT_URL if FUNPAY_TOPUP_LOT_URL.startswith("http") else FUNPAY_PAYMENT_URL
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}amount={quote(f'{amount:.2f}')}&comment={quote(marker)}"


def _send_tg_message(chat_id: int, text: str) -> bool:
    if not TG_BOT_TOKEN:
        return False
    try:
        payload = json.dumps({"chat_id": int(chat_id), "text": text, "parse_mode": "HTML"}).encode("utf-8")
        req = Request(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=10) as resp:
            return int(getattr(resp, "status", 200)) < 300
    except Exception:
        return False


def _notify_admins_new_topup(user_id: int, amount: float, topup_id: int, payment_link: str) -> None:
    if not ADMIN_IDS:
        return
    message = (
        "<b>Новая заявка на пополнение</b>\n"
        f"ID заявки: <code>{topup_id}</code>\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Сумма: <b>{amount:.2f} RUB</b>\n"
        f"Маркер оплаты: <code>topup_{topup_id}_{user_id}</code>\n"
        f"FunPay: {payment_link}"
    )
    for admin_id in ADMIN_IDS:
        _send_tg_message(int(admin_id), message)


def handle_buy(payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    try:
        user_id = int(payload.get("user_id", 0))
        product_id = int(payload.get("product_id", 0))
        qty = int(payload.get("qty", 1))
    except Exception:
        return make_error("invalid input")

    if user_id <= 0 or product_id <= 0:
        return make_error("user_id and product_id are required")

    if qty <= 0 or qty > MAX_QTY_PER_ORDER:
        return make_error(f"qty must be between 1 and {MAX_QTY_PER_ORDER}")

    product = get_product(product_id)
    if not product:
        return make_error("product not found", 404)

    _, title, _, price, credentials, category, stock, *_ = product
    if qty > int(stock):
        return make_error("not enough stock")

    total = float(price) * qty

    if category != "tg":
        available_items = len(split_credentials_items(str(credentials or "")))
        if available_items < qty:
            return make_error("delivery data exhausted, contact admin")

    if not try_spend_balance(user_id, total):
        return make_error("insufficient balance")

    if category == "tg":
        update_stock(product_id, -qty)
        order_id = create_order(user_id, product_id, qty, total, "waiting_phone")
        cashback = calculate_cashback(total)
        if cashback > 0:
            change_balance(user_id, cashback)
        balance = get_balance(user_id)
        return 200, {
            "ok": True,
            "order_id": order_id,
            "status": "waiting_phone",
            "title": title,
            "total": round(total, 2),
            "cashback": cashback,
            "balance": round(balance, 2),
            "message": "order accepted, send your phone in Telegram bot",
        }

    delivery_data, _ = consume_product_credentials(product_id, qty)
    if not delivery_data:
        change_balance(user_id, total)
        return make_error("stock changed during checkout, funds returned", 409)

    order_id = create_order(user_id, product_id, qty, total, "delivered")
    set_order_code(order_id, delivery_data)
    cashback = calculate_cashback(total)
    if cashback > 0:
        change_balance(user_id, cashback)

    balance = get_balance(user_id)
    return 200, {
        "ok": True,
        "order_id": order_id,
        "status": "delivered",
        "title": title,
        "total": round(total, 2),
        "cashback": cashback,
        "balance": round(balance, 2),
        "delivery_data": delivery_data,
    }


class ShopHandler(BaseHTTPRequestHandler):
    server_version = "LuneShopHTTP/1.0"

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(404)
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path

        if route == "/":
            return self._send_file(INDEX_PATH, "text/html; charset=utf-8")

        if route == "/api/products":
            rows = list_web_products(None)
            products = []
            for product_id, title, description, price, stock, category in rows:
                products.append(
                    {
                        "id": int(product_id),
                        "title": str(title or ""),
                        "description": str(description or ""),
                        "price": float(price),
                        "stock": int(stock),
                        "category": str(category or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "products": products})

        if route == "/api/categories":
            rows = list_categories_admin(active_only=True)
            if rows:
                items = [
                    {
                        "slug": str(slug or ""),
                        "title": str(title or str(slug or "").upper()),
                    }
                    for _, slug, title, _, _ in rows
                ]
            else:
                slugs = list_categories()
                items = [{"slug": str(slug), "title": str(slug).upper()} for slug in slugs]
            return self._send_json(200, {"ok": True, "categories": items})

        if route == "/api/profile":
            query = parse_qs(parsed.query)
            try:
                user_id = int((query.get("user_id") or ["0"])[0])
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid user_id"})
            if user_id <= 0:
                return self._send_json(400, {"ok": False, "error": "user_id is required"})
            return self._send_json(
                200,
                {
                    "ok": True,
                    "user_id": user_id,
                    "balance": round(get_balance(user_id), 2),
                    "cashback_percent": PURCHASE_CASHBACK_PERCENT,
                },
            )

        if route == "/api/meta":
            return self._send_json(
                200,
                {
                    "ok": True,
                    "shop_name": "Lune Shop",
                    "support_contact": SUPPORT_CONTACT,
                    "funpay_payment_url": FUNPAY_PAYMENT_URL,
                    "agreement_text": AGREEMENT_TEXT,
                },
            )

        if route == "/api/admin/products":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            rows = list_all_products_admin(admin_id=int(info["user_id"]))
            products = []
            for product_id, title, category, price, stock, auto_restock in rows:
                product = get_product(int(product_id))
                description = product[2] if product else ""
                products.append(
                    {
                        "id": int(product_id),
                        "title": str(title or ""),
                        "description": str(description or ""),
                        "category": str(category or ""),
                        "price": float(price),
                        "stock": int(stock),
                        "auto_restock": int(auto_restock),
                    }
                )
            return self._send_json(200, {"ok": True, "products": products})

        if route == "/api/admin/categories":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            rows = list_categories_admin(active_only=False)
            categories = []
            for category_id, slug, title, is_active, created_at in rows:
                categories.append(
                    {
                        "id": int(category_id),
                        "slug": str(slug or ""),
                        "title": str(title or ""),
                        "is_active": int(is_active),
                        "created_at": str(created_at or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "categories": categories})

        if route == "/api/admin/topup-requests":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            rows = list_pending_topups_for_auto(limit=200)
            items = []
            for topup_id, user_id, amount, external_payment_id in rows:
                items.append(
                    {
                        "topup_id": int(topup_id),
                        "user_id": int(user_id),
                        "amount": float(amount),
                        "external_payment_id": str(external_payment_id or ""),
                        "marker": f"topup_{int(topup_id)}_{int(user_id)}",
                    }
                )
            return self._send_json(200, {"ok": True, "items": items})

        if route == "/api/admin/logs":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            rows = list_admin_logs(limit=150)
            logs = []
            for log_id, admin_id, action, details, created_at in rows:
                logs.append(
                    {
                        "id": int(log_id),
                        "admin_id": int(admin_id),
                        "action": str(action or ""),
                        "details": str(details or ""),
                        "created_at": str(created_at or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "logs": logs})

        if route == "/api/admin/users":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            query = parse_qs(parsed.query)
            page = int((query.get("page") or ["0"])[0]) if (query.get("page") or [""])[0].isdigit() else 0
            page_size_raw = (query.get("page_size") or ["50"])[0]
            page_size = int(page_size_raw) if str(page_size_raw).isdigit() else 50

            rows, total_count = list_users_page(page=page, page_size=page_size)
            users = []
            for user_id, balance, created_at in rows:
                users.append(
                    {
                        "user_id": int(user_id),
                        "balance": float(balance),
                        "created_at": str(created_at or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "users": users, "total_count": int(total_count)})

        if route == "/api/admin/orders":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            query = parse_qs(parsed.query)
            page = int((query.get("page") or ["0"])[0]) if (query.get("page") or [""])[0].isdigit() else 0
            page_size_raw = (query.get("page_size") or ["50"])[0]
            page_size = int(page_size_raw) if str(page_size_raw).isdigit() else 50

            rows, total_count = list_orders_page(page=page, page_size=page_size)
            orders = []
            for order_id, user_id, product_id, title, quantity, total_price, status, created_at in rows:
                orders.append(
                    {
                        "order_id": int(order_id),
                        "user_id": int(user_id),
                        "product_id": int(product_id),
                        "title": str(title or ""),
                        "quantity": int(quantity),
                        "total_price": float(total_price),
                        "status": str(status or ""),
                        "created_at": str(created_at or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "orders": orders, "total_count": int(total_count)})

        if route == "/api/admin/promos":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            rows = list_promo_codes(limit=200)
            promos = []
            for code, amount, uses_left, is_active, created_at in rows:
                promos.append(
                    {
                        "code": str(code or ""),
                        "amount": float(amount),
                        "uses_left": int(uses_left),
                        "is_active": int(is_active),
                        "created_at": str(created_at or ""),
                    }
                )
            return self._send_json(200, {"ok": True, "promos": promos})

        self.send_error(404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path

        content_len = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(content_len) if content_len > 0 else b"{}"

        try:
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            return self._send_json(400, {"ok": False, "error": "invalid json"})

        if route == "/api/topup":
            try:
                user_id = int(payload.get("user_id", 0))
                amount = float(payload.get("amount", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if user_id <= 0 or amount <= 0:
                return self._send_json(400, {"ok": False, "error": "user_id and amount are required"})

            # Create pending request first: balance is credited only after admin confirmation.
            topup_id = create_topup(user_id, amount, payment_link="", external_payment_id=f"fp_req_{user_id}_{int(time.time())}")
            payment_link = _build_funpay_payment_link(amount, user_id, topup_id)
            _notify_admins_new_topup(user_id=user_id, amount=amount, topup_id=topup_id, payment_link=payment_link)

            return self._send_json(
                200,
                {
                    "ok": True,
                    "mode": "pending_admin_confirmation",
                    "topup_id": int(topup_id),
                    "user_id": int(user_id),
                    "amount": float(amount),
                    "payment_link": payment_link,
                    "marker": f"topup_{topup_id}_{user_id}",
                    "message": "Оплати через FunPay и дождись ручного подтверждения админом.",
                },
            )

        if route == "/api/promo":
            try:
                user_id = int(payload.get("user_id", 0))
                code = str(payload.get("code", "")).strip()
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if user_id <= 0 or not code:
                return self._send_json(400, {"ok": False, "error": "user_id and code are required"})

            ok, message, amount = activate_promo(user_id, code)
            if not ok:
                return self._send_json(400, {"ok": False, "error": message})

            return self._send_json(
                200,
                {
                    "ok": True,
                    "message": message,
                    "amount": round(float(amount), 2),
                    "balance": round(get_balance(user_id), 2),
                },
            )

        if route == "/api/buy":
            status, response = handle_buy(payload)
            return self._send_json(status, response)

        if route == "/api/admin/login":
            try:
                user_id = int(payload.get("user_id", 0))
                login = str(payload.get("login", "")).strip()
                password = str(payload.get("password", "")).strip()
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if user_id <= 0:
                return self._send_json(400, {"ok": False, "error": "user_id is required"})

            if ADMIN_IDS and user_id not in ADMIN_IDS:
                return self._send_json(403, {"ok": False, "error": "access denied: user is not admin"})

            if login != ADMIN_LOGIN or password != ADMIN_PASSWORD:
                return self._send_json(401, {"ok": False, "error": "invalid login or password"})

            token = _create_admin_token(user_id, login)
            try:
                log_admin_action(user_id, "web_admin_login", "Successful login in web admin panel")
            except Exception:
                pass
            return self._send_json(
                200,
                {
                    "ok": True,
                    "token": token,
                    "expires_in": ADMIN_TOKEN_TTL_SECONDS,
                    "admin_login": login,
                },
            )

        if route == "/api/admin/set-stock":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                product_id = int(payload.get("product_id", 0))
                stock = int(payload.get("stock", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if product_id <= 0:
                return self._send_json(400, {"ok": False, "error": "product_id is required"})

            set_stock(product_id, max(0, stock))
            try:
                log_admin_action(int(info["user_id"]), "web_set_stock", f"product_id={product_id}, stock={max(0, stock)}")
            except Exception:
                pass
            return self._send_json(200, {"ok": True, "message": "stock updated"})

        if route == "/api/admin/append-credentials":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                product_id = int(payload.get("product_id", 0))
                credentials = str(payload.get("credentials", ""))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if product_id <= 0:
                return self._send_json(400, {"ok": False, "error": "product_id is required"})
            if not credentials.strip():
                return self._send_json(400, {"ok": False, "error": "credentials are required"})

            result = append_product_credentials_with_stock(product_id, credentials)
            if not result:
                return self._send_json(404, {"ok": False, "error": "product not found or inactive"})

            added_count, new_stock, _ = result
            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_append_credentials",
                    f"product_id={product_id}, added_count={int(added_count)}, new_stock={int(new_stock)}",
                )
            except Exception:
                pass
            return self._send_json(
                200,
                {
                    "ok": True,
                    "message": "credentials appended",
                    "added_count": int(added_count),
                    "new_stock": int(new_stock),
                },
            )

        if route == "/api/admin/confirm-topup":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                topup_id = int(payload.get("topup_id", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if topup_id <= 0:
                return self._send_json(400, {"ok": False, "error": "topup_id is required"})

            result = confirm_topup(topup_id)
            if not result:
                return self._send_json(404, {"ok": False, "error": "topup not found"})

            _, user_id, amount, status = result
            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_confirm_topup",
                    f"topup_id={topup_id}, user_id={int(user_id)}, amount={float(amount):.2f}, status={status}",
                )
            except Exception:
                pass

            return self._send_json(
                200,
                {
                    "ok": True,
                    "topup_id": int(topup_id),
                    "user_id": int(user_id),
                    "amount": float(amount),
                    "status": str(status),
                    "new_balance": round(get_balance(int(user_id)), 2),
                },
            )

        if route == "/api/admin/add-balance":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                user_id = int(payload.get("user_id", 0))
                amount = float(payload.get("amount", 0))
                reason = str(payload.get("reason", "manual_topup")).strip() or "manual_topup"
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if user_id <= 0:
                return self._send_json(400, {"ok": False, "error": "user_id is required"})
            if amount <= 0:
                return self._send_json(400, {"ok": False, "error": "amount must be greater than 0"})

            change_balance(user_id, amount)
            new_balance = round(get_balance(user_id), 2)

            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_add_balance",
                    f"target_user_id={user_id}, amount={amount:.2f}, reason={reason}, new_balance={new_balance:.2f}",
                )
            except Exception:
                pass

            return self._send_json(
                200,
                {
                    "ok": True,
                    "user_id": int(user_id),
                    "amount": float(amount),
                    "reason": reason,
                    "new_balance": float(new_balance),
                },
            )

        if route == "/api/admin/add-product":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                title = str(payload.get("title", "")).strip()
                category = str(payload.get("category", "")).strip().lower()
                description = str(payload.get("description", "")).strip()
                credentials = str(payload.get("credentials", "")).strip()
                price = float(payload.get("price", 0))
                stock = int(payload.get("stock", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if not title:
                return self._send_json(400, {"ok": False, "error": "title is required"})
            if not category:
                return self._send_json(400, {"ok": False, "error": "category is required"})
            if price <= 0:
                return self._send_json(400, {"ok": False, "error": "price must be greater than 0"})

            product_id = add_product(
                title=title,
                price=price,
                credentials=credentials,
                category=category,
                description=description,
                stock=max(0, stock),
                auto_restock=0,
                restock_every_minutes=5,
                restock_amount=1,
                created_by_admin=int(info["user_id"]),
            )

            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_add_product",
                    f"product_id={product_id}, category={category}, price={price:.2f}, stock={max(0, stock)}",
                )
            except Exception:
                pass

            return self._send_json(
                200,
                {
                    "ok": True,
                    "product_id": int(product_id),
                    "title": title,
                    "category": category,
                },
            )

        if route == "/api/admin/update-product":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                product_id = int(payload.get("product_id", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid product_id"})

            if product_id <= 0:
                return self._send_json(400, {"ok": False, "error": "product_id is required"})

            category = payload.get("category", None)
            if category is not None:
                category = str(category).strip().lower()
                if not category:
                    return self._send_json(400, {"ok": False, "error": "category must not be empty"})

            price = payload.get("price", None)
            if price is not None:
                try:
                    price = float(price)
                except Exception:
                    return self._send_json(400, {"ok": False, "error": "invalid price"})
                if price <= 0:
                    return self._send_json(400, {"ok": False, "error": "price must be greater than 0"})

            title = payload.get("title", None)
            if title is not None:
                title = str(title).strip()
                if not title:
                    return self._send_json(400, {"ok": False, "error": "title must not be empty"})

            description = payload.get("description", None)
            if description is not None:
                description = str(description).strip()

            changed = update_product_fields(
                product_id=product_id,
                title=title,
                description=description,
                category=category,
                price=price,
            )
            if not changed:
                return self._send_json(404, {"ok": False, "error": "product not found or nothing to update"})

            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_update_product",
                    f"product_id={product_id}, title={title or '-'}, category={category or '-'}, price={price if price is not None else '-'}",
                )
            except Exception:
                pass

            return self._send_json(200, {"ok": True, "product_id": int(product_id)})

        if route == "/api/admin/toggle-product":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                product_id = int(payload.get("product_id", 0))
                is_active = int(payload.get("is_active", 1))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if product_id <= 0:
                return self._send_json(400, {"ok": False, "error": "product_id is required"})

            changed = set_product_active(product_id=product_id, is_active=bool(is_active))
            if not changed:
                return self._send_json(404, {"ok": False, "error": "product not found"})

            action_name = "enabled" if bool(is_active) else "disabled"
            try:
                log_admin_action(int(info["user_id"]), "web_toggle_product", f"product_id={product_id}, state={action_name}")
            except Exception:
                pass

            return self._send_json(200, {"ok": True, "product_id": int(product_id), "is_active": 1 if bool(is_active) else 0})

        if route == "/api/admin/create-promo":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            try:
                code = str(payload.get("code", "")).strip().upper()
                amount = float(payload.get("amount", 0))
                uses_left = int(payload.get("uses_left", 0))
            except Exception:
                return self._send_json(400, {"ok": False, "error": "invalid input"})

            if not code or len(code) < 3:
                return self._send_json(400, {"ok": False, "error": "code length must be >= 3"})
            if amount <= 0:
                return self._send_json(400, {"ok": False, "error": "amount must be greater than 0"})
            if uses_left <= 0:
                return self._send_json(400, {"ok": False, "error": "uses_left must be greater than 0"})

            create_promo(code=code, amount=amount, uses_left=uses_left)
            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_create_promo",
                    f"code={code}, amount={amount:.2f}, uses_left={uses_left}",
                )
            except Exception:
                pass

            return self._send_json(200, {"ok": True, "code": code, "amount": float(amount), "uses_left": int(uses_left)})

        if route == "/api/admin/create-category":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            slug = str(payload.get("slug", "")).strip().lower()
            title = str(payload.get("title", "")).strip()
            if not slug:
                return self._send_json(400, {"ok": False, "error": "slug is required"})

            ok, result = create_category(slug=slug, title=title)
            if not ok:
                return self._send_json(400, {"ok": False, "error": str(result)})

            try:
                log_admin_action(int(info["user_id"]), "web_create_category", f"slug={slug}, title={title or slug.upper()}")
            except Exception:
                pass
            return self._send_json(200, {"ok": True, "slug": slug, "title": title or slug.upper()})

        if route == "/api/admin/update-category":
            allowed, info = _require_admin(self.headers)
            if not allowed:
                return self._send_json(401, info)

            slug = str(payload.get("slug", "")).strip().lower()
            title = payload.get("title", None)
            is_active = payload.get("is_active", None)
            if not slug:
                return self._send_json(400, {"ok": False, "error": "slug is required"})

            if title is not None:
                title = str(title).strip()
            if is_active is not None:
                try:
                    is_active = 1 if int(is_active) else 0
                except Exception:
                    return self._send_json(400, {"ok": False, "error": "is_active must be 0/1"})

            changed = update_category(slug=slug, title=title, is_active=is_active)
            if not changed:
                return self._send_json(404, {"ok": False, "error": "category not found or nothing to update"})

            try:
                log_admin_action(
                    int(info["user_id"]),
                    "web_update_category",
                    f"slug={slug}, title={title or '-'}, is_active={is_active if is_active is not None else '-'}",
                )
            except Exception:
                pass
            return self._send_json(200, {"ok": True, "slug": slug})

        self.send_error(404)

    def log_message(self, format: str, *args: Any) -> None:
        logging.info("%s - %s", self.address_string(), format % args)


def main() -> None:
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), ShopHandler)
    print(f"Web shop started: http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
