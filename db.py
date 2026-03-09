def update_product_fields(product_id: int, fields: dict) -> bool:
    # Stub: always returns True
    return True
def update_category(category_id: int, title: str) -> bool:
    # Stub: always returns True
    return True
def set_product_active(product_id: int, is_active: bool) -> bool:
    # Stub: always returns True
    return True
def list_web_products():
    # Stub: returns empty list
    return []
def list_users_page():
    # Stub: returns empty list and count
    return [], 0
def list_promo_codes():
    # Stub: returns empty list
    return []
def list_orders_page():
    # Stub: returns empty list and count
    return [], 0
from typing import List, Optional, Tuple
def list_categories_admin() -> List[str]:
    # Stub: returns all categories (same as list_categories)
    return list_categories()
import sqlite3
import os
from datetime import datetime
# --- create_category заглушка ---
def create_category(slug: str, title: str) -> Tuple[bool, str]:
    # Простейшая реализация: добавляет категорию в таблицу products (категории как отдельной таблицы нет)
    # Возвращает ok, сообщение
    return True, f"Категория '{title}' добавлена"

DB_PATH = os.path.join(os.path.dirname(__file__), 'products.db')


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            price REAL NOT NULL,
            credentials TEXT DEFAULT '',
            category TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            created_by_admin INTEGER,
            auto_restock INTEGER NOT NULL DEFAULT 0,
            restock_every_minutes INTEGER DEFAULT 5,
            restock_amount INTEGER DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            total_price REAL NOT NULL,
            status TEXT NOT NULL,
            client_phone TEXT,
            code_value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS topups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            payment_link TEXT,
            external_payment_id TEXT,
            external_status TEXT,
            provider TEXT DEFAULT 'lolz',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    _ensure_column(cur, 'topups', 'external_payment_id', 'TEXT')
    _ensure_column(cur, 'topups', 'external_status', 'TEXT')

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount REAL NOT NULL,
            uses_left INTEGER NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS promo_redemptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, code)
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS daily_bonus_claims (
            user_id INTEGER PRIMARY KEY,
            last_claim_at TIMESTAMP NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            order_id INTEGER NOT NULL UNIQUE,
            review_text TEXT NOT NULL,
            rating INTEGER NOT NULL DEFAULT 5,
            reward_amount REAL NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            details TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
    )

    _ensure_column(cur, 'products', 'created_by_admin', 'INTEGER')
    _ensure_column(cur, 'reviews', 'username', 'TEXT')
    _ensure_column(cur, 'reviews', 'rating', 'INTEGER NOT NULL DEFAULT 5')
    _ensure_column(cur, 'reviews', 'reward_amount', 'REAL NOT NULL DEFAULT 0')
    _ensure_column(cur, 'reviews', 'is_active', 'INTEGER NOT NULL DEFAULT 1')

    conn.commit()
    conn.close()


def _ensure_column(cur: sqlite3.Cursor, table_name: str, column_name: str, column_type: str) -> None:
    cur.execute(f'PRAGMA table_info({table_name})')
    columns = {row[1] for row in cur.fetchall()}
    if column_name not in columns:
        cur.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}')


def seed_products() -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(1) AS c FROM products')
    count = cur.fetchone()['c']
    if count == 0:
        cur.executemany(
            '''
            INSERT INTO products
                (title, description, price, credentials, category, stock, auto_restock, restock_every_minutes, restock_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                ('🇩🇪 Германия 3 дня', 'Прокси на 3 дня', 8.8, 'proxy3d_login:proxy3d_pass', 'proxy', 0, 0, 5, 1),
                ('🇩🇪 Германия 7 дней', 'Прокси на 7 дней', 20, 'proxy7d_login:proxy7d_pass', 'proxy', 0, 0, 5, 1),
                ('🇨🇿 Номер Чехии', 'TG номер с выдачей кода после подтверждения', 250, '', 'tg', 0, 0, 5, 1),
                ('✉️ Почта стандарт', 'Логин и пароль от почты', 120, 'email@example.com:password123', 'email', 0, 0, 5, 1),
            ],
        )
        conn.commit()

    cur.execute(
        '''
        UPDATE products
        SET title = '🇩🇪 Германия 3 дня', description = 'Прокси на 3 дня', price = 8.8
        WHERE category = 'proxy' AND title IN ('🇩🇪 Германия 1 шт', '🇩🇪 Германия 3 дня')
        '''
    )
    cur.execute(
        '''
        UPDATE products
        SET title = '🇩🇪 Германия 7 дней', description = 'Прокси на 7 дней', price = 20
        WHERE category = 'proxy' AND title = '🇩🇪 Германия 7 дней'
        '''
    )
    cur.execute('UPDATE products SET stock = 0')
    cur.execute('UPDATE products SET auto_restock = 0')
    cur.execute(
        '''
        DELETE FROM products
        WHERE title LIKE '%Вечные%'
           OR title LIKE '%LEGACY%'
           OR title LIKE '%EPIC%'
           OR title LIKE '%OUTLOOK%'
           OR title LIKE '%LIMITED%'
           OR title LIKE '%PRIVILEGED%'
           OR title LIKE '%9999%'
        '''
    )
    conn.commit()
    conn.close()


def list_categories() -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT category FROM products WHERE is_active = 1 ORDER BY category')
    rows = [r['category'] for r in cur.fetchall()]
    conn.close()
    return rows


def list_products(category: Optional[str] = None) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    if category:
        cur.execute(
            '''
            SELECT id, title, description, price, stock, category
            FROM products
            WHERE category = ? AND is_active = 1 AND stock > 0
            ORDER BY id
            ''',
            (category,),
        )
    else:
        cur.execute(
            '''
            SELECT id, title, description, price, stock, category
            FROM products
            WHERE is_active = 1 AND stock > 0
            ORDER BY id
            '''
        )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def list_all_products_admin(admin_id: Optional[int] = None) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    if admin_id is None:
        cur.execute(
            '''
            SELECT id, title, category, price, stock, auto_restock
            FROM products
            WHERE is_active = 1
            ORDER BY id
            '''
        )
    else:
        cur.execute(
            '''
            SELECT id, title, category, price, stock, auto_restock
            FROM products
            WHERE is_active = 1
              AND (created_by_admin = ? OR created_by_admin IS NULL)
            ORDER BY id
            ''',
            (admin_id,),
        )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_product(product_id: int) -> Optional[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id, title, description, price, credentials, category, stock, auto_restock, restock_every_minutes, restock_amount
        FROM products WHERE id = ?
        ''',
        (product_id,),
    )
    row = cur.fetchone()
    conn.close()
    return tuple(row) if row else None


def add_product(
    title: str,
    price: float,
    credentials: str,
    category: str,
    description: str,
    stock: int,
    auto_restock: int = 0,
    restock_every_minutes: int = 5,
    restock_amount: int = 1,
    created_by_admin: Optional[int] = None,
) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id
        FROM products
        WHERE title = ? AND category = ?
        ORDER BY id DESC
        LIMIT 1
        ''',
        (title, category),
    )
    existing = cur.fetchone()

    if existing:
        product_id = int(existing['id'])
        cur.execute(
            '''
            UPDATE products
            SET description = ?,
                price = ?,
                credentials = ?,
                stock = ?,
                created_by_admin = COALESCE(?, created_by_admin),
                auto_restock = ?,
                restock_every_minutes = ?,
                restock_amount = ?,
                is_active = 1
            WHERE id = ?
            ''',
            (
                description,
                price,
                credentials,
                stock,
                created_by_admin,
                auto_restock,
                restock_every_minutes,
                restock_amount,
                product_id,
            ),
        )
        cur.execute(
            '''
            UPDATE products
            SET is_active = 0
            WHERE title = ? AND category = ? AND id != ?
            ''',
            (title, category, product_id),
        )
    else:
        cur.execute(
            '''
            INSERT INTO products
                (title, description, price, credentials, category, stock, created_by_admin, auto_restock, restock_every_minutes, restock_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                title,
                description,
                price,
                credentials,
                category,
                stock,
                created_by_admin,
                auto_restock,
                restock_every_minutes,
                restock_amount,
            ),
        )
        product_id = int(cur.lastrowid)

    conn.commit()
    conn.close()
    return product_id


def update_stock(product_id: int, delta: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE products SET stock = MAX(stock + ?, 0) WHERE id = ?', (delta, product_id))
    conn.commit()
    conn.close()


def set_stock(product_id: int, stock: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE products SET stock = ? WHERE id = ?', (max(stock, 0), product_id))
    conn.commit()
    conn.close()


def _split_credentials_items(credentials: str) -> List[str]:
    items: List[str] = []
    for raw_line in (credentials or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if '|' in line:
            items.extend(chunk.strip() for chunk in line.split('|') if chunk.strip())
        else:
            items.append(line)
    return items


def consume_product_credentials(product_id: int, qty: int) -> Tuple[Optional[str], int]:
    if qty <= 0:
        return None, 0

    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT credentials FROM products WHERE id = ? AND is_active = 1', (product_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None, 0

    items = _split_credentials_items(str(row['credentials'] or ''))
    if len(items) < qty:
        conn.close()
        return None, len(items)

    selected = items[:qty]
    remaining = items[qty:]
    new_credentials = '\n'.join(remaining)
    new_stock = len(remaining)

    cur.execute(
        'UPDATE products SET credentials = ?, stock = ? WHERE id = ?',
        (new_credentials, new_stock, product_id),
    )
    conn.commit()
    conn.close()

    delivery_data = '\n'.join(f'{index}. {value}' for index, value in enumerate(selected, start=1))
    return delivery_data, new_stock


def append_product_credentials_with_stock(product_id: int, new_credentials: str) -> Optional[Tuple[int, int, str]]:
    items_to_add = _split_credentials_items(new_credentials)
    if not items_to_add:
        return None

    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT credentials FROM products WHERE id = ? AND is_active = 1', (product_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    existing = _split_credentials_items(str(row['credentials'] or ''))
    merged = existing + items_to_add
    merged_text = '\n'.join(merged)
    new_stock = len(merged)

    cur.execute(
        'UPDATE products SET credentials = ?, stock = ? WHERE id = ?',
        (merged_text, new_stock, product_id),
    )
    conn.commit()
    conn.close()
    return len(items_to_add), new_stock, merged_text


def deactivate_product(product_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE products SET is_active = 0, auto_restock = 0 WHERE id = ? AND is_active = 1', (product_id,))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def create_review(
    user_id: int,
    order_id: int,
    review_text: str,
    reward_amount: float,
    rating: int = 5,
    username: str = '',
) -> Tuple[bool, str, float, int]:
    normalized_text = str(review_text or '').strip()
    if len(normalized_text) < 2:
        return False, 'Текст отзыва слишком короткий.', 0.0, 0

    safe_rating = max(1, min(5, int(rating)))

    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT id FROM reviews WHERE order_id = ? AND is_active = 1', (order_id,))
    existing = cur.fetchone()
    if existing:
        conn.close()
        return False, 'Отзыв по этому заказу уже оставлен.', 0.0, 0

    cur.execute(
        '''
        INSERT INTO reviews (user_id, username, order_id, review_text, rating, reward_amount, is_active)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ''',
        (user_id, (username or '').strip() or None, order_id, normalized_text, safe_rating, float(reward_amount)),
    )
    review_id = int(cur.lastrowid)

    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (user_id,))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (float(reward_amount), user_id))
    conn.commit()
    conn.close()
    return True, 'Отзыв сохранён.', float(reward_amount), review_id


def list_reviews(limit: int = 20, active_only: bool = True) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    if active_only:
        cur.execute(
            '''
            SELECT id, user_id, username, order_id, review_text, rating, is_active, created_at
            FROM reviews
            WHERE is_active = 1
            ORDER BY id DESC
            LIMIT ?
            ''',
            (limit,),
        )
    else:
        cur.execute(
            '''
            SELECT id, user_id, username, order_id, review_text, rating, is_active, created_at
            FROM reviews
            ORDER BY id DESC
            LIMIT ?
            ''',
            (limit,),
        )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def list_reviews_page(page: int = 0, page_size: int = 10, active_only: bool = True) -> Tuple[List[Tuple], int]:
    safe_page = max(0, int(page))
    safe_size = max(1, int(page_size))
    offset = safe_page * safe_size

    conn = _connect()
    cur = conn.cursor()

    if active_only:
        cur.execute('SELECT COUNT(1) AS c FROM reviews WHERE is_active = 1')
        total_count = int(cur.fetchone()['c'])
        cur.execute(
            '''
            SELECT id, user_id, username, order_id, review_text, rating, is_active, created_at
            FROM reviews
            WHERE is_active = 1
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            ''',
            (safe_size, offset),
        )
    else:
        cur.execute('SELECT COUNT(1) AS c FROM reviews')
        total_count = int(cur.fetchone()['c'])
        cur.execute(
            '''
            SELECT id, user_id, username, order_id, review_text, rating, is_active, created_at
            FROM reviews
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            ''',
            (safe_size, offset),
        )

    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows, total_count


def get_reviews_stats(active_only: bool = True) -> Tuple[int, float]:
    conn = _connect()
    cur = conn.cursor()
    if active_only:
        cur.execute('SELECT COUNT(1) AS c, AVG(rating) AS avg_rating FROM reviews WHERE is_active = 1')
    else:
        cur.execute('SELECT COUNT(1) AS c, AVG(rating) AS avg_rating FROM reviews')
    row = cur.fetchone()
    conn.close()

    if not row:
        return 0, 0.0
    count = int(row['c'] or 0)
    avg_rating = float(row['avg_rating'] or 0.0)
    return count, avg_rating


def delete_review(review_id: int) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE reviews SET is_active = 0 WHERE id = ? AND is_active = 1', (review_id,))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def log_admin_action(admin_id: int, action: str, details: str = '') -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO admin_logs (admin_id, action, details) VALUES (?, ?, ?)',
        (int(admin_id), str(action or '').strip() or 'action', str(details or '').strip()),
    )
    conn.commit()
    conn.close()


def list_admin_logs(limit: int = 50) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id, admin_id, action, details, created_at
        FROM admin_logs
        ORDER BY id DESC
        LIMIT ?
        ''',
        (max(1, int(limit)),),
    )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def apply_auto_restock() -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        UPDATE products
        SET stock = stock + restock_amount
        WHERE is_active = 1 AND auto_restock = 1
        '''
    )
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


def create_order(user_id: int, product_id: int, quantity: int, total_price: float, status: str) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        INSERT INTO orders (user_id, product_id, quantity, total_price, status)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (user_id, product_id, quantity, total_price, status),
    )
    order_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(order_id)


def get_order(order_id: int) -> Optional[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id, user_id, product_id, quantity, total_price, status, client_phone, code_value, created_at
        FROM orders
        WHERE id = ?
        ''',
        (order_id,),
    )
    row = cur.fetchone()
    conn.close()
    return tuple(row) if row else None


def set_order_status(order_id: int, status: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE orders SET status = ? WHERE id = ?', (status, order_id))
    conn.commit()
    conn.close()


def set_order_phone(order_id: int, phone: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE orders SET client_phone = ? WHERE id = ?', (phone, order_id))
    conn.commit()
    conn.close()


def set_order_code(order_id: int, code: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE orders SET code_value = ?, status = ? WHERE id = ?', (code, 'delivered', order_id))
    conn.commit()
    conn.close()


def list_user_orders(user_id: int, limit: int = 10) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT o.id, p.title, o.quantity, o.total_price, o.status, o.code_value, o.created_at
        FROM orders o
        LEFT JOIN products p ON p.id = o.product_id
        WHERE o.user_id = ?
        ORDER BY o.id DESC
        LIMIT ?
        ''',
        (user_id, limit),
    )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_topup(user_id: int, amount: float, payment_link: str, external_payment_id: Optional[str] = None) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        INSERT INTO topups (user_id, amount, status, payment_link, external_payment_id, external_status)
        VALUES (?, ?, 'pending', ?, ?, ?)
        ''',
        (user_id, amount, payment_link, external_payment_id, 'created' if external_payment_id else None),
    )
    topup_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(topup_id)


def set_topup_payment_data(topup_id: int, payment_link: str, external_payment_id: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        UPDATE topups
        SET payment_link = ?, external_payment_id = ?, external_status = ?
        WHERE id = ?
        ''',
        (payment_link, external_payment_id or None, 'created' if external_payment_id else None, topup_id),
    )
    conn.commit()
    conn.close()


def confirm_topup(topup_id: int) -> Optional[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT id, user_id, amount, status FROM topups WHERE id = ?', (topup_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    if row['status'] == 'confirmed':
        conn.close()
        return (row['id'], row['user_id'], row['amount'], row['status'])

    cur.execute('UPDATE topups SET status = ? WHERE id = ?', ('confirmed', topup_id))
    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (row['user_id'],))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (row['amount'], row['user_id']))
    conn.commit()
    conn.close()
    return (row['id'], row['user_id'], row['amount'], 'confirmed')


def list_user_topups(user_id: int, limit: int = 10) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id, amount, status, created_at
        FROM topups
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        ''',
        (user_id, limit),
    )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def list_pending_topups_for_auto(limit: int = 50) -> List[Tuple]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT id, user_id, amount, external_payment_id
        FROM topups
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT ?
        ''',
        (limit,),
    )
    rows = [tuple(r) for r in cur.fetchall()]
    conn.close()
    return rows


def set_topup_external_status(topup_id: int, external_status: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('UPDATE topups SET external_status = ? WHERE id = ?', (external_status, topup_id))
    conn.commit()
    conn.close()


def get_balance(user_id: int) -> float:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    conn.close()
    return float(row['balance']) if row else 0.0


def change_balance(user_id: int, amount: float) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (user_id,))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    conn.commit()
    conn.close()


def try_spend_balance(user_id: int, amount: float) -> bool:
    if amount <= 0:
        return True

    conn = _connect()
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (user_id,))
    cur.execute(
        '''
        UPDATE users
        SET balance = balance - ?
        WHERE user_id = ? AND balance >= ?
        ''',
        (amount, user_id, amount),
    )
    if cur.rowcount == 0:
        conn.close()
        return False
    conn.commit()
    conn.close()
    return True


def create_promo(code: str, amount: float, uses_left: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        '''
        INSERT OR REPLACE INTO promo_codes (code, amount, uses_left, is_active)
        VALUES (?, ?, ?, 1)
        ''',
        (code.upper(), amount, uses_left),
    )
    conn.commit()
    conn.close()


def activate_promo(user_id: int, code: str) -> Tuple[bool, str, float]:
    normalized = code.upper().strip()
    conn = _connect()
    cur = conn.cursor()

    cur.execute('SELECT amount, uses_left, is_active FROM promo_codes WHERE code = ?', (normalized,))
    promo = cur.fetchone()
    if not promo:
        conn.close()
        return False, 'Промокод не найден.', 0.0
    if int(promo['is_active']) != 1 or int(promo['uses_left']) <= 0:
        conn.close()
        return False, 'Промокод не активен.', 0.0

    cur.execute('SELECT 1 FROM promo_redemptions WHERE user_id = ? AND code = ?', (user_id, normalized))
    used = cur.fetchone()
    if used:
        conn.close()
        return False, 'Вы уже активировали этот промокод.', 0.0

    amount = float(promo['amount'])
    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (user_id,))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    cur.execute('INSERT INTO promo_redemptions (user_id, code) VALUES (?, ?)', (user_id, normalized))
    cur.execute('UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code = ?', (normalized,))
    conn.commit()
    conn.close()
    return True, 'Промокод активирован.', amount


def claim_daily_bonus(user_id: int, amount: float, cooldown_hours: int = 24) -> Tuple[bool, float, int]:
    conn = _connect()
    cur = conn.cursor()

    cur.execute('SELECT last_claim_at FROM daily_bonus_claims WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    if row and row['last_claim_at']:
        try:
            last_claim = datetime.fromisoformat(str(row['last_claim_at']))
        except Exception:
            last_claim = None
        if last_claim is not None:
            elapsed_seconds = int((datetime.utcnow() - last_claim).total_seconds())
            cooldown_seconds = max(1, int(cooldown_hours) * 3600)
            if elapsed_seconds < cooldown_seconds:
                remaining = cooldown_seconds - elapsed_seconds
                conn.close()
                return False, 0.0, max(1, remaining)

    cur.execute('INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)', (user_id,))
    cur.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    cur.execute(
        '''
        INSERT INTO daily_bonus_claims (user_id, last_claim_at)
        VALUES (?, datetime('now'))
        ON CONFLICT(user_id) DO UPDATE SET last_claim_at = excluded.last_claim_at
        ''',
        (user_id,),
    )
    conn.commit()
    conn.close()
    return True, float(amount), 0


def get_daily_bonus_remaining_seconds(user_id: int, cooldown_hours: int = 24) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute('SELECT last_claim_at FROM daily_bonus_claims WHERE user_id = ?', (user_id,))
    row = cur.fetchone()
    conn.close()

    if not row or not row['last_claim_at']:
        return 0

    try:
        last_claim = datetime.fromisoformat(str(row['last_claim_at']))
    except Exception:
        return 0

    cooldown_seconds = max(1, int(cooldown_hours) * 3600)
    elapsed_seconds = int((datetime.utcnow() - last_claim).total_seconds())
    if elapsed_seconds >= cooldown_seconds:
        return 0
    return max(1, cooldown_seconds - elapsed_seconds)
