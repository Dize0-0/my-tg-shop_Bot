# Мини веб-магазин для Telegram

- Запуск: `python app.py`
- Открывается на http://localhost:8080
- Каталог товаров, кнопка покупки

## Интеграция с Telegram WebApp

1. В боте отправьте пользователю кнопку с параметром `web_app`:

```python
from aiogram.types import WebAppInfo, InlineKeyboardButton, InlineKeyboardMarkup

kb = InlineKeyboardMarkup()
kb.add(InlineKeyboardButton('Открыть магазин', web_app=WebAppInfo(url='http://localhost:8080')))
```

2. Пользователь откроет веб-магазин прямо в Telegram.

3. Для связи с ботом используйте API, webhook или отправку данных через Telegram WebApp API.

## Доработка
- Добавьте обработку заказа, интеграцию с ботом, оформление оплаты.
- Можно расширить каталог, добавить корзину, авторизацию.
