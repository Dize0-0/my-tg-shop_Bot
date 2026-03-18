import os
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils import executor

API_TOKEN = os.getenv('TG_BOT_TOKEN', 'YOUR_BOT_TOKEN')
LOLZ_TOKEN = os.getenv('LOLZ_TOKEN', 'YOUR_LOLZ_TOKEN')

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

def find_and_buy_account():
    url = "https://api.lzt.market/telegram?order_by=price_to_up&pmin=1&pmax=999999&spam=no&allow_geo_spamblock=false&password=no"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {LOLZ_TOKEN}"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return None, "Ошибка поиска аккаунтов: " + resp.text
    data = resp.json()
    items = data.get("items", [])
    if not items:
        return None, "Нет доступных аккаунтов."
    account = items[0]
    item_id = account.get("item_id")
    buy_url = f"https://api.lzt.market/telegram/buy/{item_id}"
    buy_resp = requests.post(buy_url, headers=headers)
    if buy_resp.status_code != 200:
        return None, "Ошибка покупки: " + buy_resp.text
    buy_data = buy_resp.json()
    login_data = buy_data.get("item", {}).get("loginData", {})
    phone = login_data.get("login")
    password = login_data.get("password")
    return (phone, password), None

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🛒 Купить ТГ аккаунт', callback_data='cat_tg'))
    await message.answer('Добро пожаловать! Нажмите кнопку ниже, чтобы купить Telegram-аккаунт.', reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == 'cat_tg')
async def cat_tg_callback(cb: types.CallbackQuery):
    await cb.answer("Поиск аккаунта...", show_alert=False)
    result, error = find_and_buy_account()
    if error:
        await cb.message.answer(f"❌ {error}")
    else:
        phone, password = result
        recommendations = (
            "<b>Рекомендации по использованию:</b>\n"
            "1. Не используйте VPN для входа. VPN, особенно общий, может вызвать подозрения у систем безопасности.\n"
            "2. Входите через прокси. Купите прокси и используйте его не более, чем для 5-ти аккаунтов.\n"
            "3. Включите двухфакторную аутентификацию (2FA). Защитите свой аккаунт. Активируйте 2FA здесь:\n"
            "   ╰ Настройки → Конфиденциальность и безопасность → Двухфакторная аутентификация.\n"
            "4. Дайте аккаунту «отлежаться» 2 дня. Просто не выполняйте активных действий.\n"
            "5. На 3-й день симулируйте активность. Попросите 3–4 друзей написать вам в разное время. Поговорите с ними.\n"
            "6. На 4-й день начните работу. Ваш аккаунт готов."
        )
        text = (
            f"✅ <b>Ваш Telegram-аккаунт:</b>\n"
            f"<b>Телефон:</b> <code>{phone}</code>\n"
            f"<b>Пароль:</b> <code>{password}</code>\n\n"
            f"{recommendations}"
        )
        await cb.message.answer(text, parse_mode='HTML')

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
import os
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from aiogram.utils import executor
import requests

API_TOKEN = os.getenv('TG_BOT_TOKEN', 'YOUR_BOT_TOKEN')
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Ваш токен от lolz
LOLZ_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiJ9.eyJzdWIiOjEwMTU4NDcwLCJpc3MiOiJsenQiLCJpYXQiOjE3NzM4NDQ4NTIsImp0aSI6Ijk0ODE0MiIsInNjb3BlIjoiYmFzaWMgcmVhZCBwb3N0IGNvbnZlcnNhdGUgcGF5bWVudCBpbnZvaWNlIGNoYXRib3ggbWFya2V0IiwiZXhwIjoxOTMxNTI0ODUyfQ.wMQt9VChoYtBtDT9RoL39TagkBNj0oAKTu87sWIYh3FQSVtY5c6JDDUBXJNejhZUfWNU8WAxpsCTtQT6M8k-MJOs8X8yC8RiedxEI83gg5Saede1QZRsJwcznxnSTDgeMA1e2RlLFsR5ddFAD8iqMXGDWO1zUmjzwFTD8N2QWQQ"

def find_and_buy_account():
    url = "https://api.lzt.market/telegram?order_by=price_to_up&pmin=1&pmax=999999&spam=no&allow_geo_spamblock=false&password=no"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {LOLZ_TOKEN}"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return None, "Ошибка поиска аккаунтов: " + resp.text
    data = resp.json()
    items = data.get("items", [])
    if not items:
        return None, "Нет доступных аккаунтов."
    # Берём самый дешёвый
    account = items[0]
    item_id = account.get("item_id")
    # Покупка
    buy_url = f"https://api.lzt.market/telegram/buy/{item_id}"
    buy_resp = requests.post(buy_url, headers=headers)
    if buy_resp.status_code != 200:
        return None, "Ошибка покупки: " + buy_resp.text
    buy_data = buy_resp.json()
    login_data = buy_data.get("item", {}).get("loginData", {})
    phone = login_data.get("login")
    password = login_data.get("password")
    return (phone, password), None

@dp.callback_query_handler(lambda c: c.data == 'cat_tg')
async def cat_tg_callback(cb: types.CallbackQuery):
    await cb.answer("Поиск аккаунта...", show_alert=False)
    result, error = find_and_buy_account()
    if error:
        await cb.message.answer(f"❌ {error}")
    else:
        phone, password = result
        recommendations = (
            "<b>Рекомендации по использованию:</b>\n"
            "1. Не используйте VPN для входа. VPN, особенно общий, может вызвать подозрения у систем безопасности.\n"
            "2. Входите через прокси. Купите прокси и используйте его не более, чем для 5-ти аккаунтов.\n"
            "3. Включите двухфакторную аутентификацию (2FA). Защитите свой аккаунт. Активируйте 2FA здесь:\n"
            "   ╰ Настройки → Конфиденциальность и безопасность → Двухфакторная аутентификация.\n"
            "4. Дайте аккаунту «отлежаться» 2 дня. Просто не выполняйте активных действий.\n"
            "5. На 3-й день симулируйте активность. Попросите 3–4 друзей написать вам в разное время. Поговорите с ними.\n"
            "6. На 4-й день начните работу. Ваш аккаунт готов."
        )
        text = (
            f"✅ <b>Ваш Telegram-аккаунт:</b>\n"
            f"<b>Телефон:</b> <code>{phone}</code>\n"
            f"<b>Пароль:</b> <code>{password}</code>\n\n"
            f"{recommendations}"
        )
        await cb.message.answer(text, parse_mode='HTML')

import os
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from aiogram.utils import executor

API_TOKEN = os.getenv('TG_BOT_TOKEN', 'YOUR_BOT_TOKEN')
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

WEBAPP_URL = 'http://localhost:8080'  # Замените на ваш реальный URL при деплое

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    name = message.from_user.full_name or message.from_user.username or 'Пользователь'
    text = (
        f'Добро пожаловать, {name}!\n\n'
        'Это официальный бот магазина veemp\'s plugins.\n\n'
        'Здесь вы можете:\n'
        '🔵 Просматривать свой профиль\n'
        '🔵 Управлять лицензиями\n'
        '🔵 Получать уведомления о покупках'
    )
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton('🛒 Магазин', callback_data='shop'),
        InlineKeyboardButton('🌐 Сайт', url='https://veemp.com'),
        InlineKeyboardButton('❓ Помощь', callback_data='help')
    )
    await message.answer(text, reply_markup=kb)
@dp.callback_query_handler(lambda c: c.data == 'shop')
async def shop_callback(cb: types.CallbackQuery):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('👤 ТГ аккаунты', callback_data='cat_tg'),
        InlineKeyboardButton('📧 Почты', callback_data='cat_mail')
    )
    text = (
        '🛒 <b>Магазин</b>\n\n'
        'Выберите категорию товара:\n'
        '👤 <b>ТГ аккаунты</b> — Telegram-аккаунты\n'
        '📧 <b>Почты</b> — email-аккаунты\n'
    )
    await cb.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

@dp.callback_query_handler(lambda c: c.data == 'help')
async def help_callback(cb: types.CallbackQuery):
    await cb.answer('Справочная информация', show_alert=True)
    await bot.send_message(cb.from_user.id, '❓ Помощь по боту: вопросы, поддержка, инструкции.')

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    await message.answer('❓ Помощь по боту: вопросы, поддержка, инструкции.')

@dp.message_handler(commands=['profile'])
async def cmd_profile(message: types.Message):
    user_id = message.from_user.id
    name = message.from_user.full_name or message.from_user.username or 'Пользователь'
    text = f'Профиль пользователя:\nИмя: {name}\nID: {user_id}\nИстория покупок: скоро будет\nЛицензии: скоро будет'
    await message.answer(text)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
