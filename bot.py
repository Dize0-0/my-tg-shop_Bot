import logging
from aiogram import Bot, Dispatcher, types, executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
import os

API_TOKEN = os.getenv("TG_BOT_TOKEN", "8381711672:AAHhUrnAIzpZ6J4uF5i4mj_jkMKh3Tu76bE")  # Задай токен через переменную окружения!

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Главное меню (внизу)
main_kb = ReplyKeyboardMarkup(resize_keyboard=True)
main_kb.add(KeyboardButton("🛒 Магазин"), KeyboardButton("🌐 Сайт"))
main_kb.add(KeyboardButton("❓ Помощь"))

# Кнопки профиля
profile_kb = InlineKeyboardMarkup()
profile_kb.add(InlineKeyboardButton("👤 Профиль", callback_data="profile"))
profile_kb.add(InlineKeyboardButton("🔑 Лицензии", callback_data="licenses"))
profile_kb.add(InlineKeyboardButton("🔔 Уведомления о покупках", callback_data="notify"))

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    text = f"\U0001F539 Добро пожаловать, {message.from_user.full_name}!\n\nЭто официальный бот магазина veemp's plugins.\n\nЗдесь вы можете:\n\U0001F539 Просматривать свой профиль\n\U0001F539 Управлять лицензиями\n\U0001F539 Получать уведомления о покупках"
    await message.answer(text, reply_markup=main_kb)
    await message.answer("Выберите действие:", reply_markup=profile_kb)

@dp.callback_query_handler(lambda c: c.data == 'profile')
async def profile_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Ваш профиль: (пример)")
    await callback_query.message.answer("Профиль: Имя, покупки, лицензии...")

@dp.callback_query_handler(lambda c: c.data == 'licenses')
async def licenses_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Ваши лицензии: (пример)")
    await callback_query.message.answer("Лицензии: ...")

@dp.callback_query_handler(lambda c: c.data == 'notify')
async def notify_callback(callback_query: types.CallbackQuery):
    await callback_query.answer("Уведомления о покупках: (пример)")
    await callback_query.message.answer("Уведомления: ...")

@dp.message_handler(lambda m: m.text == "🛒 Магазин")
async def shop_handler(message: types.Message):
    await message.answer("Магазин: Здесь будут товары...")

@dp.message_handler(lambda m: m.text == "🌐 Сайт")
async def site_handler(message: types.Message):
    await message.answer("Сайт: https://ваш-сайт.рф")

@dp.message_handler(lambda m: m.text == "❓ Помощь")
async def help_handler(message: types.Message):
    await message.answer("Помощь: Напишите /start для возврата в главное меню.")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
