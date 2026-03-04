import os
import asyncio
import psycopg2

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# подключение к базе
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()


@dp.message(Command("start"))
async def start_handler(message: types.Message):

    tg_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    cursor.execute(
        """
        INSERT INTO users (tg_user_id, username)
        VALUES (%s, %s)
        ON CONFLICT (tg_user_id) DO NOTHING
        """,
        (tg_id, username)
    )

    conn.commit()

    await message.answer(
        f"Привет, {first_name}! 🐶🐱\n\n"
        "Это дневник питомца.\n"
        "Скоро здесь можно будет добавить собакена или котофея."
    )


async def main():
    print("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
