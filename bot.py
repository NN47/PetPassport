import os
import asyncio
import psycopg2

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

from aiohttp import web

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://petpass-aerc.onrender.com/"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

kb = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📱 Открыть дневник питомца",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        ]
    ]
)

# DB connection (простая версия)
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    tg_id = message.from_user.id
    username = message.from_user.username

    cursor.execute(
        """
        INSERT INTO users (tg_user_id, username)
        VALUES (%s, %s)
        ON CONFLICT (tg_user_id) DO NOTHING
        """,
        (tg_id, username),
    )
    conn.commit()

    first_name = message.from_user.first_name or "друг"
    await message.answer(
        f"Привет, {first_name}! 🐶🐱\n\n"
        "Это дневник питомца.\n"
        "Скоро здесь можно будет добавить собакена или котофея."
    )
    await message.answer("Открывай дневник 👇", reply_markup=kb)


# ---------- Healthcheck HTTP server ----------
async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def start_web_server() -> None:
    app = web.Application()
    app.router.add_get("/", lambda request: web.FileResponse("index.html"))
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    # Render прокидывает PORT для Web Service
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()

    print(f"Health server started on 0.0.0.0:{port}")


async def main():
    print("Starting...")
    await start_web_server()         # запускаем HTTP “ok”
    print("Bot started")
    await dp.start_polling(bot)      # polling


if __name__ == "__main__":
    asyncio.run(main())
