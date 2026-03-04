import os
import asyncio
import hashlib
import hmac
import json
import time
from urllib.parse import parse_qsl

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


def ensure_user_columns() -> None:
    with conn.cursor() as db_cursor:
        db_cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT")
        db_cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT")
    conn.commit()


def validate_init_data(init_data: str) -> dict:
    if not init_data:
        raise ValueError("initData is empty")

    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise ValueError("hash is missing")

    auth_date = parsed.get("auth_date")
    if not auth_date:
        raise ValueError("auth_date is missing")

    auth_ts = int(auth_date)
    now_ts = int(time.time())
    if now_ts - auth_ts > 24 * 60 * 60:
        raise ValueError("initData is expired")
    if auth_ts - now_ts > 60:
        raise ValueError("auth_date is invalid")

    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, received_hash):
        raise ValueError("hash is invalid")

    raw_user = parsed.get("user")
    if not raw_user:
        raise ValueError("user is missing")

    user = json.loads(raw_user)
    if "id" not in user:
        raise ValueError("user.id is missing")
    return user


def upsert_user_and_get_pets(user_data: dict) -> tuple[dict, list[dict]]:
    tg_user_id = int(user_data["id"])
    username = user_data.get("username")
    first_name = user_data.get("first_name")
    last_name = user_data.get("last_name")

    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO users (tg_user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tg_user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name
            RETURNING id, tg_user_id, username, first_name, last_name
            """,
            (tg_user_id, username, first_name, last_name),
        )
        user_row = db_cursor.fetchone()

        db_cursor.execute(
            """
            SELECT id, name, type
            FROM pets
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            (user_row[0],),
        )
        pet_rows = db_cursor.fetchall()
    conn.commit()

    user_payload = {
        "tg_user_id": user_row[1],
        "username": user_row[2],
        "first_name": user_row[3],
        "last_name": user_row[4],
    }
    pets_payload = [{"id": row[0], "name": row[1], "type": row[2]} for row in pet_rows]
    return user_payload, pets_payload


def ensure_user_by_tg_id(tg_user_id: int, username: str | None = None) -> int:
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO users (tg_user_id, username)
            VALUES (%s, %s)
            ON CONFLICT (tg_user_id)
            DO UPDATE SET username = COALESCE(EXCLUDED.username, users.username)
            RETURNING id
            """,
            (tg_user_id, username),
        )
        user_row = db_cursor.fetchone()
    conn.commit()
    return user_row[0]


def get_tg_user_id_from_header(request: web.Request) -> int:
    raw_tg_user_id = request.headers.get("X-TG-USER-ID", "").strip()
    if not raw_tg_user_id:
        raise ValueError("X-TG-USER-ID header is required")

    try:
        tg_user_id = int(raw_tg_user_id)
    except ValueError as exc:
        raise ValueError("X-TG-USER-ID must be an integer") from exc

    if tg_user_id <= 0:
        raise ValueError("X-TG-USER-ID must be positive")
    return tg_user_id


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    tg_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO users (tg_user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tg_user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name
            """,
            (tg_id, username, first_name, last_name),
        )
    conn.commit()

    hello_name = first_name or "друг"
    await message.answer(
        f"Привет, {hello_name}! 🐶🐱\n\n"
        "Это дневник питомца.\n"
        "Скоро здесь можно будет добавить собакена или котофея."
    )
    await message.answer("Открывай дневник 👇", reply_markup=kb)


# ---------- Healthcheck HTTP server ----------
async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def auth_handler(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    init_data = payload.get("initData")
    if not isinstance(init_data, str):
        return web.json_response({"error": "initData is required"}, status=400)

    try:
        tg_user = validate_init_data(init_data)
    except (ValueError, json.JSONDecodeError):
        return web.json_response({"error": "Unauthorized"}, status=401)

    user, pets = upsert_user_and_get_pets(tg_user)
    return web.json_response({"user": user, "pets": pets})


async def pets_handler(request: web.Request) -> web.Response:
    init_data = request.query.get("initData")
    if not init_data:
        return web.json_response({"error": "initData is required"}, status=400)

    try:
        tg_user = validate_init_data(init_data)
    except (ValueError, json.JSONDecodeError):
        return web.json_response({"error": "Unauthorized"}, status=401)

    _, pets = upsert_user_and_get_pets(tg_user)
    return web.json_response({"pets": pets})


async def api_get_pets(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    user_id = ensure_user_by_tg_id(tg_user_id)
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT id, name, type
            FROM pets
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        pet_rows = db_cursor.fetchall()

    pets_payload = [{"id": row[0], "name": row[1], "type": row[2]} for row in pet_rows]
    return web.json_response({"pets": pets_payload})


async def api_create_pet(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    name = str(payload.get("name", "")).strip()
    pet_type = str(payload.get("type", "")).strip().lower()

    if not name:
        return web.json_response({"error": "name is required"}, status=400)
    if pet_type not in {"dog", "cat"}:
        return web.json_response({"error": "type must be dog or cat"}, status=400)

    user_id = ensure_user_by_tg_id(tg_user_id)

    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO pets (user_id, name, type)
            VALUES (%s, %s, %s)
            RETURNING id, name, type
            """,
            (user_id, name, pet_type),
        )
        pet_row = db_cursor.fetchone()
    conn.commit()

    pet_payload = {"id": pet_row[0], "name": pet_row[1], "type": pet_row[2]}
    return web.json_response({"ok": True, "pet": pet_payload})


async def start_web_server() -> None:
    ensure_user_columns()

    app = web.Application()
    app.router.add_get("/", lambda request: web.FileResponse("index.html"))
    app.router.add_get("/health", health)
    app.router.add_post("/api/auth", auth_handler)
    app.router.add_get("/api/pets", api_get_pets)
    app.router.add_post("/api/pets", api_create_pet)

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
