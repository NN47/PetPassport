import os
import asyncio
import hashlib
import hmac
import json
import time
from datetime import date, datetime
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

_db_conn = None
_pets_columns_cache: set[str] | None = None
_weights_columns_cache: set[str] | None = None
_walks_columns_cache: set[str] | None = None


def get_db_connection():
    global _db_conn

    if _db_conn is None or _db_conn.closed != 0:
        if _db_conn is not None:
            print("Reconnecting to database...")
        _db_conn = psycopg2.connect(DATABASE_URL)
        print("Database connected")

    return _db_conn


def ensure_user_columns() -> None:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT")
        db_cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT")
    conn.commit()


def get_pets_columns() -> set[str]:
    global _pets_columns_cache

    if _pets_columns_cache is not None:
        return _pets_columns_cache

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'pets'
            """
        )
        rows = db_cursor.fetchall()

    _pets_columns_cache = {row[0] for row in rows}
    return _pets_columns_cache


def get_weights_columns() -> set[str]:
    global _weights_columns_cache

    if _weights_columns_cache is not None:
        return _weights_columns_cache

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'weights'
            """
        )
        rows = db_cursor.fetchall()

    _weights_columns_cache = {row[0] for row in rows}
    return _weights_columns_cache


def get_weights_mapping(columns: set[str]) -> tuple[str, str]:
    if "date_recorded" in columns:
        date_column = "date_recorded"
    elif "measured_at" in columns:
        date_column = "measured_at"
    else:
        raise RuntimeError("weights table is missing date column")

    if "weight" in columns:
        weight_column = "weight"
    elif "weight_kg" in columns:
        weight_column = "weight_kg"
    else:
        raise RuntimeError("weights table is missing weight column")

    return date_column, weight_column




def get_walks_columns() -> set[str]:
    global _walks_columns_cache

    if _walks_columns_cache is not None:
        return _walks_columns_cache

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'walks'
            """
        )
        rows = db_cursor.fetchall()

    _walks_columns_cache = {row[0] for row in rows}
    return _walks_columns_cache


def get_walks_mapping(columns: set[str]) -> tuple[str, str]:
    if "started_at" in columns:
        started_column = "started_at"
    elif "walk_date" in columns:
        started_column = "walk_date"
    else:
        raise RuntimeError("walks table is missing started_at/walk_date column")

    if "duration_min" in columns:
        duration_column = "duration_min"
    elif "duration_minutes" in columns:
        duration_column = "duration_minutes"
    else:
        raise RuntimeError("walks table is missing duration_min/duration_minutes column")

    return started_column, duration_column


def parse_walk_started_at(raw_started_at: object) -> datetime | None:
    if not isinstance(raw_started_at, str):
        return None

    started_at_str = raw_started_at.strip()
    if not started_at_str:
        return None

    normalized = started_at_str.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None

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

    conn = get_db_connection()
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


def get_user_id_by_tg_id(tg_user_id: int) -> int | None:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute("SELECT id FROM users WHERE tg_user_id = %s", (tg_user_id,))
        user_row = db_cursor.fetchone()
    return user_row[0] if user_row else None


def ensure_user_by_tg_id(tg_user_id: int, username: str | None = None) -> int:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO users (tg_user_id, username)
            VALUES (%s, %s)
            ON CONFLICT (tg_user_id) DO NOTHING
            """,
            (tg_user_id, username),
        )
        db_cursor.execute("SELECT id FROM users WHERE tg_user_id = %s", (tg_user_id,))
        user_row = db_cursor.fetchone()
    conn.commit()
    return user_row[0]


def get_owned_pet_id(tg_user_id: int, pet_id: int) -> int | None:
    user_id = get_user_id_by_tg_id(tg_user_id)
    if user_id is None:
        return None

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute("SELECT id FROM pets WHERE id = %s AND user_id = %s", (pet_id, user_id))
        pet_row = db_cursor.fetchone()
    return pet_row[0] if pet_row else None


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

    conn = get_db_connection()
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

    user_id = get_user_id_by_tg_id(tg_user_id)
    if user_id is None:
        return web.json_response({"pets": []})

    conn = get_db_connection()
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

    conn = get_db_connection()
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


async def api_get_pet(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    user_id = get_user_id_by_tg_id(tg_user_id)
    if user_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    pets_columns = get_pets_columns()
    select_fields = ["id", "name", "type", "sex", "birth_date", "chip_number"]
    if "birth_year" in pets_columns:
        select_fields.append("birth_year")

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            f"""
            SELECT {', '.join(select_fields)}
            FROM pets
            WHERE id = %s AND user_id = %s
            """,
            (pet_id, user_id),
        )
        pet_row = db_cursor.fetchone()

    if pet_row is None:
        return web.json_response({"error": "Not found"}, status=404)

    pet_payload = {
        "id": pet_row[0],
        "name": pet_row[1],
        "type": pet_row[2],
        "sex": pet_row[3],
        "birth_date": pet_row[4].isoformat() if pet_row[4] else None,
        "chip_number": pet_row[5],
        "birth_year": None,
    }

    if "birth_year" in pets_columns:
        pet_payload["birth_year"] = pet_row[6]

    return web.json_response({"pet": pet_payload})


async def api_patch_pet(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    user_id = get_user_id_by_tg_id(tg_user_id)
    if user_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    pets_columns = get_pets_columns()
    update_values: dict[str, object] = {}

    if "name" in payload:
        name = str(payload.get("name", "")).strip()
        if not name:
            return web.json_response({"error": "name must be a non-empty string"}, status=400)
        update_values["name"] = name

    if "sex" in payload:
        sex = payload.get("sex")
        if sex not in {"male", "female", "unknown"}:
            return web.json_response({"error": "sex must be male, female or unknown"}, status=400)
        update_values["sex"] = sex

    if "chip_number" in payload:
        chip_number = payload.get("chip_number")
        if chip_number is None:
            update_values["chip_number"] = None
        elif isinstance(chip_number, str):
            update_values["chip_number"] = chip_number.strip() or None
        else:
            return web.json_response({"error": "chip_number must be string or null"}, status=400)

    birth_mode = payload.get("birth_mode") if "birth_mode" in payload else None
    if birth_mode is not None and birth_mode not in {"date", "year"}:
        return web.json_response({"error": "birth_mode must be date or year"}, status=400)

    if birth_mode == "date":
        birth_date = payload.get("birth_date")
        if not isinstance(birth_date, str):
            return web.json_response({"error": "birth_date is required for birth_mode=date"}, status=400)
        try:
            parsed_birth_date = time.strptime(birth_date, "%Y-%m-%d")
        except ValueError:
            return web.json_response({"error": "birth_date must be YYYY-MM-DD"}, status=400)
        update_values["birth_date"] = time.strftime("%Y-%m-%d", parsed_birth_date)
        if "birth_year" in pets_columns:
            update_values["birth_year"] = None

    if birth_mode == "year":
        birth_year = payload.get("birth_year")
        if not isinstance(birth_year, int):
            return web.json_response({"error": "birth_year is required for birth_mode=year"}, status=400)
        if birth_year < 1900 or birth_year > 2100:
            return web.json_response({"error": "birth_year must be between 1900 and 2100"}, status=400)
        if "birth_year" in pets_columns:
            update_values["birth_year"] = birth_year
        update_values["birth_date"] = None

    supported_columns = {"name", "sex", "birth_date", "chip_number", "birth_year"}
    update_values = {key: value for key, value in update_values.items() if key in pets_columns and key in supported_columns}

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute("SELECT 1 FROM pets WHERE id = %s AND user_id = %s", (pet_id, user_id))
        pet_exists = db_cursor.fetchone()

        if pet_exists is None:
            return web.json_response({"error": "Not found"}, status=404)

        if update_values:
            set_clauses = []
            values = []
            for idx, (column, value) in enumerate(update_values.items(), start=1):
                set_clauses.append(f"{column} = %s")
                values.append(value)

            values.extend([pet_id, user_id])
            db_cursor.execute(
                f"""
                UPDATE pets
                SET {', '.join(set_clauses)}
                WHERE id = %s AND user_id = %s
                """,
                tuple(values),
            )

        select_fields = ["id", "name", "type", "sex", "birth_date", "chip_number"]
        if "birth_year" in pets_columns:
            select_fields.append("birth_year")

        db_cursor.execute(
            f"""
            SELECT {', '.join(select_fields)}
            FROM pets
            WHERE id = %s AND user_id = %s
            """,
            (pet_id, user_id),
        )
        pet_row = db_cursor.fetchone()
    conn.commit()

    pet_payload = {
        "id": pet_row[0],
        "name": pet_row[1],
        "type": pet_row[2],
        "sex": pet_row[3],
        "birth_date": pet_row[4].isoformat() if pet_row[4] else None,
        "chip_number": pet_row[5],
        "birth_year": None,
    }
    if "birth_year" in pets_columns:
        pet_payload["birth_year"] = pet_row[6]

    return web.json_response({"ok": True, "pet": pet_payload})


async def api_get_pet_vaccinations(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT id, vaccine_name, date_given, next_due, notes
            FROM vaccinations
            WHERE pet_id = %s
            ORDER BY date_given DESC, id DESC
            """,
            (owned_pet_id,),
        )
        rows = db_cursor.fetchall()

    vaccinations = [
        {
            "id": row[0],
            "vaccine_name": row[1],
            "date_given": row[2].isoformat(),
            "next_due": row[3].isoformat() if row[3] else None,
            "notes": row[4],
        }
        for row in rows
    ]
    return web.json_response({"vaccinations": vaccinations})


async def api_create_pet_vaccination(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    vaccine_name = str(payload.get("vaccine_name", "")).strip()
    if not vaccine_name:
        return web.json_response({"error": "vaccine_name is required"}, status=400)

    date_given_raw = payload.get("date_given")
    if not isinstance(date_given_raw, str):
        return web.json_response({"error": "date_given is required"}, status=400)

    try:
        date_given = date.fromisoformat(date_given_raw)
    except ValueError:
        return web.json_response({"error": "date_given must be YYYY-MM-DD"}, status=400)

    next_due_raw = payload.get("next_due")
    next_due = None
    if next_due_raw is not None:
        if not isinstance(next_due_raw, str):
            return web.json_response({"error": "next_due must be YYYY-MM-DD"}, status=400)
        if next_due_raw.strip() == "":
            next_due = None
        else:
            try:
                next_due = date.fromisoformat(next_due_raw)
            except ValueError:
                return web.json_response({"error": "next_due must be YYYY-MM-DD"}, status=400)

    notes = payload.get("notes")
    if notes is None:
        prepared_notes = None
    elif isinstance(notes, str):
        prepared_notes = notes.strip() or None
    else:
        return web.json_response({"error": "notes must be string"}, status=400)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO vaccinations (pet_id, vaccine_name, date_given, next_due, notes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, vaccine_name, date_given, next_due, notes
            """,
            (owned_pet_id, vaccine_name, date_given, next_due, prepared_notes),
        )
        row = db_cursor.fetchone()
    conn.commit()

    vaccination_payload = {
        "id": row[0],
        "vaccine_name": row[1],
        "date_given": row[2].isoformat(),
        "next_due": row[3].isoformat() if row[3] else None,
        "notes": row[4],
    }
    return web.json_response({"ok": True, "vaccination": vaccination_payload})


async def api_get_pet_weights(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    weights_columns = get_weights_columns()
    date_column, weight_column = get_weights_mapping(weights_columns)
    notes_select = "notes" if "notes" in weights_columns else "NULL AS notes"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            f"""
            SELECT id, {date_column}, {weight_column}, {notes_select}
            FROM weights
            WHERE pet_id = %s
            ORDER BY {date_column} DESC, id DESC
            """,
            (owned_pet_id,),
        )
        rows = db_cursor.fetchall()

    weights = [
        {
            "id": row[0],
            "date": row[1].isoformat(),
            "weight": float(row[2]),
            "notes": row[3],
        }
        for row in rows
    ]
    return web.json_response({"weights": weights})


async def api_create_pet_weight(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    date_raw = payload.get("date")
    if not isinstance(date_raw, str):
        return web.json_response({"error": "date is required"}, status=400)

    try:
        measured_date = date.fromisoformat(date_raw)
    except ValueError:
        return web.json_response({"error": "date must be YYYY-MM-DD"}, status=400)

    raw_weight = payload.get("weight")
    if not isinstance(raw_weight, (int, float)):
        return web.json_response({"error": "weight is required"}, status=400)

    weight_value = float(raw_weight)

    notes = payload.get("notes")
    if notes is None:
        prepared_notes = None
    elif isinstance(notes, str):
        prepared_notes = notes.strip() or None
    else:
        return web.json_response({"error": "notes must be string"}, status=400)

    weights_columns = get_weights_columns()
    date_column, weight_column = get_weights_mapping(weights_columns)

    insert_columns = ["pet_id", date_column, weight_column]
    insert_values = [owned_pet_id, measured_date, weight_value]
    if "notes" in weights_columns:
        insert_columns.append("notes")
        insert_values.append(prepared_notes)

    returning_notes = "notes" if "notes" in weights_columns else "NULL AS notes"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        placeholders = ", ".join(["%s"] * len(insert_columns))
        db_cursor.execute(
            f"""
            INSERT INTO weights ({', '.join(insert_columns)})
            VALUES ({placeholders})
            RETURNING id, {date_column}, {weight_column}, {returning_notes}
            """,
            tuple(insert_values),
        )
        row = db_cursor.fetchone()
    conn.commit()

    weight_payload = {
        "id": row[0],
        "date": row[1].isoformat(),
        "weight": float(row[2]),
        "notes": row[3],
    }
    return web.json_response({"ok": True, "weight": weight_payload})




async def api_get_pet_walks(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    walks_columns = get_walks_columns()
    started_column, duration_column = get_walks_mapping(walks_columns)
    notes_select = "notes" if "notes" in walks_columns else "NULL AS notes"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            f"""
            SELECT id, {started_column}, {duration_column}, {notes_select}
            FROM walks
            WHERE pet_id = %s
            ORDER BY {started_column} DESC, id DESC
            """,
            (owned_pet_id,),
        )
        rows = db_cursor.fetchall()

    walks = [
        {
            "id": row[0],
            "started_at": row[1].isoformat() if row[1] else None,
            "duration_min": int(row[2]),
            "notes": row[3],
        }
        for row in rows
    ]
    return web.json_response({"walks": walks})


async def api_create_pet_walk(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    started_at = parse_walk_started_at(payload.get("started_at"))
    if started_at is None:
        return web.json_response({"error": "started_at is required (ISO datetime)"}, status=400)

    raw_duration = payload.get("duration_min")
    if not isinstance(raw_duration, int) or raw_duration <= 0:
        return web.json_response({"error": "duration_min must be a positive integer"}, status=400)

    notes = payload.get("notes")
    if notes is None:
        prepared_notes = None
    elif isinstance(notes, str):
        prepared_notes = notes.strip() or None
    else:
        return web.json_response({"error": "notes must be string"}, status=400)

    distance_km = payload.get("distance_km")
    if distance_km is not None and not isinstance(distance_km, (int, float)):
        return web.json_response({"error": "distance_km must be a number"}, status=400)

    walks_columns = get_walks_columns()
    started_column, duration_column = get_walks_mapping(walks_columns)

    insert_columns = ["pet_id", started_column, duration_column]
    insert_values: list[object] = [owned_pet_id, started_at, raw_duration]

    if "notes" in walks_columns:
        insert_columns.append("notes")
        insert_values.append(prepared_notes)

    if "distance_km" in walks_columns and distance_km is not None:
        insert_columns.append("distance_km")
        insert_values.append(float(distance_km))

    returning_notes = "notes" if "notes" in walks_columns else "NULL AS notes"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        placeholders = ", ".join(["%s"] * len(insert_columns))
        db_cursor.execute(
            f"""
            INSERT INTO walks ({', '.join(insert_columns)})
            VALUES ({placeholders})
            RETURNING id, {started_column}, {duration_column}, {returning_notes}
            """,
            tuple(insert_values),
        )
        row = db_cursor.fetchone()
    conn.commit()

    walk_payload = {
        "id": row[0],
        "started_at": row[1].isoformat() if row[1] else None,
        "duration_min": int(row[2]),
        "notes": row[3],
    }

    return web.json_response({"ok": True, "walk": walk_payload})

def validate_treatment_type(raw_type: object) -> str | None:
    if not isinstance(raw_type, str):
        return None

    treatment_type = raw_type.strip().lower()
    if treatment_type not in {"fleas", "worms"}:
        return None
    return treatment_type


async def api_get_pet_treatments(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    treatment_type = validate_treatment_type(request.query.get("type"))
    if treatment_type is None:
        return web.json_response({"error": "type must be fleas or worms"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT id, type, product_name, date_given, next_due, notes
            FROM treatments
            WHERE pet_id = %s AND type = %s
            ORDER BY date_given DESC, id DESC
            """,
            (owned_pet_id, treatment_type),
        )
        rows = db_cursor.fetchall()

    treatments = [
        {
            "id": row[0],
            "type": row[1],
            "product_name": row[2],
            "date_given": row[3].isoformat(),
            "next_due": row[4].isoformat() if row[4] else None,
            "notes": row[5],
        }
        for row in rows
    ]
    return web.json_response({"treatments": treatments})


async def api_create_pet_treatment(request: web.Request) -> web.Response:
    try:
        tg_user_id = get_tg_user_id_from_header(request)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    treatment_type = validate_treatment_type(payload.get("type"))
    if treatment_type is None:
        return web.json_response({"error": "type must be fleas or worms"}, status=400)

    product_name = str(payload.get("product_name", "")).strip()
    if not product_name:
        return web.json_response({"error": "product_name is required"}, status=400)

    date_given_raw = payload.get("date_given")
    if not isinstance(date_given_raw, str):
        return web.json_response({"error": "date_given is required"}, status=400)

    try:
        date_given = date.fromisoformat(date_given_raw)
    except ValueError:
        return web.json_response({"error": "date_given must be YYYY-MM-DD"}, status=400)

    next_due_raw = payload.get("next_due")
    next_due = None
    if next_due_raw is not None:
        if not isinstance(next_due_raw, str):
            return web.json_response({"error": "next_due must be YYYY-MM-DD"}, status=400)
        if next_due_raw.strip() == "":
            next_due = None
        else:
            try:
                next_due = date.fromisoformat(next_due_raw)
            except ValueError:
                return web.json_response({"error": "next_due must be YYYY-MM-DD"}, status=400)

    notes = payload.get("notes")
    if notes is None:
        prepared_notes = None
    elif isinstance(notes, str):
        prepared_notes = notes.strip() or None
    else:
        return web.json_response({"error": "notes must be string"}, status=400)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO treatments (pet_id, type, product_name, date_given, next_due, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, type, product_name, date_given, next_due, notes
            """,
            (owned_pet_id, treatment_type, product_name, date_given, next_due, prepared_notes),
        )
        row = db_cursor.fetchone()
    conn.commit()

    treatment_payload = {
        "id": row[0],
        "type": row[1],
        "product_name": row[2],
        "date_given": row[3].isoformat(),
        "next_due": row[4].isoformat() if row[4] else None,
        "notes": row[5],
    }
    return web.json_response({"ok": True, "treatment": treatment_payload})


async def start_web_server() -> None:
    ensure_user_columns()

    app = web.Application()
    app.router.add_get("/", lambda request: web.FileResponse("index.html"))
    app.router.add_get("/health", health)
    app.router.add_post("/api/auth", auth_handler)
    app.router.add_get("/api/pets", api_get_pets)
    app.router.add_post("/api/pets", api_create_pet)
    app.router.add_get("/api/pets/{pet_id}", api_get_pet)
    app.router.add_patch("/api/pets/{pet_id}", api_patch_pet)
    app.router.add_get("/api/pets/{pet_id}/vaccinations", api_get_pet_vaccinations)
    app.router.add_post("/api/pets/{pet_id}/vaccinations", api_create_pet_vaccination)
    app.router.add_get("/api/pets/{pet_id}/weights", api_get_pet_weights)
    app.router.add_post("/api/pets/{pet_id}/weights", api_create_pet_weight)
    app.router.add_get("/api/pets/{pet_id}/walks", api_get_pet_walks)
    app.router.add_post("/api/pets/{pet_id}/walks", api_create_pet_walk)
    app.router.add_get("/api/pets/{pet_id}/treatments", api_get_pet_treatments)
    app.router.add_post("/api/pets/{pet_id}/treatments", api_create_pet_treatment)

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
