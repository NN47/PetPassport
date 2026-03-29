import os
import asyncio
import hashlib
import hmac
import json
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qsl

import psycopg2

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo

from aiohttp import web

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://petpass-aerc.onrender.com/"
BASE_DIR = Path(__file__).parent
ASSETS_DIR = BASE_DIR / "assets"

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
_events_columns_cache: set[str] | None = None
_vaccinations_due_column_cache: str | None = None
_treatments_due_column_cache: str | None = None
ALLOWED_REMIND_DAYS = {1, 3, 7, 14}


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


def ensure_notification_log_table() -> None:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_log (
              id SERIAL PRIMARY KEY,
              tg_user_id BIGINT NOT NULL,
              kind TEXT NOT NULL,
              ref_id INTEGER NOT NULL,
              due_date DATE NOT NULL,
              sent_date DATE NOT NULL,
              created_at TIMESTAMPTZ DEFAULT NOW(),
              UNIQUE (tg_user_id, kind, ref_id, due_date, sent_date)
            )
            """
        )
    conn.commit()


def ensure_user_settings_table() -> None:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_settings (
              id SERIAL PRIMARY KEY,
              tg_user_id BIGINT UNIQUE NOT NULL,
              reminders_enabled BOOLEAN NOT NULL DEFAULT TRUE,
              remind_days INTEGER NOT NULL DEFAULT 3,
              remind_time TEXT NOT NULL DEFAULT '09:00'
            )
            """
        )
    conn.commit()


def ensure_feedings_table() -> None:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feedings (
              id SERIAL PRIMARY KEY,
              pet_id INTEGER NOT NULL REFERENCES pets(id) ON DELETE CASCADE,
              fed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              food TEXT,
              notes TEXT
            )
            """
        )
        db_cursor.execute("ALTER TABLE feedings ADD COLUMN IF NOT EXISTS food TEXT")
    conn.commit()


def is_valid_remind_time(value: str) -> bool:
    try:
        parsed = datetime.strptime(value, "%H:%M")
        return parsed.strftime("%H:%M") == value
    except ValueError:
        return False


def get_or_create_user_settings(tg_user_id: int) -> dict:
    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO user_settings (tg_user_id)
            VALUES (%s)
            ON CONFLICT (tg_user_id) DO NOTHING
            """,
            (tg_user_id,),
        )
        db_cursor.execute(
            """
            SELECT reminders_enabled, remind_days, remind_time
            FROM user_settings
            WHERE tg_user_id = %s
            """,
            (tg_user_id,),
        )
        row = db_cursor.fetchone()
    conn.commit()
    return {
        "reminders_enabled": bool(row[0]),
        "remind_days": int(row[1]),
        "remind_time": str(row[2]),
    }


def get_due_column(table_name: str) -> str:
    global _vaccinations_due_column_cache
    global _treatments_due_column_cache

    if table_name == "vaccinations" and _vaccinations_due_column_cache is not None:
        return _vaccinations_due_column_cache
    if table_name == "treatments" and _treatments_due_column_cache is not None:
        return _treatments_due_column_cache

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
              AND column_name IN ('next_due_date', 'next_due')
            ORDER BY CASE column_name WHEN 'next_due_date' THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (table_name,),
        )
        row = db_cursor.fetchone()

    if row is None:
        raise RuntimeError(f"{table_name} table is missing next_due/next_due_date column")

    due_column = row[0]
    if table_name == "vaccinations":
        _vaccinations_due_column_cache = due_column
    elif table_name == "treatments":
        _treatments_due_column_cache = due_column

    return due_column


def reminder_due_label(due_date: date, today: date) -> str:
    delta = (due_date - today).days
    if delta < 0:
        return "ПРОСРОЧЕНО"
    if delta == 0:
        return "сегодня"
    if delta == 1:
        return "завтра"
    if delta == 2:
        return "через 2 дня"
    return "через 3 дня"


def reminder_icon_and_title(kind: str) -> tuple[str, str]:
    if kind == "vaccination":
        return "💉", "Вакцина"
    if kind == "treatment_fleas":
        return "🧼", "Блохи"
    if kind == "treatment_worms":
        return "🧼", "Глисты"
    return "🔔", "Напоминание"


def build_reminder_message(per_pet_items: dict[str, list[dict]], today: date) -> str:
    lines = ["🐾 PetPass — напоминания", ""]
    for pet_name in sorted(per_pet_items.keys(), key=str.lower):
        lines.append(f"🐶 {pet_name}:")
        items = sorted(per_pet_items[pet_name], key=lambda item: (item["due_date"], item["kind"], item["ref_id"]))
        for item in items:
            icon, title = reminder_icon_and_title(item["kind"])
            due = item["due_date"]
            label = reminder_due_label(due, today)
            if label == "ПРОСРОЧЕНО":
                due_text = f"ПРОСРОЧЕНО ({due.isoformat()})"
            else:
                due_text = f"{label} ({due.isoformat()})"
            lines.append(f"  {icon} {title}: {item['item_name']} — {due_text}")
        lines.append("")

    return "\n".join(lines).strip()


async def check_and_send_reminders(bot_instance: Bot) -> None:
    conn = get_db_connection()
    today = date.today()
    now_hhmm = datetime.now().strftime("%H:%M")
    grouped: dict[int, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

    with conn.cursor() as db_cursor:
        ensure_notification_log_table()
        vaccination_due_column = get_due_column("vaccinations")
        treatments_due_column = get_due_column("treatments")

        db_cursor.execute(
            f"""
            SELECT u.tg_user_id, p.name, v.id, v.vaccine_name, v.{vaccination_due_column}::date AS due_date
            FROM users u
            JOIN pets p ON p.user_id = u.id
            JOIN vaccinations v ON v.pet_id = p.id
            LEFT JOIN user_settings us ON us.tg_user_id = u.tg_user_id
            WHERE v.{vaccination_due_column} IS NOT NULL
              AND COALESCE(us.reminders_enabled, TRUE) = TRUE
              AND COALESCE(us.remind_time, '09:00') = %s
              AND v.{vaccination_due_column}::date <= (CURRENT_DATE + COALESCE(us.remind_days, 3))
            ORDER BY u.tg_user_id, p.name, v.{vaccination_due_column}::date, v.id
            """,
            (now_hhmm,),
        )
        vaccination_rows = db_cursor.fetchall()

        for tg_user_id, pet_name, ref_id, vaccine_name, due_date in vaccination_rows:
            db_cursor.execute(
                """
                INSERT INTO notification_log (tg_user_id, kind, ref_id, due_date, sent_date)
                VALUES (%s, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (tg_user_id, "vaccination", ref_id, due_date),
            )
            inserted = db_cursor.fetchone()
            if inserted is None:
                continue

            grouped[tg_user_id][pet_name].append(
                {
                    "kind": "vaccination",
                    "ref_id": ref_id,
                    "due_date": due_date,
                    "item_name": vaccine_name,
                }
            )

        db_cursor.execute(
            f"""
            SELECT
                u.tg_user_id,
                p.name,
                t.id,
                t.product_name,
                t.type,
                t.{treatments_due_column}::date AS due_date,
                CASE
                    WHEN t.type = 'fleas' THEN 'treatment_fleas'
                    WHEN t.type = 'worms' THEN 'treatment_worms'
                END AS kind
            FROM users u
            JOIN pets p ON p.user_id = u.id
            JOIN (
                SELECT latest.*
                FROM (
                    SELECT
                        t.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.pet_id, t.type
                            ORDER BY t.date_given DESC, t.id DESC
                        ) AS row_num
                    FROM treatments t
                    WHERE t.type IN ('fleas', 'worms')
                ) latest
                WHERE latest.row_num = 1
            ) t ON t.pet_id = p.id
            LEFT JOIN user_settings us ON us.tg_user_id = u.tg_user_id
            WHERE t.{treatments_due_column} IS NOT NULL
              AND COALESCE(us.reminders_enabled, TRUE) = TRUE
              AND COALESCE(us.remind_time, '09:00') = %s
              AND t.{treatments_due_column}::date <= (CURRENT_DATE + COALESCE(us.remind_days, 3))
            ORDER BY u.tg_user_id, p.name, t.{treatments_due_column}::date, t.id
            """,
            (now_hhmm,),
        )
        treatment_rows = db_cursor.fetchall()

        for tg_user_id, pet_name, ref_id, product_name, treatment_type, due_date, kind in treatment_rows:
            db_cursor.execute(
                """
                INSERT INTO notification_log (tg_user_id, kind, ref_id, due_date, sent_date)
                VALUES (%s, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (tg_user_id, kind, ref_id, due_date),
            )
            inserted = db_cursor.fetchone()
            if inserted is None:
                continue

            grouped[tg_user_id][pet_name].append(
                {
                    "kind": kind,
                    "ref_id": ref_id,
                    "due_date": due_date,
                    "item_name": product_name,
                    "treatment_type": treatment_type,
                }
            )

    conn.commit()

    for tg_user_id, per_pet_items in grouped.items():
        message_text = build_reminder_message(per_pet_items, today)
        try:
            await bot_instance.send_message(chat_id=tg_user_id, text=message_text)
            print(f"Reminders sent to {tg_user_id}: {sum(len(items) for items in per_pet_items.values())} items")
        except Exception as exc:
            print(f"Failed to send reminders to {tg_user_id}: {exc}")


async def reminder_loop(bot_instance: Bot) -> None:
    await asyncio.sleep(15)
    while True:
        try:
            await check_and_send_reminders(bot_instance)
        except Exception as exc:
            print("reminder_loop error:", exc)
        await asyncio.sleep(300)


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


def get_events_columns() -> set[str]:
    global _events_columns_cache

    if _events_columns_cache is not None:
        return _events_columns_cache

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'events'
            """
        )
        rows = db_cursor.fetchall()

    _events_columns_cache = {row[0] for row in rows}
    return _events_columns_cache


def get_events_date_column(columns: set[str]) -> str:
    if "event_date" in columns:
        return "event_date"
    if "happened_at" in columns:
        return "happened_at"
    raise RuntimeError("events table is missing event_date/happened_at column")


def parse_event_datetime(raw_event_date: object) -> datetime | None:
    if raw_event_date is None:
        return None
    if not isinstance(raw_event_date, str):
        return None

    event_date_str = raw_event_date.strip()
    if not event_date_str:
        return None

    normalized = event_date_str.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


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

def validate_init_data(init_data: str, bot_token: str, max_age_seconds: int = 86400) -> dict | None:
    if not init_data or not bot_token:
        return None

    try:
        parsed_pairs = parse_qsl(init_data, keep_blank_values=True)
        parsed = dict(parsed_pairs)
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None

        data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed_hash, received_hash):
            return None

        auth_date_raw = parsed.get("auth_date")
        auth_ts = int(auth_date_raw)
        now_ts = int(time.time())
        if auth_ts > now_ts or now_ts - auth_ts > max_age_seconds:
            return None

        raw_user = parsed.get("user")
        if not raw_user:
            return None

        user_payload = json.loads(raw_user)
        tg_user_id = int(user_payload["id"])
        if tg_user_id <= 0:
            return None

        user_payload["tg_user_id"] = tg_user_id
        return user_payload
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None


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


def get_tg_user_id(request: web.Request) -> tuple[int, dict] | None:
    init_data = request.headers.get("X-TG-INIT-DATA", "").strip()
    validated_user = validate_init_data(init_data, BOT_TOKEN)
    if validated_user is not None:
        return validated_user["tg_user_id"], validated_user

    fallback_user_id = request.headers.get("X-TG-USER-ID", "").strip()
    try:
        tg_user_id = int(fallback_user_id)
        if tg_user_id <= 0:
            return None
    except ValueError:
        return None

    return tg_user_id, {"tg_user_id": tg_user_id, "id": tg_user_id}


async def api_get_settings(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, _ = auth_context

    settings = get_or_create_user_settings(tg_user_id)
    return web.json_response(settings)


async def api_patch_settings(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, _ = auth_context

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON object is required"}, status=400)

    updates: dict[str, object] = {}

    if "reminders_enabled" in payload:
        reminders_enabled = payload.get("reminders_enabled")
        if not isinstance(reminders_enabled, bool):
            return web.json_response({"error": "reminders_enabled must be boolean"}, status=400)
        updates["reminders_enabled"] = reminders_enabled

    if "remind_days" in payload:
        remind_days = payload.get("remind_days")
        if not isinstance(remind_days, int) or remind_days not in ALLOWED_REMIND_DAYS:
            return web.json_response({"error": "remind_days must be one of [1, 3, 7, 14]"}, status=400)
        updates["remind_days"] = remind_days

    if "remind_time" in payload:
        remind_time = str(payload.get("remind_time", "")).strip()
        if not is_valid_remind_time(remind_time):
            return web.json_response({"error": "remind_time must be in HH:MM format"}, status=400)
        updates["remind_time"] = remind_time

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            INSERT INTO user_settings (tg_user_id)
            VALUES (%s)
            ON CONFLICT (tg_user_id) DO NOTHING
            """,
            (tg_user_id,),
        )

        if updates:
            set_clause = ", ".join(f"{column} = %s" for column in updates.keys())
            values = list(updates.values()) + [tg_user_id]
            db_cursor.execute(
                f"""
                UPDATE user_settings
                SET {set_clause}
                WHERE tg_user_id = %s
                """,
                values,
            )

    conn.commit()
    return web.json_response(get_or_create_user_settings(tg_user_id))


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
        "Добро пожаловать в дневник питомца!"
    )
    await message.answer("Открывай дневник 👇", reply_markup=kb)


# ---------- Healthcheck HTTP server ----------
async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def auth_handler(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    _, tg_user = auth_context

    user, pets = upsert_user_and_get_pets(tg_user)
    return web.json_response({"user": user, "pets": pets})


async def pets_handler(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    _, tg_user = auth_context

    _, pets = upsert_user_and_get_pets(tg_user)
    return web.json_response({"pets": pets})


async def api_get_pets(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


def _iso_value(raw_value: object) -> str | None:
    if raw_value is None:
        return None
    if hasattr(raw_value, "isoformat"):
        return raw_value.isoformat()
    return str(raw_value)


async def api_get_pet_summary(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    weights_columns = get_weights_columns()
    weight_date_column, weight_value_column = get_weights_mapping(weights_columns)

    walks_columns = get_walks_columns()
    walk_started_column, walk_duration_column = get_walks_mapping(walks_columns)

    events_columns = get_events_columns()
    event_date_column = get_events_date_column(events_columns)
    event_type_select = "type" if "type" in events_columns else "NULL AS type"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT id, name, type
            FROM pets
            WHERE id = %s
            """,
            (owned_pet_id,),
        )
        pet_row = db_cursor.fetchone()

        db_cursor.execute(
            f"""
            SELECT {weight_date_column}, {weight_value_column}
            FROM weights
            WHERE pet_id = %s
            ORDER BY {weight_date_column} DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_weight_row = db_cursor.fetchone()

        db_cursor.execute(
            """
            SELECT date_given, vaccine_name, next_due
            FROM vaccinations
            WHERE pet_id = %s
            ORDER BY date_given DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_vaccination_row = db_cursor.fetchone()

        db_cursor.execute(
            """
            SELECT date_given, product_name, next_due
            FROM treatments
            WHERE pet_id = %s AND type = 'fleas'
            ORDER BY date_given DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_treatment_fleas_row = db_cursor.fetchone()

        db_cursor.execute(
            """
            SELECT date_given, product_name, next_due
            FROM treatments
            WHERE pet_id = %s AND type = 'worms'
            ORDER BY date_given DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_treatment_worms_row = db_cursor.fetchone()

        db_cursor.execute(
            f"""
            SELECT {walk_started_column}, {walk_duration_column}
            FROM walks
            WHERE pet_id = %s
              AND DATE({walk_started_column}) = CURRENT_DATE
            ORDER BY {walk_started_column} DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_walk_row = db_cursor.fetchone()

        db_cursor.execute(
            f"""
            SELECT COALESCE(SUM({walk_duration_column}), 0)
            FROM walks
            WHERE pet_id = %s
              AND DATE({walk_started_column}) = CURRENT_DATE
            """,
            (owned_pet_id,),
        )
        total_walk_duration_today_row = db_cursor.fetchone()

        db_cursor.execute(
            f"""
            SELECT {event_date_column}, {event_type_select}, title
            FROM events
            WHERE pet_id = %s
              AND DATE({event_date_column}) = CURRENT_DATE
            ORDER BY {event_date_column} DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_event_row = db_cursor.fetchone()

        db_cursor.execute(
            """
            SELECT fed_at, notes
            FROM feedings
            WHERE pet_id = %s
              AND DATE(fed_at) = CURRENT_DATE
            ORDER BY fed_at DESC, id DESC
            LIMIT 1
            """,
            (owned_pet_id,),
        )
        latest_feeding_row = db_cursor.fetchone()

    if pet_row is None:
        return web.json_response({"error": "Not found"}, status=404)

    latest_weight = None
    if latest_weight_row is not None:
        latest_weight = {
            "date": _iso_value(latest_weight_row[0]),
            "weight": float(latest_weight_row[1]) if latest_weight_row[1] is not None else None,
        }

    latest_vaccination = None
    if latest_vaccination_row is not None:
        latest_vaccination = {
            "date_given": _iso_value(latest_vaccination_row[0]),
            "vaccine_name": latest_vaccination_row[1],
            "next_due": _iso_value(latest_vaccination_row[2]),
        }

    latest_treatment_fleas = None
    if latest_treatment_fleas_row is not None:
        latest_treatment_fleas = {
            "date_given": _iso_value(latest_treatment_fleas_row[0]),
            "product_name": latest_treatment_fleas_row[1],
            "next_due": _iso_value(latest_treatment_fleas_row[2]),
        }

    latest_treatment_worms = None
    if latest_treatment_worms_row is not None:
        latest_treatment_worms = {
            "date_given": _iso_value(latest_treatment_worms_row[0]),
            "product_name": latest_treatment_worms_row[1],
            "next_due": _iso_value(latest_treatment_worms_row[2]),
        }

    latest_walk = None
    if latest_walk_row is not None:
        latest_walk = {
            "started_at": _iso_value(latest_walk_row[0]),
            "duration_min": int(latest_walk_row[1]) if latest_walk_row[1] is not None else None,
        }

    total_walk_duration_today_min = 0
    if total_walk_duration_today_row is not None and total_walk_duration_today_row[0] is not None:
        total_walk_duration_today_min = int(total_walk_duration_today_row[0])

    latest_event = None
    if latest_event_row is not None:
        latest_event = {
            "event_date": _iso_value(latest_event_row[0]),
            "type": latest_event_row[1],
            "title": latest_event_row[2],
        }

    latest_feeding = None
    if latest_feeding_row is not None:
        latest_feeding = {
            "fed_at": _iso_value(latest_feeding_row[0]),
            "notes": latest_feeding_row[1],
        }

    return web.json_response(
        {
            "pet": {"id": pet_row[0], "name": pet_row[1], "type": pet_row[2]},
            "latest_weight": latest_weight,
            "latest_vaccination": latest_vaccination,
            "latest_treatment_fleas": latest_treatment_fleas,
            "latest_treatment_worms": latest_treatment_worms,
            "latest_walk": latest_walk,
            "total_walk_duration_today_min": total_walk_duration_today_min,
            "latest_event": latest_event,
            "latest_feeding": latest_feeding,
        }
    )


async def api_patch_pet(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


async def api_delete_pet(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    user_id = get_user_id_by_tg_id(tg_user_id)
    if user_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM pets
            WHERE id = %s AND user_id = %s
            RETURNING id
            """,
            (pet_id, user_id),
        )
        deleted_row = db_cursor.fetchone()
    conn.commit()

    if deleted_row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})


async def api_get_pet_vaccinations(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


async def api_delete_pet_vaccination(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    vaccination_id_raw = request.match_info.get("vaccination_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        vaccination_id = int(vaccination_id_raw)
    except ValueError:
        return web.json_response({"error": "vaccination_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM vaccinations
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (vaccination_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})


async def api_get_pet_weights(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


async def api_delete_pet_weight(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    weight_id_raw = request.match_info.get("weight_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        weight_id = int(weight_id_raw)
    except ValueError:
        return web.json_response({"error": "weight_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM weights
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (weight_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})




async def api_get_pet_walks(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


async def api_delete_pet_walk(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    walk_id_raw = request.match_info.get("walk_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        walk_id = int(walk_id_raw)
    except ValueError:
        return web.json_response({"error": "walk_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM walks
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (walk_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})


def validate_event_type(raw_type: object) -> str | None:
    if not isinstance(raw_type, str):
        return None

    event_type = raw_type.strip().lower()
    if event_type not in {"vet", "health", "note", "other"}:
        return None
    return event_type


def encode_event_type_in_title(event_type: str, title: str) -> str:
    if event_type == "note":
        return title

    type_prefix_map = {
        "vet": "VET",
        "health": "HEALTH",
        "other": "OTHER",
    }
    return f"{type_prefix_map.get(event_type, 'NOTE')}: {title}"


async def api_get_pet_events(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    events_columns = get_events_columns()
    event_date_column = get_events_date_column(events_columns)
    type_select = "type" if "type" in events_columns else "NULL AS type"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            f"""
            SELECT id, {type_select}, title, description, {event_date_column}
            FROM events
            WHERE pet_id = %s
            ORDER BY {event_date_column} DESC, id DESC
            """,
            (owned_pet_id,),
        )
        rows = db_cursor.fetchall()

    events_payload = [
        {
            "id": row[0],
            "type": row[1] if row[1] in {"vet", "health", "note", "other"} else "note",
            "title": row[2],
            "description": row[3],
            "event_date": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]

    return web.json_response({"events": events_payload})


async def api_create_pet_event(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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

    event_type = validate_event_type(payload.get("type"))
    if event_type is None:
        return web.json_response({"error": "type must be vet, health, note or other"}, status=400)

    title = str(payload.get("title", "")).strip()
    if not title:
        return web.json_response({"error": "title is required"}, status=400)

    description = payload.get("description")
    if description is None:
        prepared_description = None
    elif isinstance(description, str):
        prepared_description = description.strip() or None
    else:
        return web.json_response({"error": "description must be string"}, status=400)

    raw_event_date = payload.get("event_date")
    if raw_event_date is None:
        event_date = datetime.now()
    else:
        event_date = parse_event_datetime(raw_event_date)
        if event_date is None:
            return web.json_response({"error": "event_date must be ISO datetime or YYYY-MM-DDTHH:MM"}, status=400)

    events_columns = get_events_columns()
    event_date_column = get_events_date_column(events_columns)

    stored_title = title
    if "type" not in events_columns:
        stored_title = encode_event_type_in_title(event_type, title)

    insert_columns = ["pet_id", "title", "description", event_date_column]
    insert_values: list[object] = [owned_pet_id, stored_title, prepared_description, event_date]

    if "type" in events_columns:
        insert_columns.append("type")
        insert_values.append(event_type)

    returning_type = "type" if "type" in events_columns else "NULL AS type"

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        placeholders = ", ".join(["%s"] * len(insert_columns))
        db_cursor.execute(
            f"""
            INSERT INTO events ({', '.join(insert_columns)})
            VALUES ({placeholders})
            RETURNING id, {returning_type}, title, description, {event_date_column}
            """,
            tuple(insert_values),
        )
        row = db_cursor.fetchone()
    conn.commit()

    event_payload = {
        "id": row[0],
        "type": row[1] if row[1] in {"vet", "health", "note", "other"} else event_type,
        "title": row[2],
        "description": row[3],
        "event_date": row[4].isoformat() if row[4] else None,
    }
    return web.json_response({"ok": True, "event": event_payload})


async def api_delete_pet_event(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    event_id_raw = request.match_info.get("event_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        event_id = int(event_id_raw)
    except ValueError:
        return web.json_response({"error": "event_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM events
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (event_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})

def validate_treatment_type(raw_type: object) -> str | None:
    if not isinstance(raw_type, str):
        return None

    treatment_type = raw_type.strip().lower()
    if treatment_type not in {"fleas", "worms"}:
        return None
    return treatment_type


async def api_get_pet_treatments(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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


async def api_delete_pet_treatment(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    treatment_id_raw = request.match_info.get("treatment_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        treatment_id = int(treatment_id_raw)
    except ValueError:
        return web.json_response({"error": "treatment_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM treatments
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (treatment_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})


async def api_get_pet_feedings(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
            SELECT id, fed_at, food, notes
            FROM feedings
            WHERE pet_id = %s
            ORDER BY fed_at DESC, id DESC
            """,
            (owned_pet_id,),
        )
        rows = db_cursor.fetchall()

    feedings = [
        {
            "id": row[0],
            "fed_at": row[1].isoformat() if row[1] else None,
            "food": row[2],
            "notes": row[3],
        }
        for row in rows
    ]
    return web.json_response({"feedings": feedings})


async def api_create_pet_feeding(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

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
        payload = {}

    if not isinstance(payload, dict):
        return web.json_response({"error": "JSON body must be an object"}, status=400)

    fed_at_raw = payload.get("fed_at")
    fed_at = datetime.now()
    if fed_at_raw is not None:
        if not isinstance(fed_at_raw, str):
            return web.json_response({"error": "fed_at must be ISO datetime string"}, status=400)
        try:
            fed_at = datetime.fromisoformat(fed_at_raw)
        except ValueError:
            return web.json_response({"error": "fed_at must be ISO datetime string"}, status=400)

    food = payload.get("food")
    if not isinstance(food, str) or not food.strip():
        return web.json_response({"error": "food is required"}, status=400)
    prepared_food = food.strip()

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
            INSERT INTO feedings (pet_id, fed_at, food, notes)
            VALUES (%s, %s, %s, %s)
            RETURNING id, fed_at, food, notes
            """,
            (owned_pet_id, fed_at, prepared_food, prepared_notes),
        )
        row = db_cursor.fetchone()
    conn.commit()

    feeding_payload = {
        "id": row[0],
        "fed_at": row[1].isoformat() if row[1] else None,
        "food": row[2],
        "notes": row[3],
    }
    return web.json_response({"ok": True, "feeding": feeding_payload})


async def api_delete_pet_feeding(request: web.Request) -> web.Response:
    auth_context = get_tg_user_id(request)
    if auth_context is None:
        return web.json_response({"error": "unauthorized"}, status=401)
    tg_user_id, tg_user = auth_context

    pet_id_raw = request.match_info.get("pet_id", "")
    feeding_id_raw = request.match_info.get("feeding_id", "")

    try:
        pet_id = int(pet_id_raw)
    except ValueError:
        return web.json_response({"error": "pet_id must be an integer"}, status=400)

    try:
        feeding_id = int(feeding_id_raw)
    except ValueError:
        return web.json_response({"error": "feeding_id must be an integer"}, status=400)

    owned_pet_id = get_owned_pet_id(tg_user_id, pet_id)
    if owned_pet_id is None:
        return web.json_response({"error": "Not found"}, status=404)

    conn = get_db_connection()
    with conn.cursor() as db_cursor:
        db_cursor.execute(
            """
            DELETE FROM feedings
            WHERE id = %s AND pet_id = %s
            RETURNING id
            """,
            (feeding_id, owned_pet_id),
        )
        row = db_cursor.fetchone()
    conn.commit()

    if row is None:
        return web.json_response({"error": "Not found"}, status=404)

    return web.json_response({"ok": True})


async def start_web_server() -> None:
    ensure_user_columns()
    ensure_notification_log_table()
    ensure_user_settings_table()
    ensure_feedings_table()

    app = web.Application()
    app.router.add_get("/", lambda request: web.FileResponse("index.html"))
    app.router.add_static("/assets/", path=ASSETS_DIR, name="assets")
    app.router.add_get("/health", health)
    app.router.add_post("/api/auth", auth_handler)
    app.router.add_get("/api/settings", api_get_settings)
    app.router.add_patch("/api/settings", api_patch_settings)
    app.router.add_get("/api/pets", api_get_pets)
    app.router.add_post("/api/pets", api_create_pet)
    app.router.add_get("/api/pets/{pet_id}", api_get_pet)
    app.router.add_get("/api/pets/{pet_id}/summary", api_get_pet_summary)
    app.router.add_patch("/api/pets/{pet_id}", api_patch_pet)
    app.router.add_delete("/api/pets/{pet_id}", api_delete_pet)
    app.router.add_get("/api/pets/{pet_id}/vaccinations", api_get_pet_vaccinations)
    app.router.add_post("/api/pets/{pet_id}/vaccinations", api_create_pet_vaccination)
    app.router.add_delete("/api/pets/{pet_id}/vaccinations/{vaccination_id}", api_delete_pet_vaccination)
    app.router.add_get("/api/pets/{pet_id}/weights", api_get_pet_weights)
    app.router.add_post("/api/pets/{pet_id}/weights", api_create_pet_weight)
    app.router.add_delete("/api/pets/{pet_id}/weights/{weight_id}", api_delete_pet_weight)
    app.router.add_get("/api/pets/{pet_id}/walks", api_get_pet_walks)
    app.router.add_post("/api/pets/{pet_id}/walks", api_create_pet_walk)
    app.router.add_delete("/api/pets/{pet_id}/walks/{walk_id}", api_delete_pet_walk)
    app.router.add_get("/api/pets/{pet_id}/events", api_get_pet_events)
    app.router.add_post("/api/pets/{pet_id}/events", api_create_pet_event)
    app.router.add_delete("/api/pets/{pet_id}/events/{event_id}", api_delete_pet_event)
    app.router.add_get("/api/pets/{pet_id}/treatments", api_get_pet_treatments)
    app.router.add_post("/api/pets/{pet_id}/treatments", api_create_pet_treatment)
    app.router.add_delete("/api/pets/{pet_id}/treatments/{treatment_id}", api_delete_pet_treatment)
    app.router.add_get("/api/pets/{pet_id}/feedings", api_get_pet_feedings)
    app.router.add_post("/api/pets/{pet_id}/feedings", api_create_pet_feeding)
    app.router.add_delete("/api/pets/{pet_id}/feedings/{feeding_id}", api_delete_pet_feeding)

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
    asyncio.create_task(reminder_loop(bot))
    print("Bot started")
    await dp.start_polling(bot)      # polling


if __name__ == "__main__":
    asyncio.run(main())
