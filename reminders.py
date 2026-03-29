import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

import psycopg2
from aiogram import Bot

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reminders")


@dataclass
class ReminderItem:
    kind: str
    ref_id: int
    due_date: date
    pet_name: str
    item_name: str
    treatment_type: str | None = None


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def detect_due_column(cursor, table_name: str) -> str:
    cursor.execute(
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
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"No next_due/next_due_date column found in {table_name}")
    return row[0]


def ensure_notification_log(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_log (
            id SERIAL PRIMARY KEY,
            tg_user_id BIGINT NOT NULL,
            kind TEXT NOT NULL,
            ref_id INT NOT NULL,
            due_date DATE NOT NULL,
            sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def fetch_pending_reminders(conn) -> dict[int, dict[str, list[ReminderItem]]]:
    grouped: dict[int, dict[str, list[ReminderItem]]] = defaultdict(lambda: defaultdict(list))

    with conn.cursor() as cursor:
        ensure_notification_log(cursor)
        vaccination_due_col = detect_due_column(cursor, "vaccinations")
        treatment_due_col = detect_due_column(cursor, "treatments")

        vaccination_query = f"""
            SELECT
                u.tg_user_id,
                p.name AS pet_name,
                v.id,
                v.vaccine_name,
                v.{vaccination_due_col}::date AS due_date,
                'vaccination' AS kind
            FROM users u
            JOIN pets p ON p.user_id = u.id
            JOIN vaccinations v ON v.pet_id = p.id
            WHERE v.{vaccination_due_col} IS NOT NULL
              AND v.{vaccination_due_col}::date <= (CURRENT_DATE + INTERVAL '3 day')::date
              AND NOT EXISTS (
                    SELECT 1
                    FROM notification_log nl
                    WHERE nl.tg_user_id = u.tg_user_id
                      AND nl.kind = 'vaccination'
                      AND nl.ref_id = v.id
                      AND nl.due_date = v.{vaccination_due_col}::date
                      AND nl.sent_at::date = CURRENT_DATE
              )
        """
        cursor.execute(vaccination_query)
        for tg_user_id, pet_name, ref_id, vaccine_name, due_date, kind in cursor.fetchall():
            grouped[tg_user_id][pet_name].append(
                ReminderItem(
                    kind=kind,
                    ref_id=ref_id,
                    due_date=due_date,
                    pet_name=pet_name,
                    item_name=vaccine_name,
                )
            )

        treatment_query = f"""
            SELECT
                u.tg_user_id,
                p.name AS pet_name,
                t.id,
                t.product_name,
                t.type,
                t.{treatment_due_col}::date AS due_date,
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
            WHERE t.{treatment_due_col} IS NOT NULL
              AND t.{treatment_due_col}::date <= (CURRENT_DATE + INTERVAL '3 day')::date
              AND NOT EXISTS (
                    SELECT 1
                    FROM notification_log nl
                    WHERE nl.tg_user_id = u.tg_user_id
                      AND nl.kind = CASE
                            WHEN t.type = 'fleas' THEN 'treatment_fleas'
                            WHEN t.type = 'worms' THEN 'treatment_worms'
                          END
                      AND nl.ref_id = t.id
                      AND nl.due_date = t.{treatment_due_col}::date
                      AND nl.sent_at::date = CURRENT_DATE
              )
        """
        cursor.execute(treatment_query)
        for tg_user_id, pet_name, ref_id, product_name, treatment_type, due_date, kind in cursor.fetchall():
            grouped[tg_user_id][pet_name].append(
                ReminderItem(
                    kind=kind,
                    ref_id=ref_id,
                    due_date=due_date,
                    pet_name=pet_name,
                    item_name=product_name,
                    treatment_type=treatment_type,
                )
            )

    return grouped


def due_label(due_date: date, today: date) -> str:
    delta = (due_date - today).days
    if delta < 0:
        return "ПРОСРОЧЕНО"
    if delta == 0:
        return "сегодня"
    if delta == 1:
        return "завтра"
    if delta == 2:
        return "через 2 дня"
    if delta == 3:
        return "через 3 дня"
    return f"через {delta} дн."


def render_item_line(item: ReminderItem, today: date) -> str:
    label = due_label(item.due_date, today)
    if item.kind == "vaccination":
        return f"  💉 Вакцина: {item.item_name} — {label} ({item.due_date.isoformat()})"

    treatment_title = "Обработка"
    if item.treatment_type == "fleas":
        treatment_title = "Блохи"
    elif item.treatment_type == "worms":
        treatment_title = "Глисты"
    return f"  🧼 {treatment_title}: {item.item_name} — {label} ({item.due_date.isoformat()})"


def build_message(per_pet_items: dict[str, list[ReminderItem]], today: date) -> str:
    lines = ["🐾 PetPass — напоминания"]
    for pet_name, items in sorted(per_pet_items.items(), key=lambda kv: kv[0].lower()):
        lines.append(f"🐶 {pet_name}:")
        for item in sorted(items, key=lambda x: (x.due_date, x.kind, x.ref_id)):
            lines.append(render_item_line(item, today))
    return "\n".join(lines)


def store_notification_log(conn, tg_user_id: int, items: list[ReminderItem]) -> None:
    with conn.cursor() as cursor:
        for item in items:
            cursor.execute(
                """
                INSERT INTO notification_log (tg_user_id, kind, ref_id, due_date)
                VALUES (%s, %s, %s, %s)
                """,
                (tg_user_id, item.kind, item.ref_id, item.due_date),
            )
    conn.commit()


async def run() -> None:
    bot_token = get_required_env("BOT_TOKEN")
    database_url = get_required_env("DATABASE_URL")

    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    try:
        reminders = fetch_pending_reminders(conn)
        today = date.today()

        if not reminders:
            logger.info("No reminders to send")
            return

        bot = Bot(token=bot_token)
        try:
            for tg_user_id, per_pet_items in reminders.items():
                flattened = [item for items in per_pet_items.values() for item in items]
                message = build_message(per_pet_items, today)
                try:
                    await bot.send_message(chat_id=tg_user_id, text=message)
                    store_notification_log(conn, tg_user_id, flattened)
                    logger.info("Sent reminder to %s (%s entries)", tg_user_id, len(flattened))
                except Exception:
                    conn.rollback()
                    logger.exception("Failed to send reminder to %s", tg_user_id)
        finally:
            await bot.session.close()
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(run())
