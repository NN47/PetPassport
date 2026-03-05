# PetPassport

## Ежедневные напоминания (Telegram)

Добавлен скрипт `reminders.py` для серверной рассылки напоминаний по записям вакцинаций и обработок.

### Что нужно в окружении

- `BOT_TOKEN`
- `DATABASE_URL`

### Миграция

Создайте таблицу лога уведомлений (защита от дублей в течение суток):

```bash
psql "$DATABASE_URL" -f sql/create_notification_log.sql
```

### Запуск вручную

```bash
python reminders.py
```

### Настройка Render Cron Job

Создайте Cron Job (или отдельный service) с командой:

```bash
python reminders.py
```

Рекомендуемое расписание: ежедневно в `09:00` (по времени сервера Render).

> Скрипт не использует Telegram Mini App auth `initData`, так как это фоновая серверная задача.
