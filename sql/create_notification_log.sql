CREATE TABLE IF NOT EXISTS notification_log (
    id SERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    ref_id INT NOT NULL,
    due_date DATE NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
