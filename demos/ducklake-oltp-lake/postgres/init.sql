-- Demo E: OLTP-to-Lake Loop — PostgreSQL initialization

CREATE TABLE IF NOT EXISTS orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
