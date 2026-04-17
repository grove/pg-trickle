-- =============================================================================
-- pg_trickle Real-time E-commerce Analytics Demo — Schema & Seed Data
--
-- DAG topology (6 stream tables across 3 layers):
--
--   customers ──┐
--   orders ─────┼──→  customer_stats   (L1, DIFFERENTIAL, 1s)
--               │              │
--   categories ─┼──→  category_revenue (L1, DIFFERENTIAL, 1s)
--   products ───┤              │
--               └──→  product_sales    (L1, DIFFERENTIAL, 1s)
--
--   customer_stats ──────────────────────────────→  country_revenue   (L2, DIFFERENTIAL, calc)
--
--   product_catalog ─────────────────────────────→  catalog_price_impact (L2, DIFFERENTIAL, 5s)
--   (slowly-changing; generator updates 1 product price every ~30 cycles)
--   products ────────────────────────────────────→  (same)
--
--   customer_stats ──────────────────────────────→  top_10_customers  (L3, DIFFERENTIAL, calc)
--
-- Differential showcase #1: catalog_price_impact joins ONLY product_catalog
--   (a slowly-changing table: 1 of 15 rows updated per ~30 cycles).
--   Change ratio ≈ 0.07 — Mode Advisor recommends KEEP DIFFERENTIAL.
-- Differential showcase #2: top_10_customers is LIMIT 10 of customer_stats;
--   only rank shifts matter → change ratio ≈ 0.1–0.2.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- ── Base tables ───────────────────────────────────────────────────────────────

CREATE TABLE categories (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT          NOT NULL,
    category_id BIGINT        NOT NULL REFERENCES categories(id),
    base_price  NUMERIC(10,2) NOT NULL
);

CREATE TABLE customers (
    id        BIGSERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    country   TEXT NOT NULL
);

-- Append-only order stream — the generator inserts here continuously.
CREATE TABLE orders (
    id          BIGSERIAL        PRIMARY KEY,
    customer_id BIGINT           NOT NULL REFERENCES customers(id),
    product_id  BIGINT           NOT NULL REFERENCES products(id),
    quantity    INT              NOT NULL DEFAULT 1,
    unit_price  NUMERIC(10,2)   NOT NULL,
    ordered_at  TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Slowly-changing product price catalog.
-- The generator updates one product's current_price every ~30 cycles
-- (≈ once per minute).  The catalog_price_impact stream table joins ONLY
-- this table (not orders), giving a change ratio of ~0.07 instead of 1.0.
CREATE TABLE product_catalog (
    product_id    BIGINT        PRIMARY KEY REFERENCES products(id),
    current_price NUMERIC(10,2) NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ── Seed data: categories ────────────────────────────────────────────────────

INSERT INTO categories (name) VALUES
    ('Electronics'),
    ('Clothing'),
    ('Home & Garden'),
    ('Books'),
    ('Sports'),
    ('Food & Drink'),
    ('Beauty'),
    ('Toys');

-- ── Seed data: products ──────────────────────────────────────────────────────

INSERT INTO products (name, category_id, base_price) VALUES
    ('Wireless Headphones',  1,  79.99),
    ('Smart Watch',          1, 199.99),
    ('USB-C Hub',            1,  39.99),
    ('Running Shoes',        5,  89.99),
    ('Yoga Mat',             5,  34.99),
    ('T-Shirt',              2,  19.99),
    ('Winter Jacket',        2, 119.99),
    ('Coffee Maker',         3,  59.99),
    ('Plant Pot Set',        3,  24.99),
    ('JavaScript Book',      4,  44.99),
    ('Cookbook',             4,  29.99),
    ('Protein Powder',       6,  49.99),
    ('Face Moisturizer',     7,  32.99),
    ('LEGO Set',             8,  64.99),
    ('Puzzle 1000pc',        8,  22.99);

-- ── Seed data: customers (30 rows) ──────────────────────────────────────────

INSERT INTO customers (name, country) VALUES
    ('Alice Chen',        'US'),
    ('Bob Kumar',         'UK'),
    ('Carlos Rivera',     'MX'),
    ('Diana Park',        'KR'),
    ('Ethan Williams',    'US'),
    ('Fatima Al-Rashid',  'AE'),
    ('George Müller',     'DE'),
    ('Hannah Johansson',  'SE'),
    ('Ivan Petrov',       'RU'),
    ('Julia Santos',      'BR'),
    ('Kenji Tanaka',      'JP'),
    ('Layla Hassan',      'EG'),
    ('Marco Rossi',       'IT'),
    ('Nadia Okonkwo',     'NG'),
    ('Oscar Lindberg',    'SE'),
    ('Priya Sharma',      'IN'),
    ('Quentin Dubois',    'FR'),
    ('Rachel Kim',        'KR'),
    ('Samuel Adeyemi',    'NG'),
    ('Tanya Morozova',    'RU'),
    ('Ursula Weber',      'DE'),
    ('Victor Huang',      'CN'),
    ('Wendy Thompson',    'US'),
    ('Xavier Patel',      'IN'),
    ('Yuki Nakamura',     'JP'),
    ('Zara Ahmed',        'PK'),
    ('Aaron Berg',        'US'),
    ('Beatriz Oliveira',  'BR'),
    ('Chen Wei',          'CN'),
    ('Deepak Nair',       'IN');

-- ── Seed data: product_catalog (starts at base_price) ───────────────────────

INSERT INTO product_catalog (product_id, current_price)
SELECT id, base_price FROM products;

-- ── Seed orders (primes stream tables with initial data) ─────────────────────

INSERT INTO orders (customer_id, product_id, quantity, unit_price, ordered_at)
SELECT
    (floor(random() * 30) + 1)::bigint,
    (floor(random() * 15) + 1)::bigint,
    (floor(random() * 3) + 1)::int,
    p.base_price * (0.9 + random() * 0.2),
    now() - (random() * INTERVAL '2 hours')
FROM generate_series(1, 50) gs
JOIN products p ON p.id = (floor(random() * 15) + 1)::bigint;
