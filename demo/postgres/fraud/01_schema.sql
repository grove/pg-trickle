-- =============================================================================
-- pg_trickle Real-time Fraud Detection Demo — Schema & Seed Data
--
-- DAG topology (8 stream tables across 3 layers):
--
--   users ──────┐
--   transactions┼──→  user_velocity   (L1, DIFFERENTIAL, 1s)
--               │              │
--   merchants ──┼──→  merchant_stats  (L1, DIFFERENTIAL, 1s)
--               │              │
--               └──→  category_volume (L1, DIFFERENTIAL, 1s)
--
--   user_velocity ─────────────┐
--   merchant_stats ────────────┴──→  risk_scores       (L2, FULL, calculated)
--   transactions ──────────────┘         │
--                                        ├──→  alert_summary        (L3, DIFF, calculated)
--   user_velocity + users ──→  country_risk   (L2, DIFF, calculated)
--                                        └──→  top_risky_merchants  (L3, DIFF, calculated)
--
--   merchant_risk_tier ─────────────────────────────────────────────────────────
--   (slowly-changing lookup; generator rotates 1 row every ~30 cycles)
--   merchant_risk_tier + transactions →  merchant_tier_stats (DIFFERENTIAL, 5s)
--
-- Diamond: transactions feeds BOTH user_velocity AND merchant_stats,
--          which BOTH feed risk_scores — a genuine diamond dependency.
-- Differential showcase: merchant_tier_stats has change ratio ~0.07 because
--   only 1 of 15 merchant rows is touched per cycle.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- ── Base tables ───────────────────────────────────────────────────────────────

CREATE TABLE users (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    country          TEXT NOT NULL,
    account_age_days INT  NOT NULL DEFAULT 365
);

CREATE TABLE merchants (
    id       BIGSERIAL PRIMARY KEY,
    name     TEXT NOT NULL,
    category TEXT NOT NULL,
    country  TEXT NOT NULL
);

-- Append-only transaction stream — the generator inserts here continuously.
CREATE TABLE transactions (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT        NOT NULL REFERENCES users(id),
    merchant_id BIGINT        NOT NULL REFERENCES merchants(id),
    amount      NUMERIC(12,2) NOT NULL,
    txn_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ── Reference data: users ─────────────────────────────────────────────────────

INSERT INTO users (name, country, account_age_days) VALUES
    ('Alice Chen',        'US',  2190),
    ('Bob Kumar',         'UK',  1825),
    ('Carlos Rivera',     'MX',   730),
    ('Diana Park',        'KR',   365),
    ('Ethan Williams',    'US',  1460),
    ('Fatima Al-Rashid',  'AE',   548),
    ('George Müller',     'DE',   900),
    ('Hannah Johansson',  'SE',  1200),
    ('Ivan Petrov',       'RU',   300),
    ('Julia Santos',      'BR',   720),
    ('Kenji Tanaka',      'JP',  1800),
    ('Layla Hassan',      'EG',   450),
    ('Marco Rossi',       'IT',  1100),
    ('Nadia Okonkwo',     'NG',   180),
    ('Oscar Lindberg',    'SE',   980),
    ('Priya Sharma',      'IN',  2000),
    ('Quentin Dubois',    'FR',   670),
    ('Rachel Kim',        'KR',   830),
    ('Samuel Adeyemi',    'NG',   240),
    ('Tanya Morozova',    'RU',  1350),
    ('Ursula Weber',      'DE',   500),
    ('Victor Huang',      'CN',  1600),
    ('Wendy Thompson',    'US',   760),
    ('Xavier Patel',      'IN',   420),
    ('Yuki Nakamura',     'JP',  1950),
    ('Zara Ahmed',        'PK',   300),
    ('Aaron Berg',        'US',  1100),
    ('Beatriz Oliveira',  'BR',   560),
    ('Chen Wei',          'CN',  2100),
    ('Deepak Nair',       'IN',   880);

-- ── Reference data: merchants ─────────────────────────────────────────────────

INSERT INTO merchants (name, category, country) VALUES
    ('Amazon',             'Retail',       'US'),
    ('Apple Store',        'Electronics',  'US'),
    ('Expedia',            'Travel',       'US'),
    ('Airbnb',             'Travel',       'US'),
    ('BetOnSports',        'Gambling',     'MT'),
    ('CryptoExchange Pro', 'Crypto',       'SG'),
    ('McDonald''s',        'Food',         'US'),
    ('Walgreens',          'Pharmacy',     'US'),
    ('Samsung Shop',       'Electronics',  'KR'),
    ('Booking.com',        'Travel',       'NL'),
    ('Lucky Casino',       'Gambling',     'GI'),
    ('BitSwap',            'Crypto',       'BS'),
    ('Walmart',            'Retail',       'US'),
    ('Uber Eats',          'Food',         'US'),
    ('Best Buy',           'Electronics',  'US');

-- ── Slowly-changing merchant risk tier lookup ────────────────────────────────
-- The generator updates one merchant's tier every ~30 cycles (≈ once per
-- minute). The merchant_tier_stats stream table JOINs against this table
-- using DIFFERENTIAL refresh, so only the affected merchant's row is
-- re-processed — giving a change ratio of ~0.07 instead of 1.0.
CREATE TABLE merchant_risk_tier (
    merchant_id  BIGINT      PRIMARY KEY REFERENCES merchants(id),
    tier         TEXT        NOT NULL DEFAULT 'STANDARD'
                             CHECK (tier IN ('STANDARD', 'ELEVATED', 'HIGH')),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO merchant_risk_tier (merchant_id, tier) VALUES
    (1,  'STANDARD'),   -- Amazon
    (2,  'STANDARD'),   -- Apple Store
    (3,  'STANDARD'),   -- Expedia
    (4,  'STANDARD'),   -- Airbnb
    (5,  'ELEVATED'),   -- BetOnSports
    (6,  'HIGH'),       -- CryptoExchange Pro
    (7,  'STANDARD'),   -- McDonald's
    (8,  'STANDARD'),   -- Walgreens
    (9,  'STANDARD'),   -- Samsung Shop
    (10, 'STANDARD'),   -- Booking.com
    (11, 'ELEVATED'),   -- Lucky Casino
    (12, 'HIGH'),       -- BitSwap
    (13, 'STANDARD'),   -- Walmart
    (14, 'STANDARD'),   -- Uber Eats
    (15, 'STANDARD');   -- Best Buy

-- ── Seed transactions (primes the stream tables with initial data) ─────────────

INSERT INTO transactions (user_id, merchant_id, amount, txn_at)
SELECT
    (floor(random() * 30) + 1)::bigint,
    (floor(random() * 15) + 1)::bigint,
    ROUND((random() * 280 + 20)::numeric, 2),
    now() - (random() * INTERVAL '2 hours')
FROM generate_series(1, 40);
