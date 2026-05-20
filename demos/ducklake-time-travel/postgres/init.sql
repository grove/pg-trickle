-- Demo B: DuckLake Time-Travel Debugging initialisation
-- Applied at container startup.

-- Install DuckLake extension (requires DuckLake to be pre-installed in the image)
CREATE EXTENSION IF NOT EXISTS ducklake;

-- Initialise a DuckLake catalog
SELECT ducklake_init('demo_lake');

-- Create the events table in DuckLake
CREATE TABLE IF NOT EXISTS lake.events (
    event_id   BIGINT PRIMARY KEY,
    event_type TEXT   NOT NULL,
    user_id    BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Install pg_trickle
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- Create a stream table over the DuckLake change-feed
SELECT pgtrickle.create_stream_table(
    'public',
    'event_summary',
    $$
        SELECT
            date_trunc('minute', created_at) AS minute,
            event_type,
            COUNT(*)                          AS event_count,
            COUNT(DISTINCT user_id)           AS unique_users
        FROM lake.events
        GROUP BY 1, 2
    $$,
    '5s',
    'DIFFERENTIAL'
);
