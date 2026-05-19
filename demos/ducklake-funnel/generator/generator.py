#!/usr/bin/env python3
"""
DuckLake Funnel Demo — Event Generator

Inserts synthetic e-commerce events into the events_bridge table at a
configurable rate.  Simulates the write pattern that DuckLake would produce
via its inlined-data feature or postgres_query() bridge.
"""

import os
import random
import time
import psycopg2
from datetime import datetime, timezone

DATABASE_URL   = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/lake_demo")
EVENTS_PER_SEC = int(os.environ.get("EVENTS_PER_SECOND", "50"))
PRODUCT_COUNT  = int(os.environ.get("PRODUCT_COUNT", "20"))
USER_COUNT     = int(os.environ.get("USER_COUNT", "500"))

# Funnel probabilities: view → add_to_cart → purchase
VIEW_RATIO     = 0.60
CART_RATIO     = 0.25
PURCHASE_RATIO = 0.15

BATCH_SIZE = max(1, EVENTS_PER_SEC // 5)   # 5 batches per second


def generate_batch(batch_size: int) -> list[tuple]:
    now = datetime.now(timezone.utc)
    rows = []
    for _ in range(batch_size):
        user_id    = random.randint(1, USER_COUNT)
        product_id = random.randint(1, PRODUCT_COUNT)
        rng        = random.random()
        if rng < PURCHASE_RATIO:
            event_type  = "purchase"
            revenue_usd = round(random.uniform(5.0, 250.0), 2)
        elif rng < PURCHASE_RATIO + CART_RATIO:
            event_type  = "add_to_cart"
            revenue_usd = None
        else:
            event_type  = "view"
            revenue_usd = None
        rows.append((user_id, product_id, event_type, revenue_usd, now))
    return rows


def main() -> None:
    print(f"Connecting to {DATABASE_URL} ...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur  = conn.cursor()
    print(f"Starting event generator: {EVENTS_PER_SEC} events/s, "
          f"{PRODUCT_COUNT} products, {USER_COUNT} users")

    insert_sql = """
        INSERT INTO events_bridge (user_id, product_id, event_type, revenue_usd, occurred_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    sleep_per_batch = BATCH_SIZE / EVENTS_PER_SEC

    total = 0
    while True:
        batch = generate_batch(BATCH_SIZE)
        cur.executemany(insert_sql, batch)
        total += len(batch)
        if total % 1000 == 0:
            print(f"Inserted {total} events", flush=True)
        time.sleep(sleep_per_batch)


if __name__ == "__main__":
    main()
