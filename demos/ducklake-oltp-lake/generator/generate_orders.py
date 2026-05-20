"""Order generator for Demo E: OLTP-to-Lake Loop.

Inserts 10 synthetic e-commerce orders per second into the `orders` table.
"""
import os
import random
import time

import psycopg2

DSN = os.environ.get("PG_DSN", "host=localhost dbname=postgres user=postgres password=postgres")

REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]

def main():
    print("Order generator starting …", flush=True)
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    count = 0
    try:
        while True:
            batch = []
            for _ in range(10):
                region = random.choice(REGIONS)
                amount = round(random.uniform(9.99, 499.99), 2)
                batch.append((region, amount))
            cur.executemany(
                "INSERT INTO orders (region, amount) VALUES (%s, %s)",
                batch,
            )
            count += len(batch)
            if count % 100 == 0:
                print(f"Inserted {count} orders", flush=True)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Generator stopped.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
