"""
pg_trickle demo — e-commerce scenario generator.

Demonstrates differential refresh effectiveness with **realistic low-churn**:
  - Steady order stream (0.5–2 orders per second)
  - Modest concentration: 15% of customers, 20% of products drive most activity
  - Infrequent price updates (every ~30-60 minutes)
  - Occasional "flash sale" bursts (every 5-10 minutes)

Result: Stream table aggregates have low-moderate change ratios (~0.2-0.4) where
differential refresh significantly outperforms full refresh in a realistic workload.
"""

import random
import time

import psycopg2

# Price multiplier range (discount/premium) applied during normal orders
PRICE_VARIANCE = 0.10  # ±10% from current catalog price

# Price drift applied when a product "reprices" (slowly-changing dimension)
PRICE_DRIFT_PCT = (-0.10, 0.10)  # –10% to +10% from base_price

PRICE_UPDATE_INTERVAL = 1500  # reprice one product every ~N cycles (~30-60 min)
FLASH_SALE_INTERVAL   = 300   # trigger a flash sale every ~N cycles (~5-10 min)
FLASH_SALE_SIZE       = (8, 16)  # moderate flash sale bursts

# Order generation intervals (seconds) — steady, visible activity
ORDER_INTERVAL_NORMAL = (0.5, 2.0)  # normal orders: every 0.5-2 seconds
ORDER_INTERVAL_FLASH  = (0.10, 0.35)  # flash sale burst: rapid sequence


def fetch_lookups(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM customers ORDER BY id")
        customers = [row[0] for row in cur.fetchall()]

        cur.execute("""
            SELECT p.id, p.base_price, pc.current_price, p.category_id
            FROM products p
            JOIN product_catalog pc ON pc.product_id = p.id
            ORDER BY p.id
        """)
        products = [(row[0], float(row[1]), float(row[2]), row[3]) for row in cur.fetchall()]

        cur.execute("SELECT id FROM categories ORDER BY id")
        categories = [row[0] for row in cur.fetchall()]

    return customers, products, categories


def update_product_price(conn, product_id: int, base_price: float) -> float:
    """Drift one product's current_price within ±20% of its base price."""
    lo = base_price * (1 + PRICE_DRIFT_PCT[0])
    hi = base_price * (1 + PRICE_DRIFT_PCT[1])
    new_price = round(random.uniform(lo, hi), 2)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE product_catalog SET current_price = %s, updated_at = now() "
            "WHERE product_id = %s",
            (new_price, product_id),
        )
    print(
        f"[PRICE] product {product_id:>2}: ${base_price:.2f} → ${new_price:.2f}",
        flush=True,
    )
    return new_price


def insert_order(
    conn,
    customer_id: int,
    product_id: int,
    quantity: int,
    unit_price: float,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, product_id, quantity, unit_price) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (customer_id, product_id, quantity, round(unit_price, 2)),
        )
        return cur.fetchone()[0]


def sample_price(current_price: float) -> float:
    variance = current_price * PRICE_VARIANCE
    return max(0.01, current_price + random.uniform(-variance, variance))


def run(conn) -> None:
    customers, products, categories = fetch_lookups(conn)
    product_by_id = {p[0]: p for p in products}  # id → (id, base, current, cat_id)
    all_product_ids = [p[0] for p in products]

    # Implement realistic concentration: 15% of customers and 20% of products
    # drive most activity. This creates low-moderate change ratios where
    # differential refresh is clearly more efficient than full refresh.
    top_customers_pct = int(max(1, len(customers) * 0.15))  # 15% active
    active_customers = customers[:top_customers_pct]
    
    top_products_pct = int(max(1, len(products) * 0.20))  # 20% active
    active_products = all_product_ids[:top_products_pct]

    print(
        f"[GENERATOR] ecommerce (differential-optimized): "
        f"{len(active_customers)} active customers (of {len(customers)}), "
        f"{len(active_products)} active products (of {len(products)}). "
        f"Steady order stream: 0.5-2 per sec. Starting…",
        flush=True,
    )

    cycle = 0
    flash_category: int | None = None
    flash_remaining = 0
    flash_products: list[int] = []

    while True:
        cycle += 1

        # Flash sale: occasional burst of orders for one category
        if flash_remaining == 0 and cycle % FLASH_SALE_INTERVAL == 0:
            flash_category = random.choice(categories)
            flash_products = [p[0] for p in products if p[3] == flash_category]
            if flash_products:
                flash_remaining = random.randint(*FLASH_SALE_SIZE)
                print(
                    f"[GENERATOR] FLASH SALE — category {flash_category} "
                    f"({flash_remaining} orders)",
                    flush=True,
                )

        # Price update: slowly-changing dimension
        if cycle % PRICE_UPDATE_INTERVAL == 0:
            pid, base, current, cat_id = random.choice(products)
            try:
                new_price = update_product_price(conn, pid, base)
                product_by_id[pid] = (pid, base, new_price, cat_id)
            except psycopg2.Error as exc:
                print(f"[GENERATOR] Price update error: {exc}", flush=True)

        try:
            if flash_remaining > 0 and flash_products:
                # During flash sale: any customer, but flash sale products
                customer_id  = random.choice(customers)
                product_id   = random.choice(flash_products)
                quantity     = random.randint(1, 3)
                _, base, current, _ = product_by_id[product_id]
                # Flash sale = discounted price (70–85% of current)
                unit_price   = current * random.uniform(0.70, 0.85)
                flash_remaining -= 1
                if flash_remaining == 0:
                    flash_category = None
                    flash_products = []
                sleep_s = random.uniform(*ORDER_INTERVAL_FLASH)
            else:
                # Normal orders: prefer active customers and products
                customer_id  = random.choice(active_customers)
                product_id   = random.choice(active_products)
                quantity     = random.randint(1, 2)
                _, base, current, _ = product_by_id[product_id]
                unit_price   = sample_price(current)
                sleep_s = random.uniform(*ORDER_INTERVAL_NORMAL)

            order_id = insert_order(conn, customer_id, product_id, quantity, unit_price)
            print(
                f"[ORDER] id={order_id:>6}  customer={customer_id:>2}  "
                f"product={product_id:>2}  qty={quantity}  "
                f"${unit_price:>8.2f}",
                flush=True,
            )

        except psycopg2.Error as exc:
            print(f"[GENERATOR] Insert error: {exc}", flush=True)
            try:
                conn.close()
            except Exception:
                pass
            raise  # caller reconnects

        time.sleep(sleep_s)
