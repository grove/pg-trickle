"""
pg_trickle demo — fraud scenario generator.

Inserts ~1 transaction per second with occasional "suspicious burst" patterns
(rapid transactions from the same user at Crypto/Gambling merchants with
escalating amounts) that drive HIGH-risk scores in the fraud detection DAG.

Also rotates one merchant's risk tier every ~30 cycles, illustrating
differential efficiency in merchant_tier_stats (change ratio ~0.07).
"""

import math
import random
import time

import psycopg2

# Per-category (low, high) amount range in USD
CATEGORY_AMOUNTS: dict[str, tuple[float, float]] = {
    "Retail":      (15.0,   300.0),
    "Electronics": (80.0,  1500.0),
    "Travel":     (150.0,  2500.0),
    "Gambling":    (25.0,   600.0),
    "Crypto":      (50.0,  4000.0),
    "Food":         (8.0,    90.0),
    "Pharmacy":    (12.0,   180.0),
}

RISKY_CATEGORIES = {"Crypto", "Gambling"}

TIER_ORDER = ["STANDARD", "ELEVATED", "HIGH"]
TIER_UPDATE_INTERVAL = 30  # rotate one merchant tier every ~N cycles


def fetch_lookups(conn) -> tuple[list[int], list[tuple[int, str]]]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users ORDER BY id")
        users = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT id, category FROM merchants ORDER BY id")
        merchants = [(row[0], row[1]) for row in cur.fetchall()]
    return users, merchants


def sample_amount(category: str, multiplier: float = 1.0) -> float:
    lo, hi = CATEGORY_AMOUNTS.get(category, (20.0, 200.0))
    mu = math.log((lo + hi) / 2.0)
    raw = random.lognormvariate(mu, 0.55) * multiplier
    return round(max(1.0, min(raw, 9_999.99)), 2)


def rotate_merchant_tier(conn, merchant_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tier FROM merchant_risk_tier WHERE merchant_id = %s",
            (merchant_id,),
        )
        row = cur.fetchone()
        if row:
            current = row[0] if row[0] in TIER_ORDER else TIER_ORDER[0]
            new_tier = TIER_ORDER[(TIER_ORDER.index(current) + 1) % len(TIER_ORDER)]
            cur.execute(
                "UPDATE merchant_risk_tier SET tier = %s, updated_at = now() "
                "WHERE merchant_id = %s",
                (new_tier, merchant_id),
            )
            print(f"[TIER]  merchant {merchant_id:>2} → {new_tier}", flush=True)


def insert_txn(conn, user_id: int, merchant_id: int, amount: float) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transactions (user_id, merchant_id, amount) "
            "VALUES (%s, %s, %s) RETURNING id",
            (user_id, merchant_id, amount),
        )
        return cur.fetchone()[0]


def run(conn) -> None:
    users, merchants = fetch_lookups(conn)
    merchant_by_id = {mid: cat for mid, cat in merchants}
    all_ids = [m[0] for m in merchants]
    risky_ids = [m[0] for m in merchants if m[1] in RISKY_CATEGORIES] or all_ids

    print(
        f"[GENERATOR] fraud: {len(users)} users, {len(merchants)} merchants. Starting stream…",
        flush=True,
    )

    cycle = 0
    burst_user: int | None = None
    burst_remaining = 0

    while True:
        cycle += 1

        if burst_remaining == 0 and cycle % 45 == 0:
            burst_user = random.choice(users)
            burst_remaining = random.randint(6, 14)
            print(
                f"[GENERATOR] BURST — user {burst_user} starting "
                f"({burst_remaining} rapid txns)",
                flush=True,
            )

        if cycle % TIER_UPDATE_INTERVAL == 0:
            tier_merchant = random.choice(all_ids)
            try:
                rotate_merchant_tier(conn, tier_merchant)
            except psycopg2.Error as exc:
                print(f"[GENERATOR] Tier update error: {exc}", flush=True)

        try:
            if burst_remaining > 0 and burst_user is not None:
                user_id = burst_user
                merchant_id = random.choice(risky_ids)
                category = merchant_by_id[merchant_id]
                escalation = 1.0 + (burst_remaining / 4.0)
                amount = sample_amount(category, multiplier=escalation)
                burst_remaining -= 1
                if burst_remaining == 0:
                    burst_user = None
                sleep_s = random.uniform(0.15, 0.45)
            else:
                user_id = random.choice(users)
                merchant_id = random.choice(all_ids)
                category = merchant_by_id[merchant_id]
                amount = sample_amount(category)
                sleep_s = random.uniform(0.6, 1.6)

            txn_id = insert_txn(conn, user_id, merchant_id, amount)
            print(
                f"[TXN] id={txn_id:>6}  user={user_id:>2}  "
                f"merchant={merchant_id:>2} ({merchant_by_id[merchant_id]:<14})  "
                f"${amount:>9.2f}",
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
