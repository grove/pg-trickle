"""Score generator for Demo C: Multi-Engine Leaderboard.

Inserts 50 synthetic game score events per second into the `game_scores` table.
Simulates 20 players competing across 5 games.
"""
import os
import random
import time

import psycopg2

DSN = os.environ.get("PG_DSN", "host=localhost dbname=postgres user=postgres password=postgres")

PLAYERS = [
    (i, f"Player_{i}")
    for i in range(1, 21)
]
GAMES = list(range(1, 6))

def main():
    print("Score generator starting …", flush=True)
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    count = 0
    try:
        while True:
            batch = []
            for _ in range(50):
                player_id, player_name = random.choice(PLAYERS)
                game_id = random.choice(GAMES)
                score = random.randint(10, 1000)
                batch.append((player_id, player_name, game_id, score))
            cur.executemany(
                "INSERT INTO game_scores (player_id, player_name, game_id, score) "
                "VALUES (%s, %s, %s, %s)",
                batch,
            )
            count += len(batch)
            if count % 500 == 0:
                print(f"Inserted {count} scores", flush=True)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Generator stopped.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
