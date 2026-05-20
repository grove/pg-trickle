-- Demo C: Multi-Engine Leaderboard — PostgreSQL initialization
-- Sets up source tables and stream tables for the leaderboard demo.

-- Source table
CREATE TABLE IF NOT EXISTS game_scores (
    score_id   BIGSERIAL PRIMARY KEY,
    player_id  INT NOT NULL,
    player_name TEXT NOT NULL,
    game_id    INT NOT NULL,
    score      INT NOT NULL,
    scored_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream table: top players leaderboard (written to DuckLake)
-- Note: In a real deployment with pg_trickle installed, replace this with:
-- SELECT pgtrickle.create_stream_table('top_players', ...);
-- For demo purposes without pg_trickle, we use a materialized view.
CREATE MATERIALIZED VIEW IF NOT EXISTS top_players AS
    SELECT
        player_id,
        player_name,
        SUM(score)  AS total_score,
        COUNT(*)    AS games_played,
        RANK() OVER (ORDER BY SUM(score) DESC) AS rank
    FROM game_scores
    GROUP BY player_id, player_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_top_players_pid ON top_players (player_id);

-- Stream table: scores by game
CREATE MATERIALIZED VIEW IF NOT EXISTS scores_by_game AS
    SELECT
        game_id,
        AVG(score)  AS avg_score,
        MAX(score)  AS high_score,
        COUNT(*)    AS player_count
    FROM game_scores
    GROUP BY game_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_scores_game ON scores_by_game (game_id);
