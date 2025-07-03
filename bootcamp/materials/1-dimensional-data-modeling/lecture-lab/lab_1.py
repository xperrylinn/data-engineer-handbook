import psycopg2
from psycopg2 import sql
import os

# Replace these values with your Docker container's Postgres credentials
# DB_HOST = os.environ["HOST_PORT"]
# DB_PORT = os.environ["CONTAINER_PORT"]
# DB_NAME = os.environ["POSTGRES_DB"]
# DB_USER = os.environ["POSTGRES_USER"]
# DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "postgres"
DB_PASSWORD = "postgres"

try:
    # Establish the connection
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        password=DB_PASSWORD
    )

    # Create a cursor object
    cur = conn.cursor()

    # Run a test query
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("PostgreSQL version:", version)

    # Clean up
    cur.close()
    conn.close()

except Exception as e:
    print("Failed to connect to the database:", e)


ALL_PLAYER_SEASONS = """
SELECT * FROM player_seasons;
"""

CREATE_SEASON_STATS_STRUCT_TYPE = """
    CREATE TYPE season_stats AS (
        season INTEGER,
        gp INTEGER,
        pts REAL,
        reb REAL,
        ast REAL
    )
"""

CREATE_PLAYERS_TABLE = """
CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
)
"""

SELECT_MIN_SEASON_FROM_PLAYER_SEASONS = """
    SELECT MIN(season) FROM player_seasons;
"""

INITIAL_INSERT_INTO_PLAYERS_FROM_1996 = """
    WITH yesterday AS (
        SELECT * FROM players
        WHERE current_season = 1995
    ),
    today AS (
        SELECT * FROM player_seasons
        WHERE season = 1996
    )

    INSERT INTO players 
    SELECT 
        COALESCE(t.player_name, y.player_name) AS player_name,
        COALESCE(t.height, y.height) AS height,
        COALESCE(t.college, y.college) AS college,
        COALESCE(t.country, y.country) AS country,
        COALESCE(t.draft_year, y.draft_year) AS draft_year,
        COALESCE(t.draft_round, y.draft_round) AS draft_round,
        COALESCE(t.draft_number, y.draft_number) AS draft_number,
        CASE WHEN y.season_stats IS NULL 
            THEN ARRAY[ROW(
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats]
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats]
        ELSE y.season_stats
        END AS season_stats,
        COALESCE(t.season, y.current_season + 1) as current_season
    FROM today t FULL OUTER JOIN yesterday y 
    ON t.player_name = y.player_name
"""
