import psycopg2


DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

DB_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD
}

def run_sql_file(filepath):
    with open(filepath, 'r') as f:
        sql = f.read()

    # Connect and execute
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                print(f"Executed SQL from {filepath}")
    except Exception as e:
        print(f"Error running {filepath}: {e}")
        raise


try:
    # Establish the connection
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER, 
        password=DB_PASSWORD,
    )

    # Create a cursor object
    cur = conn.cursor()

    # Run a test query
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("PostgreSQL version:", version)

    cur.execute("""
        DROP TABLE IF EXISTS players;
        CREATE TABLE players (
            player_name TEXT,
            height TEXT,
            college TEXT,
            country TEXT,
            draft_year TEXT,
            draft_round TEXT,
            draft_number TEXT,
            seasons season_stats[],
            scoring_class scoring_class,
            years_since_last_active INTEGER,
            is_active BOOLEAN,
            current_season INTEGER,
            PRIMARY KEY (player_name, current_season)
        );
    """)
    conn.commit()
    print("Created players table!")

    cur.close()
    conn.close()

except Exception as e:
    print("Failed to connect to the database:", e)

run_sql_file("./bootcamp/materials/1-dimensional-data-modeling/sql/load_players_table_day2.sql")

run_sql_file("./bootcamp/materials/1-dimensional-data-modeling/lecture-lab/players_scd_table.sql")


"""
DROP TABLE players_scd;

CREATE TABLE players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, start_season)
);

INSERT INTO players_scd
WITH with_previous AS (
	SELECT 
		player_name,
		current_season,
		scoring_class, 
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM players
	WHERE current_season <= 2021
),
with_indicators AS (
	SELECT 
		*, 
		CASE 
			WHEN scoring_class <> previous_scoring_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM with_previous
),
with_streaks AS (
	SELECT
		*,
		SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
	FROM with_indicators
)

SELECT
	player_name,
	scoring_class,
	is_active,
	MIN(current_season) AS start_season,
	MAX(current_season) AS end_season,
	2021 as current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier
"""