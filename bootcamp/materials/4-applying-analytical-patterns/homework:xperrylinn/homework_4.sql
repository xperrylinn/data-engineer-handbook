/*
The homework this week will be using the players, players_scd, and player_seasons tables from week 1

A query that does state change tracking for players

A player entering the league should be New
A player leaving the league should be Retired
A player staying in the league should be Continued Playing
A player that comes out of retirement should be Returned from Retirement
A player that stays out of the league should be Stayed Retired

*/

DROP TABLE IF EXISTS players_growth_accounting;

CREATE TABLE players_growth_accounting (
	player_name TEXT,
	first_active_season INTEGER,
	last_active_season INTEGER,
	season_active_state TEXT,
	seasons_active INTEGER[],
	season INTEGER,
	PRIMARY KEY (player_name, season)
);

INSERT INTO players_growth_accounting
WITH last_season AS (
	SELECT
		player_name,
		first_active_season,
		last_active_season,
		seasons_active,
		season AS last_season
	FROM players_growth_accounting
	WHERE season = 2007
),
this_season AS (
SELECT
	player_name,
	is_active,
	current_season AS season,
	(seasons[1]).season AS first_active_season,
	(seasons[cardinality(seasons)]).season AS last_active_season
FROM players
WHERE current_season = 2008
)
SELECT
	COALESCE(ts.player_name, ls.player_name) AS player_name,
	COALESCE(ts.first_active_season, ls.first_active_season) AS first_active_season,
	COALESCE(ts.last_active_season, ls.last_active_season) AS last_active_season,
	CASE
		WHEN ls.player_name IS NULL AND ts.is_active THEN 'New'
		WHEN ls.player_name IS NOT NULL AND ls.last_active_season = ls.last_season AND NOT ts.is_active THEN 'Retired'
		WHEN ls.player_name IS NOT NULL AND ls.last_active_season < ls.last_season AND NOT ts.is_active THEN 'Stayed Retired'
		WHEN ls.player_name IS NOT NULL AND ls.last_active_season < ls.last_season AND ts.is_active THEN 'Returned from Retirement'
		WHEN ls.player_name IS NOT NULL AND ls.last_active_season = ls.last_season AND ts.is_active THEN 'Continued Playing'
		ELSE 'Unknown'
	END AS season_active_state,
	COALESCE(ls.seasons_active, ARRAY[]::INTEGER[]) || CASE
		WHEN ts.player_name IS NOT NULL AND ts.is_active THEN ARRAY[ts.season] 
		ELSE ARRAY[]::INTEGER[]
		END AS seasons_active,
	COALESCE(ts.season, ls.last_season + 1) AS season
FROM this_season ts
FULL OUTER JOIN last_season ls
ON ts.player_name = ls.player_name;

/*
A query that uses GROUPING SETS to do efficient aggregations of game_details data

Aggregate this dataset along the following dimensions
- player and team: Answer questions like who scored the most points playing for one team?
- player and season: Answer questions like who scored the most points in one season?
- team: Answer questions like which team has won the most games?
*/

CREATE TABLE game_stats_cube (
	player_id INTEGER,
	team_id INTEGER,
	season INTEGER,
	team_abbreviation TEXT,
	points_scored INTEGER,
	games_won INTEGER
);

INSERT INTO game_stats_cube
WITH games_won AS (
	SELECT
	  g.game_id AS game_id,
	  team_data.team_id AS team_id,
	  g.season AS season,
	  team_data.game_won AS game_won
	FROM games g
	CROSS JOIN LATERAL (
	  VALUES
	    (g.home_team_id, g.home_team_wins),
	    (g.visitor_team_id, CASE WHEN g.home_team_wins = 0 THEN 1 ELSE 0 END)
	) AS team_data(team_id, game_won)
)
SELECT 
	COALESCE(gd.player_id, 999) AS player_id, -- 999 is used as a placeholder for overall
	COALESCE(gd.team_id, 999) AS team_id,
	COALESCE(g.season, 999) AS season,
	MAX(gd.team_abbreviation) AS team_abbreviation,
	SUM(gd.pts) AS points_scored,
	COUNT(DISTINCT CASE WHEN g.game_won = 1 THEN g.game_id END) AS games_won
FROM game_details AS gd
JOIN games_won AS g
ON gd.game_id = g.game_id AND gd.team_id = g.team_id
GROUP BY GROUPING SETS (
	(gd.player_id, gd.team_id),
	(gd.player_id, g.season),
	(gd.team_id)
)

/*
Who scored the most points playing for one team?

Limit the top 10 scoreers per team
*/
SELECT 
	* 
FROM game_stats_cube
WHERE season = 999 AND player_id != 999 AND points_scored IS NOT NULL
ORDER BY points_scored DESC
LIMIT 10;

/*
Who scored the most points in one season?

Limit the top 10 most points per season
*/
SELECT 
	* 
FROM game_stats_cube
WHERE season != 999 AND player_id != 999 AND points_scored IS NOT NULL
ORDER BY points_scored DESC
LIMIT 10;

/*
Which team has won the most games?
*/
SELECT 
	team_id,
	MAX(team_abbreviation) AS tem_abbreviation,
	SUM(games_won) AS games_won
FROM game_stats_cube
WHERE season = 999 AND player_id = 999 AND points_scored IS NOT NULL
GROUP BY team_id
ORDER BY games_won DESC
LIMIT 1;

