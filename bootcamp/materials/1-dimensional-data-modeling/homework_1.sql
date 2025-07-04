CREATE TYPE quality_class
AS ENUM('star', 'good', 'average', 'bad');

CREATE TYPE film_struct AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT,
	year INTEGER
);


CREATE TABLE actors (
	actor TEXT,
	year INTEGER,
	quality_class quality_class,
	is_active BOOLEAN,
	films film_struct[]
);

CREATE TABLE actors_history_scd (
	actor TEXT,
	start_date INTEGER,
	end_date INTEGER,
	quality_class quality_class,
	is_active BOOLEAN
);

INSERT INTO actors
WITH 
  years AS (
    SELECT generate_series(1970, 2021) AS year
  ),
  first_actor_year AS (
    SELECT
      actor,
      MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor
  ),
  actors_and_years AS (
    SELECT
      fay.actor,
      y.year
    FROM first_actor_year fay
    JOIN years y
      ON fay.first_year <= y.year
  ),
  most_recent_year AS (
    SELECT
      actor,
      MAX(year) AS most_recent_year
    FROM actor_films
    GROUP BY actor
  ),
  windowed AS (
    SELECT
      aay.actor,
      aay.year,
      ARRAY_AGG(
        ROW(f.film, f.votes, f.rating, f.filmid, f.year)::film_struct
        ORDER BY f.year
      ) FILTER (WHERE f.year <= aay.year) AS films
    FROM actors_and_years aay
    LEFT JOIN actor_films f
      ON f.actor = aay.actor
    GROUP BY aay.actor, aay.year
  )
SELECT
	w.actor,
	w.year,
	CASE
		WHEN (films[CARDINALITY(films)]::film_struct).rating > 8 THEN 'star'
		WHEN (films[CARDINALITY(films)]::film_struct).rating > 7 
			AND (films[CARDINALITY(films)]::film_struct).rating <= 8 THEN 'good'
		WHEN (films[CARDINALITY(films)]::film_struct).rating > 6 
			AND (films[CARDINALITY(films)]::film_struct).rating <= 7 THEN 'good'
		ELSE 'bad'
	END::quality_class AS quality_class,
	(films[CARDINALITY(films)]::film_struct).year = w.year AS is_active,
	w.films
FROM windowed w
ORDER BY w.actor, w.year;

INSERT INTO actors_history_scd
WITH streak_started AS (
    SELECT
        actor,
        year,
        quality_class,
        is_active,
        (
            LAG(quality_class) OVER (PARTITION BY actor ORDER BY year) <> quality_class
            OR LAG(is_active) OVER (PARTITION BY actor ORDER BY year) <> is_active
            OR LAG(quality_class) OVER (PARTITION BY actor ORDER BY year) IS NULL
        ) AS did_change
    FROM actors
	),
	streak_identified AS (
		SELECT
			actor,
			quality_class,
			year,
			is_active,
			SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
				OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
		FROM streak_started
	 ),
     aggregated AS (
		SELECT
			actor,
			MIN(year) AS start_date,
			MAX(year) AS end_date,
			quality_class,
			streak_identifier,
			BOOL_OR(is_active) as is_active
		FROM streak_identified
		GROUP BY actor, quality_class, streak_identifier
     )
	 SELECT actor, start_date, end_date, quality_class, is_active
	 FROM aggregated
	 ORDER BY actor, start_date;
