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
  most_recent_year_ratings AS (
    SELECT
      af.actor,
      AVG(af.rating) AS most_recent_avg_rating
    FROM actor_films af
    JOIN most_recent_year mry
      ON af.actor = mry.actor AND af.year = mry.most_recent_year
    GROUP BY af.actor
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
    WHEN r.most_recent_avg_rating > 8 THEN 'star'
    WHEN r.most_recent_avg_rating > 7 THEN 'good'
    WHEN r.most_recent_avg_rating > 6 THEN 'average'
    ELSE 'bad'
  END::quality_class AS quality_class,
  (films[CARDINALITY(films)]::film_struct).year = w.year AS is_active,
  w.films
FROM windowed w
LEFT JOIN most_recent_year_ratings r
  ON w.actor = r.actor
ORDER BY w.actor, w.year;
