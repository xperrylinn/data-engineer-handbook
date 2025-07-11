/*
Scrath queries
*/
SELECT * FROM game_details;


/*
Question 1 

A query to deduplicate game_details from Day 1 so there's no duplicates
*/
WITH deduped_game_detials AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS _row_number
	FROM game_details
)
SELECT
	*
FROM deduped_game_detials
WHERE _row_number = 1;

/*
Question 2

A DDL for an user_devices_cumulated table that has:
 - a device_activity_datelist which tracks a users active days by browser_type
 - data type here should look similar to MAP<STRING, ARRAY[DATE]> 
 or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/
-- Non-map version
CREATE TABLE user_devices_cumulated_jsonb (
	dim_device_id NUMERIC,
	dim_user_id NUMERIC,
	browser_type_activity_map JSONB
);
-- Map version
CREATE TABLE user_devices_cumulated (
	dim_device_id TEXT,
	dim_user_id TEXT,
	dim_event_time TIMESTAMP,
	m_browser_type TEXT
);

DROP TABLE user_devices_cumulated;

DROP TABLE user_devices_cumulated_jsonb;

/*
Question 3

A cumulative query to generate device_activity_datelist from events
*/
-- This query is NOT cummlative as it only tells you about activity on a given day
INSERT INTO user_devices_cumulated
WITH deduped_devices AS (
	SELECT
		CAST(device_id AS TEXT),
		browser_type,
		ROW_NUMBER() OVER (PARTITION BY device_id) AS row_number
	FROM devices
),
date_series AS (
	SELECT GENERATE_SERIES('2023-01-01', '2023-01-31', INTERVAL '1 day')::DATE AS date
),
deduped_events AS (
	SELECT
		CAST(device_id AS TEXT) AS device_id,
		CAST(user_id AS TEXT) AS user_id,
		DATE(event_time) AS event_date,
		ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_number
	FROM events
),
deduped_events_and_series AS (
	SELECT
		device_id,
		user_id,
		date,
		event_date,
		row_number
	FROM deduped_events
	JOIN date_series
	ON deduped_events.event_date <= date_series.date
)
SELECT
	dd.device_id AS dim_device_id,
	de.user_id AS dim_user_id,
	dd.browser_type as dim_browser_type,
	date AS dim_date,
	de.event_date AS dim_event_date 
FROM deduped_devices dd
JOIN deduped_events_and_series de
ON de.device_id = dd.device_id
WHERE dd.row_number = 1
AND de.row_number = 1
AND de.user_id = '1527356772363511300';


/*
Scrath queries
*/

SELECT * FROM game_details;

SELECT * FROM events;

SELECT * FROM devices;

SELECT * from user_devices_cumulated
WHERE dim_user_id = '444502572952128450';

