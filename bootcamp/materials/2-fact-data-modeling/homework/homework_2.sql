/*
Question 1 

A query to deduplicate game_details from Day 1 so there's no duplicates
*/
WITH deduped_game_details AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS _row_number
	FROM game_details
)
SELECT
	*
FROM deduped_game_details
WHERE _row_number = 1;

/*
Question 2

A DDL for an user_devices_cumulated table that has:
 - a device_activity_datelist which tracks a users active days by browser_type
 - data type here should look similar to MAP<STRING, ARRAY[DATE]> 
 or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/
CREATE TABLE user_devices_cumulated (
	dim_user_id TEXT,
	dim_date DATE,
	device_activity_datelist JSONB,
	PRIMARY KEY (dim_user_id, dim_date)
);


/*
Question 3

A cumulative query to generate device_activity_datelist from events
*/
INSERT INTO user_devices_cumulated
WITH yesterday AS (
	SELECT
		*
	FROM user_devices_cumulated
	WHERE dim_date = DATE('2023-01-30')
),
deduped_devices AS (
	SELECT
		CAST(device_id AS TEXT),
		browser_type,
		ROW_NUMBER() OVER (PARTITION BY device_id) AS row_number
	FROM devices
),
deduped_events AS (
	SELECT
		CAST(device_id AS TEXT) AS device_id,
		CAST(user_id AS TEXT) AS user_id,
		DATE(event_time) AS event_date,
		ROW_NUMBER() OVER (PARTITION BY user_id, event_time) AS row_number
	FROM events
),
devices_and_events AS (
	SELECT
		dd.device_id,
		de.user_id,
		de.event_date, 
		dd.browser_type
	FROM deduped_devices dd
	JOIN deduped_events de
	ON de.device_id = dd.device_id
	WHERE dd.row_number = 1
	AND de.row_number = 1
),
today AS (
	SELECT
		user_id,
		event_date,
		browser_type
	FROM devices_and_events
	WHERE event_date = DATE('2023-01-31')
	AND user_id IS NOT NULL
	GROUP BY user_id, event_date, browser_type
),
today_json AS (
	SELECT
	    COALESCE(t.user_id, y.dim_user_id) AS dim_user_id,
		COALESCE(t.event_date, y.dim_date + INTERVAL '1 day') AS dim_date,
	    CASE
	        WHEN y.device_activity_datelist IS NULL THEN
	            jsonb_build_object(t.browser_type, to_jsonb(ARRAY[t.event_date]))
	        WHEN t.event_date IS NULL THEN
	            y.device_activity_datelist
	        WHEN y.device_activity_datelist ? t.browser_type THEN
	            jsonb_set(
	                y.device_activity_datelist,
	                ARRAY[t.browser_type],
	                to_jsonb((y.device_activity_datelist -> t.browser_type)::jsonb || to_jsonb(t.event_date))
	            )
	        ELSE
	            jsonb_set(
	                y.device_activity_datelist,
	                ARRAY[t.browser_type],
	                to_jsonb(ARRAY[t.event_date])
	            )
	    END AS device_activity_datelist
	FROM today t
	FULL OUTER JOIN yesterday y
	    ON t.user_id = y.dim_user_id
)
SELECT
  dim_user_id,
  dim_date,
  jsonb_object_agg(key, value) AS device_activity_datelist
FROM (
  SELECT
    dim_user_id,
    dim_date,
    key,
    value
  FROM today_json,
  LATERAL jsonb_each(device_activity_datelist)
) flattened
GROUP BY dim_user_id, dim_date;


/*
Question 4

A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
*/
WITH user_devices AS (
	SELECT * 
	FROM user_devices_cumulated
	WHERE dim_date = DATE('2023-01-31')
),
series AS (
	SELECT GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')::DATE AS series_date
),
flattened_activity AS (
	SELECT
		dim_user_id,
		dim_date,
		(
			SELECT array_agg(activity_date::DATE)
			FROM jsonb_each(user_devices.device_activity_datelist) AS kv(browser_type, json_array)
			CROSS JOIN LATERAL jsonb_array_elements_text(json_array) AS activity_date
		) AS all_dates
	FROM user_devices
),
placeholder_ints AS (
	SELECT
		ud.dim_user_id,
		s.series_date,
		CASE
			WHEN s.series_date = ANY(ud.all_dates)
			THEN CAST(POW(2, 31 - (ud.dim_date - s.series_date)) AS BIGINT)
			ELSE 0
		END AS placeholder_int_value
	FROM flattened_activity ud
	CROSS JOIN series s
)
SELECT
	dim_user_id,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS activity_bits,
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS dim_is_monthly_active,
	BIT_COUNT(
		CAST('1000000000000000000000000000000' AS BIT(32)) &
		CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
	) > 0 AS dim_is_daily_active
FROM placeholder_ints
GROUP BY dim_user_id;

/*
Question 5

A DDL for hosts_cumulated table
a host_activity_datelist which logs to see which dates each 
host is experiencing any activity
*/

CREATE TABLE host_cumulated (
	host TEXT,
	date DATE,
	host_activity_datelist DATE[],
	PRIMARY KEY(host, date)
);

/*
Question 6

The incremental query to generate host_activity_datelist
*/
INSERT INTO host_cumulated
WITH yesterday AS (
	SELECT * FROM host_cumulated
	WHERE DATE(date) = DATE('2023-01-09')
),
duduped_events AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY host, event_time) AS row_number
	FROM events
),
deduped_events_acitity_array AS (
	SELECT 
		host,
		DATE(event_time) AS date,
		ARRAY[DATE(MAX(event_time))] AS host_activity_datelist
	FROM duduped_events
	WHERE row_number = 1
	GROUP BY host, date
),
today AS (
	SELECT 
		*
	FROM deduped_events_acitity_array
	WHERE date = DATE('2023-01-10')
)
SELECT
	COALESCE(t.host, y.host) AS host,
	CAST(COALESCE(t.date, y.date + INTERVAL '1 day') AS DATE),
	CASE
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
		WHEN t.date IS NULL THEN y.host_activity_datelist
		ELSE y.host_activity_datelist || ARRAY[t.date]		
	END AS host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host 
AND t.date = y.date