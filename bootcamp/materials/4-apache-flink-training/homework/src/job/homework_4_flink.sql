/*
Create and drop tables as needed
*/
DROP TABLE homework_processed_events;

DROP TABLE homework_processed_events_aggregated;

CREATE TABLE IF NOT EXISTS homework_processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR
);

CREATE TABLE IF NOT EXISTS homework_processed_events_aggregated (
	session_start TIMESTAMP(3),
	session_end TIMESTAMP(3),
	host VARCHAR,
	ip VARCHAR,
	num_events BIGINT
);

/*
Answer these questions
- What is the average number of web events of a session from a user on Tech Creator?
- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
*/

/*
The average number of web events of a session from a user on Tech Creator
*/
SELECT
	host,
	AVG(num_events) AS avg_num_events_per_session
FROM homework_processed_events_aggregated
GROUP BY host;

/*
Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
*/
SELECT
    host,
    SUM(num_events) AS total_events
FROM homework_processed_events_aggregated
WHERE host LIKE '%techcreator.io%'
GROUP BY host
ORDER BY total_events DESC;