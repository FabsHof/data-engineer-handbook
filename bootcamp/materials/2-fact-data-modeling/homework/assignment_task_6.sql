  
-- The incremental query to generate `host_activity_datelist`

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-03')
),
today AS (
    SELECT
        host,
        ARRAY_AGG(DATE(event_time)) AS host_activity_datelist,
        DATE(event_time) AS date
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-04')
    GROUP BY host, date
)
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity_datelist IS NULL
            THEN ARRAY[t.date]
        WHEN t.date IS NULL
            THEN y.host_activity_datelist
        ELSE ARRAY[t.date] || y.host_activity_datelist
    END AS dates_active,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host;

-- SELECT * FROM hosts_cumulated;