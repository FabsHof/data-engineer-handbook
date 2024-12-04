
-- - An incremental query that loads `host_activity_reduced`
--   - day-by-day

INSERT INTO host_activity_reduced
WITH yesterday AS (
    SELECT 
        DATE(month) AS month,
        host,
        hit_array,
        unique_visitors
    FROM host_activity_reduced
    WHERE DATE(month) = DATE('2023-01-01')
),
today AS (
    SELECT
        host,
        ARRAY_AGG(event_time) AS hit_array,
        DATE(event_time) AS date
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
    GROUP BY host, date
),
aggregated_hosts AS (
    SELECT
        COALESCE(t.host, y.host) AS host,
        COALESCE(DATE(y.month), date_trunc('month', t.date)) AS month,
        CASE 
            WHEN t.date IS NOT NULL
                THEN array_remove(y.hit_array, NULL) || ARRAY[1]
            ELSE
                array_remove(y.hit_array, NULL) || ARRAY[0]
        END AS hit_array
    FROM today t
        FULL OUTER JOIN yesterday y ON t.host = y.host
)
SELECT
    host,
    DATE(month) AS month,
    hit_array,
    COUNT(DISTINCT host) AS unique_visitors
FROM 
    aggregated_hosts
GROUP BY
    host, DATE(month), hit_array;