
-- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

-- Event dates are between 2023-01-01 and 2023-01-31
-- SELECT 
--     MIN(event_time) AS min,
--     MAX(event_time) AS max
-- FROM events;

WITH events_per_browser_per_user AS (
    SELECT 
        user_id,
        device_activity_datelist,
        key AS browser_type,
        UNNEST(value) AS event_times
    FROM (
        SELECT *
        FROM 
            user_devices_cumulated, 
            LATERAL jsonb_each(device_activity_datelist)
    ) AS a
)
SELECT *
FROM events_per_browser_per_user;
series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
    SELECT
        CASE
            WHEN ARRAY[DATE(jsonb_to_record(value)::TEXT)] @> ARRAY[DATE (s.series_date)] THEN POW(2, 32 - (e.event_time - DATE (s.series_date)))
            ELSE 0
        END AS placeholder_int_value,
        e.*,
        s.*
    FROM events_per_browser_per_user e
        CROSS JOIN series s
)
SELECT *
FROM placeholder_ints
WHERE user_id = '1156279712734326300';

WITH events_per_user AS (
    SELECT e.user_id, e.event_time, d.browser_type
    FROM events e
        JOIN devices d ON d.device_id = e.device_id
    WHERE
        e.user_id IS NOT NULL
),
aggregated_events AS (
    SELECT
        user_id,
        browser_type,
        ARRAY_AGG(event_time) AS event_times
    FROM events_per_user
    GROUP BY
        user_id,
        browser_type
),
browser_specific_events AS (
    SELECT
        user_id,
        json_object_agg(browser_type, event_times) AS device_activity_datelist
    FROM aggregated_events
    GROUP BY
        user_id
)
-- TODO: Transform the `device_activity_datelist` column into a `datelist_int` column
--      - The `datelist_int` column should be a JSONB column that contains a mapping of `browser_type` to a big integer
--      - The big integer should be a 32-bit integer where each bit represents a day of the month
--      - The least significant bit should represent the first day of the month
--      - The most significant bit should represent the last day of the month
SELECT
    user_id,
    jsonb_object_agg(
        device_activity_datelist->browser_type,
        (
            SELECT
                SUM(POW(2, 32 - (date(event_time) - date_trunc('month', event_time))))
            FROM UNNEST(device_activity_datelist ->event_times) AS event_time
        )
    ) AS datelist_int    
FROM browser_specific_events
GROUP BY
    user_id,
    device_activity_datelist<-browser_type;


