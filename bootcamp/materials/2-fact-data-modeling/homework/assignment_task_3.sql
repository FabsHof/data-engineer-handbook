
--- A cumulative query to generate `device_activity_datelist` from `events`
INSERT INTO user_devices_cumulated
WITH deduped_events AS (
    SELECT *
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY user_id, event_time ORDER BY event_time) AS row_num,
            *
        FROM events
        WHERE user_id IS NOT NULL
    ) AS e
    WHERE e.row_num = 1
),
deduped_devices AS (
    SELECT *
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY device_id) AS row_num,
            *
        FROM devices
    ) AS d
    WHERE d.row_num = 1
),
events_per_user AS (
    SELECT 
        e.user_id,
        e.event_time,
        d.browser_type,
        d.device_id
    FROM deduped_events e 
        JOIN deduped_devices d 
            ON d.device_id = e.device_id
    WHERE 
        e.row_num = 1 
        AND d.row_num = 1
),
aggregated_events AS (
    SELECT
        user_id,
        browser_type,
        ARRAY_AGG(event_time) AS event_times
    FROM events_per_user
    GROUP BY user_id, browser_type
)
SELECT
    user_id,
    jsonb_object_agg(browser_type, event_times) AS device_activity_datelist
FROM aggregated_events
GROUP BY user_id;

