INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
        AND user_id IS NOT NULL
    GROUP BY user_id, date_active
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE 
        WHEN y.dates_active IS NULL
            THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL 
            THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t 
    FULL OUTER JOIN yesterday y 
    ON t.user_id = y.user_id;


SELECT
    MIN(event_time) AS min,
    MAX(event_time) AS max,
    COUNT(*) AS event_count 
FROM events;

DROP TABLE IF EXISTS users_cumulated;
CREATE TABLE users_cumulated (
    user_id TEXT,
    -- List of dates in the past where the user was active
    dates_active DATE[],
    -- The current date for the user
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- Generate a datelist for one month (31 days). Use the bits of a big integer to flag the dates.
WITH users AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2023-01-31')
),
series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
    SELECT
        -- Syntax to check if the cross join of the current date is included in the series of active dates.
        CASE
            WHEN dates_active @> ARRAY[DATE (series_date)] THEN POW (
                2,
                32 - (date - DATE (series_date))
            )
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users
        CROSS JOIN series
)
SELECT
    user_id,
    -- Each activity date is represented by a number. These are summed up and transformed as a bit value.
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS dim_active_dates,
    BIT_COUNT(CAST( CAST( SUM(placeholder_int_value) AS BIGINT ) AS BIT(32) )) > 0 AS dim_is_monthly_active
FROM placeholder_ints
GROUP BY user_id;
