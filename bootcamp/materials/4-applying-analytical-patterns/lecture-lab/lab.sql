INSERT INTO users_growth_accounting
WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = '2023-01-08'   
),
today AS (
    SELECT 
        CAST(user_id AS TEXT) AS user_id,
        DATE_TRUNC('day', event_time::timestamp) AS today_date,
        COUNT(1)
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = '2023-01-09' AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE 
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL 
            THEN 'new'
        WHEN y.last_active_date = t.today_date - INTERVAL '1 day'
            THEN 'retained'
        WHEN y.last_active_date < t.today_date - INTERVAL '1 day'
            THEN 'resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'churned'
        ELSE 'stale'
    END AS daily_active_state,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL 
            THEN 'new'
        WHEN y.last_active_date >= t.today_date - INTERVAL '7 days'
            THEN 'retained'
        WHEN y.last_active_date < t.today_date - INTERVAL '7 days'
            THEN 'resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'churned'
        ELSE 'stale'
    END AS weekly_active_state,
    COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
        CASE 
            WHEN t.user_id IS NOT NULL 
            THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
            END AS date_list
FROM today t 
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;

