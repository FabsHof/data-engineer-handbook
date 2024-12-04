
-- - A monthly, reduced fact table DDL `host_activity_reduced`
--    - month
--    - host
--    - hit_array - think COUNT(1)
--    - unique_visitors array -  think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array REAL[],
    unique_visitors REAL[],
    PRIMARY KEY (month, host)
);