
-- A DDL for `hosts_cumulated` table 
  -- a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);