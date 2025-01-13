-- The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

-- - A query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`

-- SELECT * FROM players LIMIT 10;
WITH player_changes AS (
    SELECT 
        player_name,
        current_season,
        LAG(current_season) OVER (PARTITION BY player_name ORDER BY current_season) AS prev_season,
        LEAD(current_season) OVER (PARTITION BY player_name ORDER BY current_season) AS next_season
    FROM players
),
state_changes AS (
    SELECT 
        player_name,
        current_season AS season,
        CASE 
            WHEN prev_season IS NULL THEN 'New'
            WHEN next_season IS NULL THEN 'Retired'
            WHEN current_season - prev_season = 1 THEN 'Continued Playing'
            WHEN current_season - prev_season > 1 THEN 'Returned from Retirement'
            WHEN prev_season IS NOT NULL AND next_season IS NOT NULL AND current_season - prev_season > 1 THEN 'Stayed Retired'
            ELSE 'Stayed Retired'
        END AS state_change
    FROM player_changes
)
SELECT * FROM state_changes;


-- - A query that does state change tracking for `player_seasons`
-- SELECT * FROM player_seasons LIMIT 10;
WITH player_season_changes AS (
    SELECT 
        player_name,
        season,
        LAG(season) OVER (PARTITION BY player_name ORDER BY season) AS prev_season,
        LEAD(season) OVER (PARTITION BY player_name ORDER BY season) AS next_season
    FROM player_seasons
),
season_state_changes AS (
    SELECT 
        player_name,
        season,
        CASE 
            WHEN prev_season IS NULL AND next_season IS NOT NULL THEN 'New'
            WHEN prev_season IS NOT NULL AND next_season IS NULL THEN 'Retired'
            WHEN season - prev_season = 1 THEN 'Continued Playing'
            WHEN prev_season IS NOT NULL AND next_season IS NOT NULL AND season - prev_season > 1 THEN 'Returned from Retirement'
            WHEN prev_season IS NOT NULL AND next_season IS NOT NULL AND season - prev_season > 1 AND next_season - season > 1 THEN 'Stayed Retired'
            ELSE 'Stayed Retired'
        END AS state_change
    FROM player_season_changes
)
SELECT * FROM season_state_changes;





