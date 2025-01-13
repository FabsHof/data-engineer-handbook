
-- The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

-- - A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?

-- SELECT * FROM game_details LIMIT 10;
SELECT 
    details.player_name,
    details.team_id,
    SUM(details.pts) AS total_points,
    COUNT(CASE WHEN details.plus_minus > 0 THEN 1 END) AS total_wins
FROM game_details AS details
GROUP BY GROUPING SETS (
    (details.player_name, details.team_id),
    (details.player_name, details.season),
    (details.team_id)
);