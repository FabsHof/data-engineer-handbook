
-- The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

-- - A query that uses window functions on `game_details` to find out the following things:
--   - What is the most games a team has won in a 90 game stretch? 
--   - How many games in a row did LeBron James score over 10 points a game?

-- Most games a team has won in a 90 game stretch
SELECT team_id, MAX(win_count) AS max_wins_in_90_games
FROM (
    SELECT team_id, 
           SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) 
           OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS win_count
    FROM game_details
) subquery
GROUP BY team_id;

-- How many games in a row did LeBron James score over 10 points a game
SELECT player_id, MAX(streak_length) AS max_streak
FROM (
    SELECT player_id,
           game_date,
           points,
           CASE WHEN points > 10 THEN 1 ELSE 0 END AS over_10_points,
           CASE WHEN points > 10 AND LAG(points, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date) <= 10 THEN 1 ELSE 0 END AS new_streak,
           SUM(CASE WHEN points > 10 AND LAG(points, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date) <= 10 THEN 1 ELSE 0 END) 
           OVER (PARTITION BY player_id ORDER BY game_date ROWS UNBOUNDED PRECEDING) AS streak_group,
           COUNT(*) OVER (PARTITION BY player_id, 
                          SUM(CASE WHEN points > 10 AND LAG(points, 1, 0) OVER (PARTITION BY player_id ORDER BY game_date) <= 10 THEN 1 ELSE 0 END) 
                          OVER (PARTITION BY player_id ORDER BY game_date ROWS UNBOUNDED PRECEDING)) AS streak_length
    FROM game_details
    WHERE player_name = 'LeBron James'
) subquery
WHERE over_10_points = 1
GROUP BY player_id, streak_group;
