-- Active: 1732272725298@@127.0.0.1@5432@postgres@public
SELECT * FROM player_seasons;

/* Create a new struct type */
CREATE TYPE season_stats AS (
    season INTEGER,
    /* season year */ gp INTEGER,
    /* games played */ pts INTEGER,
    /* points */ reb INTEGER,
    /* rebounds */ ast INTEGER /* assists */
);

/* Static dimensions of players */
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

/* Create a Seed Query: 
 * Via MIN(season) we found out, that 1996 is the minimum season in all datasets.
/* Change yesterday and today season tuples to build a history! y = 1995, t=1996 
*/
INSERT INTO
    players
WITH
    yesterday AS (
        SELECT *
        FROM players
        WHERE
            current_season = 2000
    ),
    today AS (
        SELECT *
        FROM player_seasons
        WHERE
            season = 2001
    )
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(
        t.draft_number,
        y.draft_number
    ) AS draft_number,
    /* Player stats */
    CASE
    /* 1. There is no data from yesterday, only take today's data. */
        WHEN y.season_stats IS NULL THEN ARRAY[
            ROW (
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats
        ]
        /* 2. There is no data from today, only take yesterday's data. */
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[
            ROW (
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats
        ]
        /* 3. There is no more new data from the player (e.g. retired), only carry history. */
        ELSE y.season_stats
    END AS season_stats,
    /* Current season */
    COALESCE(
        t.season,
        y.current_season + 1
    ) AS current_season
FROM today t
    FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;

/* Unnest the historic information to retrieve all the information in single lines! */
WITH
    unnested AS (
        SELECT
            player_name,
            UNNEST(season_stats)::season_stats AS season_stats
        FROM players
        WHERE
            current_season = 2001
            AND player_name = 'Kobe Bryant'
    )
SELECT player_name, (season_stats::season_stats).*
FROM unnested;

/* Do analyses on historical data without group by, which makes it increadibly fast. */
SELECT
    player_name,
    (
        season_stats[CARDINALITY(season_stats)]::season_stats
    ).pts / CASE
        WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
        ELSE (season_stats[1]::season_stats).pts
    END AS pts_growth
FROM players
WHERE
    current_season = 2001;