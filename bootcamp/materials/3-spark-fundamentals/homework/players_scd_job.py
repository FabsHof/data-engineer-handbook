from pyspark.sql import SparkSession

query = """
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
        ]::season_stats[]
        /* 2. There is no data from today, only take yesterday's data. */
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[
            ROW (
                t.season,
                t.gp,
                t.pts,
                t.reb,
                t.ast
            )::season_stats
        ]::season_stats[]
        /* 3. There is no more new data from the player (e.g. retired), only carry history. */
        ELSE y.season_stats
    END AS season_stats,
    /* Current season */
    COALESCE(
        t.season,
        y.current_season + 1
    ) AS current_season
FROM today t
    FULL OUTER JOIN yesterday y ON t.player_name = y.player_name
"""

def do_players_scd_transformation(spark, players_df, player_seasons_df):
    players_df.createOrReplaceTempView("players")
    player_seasons_df.createOrReplaceTempView("player_seasons")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .appName("players_scd") \
        .getOrCreate()
    output_df = do_players_scd_transformation(spark, spark.table("players"), spark.table("player_seasons"))
    output_df.write.mode("overwrite").saveAsTable("players_scd")