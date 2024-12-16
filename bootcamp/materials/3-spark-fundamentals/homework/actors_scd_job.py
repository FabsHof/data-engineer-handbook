from pyspark.sql import SparkSession

query = """

/* Current year films */

WITH this_year_films AS (
    SELECT *
    FROM actor_films
    WHERE year = 1970
),
/* This CTE shows the actor quality by cumulating the ratings made for this actor up to now. */
actor_quality AS (
    SELECT
        a.actorid, 
        (CASE
            WHEN AVG(a.rating) > 8 THEN 'star'
            WHEN AVG(a.rating) > 7 THEN 'good'
            WHEN AVG(a.rating) > 6 THEN 'average'
            ELSE 'bad'
        END)::quality_class AS quality_class
    FROM actor_films a 
        JOIN this_year_films t
            ON a.actorid = t.actorid
    WHERE a.year <= t.year
    GROUP BY a.actorid
),
/* This CTE shows the actor activity by retrieving the last made film. If it is in the current year, the actor is still active. */
actor_activity AS (
    SELECT 
        a.actorid,
        MAX(a.year) = t.year AS is_active
    FROM actor_films a 
        JOIN this_year_films t
            ON a.actorid = t.actorid
    WHERE a.year <= t.year
    GROUP BY a.actorid, a.actor, t.year
)
SELECT 
    t.actor,
    t.actorid,
    ARRAY[ROW(
        t.film,
        t.votes,
        t.rating,
        t.filmid,
        t.year
    )]::film[] AS films,
    q.quality_class,
    true AS is_active
FROM this_year_films t
    JOIN actor_quality q
        ON t.actorid = q.actorid
    JOIN actor_activity a
        ON t.actorid = a.actorid
ORDER BY actorid;
"""

def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actor_films"))
    output_df.write.mode("overwrite").insertInto("actors_scd")