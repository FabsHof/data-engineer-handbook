# PySpark Testing Homework
# - Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
# - Create new PySpark jobs in `src/jobs` for these queries
# - Create tests in `src/tests` folder with fake input and expected output data

# Query 1: Populate the `actors` table one year at a time:
# INSERT INTO actors
# WITH this_year_films AS (
#     SELECT *
#     FROM actor_films
#     WHERE year = 1970
# ),
# /* This CTE shows the actor quality by cumulating the ratings made for this actor up to now. */
# actor_quality AS (
#     SELECT
#         a.actorid, 
#         (CASE
#             WHEN AVG(a.rating) > 8 THEN 'star'
#             WHEN AVG(a.rating) > 7 THEN 'good'
#             WHEN AVG(a.rating) > 6 THEN 'average'
#             ELSE 'bad'
#         END)::quality_class AS quality_class
#     FROM actor_films a 
#         JOIN this_year_films t
#             ON a.actorid = t.actorid
#     WHERE a.year <= t.year
#     GROUP BY a.actorid
# ),
# /* This CTE shows the actor activity by retrieving the last made film. If it is in the current year, the actor is still active. */
# actor_activity AS (
#     SELECT 
#         a.actorid,
#         MAX(a.year) = t.year AS is_active
#     FROM actor_films a 
#         JOIN this_year_films t
#             ON a.actorid = t.actorid
#     WHERE a.year <= t.year
#     GROUP BY a.actorid, a.actor, t.year
# )
# SELECT 
#     t.actor,
#     t.actorid,
#     ARRAY[ROW(
#         t.film,
#         t.votes,
#         t.rating,
#         t.filmid,
#         t.year
#     )]::film[] AS films,
#     q.quality_class,
#     true AS is_active
# FROM this_year_films t
#     JOIN actor_quality q
#         ON t.actorid = q.actorid
#     JOIN actor_activity a
#         ON t.actorid = a.actorid
# ORDER BY actorid;


