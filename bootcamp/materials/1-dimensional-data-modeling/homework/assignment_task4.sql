/* Task 4
Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

*/

WITH all_actors AS (
    SELECT 
        a.actorid,
        a.actor,
        MIN((a.films[CARDINALITY(a.films)]::film).year) AS start_date,
        MAX((a.films[CARDINALITY(a.films)]::film).year) AS end_date
    FROM actors a
    GROUP BY a.actorid, a.actor
),
aggregated AS (
    SELECT 
        aa.actor,
        aa.actorid,
        LEAST(aa.start_date, MIN((a.films[CARDINALITY(a.films)]::film).year)) AS start_date,
        GREATEST(aa.end_date, MAX((a.films[CARDINALITY(a.films)]::film).year)) AS end_date
        ARRAY_AGG((a.films[CARDINALITY(a.films)]::film)) AS films
    FROM actors a
        JOIN all_actors aa
            ON a.actorid = aa.actorid
    GROUP BY aa.actor, aa.actorid, aa.start_date, aa.end_date
)
SELECT *
FROM aggregated;