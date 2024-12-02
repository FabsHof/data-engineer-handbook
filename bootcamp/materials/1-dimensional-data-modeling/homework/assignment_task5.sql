/* Task 5
Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

 */

INSERT INTO actors_history_scd
WITH
    all_actors AS (
        SELECT 
            a.actorid, 
            a.actor, 
            MIN((a.films[CARDINALITY(a.films)]::film).year) AS start_date, 
            MAX((a.films[CARDINALITY(a.films)]::film).year) AS end_date,
            (
                CASE
                    WHEN AVG(af.rating) > 8 THEN 'star'
                    WHEN AVG(af.rating) > 7 THEN 'good'
                    WHEN AVG(af.rating) > 6 THEN 'average'
                    ELSE 'bad'
                END
            )::quality_class AS quality_class,
            MAX( ( a.films[CARDINALITY(a.films)]::film ).year ) = MAX(af.year) AS is_active
        FROM actors a
            JOIN actor_films af
                ON a.actorid = af.actorid
        GROUP BY
            a.actorid,
            a.actor
    ),
    aggregated AS (
        SELECT
            aa.actor,
            aa.actorid,
            LEAST(
                aa.start_date,
                MIN(
                    (
                        a.films[CARDINALITY(a.films)]::film
                    ).year
                )
            ) AS start_date,
            GREATEST(
                aa.end_date,
                MAX(
                    (
                        a.films[CARDINALITY(a.films)]::film
                    ).year
                )
            ) AS end_date,
            ARRAY_AGG(
                (
                    a.films[CARDINALITY(a.films)]::film
                )
            ) AS films,
            MAX(aa.quality_class),
            MAX(aa.is_active)
        FROM actors a
            JOIN all_actors aa ON a.actorid = aa.actorid
        GROUP BY
            aa.actor,
            aa.actorid,
            aa.start_date,
            aa.end_date
    )
SELECT *
FROM aggregated;