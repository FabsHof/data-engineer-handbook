from chispa.dataframe_comparer import *

from actors_scd_job import do_actor_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor",  "actor actorid films quality_class is_active")

def test_actors_scd(spark):
    input_data = [
        Actor(
            actor="actor1",
            actorid=1,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor2",
            actorid=2,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor3",
            actorid=3,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor4",
            actorid=4,
            films=[],
            quality_class="bad",
            is_active=True
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_actor_scd_transformation(spark, source_df)

    expected_values = [
        Actor(
            actor="actor1",
            actorid=1,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor2",
            actorid=2,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor3",
            actorid=3,
            films=[],
            quality_class="bad",
            is_active=True
        ),
        Actor(
            actor="actor4",
            actorid=4,
            films=[],
            quality_class="bad",
            is_active=True
        )
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)