from chispa.dataframe_comparer import *

from actors_scd_job import do_actor_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor",  "actor actorid films quality_class is_active")

def test_actors_scd(spark):
    input_data = [
        # Active actor with star quality
        Actor(
            actor="actor1",
            actorid=1,
            films=[
                ("film1", 100, 6.0, 1, 1970)
            ],
            quality_class="average",
            is_active=True
        ),
        Actor(
            actor="actor1",
            actorid=1,
            films=[
                ("film2", 200, 9.0, 2, 1971)
            ],
            quality_class="star",
            is_active=True
        ),
        # Actor with good quality
        Actor(
            actor="actor2",
            actorid=2,
            films=[
                ("film3", 300, 8.0, 3, 1970)
            ],
            quality_class="good",
            is_active=True
        ),
        # Inactive actor with bad quality
        Actor(
            actor="actor3",
            actorid=3,
            films=[],
            quality_class="bad",
            is_active=False
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_actor_scd_transformation(spark, source_df)

    expected_values = [
        Actor(
            actor="actor1",
            actorid=1,
            films=[
                ("film1", 100, 6.0, 1, 1970),
                ("film2", 200, 9.0, 2, 1971)
            ],
            quality_class="star",
            is_active=True
        ),
        Actor(
            actor="actor2",
            actorid=2,
            films=[
                ("film3", 300, 8.0, 3, 1970)
            ],
            quality_class="good",
            is_active=True
        ),
        Actor(
            actor="actor3",
            actorid=3,
            films=[],
            quality_class="bad",
            is_active=False
        )
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)