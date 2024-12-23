from chispa.dataframe_comparer import *

from players_scd_job import do_players_scd_transformation
from collections import namedtuple

Player = namedtuple("Player",  "player_name height college country draft_year draft_round draft_number season_stats current_season")

def test_players_scd(spark):
    input_data = [
        # Player with no season stats
        Player(
            player_name="player1",
            height=1.1,
            college="college1",
            country="country1",
            draft_year=1,
            draft_round=1,
            draft_number=1,
            season_stats=[],
            current_season=2001
        ),
        # Player with first year season stats
        Player(
            player_name="player2",
            height=2.2,
            college="college2",
            country="country2",
            draft_year=1,
            draft_round=2,
            draft_number=2,
            season_stats=[
                ("2001", 10, 20, 30, 40)
            ],
            current_season=2001
        ),
        # Player with second year season stats
        Player(
            player_name="player3",
            height=3.3,
            college="college3",
            country="country3",
            draft_year=1,
            draft_round=3,
            draft_number=3,
            season_stats=[
                ("2000", 10, 20, 30, 40)
            ],
            current_season=2000
        ),
        Player(
            player_name="player3",
            height=3.3,
            college="college3",
            country="country3",
            draft_year=2,
            draft_round=3,
            draft_number=3,
            season_stats=[
                ("2001", 10, 20, 30, 40)
            ],
            current_season=2001
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_players_scd_transformation(spark, source_df)

    expected_values = [
        Player(
            player_name="player1",
            height=1.1,
            college="college1",
            country="country1",
            draft_year=1,
            draft_round=1,
            draft_number=1,
            season_stats=[],
            current_season=2001
        ),
        Player(
            player_name="player2",
            height=2.2,
            college="college2",
            country="country2",
            draft_year=1,
            draft_round=2,
            draft_number=2,
            season_stats=[
                ("2001", 10, 20, 30, 40)
            ],
            current_season=2001
        ),
        Player(
            player_name="player3",
            height=3.3,
            college="college3",
            country="country3",
            draft_year=1,
            draft_round=3,
            draft_number=3,
            season_stats=[
                ("2000", 10, 20, 30, 40),
                ("2001", 10, 20, 30, 40)
            ],
            current_season=2001
        )
    ]

    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)