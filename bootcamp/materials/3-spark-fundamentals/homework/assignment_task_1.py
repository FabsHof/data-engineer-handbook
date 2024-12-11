#   Task 1:
# Build a Spark job that:
# Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Explicitly broadcast JOINs medals and maps
# Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
# Aggregate the joined data frame to figure out questions like:
# Which player averages the most kills per game?
# Which playlist gets played the most?
# Which map gets played the most?
# Which map do players get the most Killing Spree medals on?
# With the aggregated data set
# Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("Jupyter").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Broadcast joined data frames of medals and matches
medals_df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/maps.csv")
broadcast_joined_dfs = broadcast(medals_df).join(maps_df, medals_df["mapid"] == maps_df["mapid"])


# Bucket join match_details, matches and medal_matches_players on match_id with 16 buckets
match_details_df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/match_details.csv")
matches_df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/matches.csv")
medal_matches_players_df = spark.read.option("header", "true") \
    .csv("/home/iceberg/data/medal_matches_players.csv")
bucket_joined_dfs = match_details_df.join(matches_df, match_details_df["match_id"] == matches_df["match_id"]) \
    .join(medal_matches_players_df, match_details_df["match_id"] == medal_matches_players_df["match_id"]) \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.bucket_joined_dfs")

# Question 1: Which player averages the most kills per match?
medal_matches_players_df.groupBy("playerid") \
    .agg({"kills": "avg"}) \
    .sort(col("avg(kills)").desc()) \
    .show(1)
# Question 2: Which playlist gets played the most?
matches_df.groupBy("playlistid") \
    .count() \
    .sort(col("count").desc()) \
    .show(1)
# Question 3: Which map gets played the most?
matches_df.groupBy("mapid") \
    .count() \
    .sort(col("count").desc()) \
    .show(1)
# Question 4: Which map do players get the most Killing Sprees medals on?
medals_df.filter(col("medal") == "Killing Sprees") \
    .join(maps_df, medals_df["mapid"] == maps_df["mapid"]) \
    .groupBy("mapid") \
    .count() \
    .sort(col("count").desc()) \
    .show(1)

# Try different .sortWithinPartitions to see which has the smallest data size
maps_df.sortWithinPartitions("mapid").show()
medal_matches_players_df.sortWithinPartitions("playerid").show()
matches_df.sortWithinPartitions("match_id").show()