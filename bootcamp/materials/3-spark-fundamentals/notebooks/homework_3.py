#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, hash, max, min, avg, count

# ----------------------------------------
# Spark Session & Configs
# ----------------------------------------

spark = SparkSession.builder.appName("Homework3").getOrCreate()

# Disable broadcast join for large tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Enable bucketing support and grouping preservation for Iceberg
spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")

# ----------------------------------------
# Drop Existing Tables
# ----------------------------------------

spark.sql("DROP TABLE IF EXISTS bootcamp.matches")
spark.sql("DROP TABLE IF EXISTS bootcamp.match_details")
spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players")

# ----------------------------------------
# Create Tables with Bucketing (16 buckets on match_id)
# ----------------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details (
    match_id STRING, 
    player_gamertag STRING, 
    previous_spartan_rank STRING, 
    spartan_rank STRING, 
    previous_total_xp STRING, 
    total_xp STRING, 
    previous_csr_tier STRING, 
    previous_csr_designation STRING, 
    previous_csr STRING, 
    previous_csr_percent_to_next_tier STRING, 
    previous_csr_rank STRING, 
    current_csr_tier STRING, 
    current_csr_designation STRING, 
    current_csr STRING, 
    current_csr_percent_to_next_tier STRING, 
    current_csr_rank STRING, 
    player_rank_on_team STRING, 
    player_finished STRING, 
    player_average_life STRING, 
    player_total_kills STRING, 
    player_total_headshots STRING, 
    player_total_weapon_damage STRING, 
    player_total_shots_landed STRING, 
    player_total_melee_kills STRING, 
    player_total_melee_damage STRING, 
    player_total_assassinations STRING, 
    player_total_ground_pound_kills STRING, 
    player_total_shoulder_bash_kills STRING, 
    player_total_grenade_damage STRING, 
    player_total_power_weapon_damage STRING, 
    player_total_power_weapon_grabs STRING, 
    player_total_deaths STRING,
    player_total_assists STRING, 
    player_total_grenade_kills STRING, 
    did_win STRING, 
    team_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches (
    match_id STRING, 
    mapid STRING, 
    is_team_game STRING, 
    playlist_id STRING, 
    game_variant_id STRING, 
    is_match_over STRING, 
    completion_date STRING, 
    match_duration STRING, 
    game_mode STRING, 
    map_variant_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

# ----------------------------------------
# Read CSVs & Write to Bucketed Iceberg Tables
# ----------------------------------------

def write_bucketed_table(csv_path, table_name):
    df = spark.read.option("header", "true").csv(csv_path)
    df.write \
        .format("iceberg") \
        .bucketBy(16, "match_id") \
        .mode("overwrite") \
        .saveAsTable(table_name)

write_bucketed_table("/home/iceberg/data/matches.csv", "bootcamp.matches")
write_bucketed_table("/home/iceberg/data/match_details.csv", "bootcamp.match_details")
write_bucketed_table("/home/iceberg/data/medals_matches_players.csv", "bootcamp.medals_matches_players")

# ----------------------------------------
# Read Tables
# ----------------------------------------

matches_df = spark.table("bootcamp.matches")
match_details_df = spark.table("bootcamp.match_details")
medals_matches_players_df = spark.table("bootcamp.medals_matches_players")

# ----------------------------------------
# Bucketed Joins
# ----------------------------------------

joined_df = medals_matches_players_df \
    .join(match_details_df, on="match_id", how="inner") \
    .join(matches_df, on="match_id")

joined_df.explain()

# ----------------------------------------
# Broadcast Join with Small Dimension Tables
# ----------------------------------------

medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

joined_df = joined_df \
    .join(broadcast(medals_df), on="medal_id", how="inner") \
    .join(broadcast(maps_df), on="mapid", how="inner")

joined_df.explain()

# ----------------------------------------
# Which player averages the most kills per game?
# ----------------------------------------
most_kills_per_game_df = joined_df \
    .groupBy("match_details.player_gamertag") \
    .agg(avg(col("match_details.player_total_kills")).alias("avg_kills_per_game")) \
    .orderBy(col("avg_kills_per_game").desc())
most_kills_per_game_df.show()

# ----------------------------------------
# Which playlist gets played the most?
# ----------------------------------------
most_played_playlist_df = joined_df \
.groupBy("matches.playlist_id") \
.agg(count("matches.playlist_id").alias("played_count")) \
.orderBy(col("played_count").desc())
most_played_playlist_df.show()
