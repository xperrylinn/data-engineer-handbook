# Conversion of a Jupyter-based PySpark notebook to a standalone .py script

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, hash, max, min, avg, count, first

# Create Spark Session
spark = SparkSession.builder.appName("Homework3").getOrCreate()

# Disable Automatic Broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Enable bucketing and preserve data grouping for Iceberg
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')

# Drop existing tables (if they exist)
spark.sql("DROP TABLE IF EXISTS bootcamp.matches")
spark.sql("DROP TABLE IF EXISTS bootcamp.match_details")
spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players")
spark.sql("DROP TABLE IF EXISTS bootcamp.aggregated_data")

# Create necessary tables
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count INTEGER
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
    player_total_kills INTEGER,
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

# Load CSV data and write to tables
matches_df = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
matches_df.write.format("iceberg").bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.matches")

match_details_df = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv").withColumnRenamed("player_gamertag", "match_details_player_gamertag")
match_details_df.write.format("iceberg").bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.match_details")

medals_matches_players_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv").withColumnRenamed("player_gamertag", "medals_matches_players_player_gamertag")
medals_matches_players_df.write.format("iceberg").bucketBy(16, "match_id").mode("overwrite").saveAsTable("bootcamp.medals_matches_players")

# Read from tables
matches_df = spark.table("bootcamp.matches")
match_details_df = spark.table("bootcamp.match_details")
medal_matches_players_df = spark.table("bootcamp.medals_matches_players")

# Perform join
joined_df = medal_matches_players_df.join(match_details_df, on="match_id", how="inner")
joined_df = joined_df.join(matches_df, on="match_id")

# Read medals and maps CSVs
medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv") \
    .withColumnRenamed("name", "medal_name").withColumnRenamed("description", "medal_description")

maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv") \
    .withColumnRenamed("name", "map_name").withColumnRenamed("description", "map_description")

# Broadcast join
joined_df = joined_df.join(broadcast(medals_df), on="medal_id", how="inner")
joined_df = joined_df.join(broadcast(maps_df), on="mapid", how="inner")

# Aggregations
most_kills_per_game_df = joined_df.groupBy("match_details.match_details_player_gamertag") \
    .agg(avg(col("match_details.player_total_kills")).alias("avg_kills_per_game")) \
    .orderBy(col("avg_kills_per_game").desc())
most_kills_per_game_df.show()

most_played_playlist_df = joined_df.groupBy("matches.playlist_id") \
    .agg(count("matches.playlist_id").alias("played_count")) \
    .orderBy(col("played_count").desc())
most_played_playlist_df.show()

most_played_maps_df = joined_df.groupBy("mapid") \
    .agg(count("*").alias("played_count"), first("map_name").alias("map_name")) \
    .orderBy(col("played_count").desc())
most_played_maps_df.show()

most_kill_spree_per_map_maps_df = joined_df.filter(col("medal_name") == "Killing Spree") \
    .groupBy("mapid", "medal_id") \
    .agg(avg("count").alias("avg_killing_spree"), first("map_name").alias("map_name")) \
    .orderBy(col("avg_killing_spree").desc())
most_kill_spree_per_map_maps_df.show()

# Create output table
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.aggregated_data (
    mapid STRING,
    medal_id STRING,
    match_id STRING,
    medals_matches_players_player_gamertag STRING,
    count STRING,
    match_details_player_gamertag STRING,
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
    team_id STRING,
    is_team_game STRING,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over STRING,
    completion_date STRING,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    sprite_uri STRING,
    sprite_left STRING,
    sprite_top STRING,
    sprite_sheet_width STRING,
    sprite_sheet_height STRING,
    sprite_width STRING,
    sprite_height STRING,
    classification STRING,
    medal_description STRING,
    medal_name STRING,
    difficulty STRING,
    map_name STRING,
    map_description STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")

# Try different partition strategies
for cols in [["matches.playlist_id"], ["matches.mapid"], ["matches.playlist_id", "matches.mapid"], ["matches.match_id"]]:
    joined_df.repartition(*cols) \
        .writeTo("bootcamp.aggregated_data") \
        .using("iceberg") \
        .option("overwrite-mode", "dynamic") \
        .tableProperty("write.format.default", "parquet") \
        .overwritePartitions()

    spark.sql("SELECT SUM(file_size_in_bytes) AS size, COUNT(1) AS num_files FROM bootcamp.aggregated_data.files").show()

