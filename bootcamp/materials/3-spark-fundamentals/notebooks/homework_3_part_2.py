#!/usr/bin/env python
# coding: utf-8

# Homework 3 Part 2 - PySpark Script Version

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, struct, collect_list, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ------------------------
# Spark Session & Config
# ------------------------

spark = SparkSession.builder.appName("ActorsAnalysis").getOrCreate()

# Enable bucketing and disable automatic broadcast join
spark.conf.set('spark.sql.sources.v2.bucketing.enabled', 'true')
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping', 'true')
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# ------------------------
# Define Schemas & Load CSV
# ------------------------

actor_films_schema = StructType([
    StructField("actor", StringType(), True),
    StructField("actorid", StringType(), True),
    StructField("film", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("votes", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("filmid", StringType(), True)
])

actor_films_df = spark.read \
    .option("header", "true") \
    .schema(actor_films_schema) \
    .csv("/home/iceberg/data/actor_films.csv")

# Write to Iceberg table
actor_films_df.writeTo("bootcamp.actor_films") \
    .using("iceberg") \
    .option("overwrite-mode", "static") \
    .tableProperty("write.format.default", "parquet") \
    .overwritePartitions()

af = actor_films_df.alias("af")

# ------------------------
# Generate Actor-Year Cross Join
# ------------------------

years = [(y,) for y in range(1970, 2022)]  # 2021 inclusive
years_df = spark.createDataFrame(years, ["year"])
y = years_df.alias("y")

first_actor_year_df = actor_films_df.groupBy("actor").agg(min("year").alias("first_year"))
fay = first_actor_year_df.alias("fay")

actors_and_years_df = fay \
    .join(y, col("fay.first_year") <= col("y.year")) \
    .select("fay.actor", "y.year")

aay = actors_and_years_df.alias("aay")

# ------------------------
# Define Film Struct Schema
# ------------------------

film_struct = StructType([
    StructField("film", StringType(), True),
    StructField("votes", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("filmid", StringType(), True),
    StructField("year", IntegerType(), True),
])

# ------------------------
# Join with Actor Films and Aggregate
# ------------------------

windowed_df = aay \
    .join(af, on=col("aay.actor") == col("af.actor"), how="left") \
    .filter(col("aay.year") >= col("af.year")) \
    .groupBy("aay.actor", "aay.year") \
    .agg(
        collect_list(
            struct("af.film", "af.votes", "af.rating", "af.filmid", "af.year")
        ).alias("films")
    )

w = windowed_df.alias("w")

# ------------------------
# Add Quality Classification & is_active
# ------------------------

actors_ready_df = w.select(
    w.actor,
    w.year,
    when(col("w.films").getItem(0).rating > 8, "star")
    .when((col("w.films").getItem(0).rating > 7) & (col("w.films").getItem(0).rating <= 8), "good")
    .when((col("w.films").getItem(0).rating > 6) & (col("w.films").getItem(0).rating <= 7), "average")
    .otherwise("bad")
    .alias("quality_class"),
    when(col("w.films").getItem(0).year == w.year, True).otherwise(False).alias("is_active"),
    w.films
).orderBy("w.actor", "w.year")

# ------------------------
# Write to Iceberg
# ------------------------

actors_ready_df.writeTo("bootcamp.actors") \
    .using("iceberg") \
    .option("overwrite-mode", "static") \
    .tableProperty("write.format.default", "parquet") \
    .overwritePartitions()
