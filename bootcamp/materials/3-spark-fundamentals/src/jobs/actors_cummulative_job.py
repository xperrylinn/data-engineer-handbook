#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, broadcast, min, struct, collect_list, when
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def do_actors_cummulative_job(spark: SparkSession, actor_films_df: DataFrame):
    # Disable automatic broadcast join and enable bucketing options
    spark.conf.set('spark.sql.sources.v2.bucketing.enabled', 'true')
    spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping', 'true')
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Define schema for input CSV
    actor_films_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", StringType(), True),
        StructField("film", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("filmid", StringType(), True)
    ])

    actor_films_df = actor_films_df.select("*").orderBy("actor", "year", "film")

    af = actor_films_df.alias("af")

    # Create list of years 1970–2021
    years = [(y,) for y in range(1970, 2007)]
    years_df = spark.createDataFrame(years, ["year"])
    y = years_df.alias("y")

    # First year per actor
    first_actor_year_df = actor_films_df.groupBy("actor").agg(min("year").alias("first_year"))
    fay = first_actor_year_df.alias("fay")

    # Join to get all valid (actor, year) pairs
    actors_and_years_df = fay.join(broadcast(y), col("fay.first_year") <= col("y.year")) \
        .select("fay.actor", "y.year")
    aay = actors_and_years_df.alias("aay")

    # Join (actor, year) with films up to that year
    windowed_df = aay \
        .join(af, on=col("aay.actor") == col("af.actor"), how="left") \
        .filter(col("aay.year") >= col("af.year")) \
        .groupBy("aay.actor", "aay.year") \
        .agg(
        collect_list(
            struct("af.film", "af.votes", "af.rating", "af.filmid", "af.year")
        ).alias("films"),
        avg("af.rating").alias("avg_rating")
    )
    w = windowed_df.alias("w")

    # Derive final actor-year dataset
    actors_ready_df = w.select(
        w.actor,
        w.year,
        when(col("w.avg_rating") > 8, "star") \
            .when((col("w.avg_rating") > 7) & (col("w.avg_rating") <= 8), "good") \
            .when((col("w.avg_rating") > 6) & (col("w.avg_rating") <= 7), "average") \
            .otherwise("bad").alias("quality_class"),
        when(col("w.films").getItem(0).year == w.year, True).otherwise(False).alias("is_active"),
        w.films
    ).orderBy("w.actor", "w.year")

    # Write output to Iceberg table
    actors_ready_df.writeTo("bootcamp.actors") \
        .using("iceberg") \
        .option("overwrite-mode", "static") \
        .tableProperty("write.format.default", "parquet") \
        .overwritePartitions()

    return actors_ready_df


def main():
    spark = SparkSession.builder.appName("actor_table_generation").getOrCreate()
    actors_df = do_actors_cummulative_job(spark, spark.table("bootcamp.actor_films"))
    actors_df.write.mode("overwrite").insertInto("bootcamp.actors")


if __name__ == "__main__":
    main()
