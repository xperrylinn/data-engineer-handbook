#!/usr/bin/env python
# coding: utf-8
from chispa.dataframe_comparer import *
from ..jobs.actors_cummulative_job import do_actors_cummulative_job


def test_actors_cummulative_jon(spark, actor_films_sample, actors_cummulative_sample):
    actor_films_df = spark.createDataFrame(actor_films_sample)

    actual_actors_df = do_actors_cummulative_job(spark, actor_films_df)
    print(actual_actors_df)
    expected_actors_df = spark.createDataFrame(actors_cummulative_sample)

    assert_df_equality(actual_actors_df, expected_actors_df, ignore_nullable=True)

