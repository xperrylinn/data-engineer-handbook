import pytest
from pyspark.sql import SparkSession
import pytest
from collections import namedtuple
from pyspark.sql import Row


# Define namedtuples
ActorFilm = namedtuple("ActorFilm", "actor actor_id film year votes rating filmid")
ActorsCummulative = namedtuple("ActorsCummulative", "actor year quality_class is_active films")


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()


@pytest.fixture
def actor_films_sample():
    return [
        ActorFilm("50 Cent", 1, "Vengeance", 2006, 133, 3.5, 'tt0485920'),
        ActorFilm("50 Cent", 1, "Home of the Brave", 2006, 10500, 5.6, 'tt0763840'),
        ActorFilm("50 Cent", 1, "Get Rich or Die Tryin'", 2005, 44370, 5.4, 'tt0430308'),
    ]


@pytest.fixture
def actors_cummulative_sample():
    return [
        ActorsCummulative(
            actor="50 Cent",
            year=2005,
            quality_class="bad",
            is_active=True,
            films=[Row(film="Get Rich or Die Tryin'", votes=44370, rating=5.4, filmid='tt0430308', year=2005)]
        ),
        ActorsCummulative(
            actor="50 Cent",
            year=2006,
            quality_class="bad",
            is_active=True,
            films=[
                Row(film="Home of the Brave", votes=10500, rating=5.6, filmid='tt0763840', year=2006),
                Row(film="Vengeance", votes=133, rating=3.5, filmid='tt0485920', year=2006),
                Row(film="Get Rich or Die Tryin'", votes=44370, rating=5.4, filmid='tt0430308', year=2005),
            ]
        )
    ]
