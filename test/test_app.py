from datetime import datetime

import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

from main.app import Analyzer


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master("local[4]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.mark.parametrize("input_data, expected_output_data", [
    (
            [
                ("user1", datetime(2022, 4, 23, 14, 0), "track1", "artist1", "track-id1", "song1"),
                ("user1", datetime(2022, 4, 23, 14, 1), "track2", "artist2", "track-id2", "song2"),
                ("user2", datetime(2022, 4, 23, 14, 0), "track1", "artist1", "track-id1", "song1"),
                ("user2", datetime(2022, 4, 23, 14, 1), "track2", "artist2", "track-id2", "song2"),
                ("user2", datetime(2022, 4, 23, 14, 3), "track3", "artist3", "track-id3", "song3"),
                ("user2", datetime(2022, 4, 23, 14, 5), "track4", "artist4", "track-id4", "song4"),
            ],
            [
                ("artist1", "song1", 2),
                ("artist2", "song2", 2),
                ("artist3", "song3", 1),
                ("artist4", "song4", 1),

            ]

    )
], ids=["happy_case"])
def test_analyzer(spark_session, input_data, expected_output_data):
    # given
    analyzer = Analyzer(spark_session)

    input_schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("timestamp", TimestampType(), True),
            StructField("musicbrainz_trackid", StringType(), True),
            StructField("artist_name", StringType(), True),
            StructField("musicbrainz-track-id", StringType(), True),
            StructField("track_name", StringType(), True),
        ]
    )
    analyzer.data = spark_session.createDataFrame(input_data, schema=input_schema)

    output_schema = StructType(
        [
            StructField("artist_name", StringType(), False),
            StructField("track_name", StringType(), False),
            StructField("play_count", LongType(), False),
        ]
    )
    expected_output_df = spark_session.createDataFrame(expected_output_data, schema=output_schema)

    # when
    analyzer.analyze(song_gap_minutes=20, n_top_session=50, n_top_songs=10)

    # then
    assert_frame_equal(analyzer.top_songs.toPandas(), expected_output_df.toPandas())
