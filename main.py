import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, TimestampType
from pyspark.sql.window import Window
import pandas as pd


class Analyzer:
    def __init__(self, _spark_session: SparkSession):
        self.spark_session = _spark_session
        self.data: DataFrame = None
        self.top_songs: DataFrame = None

    def load_data(self, path: Path, input_schema: StructType):
        self.data = self.spark_session.read.format("csv") \
            .option("header", "false") \
            .option("delimiter", "\t") \
            .option("inferSchema", "false") \
            .schema(input_schema) \
            .load(path)

    def analyze(self, song_gap_minutes: int, n_top_session: int, n_top_songs: int):
        _session_window = Window.partitionBy("user").orderBy("timestamp")
        self.data = self.data.withColumn("prev_timestamp", lag("timestamp", 1).over(_session_window))
        self.data = self.data.withColumn("session_id", sum(when(col("timestamp").cast("long") - col("prev_timestamp").cast("long") > song_gap_minutes * 60, 1).otherwise(0)).over(_session_window))

        _session_count_window = Window.partitionBy("user", "session_id").orderBy("timestamp")
        self.data = self.data.withColumn("song_count", count("*").over(_session_count_window))
        self.data = self.data.filter(col("song_count") > 1)

        _session_window = Window.partitionBy("user", "session_id").orderBy("timestamp")
        self.data = self.data.withColumn("track_count", count("*").over(_session_window))
        _session_window = Window.partitionBy("user").orderBy(desc("track_count"), desc("timestamp"))
        _top_50_sessions = self.data.withColumn("rank", dense_rank().over(_session_window)).filter(col("rank") <= n_top_session)

        self.top_songs = _top_50_sessions.groupBy("artist_name", "track_name").agg(count("*").alias("play_count")).orderBy(desc("play_count"), "artist_name", "track_name").limit(n_top_songs)

    def write_output(self, path: Path):
        pandas_df = self.top_songs.toPandas()
        pandas_df.to_csv(path, sep="\t", index=False, mode="w")

    def cleanup(self):
        self.spark_session.stop()


if __name__ == "__main__":
    logger = logging.getLogger("pyspark")
    logger.setLevel(logging.INFO)

    logger.info("Getting path from environment variables...")
    input_path = os.environ.get("INPUT_PATH")
    output_path = os.environ.get("OUTPUT_PATH")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")

    logger.info("Creating Spark session...")
    spark_session = SparkSession.builder \
        .appName("LastFM Top Songs Challenge") \
        .getOrCreate()

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("musicbrainz_trackid", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("musicbrainz-track-id", StringType(), True),
        StructField("track_name", StringType(), True)
    ])

    logger.info("Starting analysis...")
    analyzer = Analyzer(spark_session)
    logger.info("Loading data...")
    analyzer.load_data(path=input_path, input_schema=schema)
    logger.info("Analyzing data...")
    analyzer.analyze(song_gap_minutes=20, n_top_session=50, n_top_songs=10)
    logger.info(f"Done analyzing data! Top songs:\n{analyzer.top_songs.show()}")
    logger.info("Writing output...")
    analyzer.write_output(path=output_path)
    logger.info("Cleaning up...")
    analyzer.cleanup()
    logger.info("Done!")
